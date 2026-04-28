//! gRPC implementation of the `OiService` defined in `proto/oi.proto`.
//!
//! Keeps the proto-to-domain conversion in one place; application code
//! should never see `oi.v1.*` types.

use oi_core::{
    instrument::InstrumentId, snapshot::OiSnapshot, traits::OiRepository, unit::UnitKind,
};
use prost_types::Timestamp;
use std::{str::FromStr, sync::Arc};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub use crate::pb;
use crate::pb::{oi_service_server::OiService, Bar, Instrument as PbInst, Unit as PbUnit};

pub struct OiGrpc {
    repo: Arc<dyn OiRepository>,
    /// Redis URL for pub/sub subscriptions. The gRPC `Subscribe` method
    /// opens a dedicated connection per client — required by the Redis
    /// pub/sub protocol.
    redis_url: String,
}

impl std::fmt::Debug for OiGrpc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OiGrpc").finish()
    }
}

impl OiGrpc {
    pub fn new(repo: Arc<dyn OiRepository>, redis_url: String) -> Self {
        Self { repo, redis_url }
    }

    pub fn into_service(self) -> pb::oi_service_server::OiServiceServer<Self> {
        pb::oi_service_server::OiServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl OiService for OiGrpc {
    async fn latest(
        &self,
        req: Request<pb::LatestRequest>,
    ) -> std::result::Result<Response<pb::LatestResponse>, Status> {
        let _t = crate::metrics::Timer::start("Latest");
        let insts = req.into_inner().instruments;
        let mut bars = Vec::with_capacity(insts.len());
        for pi in insts {
            let id = match pb_to_id(&pi) {
                Ok(x) => x,
                Err(e) => {
                    crate::metrics::inc_request("Latest", "bad_request");
                    return Err(e);
                }
            };
            match self.repo.latest(&id).await {
                Ok(Some(s)) => bars.push(snap_to_bar(s)),
                Ok(None) => {}
                Err(e) => {
                    crate::metrics::inc_request("Latest", "internal");
                    return Err(Status::internal(e.to_string()));
                }
            }
        }
        crate::metrics::inc_request("Latest", "ok");
        Ok(Response::new(pb::LatestResponse { bars }))
    }

    type RangeStream = ReceiverStream<std::result::Result<Bar, Status>>;
    async fn range(
        &self,
        req: Request<pb::RangeRequest>,
    ) -> std::result::Result<Response<Self::RangeStream>, Status> {
        let pb::RangeRequest {
            instrument,
            from,
            to,
        } = req.into_inner();
        let id = pb_to_id(
            instrument
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("instrument required"))?,
        )?;
        let from = pb_ts(from.as_ref())?;
        let to = pb_ts(to.as_ref())?;
        let snaps = self
            .repo
            .range(&id, from, to)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let (tx, rx) = mpsc::channel(512);
        tokio::spawn(async move {
            for s in snaps {
                if tx.send(Ok(snap_to_bar(s))).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeStream = ReceiverStream<std::result::Result<Bar, Status>>;
    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        use futures::StreamExt;

        // Build the filter. Empty = firehose (all instruments).
        let filters: Vec<InstrumentId> = req
            .into_inner()
            .instruments
            .iter()
            .map(pb_to_id)
            .collect::<std::result::Result<_, _>>()?;
        let filter_set: std::collections::HashSet<InstrumentId> =
            filters.into_iter().collect();

        let redis_url = self.redis_url.clone();
        let (tx, rx) = mpsc::channel::<std::result::Result<Bar, Status>>(512);

        tokio::spawn(async move {
            // If the caller's filter is all one exchange we could
            // subscribe to the per-exchange shard; firehose is simpler
            // and the throughput (~1800 msgs/min across 9 venues) is
            // well inside one connection's budget.
            let mut stream = match oi_storage::pubsub::subscribe(
                &redis_url,
                oi_storage::pubsub::FIREHOSE_CHANNEL,
            )
            .await
            {
                Ok(s) => Box::pin(s),
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::unavailable(format!("pubsub: {e}"))))
                        .await;
                    return;
                }
            };

            while let Some(snap) = stream.next().await {
                if !filter_set.is_empty() && !filter_set.contains(&snap.instrument) {
                    continue;
                }
                if tx.send(Ok(snap_to_bar(snap))).await.is_err() {
                    return; // client disconnected
                }
                crate::metrics::inc_subscribe_frame();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn latest_funding(
        &self,
        req: Request<pb::LatestFundingRequest>,
    ) -> std::result::Result<Response<pb::LatestFundingResponse>, Status> {
        let _t = crate::metrics::Timer::start("LatestFunding");
        let insts = req.into_inner().instruments;
        let mut bars = Vec::with_capacity(insts.len());
        for pi in insts {
            let id = match pb_to_id(&pi) {
                Ok(x) => x,
                Err(e) => {
                    crate::metrics::inc_request("LatestFunding", "bad_request");
                    return Err(e);
                }
            };
            match self.repo.latest_funding(&id).await {
                Ok(Some(b)) => bars.push(funding_to_pb(b)),
                Ok(None) => {}
                Err(e) => {
                    crate::metrics::inc_request("LatestFunding", "internal");
                    return Err(Status::internal(e.to_string()));
                }
            }
        }
        crate::metrics::inc_request("LatestFunding", "ok");
        Ok(Response::new(pb::LatestFundingResponse { bars }))
    }

    type FundingRangeStream = ReceiverStream<std::result::Result<pb::Funding, Status>>;
    async fn funding_range(
        &self,
        req: Request<pb::FundingRangeRequest>,
    ) -> std::result::Result<Response<Self::FundingRangeStream>, Status> {
        let _t = crate::metrics::Timer::start("FundingRange");
        let pb::FundingRangeRequest {
            instrument,
            from,
            to,
        } = req.into_inner();
        let id = pb_to_id(
            instrument
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("instrument required"))?,
        )?;
        let from = pb_ts(from.as_ref())?;
        let to = pb_ts(to.as_ref())?;
        let bars = self
            .repo
            .funding_range(&id, from, to)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let (tx, rx) = mpsc::channel(512);
        tokio::spawn(async move {
            for b in bars {
                if tx.send(Ok(funding_to_pb(b))).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn latest_funding_event(
        &self,
        req: Request<pb::LatestFundingEventRequest>,
    ) -> std::result::Result<Response<pb::LatestFundingEventResponse>, Status> {
        let _t = crate::metrics::Timer::start("LatestFundingEvent");
        let insts = req.into_inner().instruments;
        let mut events = Vec::with_capacity(insts.len());
        for pi in insts {
            let id = match pb_to_id(&pi) {
                Ok(x) => x,
                Err(e) => {
                    crate::metrics::inc_request("LatestFundingEvent", "bad_request");
                    return Err(e);
                }
            };
            match self.repo.latest_funding_event(&id).await {
                Ok(Some(e)) => events.push(funding_event_to_pb(e)),
                Ok(None) => {}
                Err(e) => {
                    crate::metrics::inc_request("LatestFundingEvent", "internal");
                    return Err(Status::internal(e.to_string()));
                }
            }
        }
        crate::metrics::inc_request("LatestFundingEvent", "ok");
        Ok(Response::new(pb::LatestFundingEventResponse { events }))
    }

    type FundingEventRangeStream =
        ReceiverStream<std::result::Result<pb::FundingEvent, Status>>;
    async fn funding_event_range(
        &self,
        req: Request<pb::FundingEventRangeRequest>,
    ) -> std::result::Result<Response<Self::FundingEventRangeStream>, Status> {
        let _t = crate::metrics::Timer::start("FundingEventRange");
        let pb::FundingEventRangeRequest {
            instrument,
            from,
            to,
        } = req.into_inner();
        let id = pb_to_id(
            instrument
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("instrument required"))?,
        )?;
        let from = pb_ts(from.as_ref())?;
        let to = pb_ts(to.as_ref())?;
        let events = self
            .repo
            .funding_events_range(&id, from, to)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let (tx, rx) = mpsc::channel(512);
        tokio::spawn(async move {
            for e in events {
                if tx.send(Ok(funding_event_to_pb(e))).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn funding_event_to_pb(e: oi_core::funding::FundingEvent) -> pb::FundingEvent {
    pb::FundingEvent {
        instrument: Some(PbInst {
            exchange: e.instrument.exchange.code().to_owned(),
            symbol: e.instrument.symbol,
        }),
        settlement_ts: Some(ts_to_pb(e.settlement_ts)),
        rate: e.rate.to_string(),
        mark_price: e.mark_price.map(|d| d.to_string()).unwrap_or_default(),
    }
}

fn funding_to_pb(b: oi_core::funding::FundingBar) -> pb::Funding {
    pb::Funding {
        instrument: Some(PbInst {
            exchange: b.instrument.exchange.code().to_owned(),
            symbol: b.instrument.symbol,
        }),
        bucket_ts: Some(ts_to_pb(b.bucket_ts)),
        recv_ts: Some(ts_to_pb(b.recv_ts)),
        rate: b.rate.to_string(),
        next_funding_ts: b.next_funding_ts.map(ts_to_pb),
        interval_hours: u32::from(b.interval_hours.unwrap_or(0)),
    }
}

fn pb_to_id(pi: &PbInst) -> std::result::Result<InstrumentId, Status> {
    let ex = pi
        .exchange
        .parse::<oi_core::exchange::Exchange>()
        .map_err(|e| Status::invalid_argument(format!("exchange: {e}")))?;
    Ok(InstrumentId::new(ex, pi.symbol.clone()))
}

fn pb_ts(t: Option<&Timestamp>) -> std::result::Result<OffsetDateTime, Status> {
    let t = t.ok_or_else(|| Status::invalid_argument("timestamp required"))?;
    let nanos = i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos);
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|e| Status::invalid_argument(format!("timestamp: {e}")))
}

fn ts_to_pb(t: OffsetDateTime) -> Timestamp {
    let nanos_total = t.unix_timestamp_nanos();
    let seconds = (nanos_total / 1_000_000_000) as i64;
    let nanos = (nanos_total % 1_000_000_000) as i32;
    Timestamp { seconds, nanos }
}

fn unit_to_pb(u: UnitKind) -> PbUnit {
    match u {
        UnitKind::Coins => PbUnit::Coins,
        UnitKind::Contracts => PbUnit::Contracts,
        UnitKind::Usd => PbUnit::Usd,
    }
}

fn snap_to_bar(s: OiSnapshot) -> Bar {
    let opt = |o: Option<rust_decimal::Decimal>| o.map(|d| d.to_string()).unwrap_or_default();
    Bar {
        instrument: Some(PbInst {
            exchange: s.instrument.exchange.code().to_owned(),
            symbol: s.instrument.symbol,
        }),
        bucket_ts: Some(ts_to_pb(s.bucket_ts)),
        first_recv_ts: Some(ts_to_pb(s.first_recv_ts)),
        last_recv_ts: Some(ts_to_pb(s.last_recv_ts)),
        samples: s.samples,
        native_unit: i32::from(unit_to_pb(s.native_unit)),
        native_open: s.native_open.to_string(),
        native_high: s.native_high.to_string(),
        native_low: s.native_low.to_string(),
        native_close: s.native_close.to_string(),
        oi_coins_open: opt(s.oi_coins_open),
        oi_coins_high: opt(s.oi_coins_high),
        oi_coins_low: opt(s.oi_coins_low),
        oi_coins_close: opt(s.oi_coins_close),
        oi_usd_open: opt(s.oi_usd_open),
        oi_usd_high: opt(s.oi_usd_high),
        oi_usd_low: opt(s.oi_usd_low),
        oi_usd_close: opt(s.oi_usd_close),
        price_used_close: opt(s.price_used_close),
    }
}

// Unused helper suppresses warnings until the REST adapter needs it.
#[allow(dead_code)]
fn decimal_from_str(s: &str) -> Option<rust_decimal::Decimal> {
    if s.is_empty() {
        None
    } else {
        rust_decimal::Decimal::from_str(s).ok()
    }
}
