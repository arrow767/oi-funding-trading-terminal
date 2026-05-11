//! Native WebSocket subscribe endpoint for browser / JS clients.
//!
//! Browsers cannot speak gRPC directly (gRPC-Web exists but requires
//! an Envoy/proxy translator). This endpoint serves the same data as
//! the gRPC `Subscribe` RPC, framed as JSON messages over a plain WS
//! upgrade, so any browser/Node/curl client can subscribe.
//!
//! ## Architecture under load
//!
//! A naive design would have each WS handler open its own Redis
//! pub/sub subscription. With 10–50k concurrent WS clients that
//! becomes 10–50k Redis subscriber connections — Redis can do it but
//! it's wasteful.
//!
//! Instead we run **one** Redis pub/sub subscriber per oi-api
//! process (`spawn_pubsub_broadcaster`) that fans out to a
//! `tokio::sync::broadcast` channel. Each WS handler subscribes to
//! that broadcast and filters by instrument client-side. Memory cost
//! per WS connection drops to a few KB of buffered messages plus the
//! TCP socket.
//!
//! ## Backpressure
//!
//! Slow clients are detected two ways:
//! 1. `broadcast::Receiver::Lagged(n)` when their position falls
//!    behind the channel ring — we log and continue.
//! 2. A 5s timeout on `sender.send()` — if a frame can't be flushed
//!    to the TCP socket in 5s, we close the connection (the client
//!    has likely vanished).
//!
//! ## Query params
//!
//! * `instruments` — comma-separated `exchange:symbol` pairs.
//!   Empty / absent = subscribe to the firehose (all 9 exchanges,
//!   all symbols).
//! * `token` — bearer token. Browsers can't set the
//!   `Authorization` header on WS, so we accept it as a query
//!   param. The TLS layer hides it from intermediaries, but it
//!   does land in the access log — rotate periodically.

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use oi_core::{exchange::Exchange, instrument::InstrumentId, snapshot::OiSnapshot};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::auth::AuthState;

/// Capacity of the in-process broadcast channel. At 50k WS clients
/// we expect ~1800 instruments × 1 final-bar-per-minute = 30 ops/sec
/// firehose throughput, plus intra-minute WS-live updates from
/// Bybit/OKX/Bitget/Hyperliquid pushing maybe another 100 ops/sec.
/// 4096 ≈ 30 s of headroom for slow consumers before they get
/// `Lagged`.
const BROADCAST_CAPACITY: usize = 4096;

/// Max time we'll wait for a single frame to flush to the TCP
/// socket. Slow clients past this are evicted.
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Heartbeat cadence. Most reverse proxies idle-timeout WS at ~60 s.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Shared state for the WS routes.
#[derive(Clone)]
pub struct WsState {
    pub auth: Option<AuthState>,
    /// Broadcast handle for fanning out snapshots. Each WS handler
    /// calls `snapshots.subscribe()` to get its own receiver.
    pub snapshots: broadcast::Sender<Arc<OiSnapshot>>,
}

impl std::fmt::Debug for WsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsState")
            .field("subscribers", &self.snapshots.receiver_count())
            .finish()
    }
}

/// Spawn the one-and-only Redis pub/sub subscriber for this oi-api
/// process. Returns a broadcast `Sender` that WS handlers
/// `subscribe()` to.
///
/// On Redis disconnect the task reconnects with a 2s backoff; while
/// disconnected, broadcast subscribers see no messages but the WS
/// connections stay open (they'll resume once we're back).
pub fn spawn_pubsub_broadcaster(redis_url: String) -> broadcast::Sender<Arc<OiSnapshot>> {
    let (tx, _) = broadcast::channel::<Arc<OiSnapshot>>(BROADCAST_CAPACITY);
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        loop {
            match oi_storage::pubsub::subscribe(
                &redis_url,
                oi_storage::pubsub::FIREHOSE_CHANNEL,
            )
            .await
            {
                Ok(stream) => {
                    info!(channel = oi_storage::pubsub::FIREHOSE_CHANNEL, "ws broadcaster connected");
                    let mut stream = Box::pin(stream);
                    while let Some(snap) = stream.next().await {
                        // Ignored receiver count: if there are no
                        // subscribers we don't care — when one
                        // appears it'll just see future messages.
                        let _ = tx_clone.send(Arc::new(snap));
                    }
                    warn!("ws broadcaster: redis stream ended; reconnecting");
                }
                Err(e) => {
                    warn!(error=%e, "ws broadcaster: redis subscribe failed; retrying in 2s");
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    tx
}

#[derive(Debug, Deserialize)]
pub struct SubscribeParams {
    /// Comma-separated `exchange:symbol`. Empty = firehose.
    #[serde(default)]
    instruments: String,
    /// Bearer token via query string (browsers can't set headers
    /// on WS upgrade).
    #[serde(default)]
    token: Option<String>,
}

pub fn router(state: WsState) -> Router {
    Router::new()
        .route("/ws/v1/oi/subscribe", get(ws_subscribe_handler))
        .with_state(state)
}

async fn ws_subscribe_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<SubscribeParams>,
    State(state): State<WsState>,
) -> impl IntoResponse {
    // Auth.
    if let Some(auth) = &state.auth {
        let valid = params
            .token
            .as_deref()
            .map(|t| auth.accepts(t))
            .unwrap_or(false);
        if !valid {
            crate::metrics::inc_request("WS_Subscribe", "unauthorized");
            return (StatusCode::UNAUTHORIZED, "missing or invalid token")
                .into_response();
        }
    }

    let filter: HashSet<InstrumentId> = params
        .instruments
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(parse_instrument)
        .collect();

    crate::metrics::inc_request("WS_Subscribe", "ok");
    ws.on_upgrade(move |socket| handle_socket(socket, state, filter))
}

fn parse_instrument(s: &str) -> Option<InstrumentId> {
    let mut parts = s.splitn(2, ':');
    let ex = parts.next()?;
    let sym = parts.next()?;
    let exchange = ex.parse::<Exchange>().ok()?;
    Some(InstrumentId::new(exchange, sym.to_owned()))
}

async fn handle_socket(socket: WebSocket, state: WsState, filter: HashSet<InstrumentId>) {
    let (mut sender, mut receiver) = socket.split();
    let mut snap_rx = state.snapshots.subscribe();

    crate::metrics::inc_ws_connections();
    let _drop_guard = scopeguard::guard((), |()| {
        crate::metrics::dec_ws_connections();
    });

    let mut heartbeat = interval(HEARTBEAT_INTERVAL);
    heartbeat.tick().await; // skip the immediate first tick

    let mut frames_sent: u64 = 0;
    debug!(filter_size = filter.len(), "ws client connected");

    loop {
        tokio::select! {
            biased;

            // Client → server (disconnect detection + ignored frames).
            recv_msg = receiver.next() => {
                match recv_msg {
                    None | Some(Err(_)) => return,
                    Some(Ok(Message::Close(_))) => return,
                    Some(Ok(_)) => {} // pong / client text — ignore
                }
            }

            // Heartbeat.
            _ = heartbeat.tick() => {
                let ping = sender.send(Message::Ping(vec![]));
                if tokio::time::timeout(SEND_TIMEOUT, ping).await.is_err() {
                    debug!("ws client did not accept ping within timeout; closing");
                    return;
                }
            }

            // Broadcast snapshot.
            snap = snap_rx.recv() => {
                match snap {
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // Client too slow — channel ring rotated.
                        // We continue serving them but tell ops via
                        // the metric.
                        crate::metrics::inc_ws_lagged(skipped);
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                    Ok(snap) => {
                        if !filter.is_empty() && !filter.contains(&snap.instrument) {
                            continue;
                        }
                        let Ok(payload) = serde_json::to_string(snap.as_ref()) else {
                            continue;
                        };
                        let send_fut = sender.send(Message::Text(payload));
                        match tokio::time::timeout(SEND_TIMEOUT, send_fut).await {
                            Err(_) => {
                                debug!("ws client send timeout; closing");
                                return;
                            }
                            Ok(Err(_)) => return,
                            Ok(Ok(())) => {}
                        }
                        frames_sent += 1;
                        // Batch metric updates: a counter increment is
                        // ~30 ns but doing it on every frame for 50k
                        // clients adds up.
                        if frames_sent % 100 == 0 {
                            crate::metrics::inc_ws_frames_sent(100);
                        }
                    }
                }
            }
        }
    }
}

// `scopeguard` is the smallest crate that gives RAII-style on-drop
// hooks; we use it for the connection count gauge.
//
// The brief drop guard lives at the top of `handle_socket`. If we
// ever stop using it (e.g. by switching to a custom future), the
// dependency can be dropped.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthState;

    #[test]
    fn parse_instrument_accepts_exchange_colon_symbol() {
        let id = parse_instrument("binance:BTCUSDT").unwrap();
        assert_eq!(id.exchange, Exchange::Binance);
        assert_eq!(id.symbol, "BTCUSDT");
    }

    #[test]
    fn parse_instrument_rejects_unknown_exchange() {
        assert!(parse_instrument("noex:BTCUSDT").is_none());
    }

    #[test]
    fn parse_instrument_rejects_missing_separator() {
        assert!(parse_instrument("BTCUSDT").is_none());
    }

    #[tokio::test]
    async fn broadcaster_starts_without_panicking() {
        // We can't actually subscribe to Redis here, but we CAN
        // verify that `spawn_pubsub_broadcaster` doesn't panic
        // synchronously — it should return a Sender and the
        // background task should log + retry on connection error.
        let tx = spawn_pubsub_broadcaster("redis://127.0.0.1:1".into());
        // No subscribers yet — sending should be a no-op (returns Err).
        assert!(tx.send(Arc::new(dummy_snapshot())).is_err());
    }

    fn dummy_snapshot() -> OiSnapshot {
        use oi_core::unit::UnitKind;
        use rust_decimal_macros::dec;
        let now = time::OffsetDateTime::now_utc();
        OiSnapshot {
            instrument: InstrumentId::new(Exchange::Binance, "BTCUSDT"),
            bucket_ts: now,
            first_recv_ts: now,
            last_recv_ts: now,
            samples: 1,
            native_unit: UnitKind::Coins,
            native_open: dec!(1),
            native_high: dec!(1),
            native_low: dec!(1),
            native_close: dec!(1),
            oi_coins_open: None,
            oi_coins_high: None,
            oi_coins_low: None,
            oi_coins_close: None,
            oi_usd_open: None,
            oi_usd_high: None,
            oi_usd_low: None,
            oi_usd_close: None,
            price_used_close: None,
        }
    }

    #[test]
    fn ws_state_subscribers_count_is_zero_at_start() {
        let (tx, _) = broadcast::channel::<Arc<OiSnapshot>>(16);
        let state = WsState { auth: None, snapshots: tx };
        assert_eq!(state.snapshots.receiver_count(), 0);
    }
}
