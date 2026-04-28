//! End-to-end gRPC client demonstration.
//!
//! Run against a live `oi-api` server:
//! ```sh
//! cargo run --example subscribe -p oi-api -- --addr http://127.0.0.1:50051 \
//!     --exchange binance --symbol BTCUSDT
//! ```
//!
//! Behavior:
//! 1. Calls `Latest` once for the requested (exchange, symbol) and
//!    prints the current bar.
//! 2. Opens a `Subscribe` stream filtered to the same instrument and
//!    prints every live tick as it arrives.
//!
//! The terminal integration can lift this pattern directly — the
//! proto-generated client types live in `oi_api::pb` so nothing extra
//! needs to be re-generated downstream.

use clap::Parser;
use oi_api::pb;
use pb::oi_service_client::OiServiceClient;
use std::error::Error;

#[derive(Debug, Parser)]
#[command(name = "subscribe", about = "Demo client for oi-api gRPC")]
struct Args {
    /// gRPC endpoint (e.g. http://127.0.0.1:50051).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
    #[arg(long)]
    exchange: String,
    #[arg(long)]
    symbol: String,
    /// Print at most this many live frames, then exit. Default 0 = no
    /// limit (runs until killed).
    #[arg(long, default_value_t = 0)]
    limit: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut client = OiServiceClient::connect(args.addr.clone()).await?;
    println!("connected to {}", args.addr);

    let inst = pb::Instrument {
        exchange: args.exchange.clone(),
        symbol: args.symbol.clone(),
    };

    // 1. One-shot Latest.
    let resp = client
        .latest(pb::LatestRequest {
            instruments: vec![inst.clone()],
        })
        .await?
        .into_inner();
    if let Some(bar) = resp.bars.first() {
        println!(
            "Latest {}:{} samples={} ({}) native O/H/L/C={}/{}/{}/{} oi_usd_close={} price_close={}",
            inst.exchange,
            inst.symbol,
            bar.samples,
            pb::Unit::try_from(bar.native_unit)
                .map(|u| format!("{:?}", u))
                .unwrap_or_else(|_| "UNSPECIFIED".into()),
            bar.native_open,
            bar.native_high,
            bar.native_low,
            bar.native_close,
            empty_or(&bar.oi_usd_close),
            empty_or(&bar.price_used_close),
        );
    } else {
        println!("Latest returned no data yet for {}:{}", inst.exchange, inst.symbol);
    }

    // 2. Live Subscribe — one stream, filtered to our instrument.
    println!(
        "subscribing to live stream for {}:{}{}",
        inst.exchange,
        inst.symbol,
        if args.limit == 0 {
            " (Ctrl+C to exit)".into()
        } else {
            format!(" (will exit after {} frames)", args.limit)
        }
    );
    let mut stream = client
        .subscribe(pb::SubscribeRequest {
            instruments: vec![inst],
        })
        .await?
        .into_inner();

    let mut seen: u64 = 0;
    while let Some(bar) = stream.message().await? {
        let inst = bar.instrument.as_ref();
        println!(
            "tick {}:{} samples={} native O/H/L/C={}/{}/{}/{} usd_close={} last_recv={}",
            inst.map_or("?", |i| i.exchange.as_str()),
            inst.map_or("?", |i| i.symbol.as_str()),
            bar.samples,
            bar.native_open,
            bar.native_high,
            bar.native_low,
            bar.native_close,
            empty_or(&bar.oi_usd_close),
            bar.last_recv_ts
                .as_ref()
                .map_or("?".to_owned(), |t| format!("{}.{:09}", t.seconds, t.nanos)),
        );
        seen += 1;
        if args.limit > 0 && seen >= args.limit {
            break;
        }
    }
    Ok(())
}

fn empty_or(s: &str) -> &str {
    if s.is_empty() {
        "null"
    } else {
        s
    }
}
