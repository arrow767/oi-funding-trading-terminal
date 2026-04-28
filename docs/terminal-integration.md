# Terminal integration

How a trading terminal consumes OI / funding / settlement-events from
the deployed `oi-api`. Concrete code samples for the two languages
most terminals are written in: **Rust** (server-side or native desktop)
and **TypeScript** (Electron / web / Tauri front-ends).

## What's available

| Method | Mode | Returns | Use for |
|---|---|---|---|
| `Latest(instruments)` | unary | `Bar[]` (current OHLC bar per instrument) | Cursor-tip read for a watchlist |
| `Range(instrument, from, to)` | server-stream | `Bar*` | Initial chart load |
| `Subscribe(instruments)` | server-stream | `Bar*` (in-progress + final) | Real-time chart updates |
| `LatestFunding(instruments)` | unary | `Funding[]` | Funding-rate widget |
| `FundingRange(instrument, from, to)` | server-stream | `Funding*` | Funding history line |
| `LatestFundingEvent(instruments)` | unary | `FundingEvent[]` | "Last paid" label |
| `FundingEventRange(instrument, from, to)` | server-stream | `FundingEvent*` | Settlement markers on chart |

Same auth (Bearer token) for every RPC.
TLS (rustls) is mandatory on the public endpoint.

## Wire layout

* gRPC: `https://oi.example.com:50051`
* REST (debug / non-gRPC clients): `https://oi.example.com:8080`
* Prometheus (private): `:9090` (collector) / `:9091` (api)

## Rust client (~50 LOC)

```toml
# Cargo.toml
[dependencies]
oi-api = { git = "https://github.com/you/trading-terminal-oi", package = "oi-api" }
tonic  = { version = "0.12", features = ["tls", "tls-roots"] }
tokio  = { version = "1", features = ["macros", "rt-multi-thread"] }
prost-types = "0.13"
```

```rust
use oi_api::pb::{
    self, oi_service_client::OiServiceClient, Instrument, SubscribeRequest,
    LatestRequest,
};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::metadata::MetadataValue;

/// Build a TLS-enabled, bearer-authenticated gRPC channel.
async fn connect(endpoint: &str, token: &str) -> anyhow::Result<OiServiceClient<Channel>> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let channel = Channel::from_shared(endpoint.to_owned())?
        .tls_config(tls)?
        .connect()
        .await?;

    let bearer: MetadataValue<_> = format!("Bearer {token}").parse()?;
    let client = OiServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
        req.metadata_mut().insert("authorization", bearer.clone());
        Ok(req)
    });
    Ok(client)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = connect("https://oi.example.com:50051", "<TOKEN>").await?;

    let inst = Instrument {
        exchange: "binance".into(),
        symbol: "BTCUSDT".into(),
    };

    // 1) Cursor tip
    let latest = client
        .latest(LatestRequest { instruments: vec![inst.clone()] })
        .await?
        .into_inner();
    if let Some(bar) = latest.bars.first() {
        println!(
            "BTCUSDT close = {} (samples={}, oi_usd_close={})",
            bar.native_close, bar.samples, bar.oi_usd_close,
        );
    }

    // 2) Live tape
    let mut stream = client
        .subscribe(SubscribeRequest { instruments: vec![inst] })
        .await?
        .into_inner();
    while let Some(bar) = stream.message().await? {
        println!(
            "tick samples={} O/H/L/C={}/{}/{}/{}",
            bar.samples, bar.native_open, bar.native_high,
            bar.native_low, bar.native_close,
        );
    }
    Ok(())
}
```

For a desktop terminal, do exactly the same in a background tokio task
and forward bars to the rendering thread via a `tokio::sync::watch` or
`crossbeam::channel`.

## TypeScript client (~80 LOC)

Uses [`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js)
with proto-loader. Works in Node and Electron (main process); for
browser-only see the gRPC-Web section below.

```sh
npm i @grpc/grpc-js @grpc/proto-loader
# Copy proto/oi.proto into your terminal repo or vendor it.
```

```ts
// oi-client.ts
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { google } from "google-protobuf/google/protobuf/timestamp_pb"; // optional, types

const PROTO_PATH = "./proto/oi.proto";
const def = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const oi = (grpc.loadPackageDefinition(def) as any).oi.v1;

const ENDPOINT = "oi.example.com:50051";
const TOKEN = process.env.OI_TOKEN!;

// Bearer auth interceptor as a CallCredentials object.
const auth = grpc.credentials.createFromMetadataGenerator((_, cb) => {
  const md = new grpc.Metadata();
  md.set("authorization", `Bearer ${TOKEN}`);
  cb(null, md);
});
const creds = grpc.credentials.combineChannelCredentials(
  grpc.credentials.createSsl(), // uses system roots; pass cert bytes if self-signed
  auth,
);

const client = new oi.OiService(ENDPOINT, creds);

// 1) Latest one-shot
client.latest(
  { instruments: [{ exchange: "binance", symbol: "BTCUSDT" }] },
  (err: any, resp: any) => {
    if (err) return console.error(err);
    const bar = resp.bars[0];
    console.log(
      `BTCUSDT close=${bar.nativeClose} samples=${bar.samples} oi_usd=${bar.oiUsdClose}`,
    );
  },
);

// 2) Live subscribe
const stream = client.subscribe({
  instruments: [{ exchange: "binance", symbol: "BTCUSDT" }],
});
stream.on("data", (bar: any) => {
  console.log(
    `tick samples=${bar.samples} O/H/L/C=${bar.nativeOpen}/${bar.nativeHigh}/${bar.nativeLow}/${bar.nativeClose}`,
  );
});
stream.on("error", (e: Error) => console.error("stream error", e));
```

### Browser / gRPC-Web

`@grpc/grpc-js` doesn't run in browsers. Two options:

**A. gRPC-Web via Envoy proxy.** Run Envoy in front of `oi-api` to
translate browser HTTP/1.1 + base64 frames into native gRPC. Sample
Envoy config in `deploy/envoy.yaml` (not yet shipped — match
[grpc-web docs](https://grpc.io/docs/platforms/web/quickstart/)).
Browser code uses `grpc-web` package.

**B. REST endpoints + EventSource.** Easier for prototypes:
* `GET /v1/oi/latest/binance/BTCUSDT` → poll every few seconds.
* `GET /v1/oi/range/binance/BTCUSDT?from=…&to=…` → chart load.
* No live push (REST has no SSE wired). For real-time use option A.

## Decimal handling

All decimal fields are wired as **strings** to preserve precision.
On the client side, parse them with a fixed-decimal library
appropriate to the language:

* Rust: `rust_decimal::Decimal::from_str(...)` (already used in our
  domain layer).
* TypeScript: `decimal.js` or `bignumber.js`. **Don't `parseFloat`** —
  large coin-unit OI values (`SHIB`, `PEPE`) lose precision in f64.

```ts
import Decimal from "decimal.js";
const oi = new Decimal(bar.nativeClose); // safe for any magnitude
const usd = oi.mul(new Decimal(bar.priceUsedClose));
```

## TradingView indicator wiring

If your terminal embeds the TradingView Charting Library (or
Lightweight Charts), here's the pattern for a custom OI overlay:

```ts
// 1. Initial load — series of OHLC bars for the chart window.
const bars: any[] = [];
const stream = client.range({
  instrument: { exchange: "binance", symbol: "BTCUSDT" },
  from: { seconds: Math.floor(fromMs / 1000), nanos: 0 },
  to:   { seconds: Math.floor(toMs   / 1000), nanos: 0 },
});
stream.on("data", (bar: any) => bars.push(bar));
stream.on("end", () => {
  oiSeries.setData(bars.map(toLightweightBar));
});

// 2. Live updates — subscribe and apply each fold to the active bar.
const live = client.subscribe({
  instruments: [{ exchange: "binance", symbol: "BTCUSDT" }],
});
live.on("data", (bar: any) => {
  oiSeries.update(toLightweightBar(bar));
});

function toLightweightBar(b: any) {
  return {
    time: Number(b.bucketTs.seconds), // unix seconds
    open:  Number(b.oiUsdOpen  || b.nativeOpen),
    high:  Number(b.oiUsdHigh  || b.nativeHigh),
    low:   Number(b.oiUsdLow   || b.nativeLow),
    close: Number(b.oiUsdClose || b.nativeClose),
  };
}
```

## Settlement markers

`FundingEventRange` returns sparse events at exact settlement
timestamps. Render them as price-line annotations or shape markers
on the chart:

```ts
const events: any[] = [];
const evStream = client.fundingEventRange({
  instrument: { exchange: "binance", symbol: "BTCUSDT" },
  from: { seconds: Math.floor(fromMs / 1000), nanos: 0 },
  to:   { seconds: Math.floor(toMs   / 1000), nanos: 0 },
});
evStream.on("data", (e: any) => events.push(e));
evStream.on("end", () => {
  oiSeries.setMarkers(events.map(e => ({
    time: Number(e.settlementTs.seconds),
    position: "aboveBar",
    color: parseFloat(e.rate) > 0 ? "#26a69a" : "#ef5350",
    shape: "circle",
    text: `funded ${(parseFloat(e.rate) * 100).toFixed(4)}%`,
  })));
});
```

## Testing the integration locally

Don't deploy until your terminal client works against a localhost
build:

```sh
# Server side, in the cloned repo
docker compose -f deploy/docker-compose.yml up -d
# Wait ~90 s for the first minute bar.

# Client side — point at localhost:50051, no TLS, no auth (default
# api.toml has tls.enabled=false and auth.enabled=false).
cargo run --example subscribe -p oi-api -- \
    --addr http://127.0.0.1:50051 \
    --exchange binance --symbol BTCUSDT --limit 10
```

Once you see live ticks in the example, swap the endpoint/token in
your terminal client and you're production-ready.

## Failure modes the terminal should handle

* **Connection drop** → reconnect with exponential backoff (start
  500 ms, cap 30 s, jitter ±20%). The server's `Subscribe` re-issues
  the latest bar on reconnect, so no manual catch-up logic is
  needed.
* **`Status::unauthenticated`** → token expired or wrong; surface as
  a UI banner, do not retry silently.
* **`Status::unavailable`** → server restart in progress; treat as
  transient, reconnect.
* **Empty `oi_usd_close`** → price feed was down for that minute;
  fall back to `native_close × <your-price-source>` if you need a
  USD figure right now.
* **`samples == 1` always** for a venue you expected live updates
  on → its WS handler is unhealthy on the server. Check the
  `oi_ws_reconnects_total{handler}` Prometheus metric.
