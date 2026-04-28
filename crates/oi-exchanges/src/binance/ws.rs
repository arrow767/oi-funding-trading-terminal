//! Binance USD-M mark-price WebSocket handler.
//!
//! Stream: `!markPrice@arr@1s` — one message per second carrying every
//! perpetual's mark price. Cheaper than polling `premiumIndex` and gives
//! sub-minute price accuracy if we ever want it.
//!
//! Docs: <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream-for-All-market>
//!
//! Heartbeat: Binance sends a ping frame every 3 minutes and expects a pong
//! inside 10 minutes. tokio-tungstenite auto-pongs when we call `.next()`
//! on the stream, so this handler only needs to keep the stream drained —
//! which it does inherently.
//!
//! This handler exists to demonstrate the WS framework and to provide a
//! lower-latency price feed once the collector is wired for it; OI itself
//! is not pushed on WS for USD-M futures.

use crate::common::ws::{Frame, WsConn, WsHandler};
use async_trait::async_trait;
use futures::stream::SplitSink;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Debug)]
#[allow(dead_code)] // Public WS handler; wired to the collector in a follow-up
pub struct BinanceMarkPriceWs;

#[async_trait]
impl WsHandler for BinanceMarkPriceWs {
    fn url(&self) -> &str {
        "wss://fstream.binance.com/stream?streams=!markPrice@arr@1s"
    }

    fn name(&self) -> &'static str {
        "binance-markprice"
    }

    async fn on_connect(
        &self,
        _sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()> {
        // Stream is subscribed via URL — no additional message needed.
        Ok(())
    }

    async fn on_message(
        &self,
        msg: Message,
        _sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<Option<Frame>> {
        let bytes = match msg {
            Message::Text(t) => t.as_str().as_bytes().to_vec(),
            Message::Binary(b) => b.to_vec(),
            _ => return Ok(None),
        };
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        Ok(Some(Frame::Payload(value)))
    }

    fn idle_timeout(&self) -> Duration {
        // Binance sends updates every 1s; if we go 30s silent, something
        // is wrong — force reconnect earlier than the default.
        Duration::from_secs(30)
    }
}
