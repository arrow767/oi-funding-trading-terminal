//! Bybit v5 `tickers` WebSocket handler.
//!
//! Stream: `wss://stream.bybit.com/v5/public/linear` with topics of the
//! form `tickers.BTCUSDT`. The first frame per topic is a `snapshot`
//! (full fields); subsequent frames are `delta` messages that only
//! include changed fields. Consumers of `Frame::Payload` are responsible
//! for maintaining per-symbol state — this handler does the transport
//! wiring (subscribe, ping/pong, decode) only.
//!
//! Constraints from the docs:
//! * Max 10 args per `op:"subscribe"` message.
//! * Max 500 topics per connection.
//! * Heartbeat: send `{"op":"ping"}` every 20s. Server replies with
//!   `{"op":"pong"}`. No app-level pong = disconnect after 5 min.
//!
//! Docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
//!        <https://bybit-exchange.github.io/docs/v5/websocket/public/ticker>

use crate::common::ws::{Frame, WsConn, WsHandler};
use async_trait::async_trait;
use futures::{stream::SplitSink, SinkExt};
use serde::Serialize;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const DEFAULT_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";

/// Maximum number of topics in a single `op: "subscribe"` message.
const MAX_ARGS_PER_SUB: usize = 10;

#[derive(Debug, Clone)]
pub struct BybitTickersWs {
    url: String,
    topics: Vec<String>,
}

impl BybitTickersWs {
    pub fn new(symbols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            url: DEFAULT_WS_URL.into(),
            topics: symbols
                .into_iter()
                .map(|s| format!("tickers.{}", s.into()))
                .collect(),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[derive(Serialize)]
struct OpMessage<'a> {
    op: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<&'a [String]>,
}

#[async_trait]
impl WsHandler for BybitTickersWs {
    fn url(&self) -> &str {
        &self.url
    }

    fn name(&self) -> &'static str {
        "bybit-tickers"
    }

    async fn on_connect(
        &self,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()> {
        // Bybit's limit is 500 topics per conn; the caller is responsible
        // for sharding; we only enforce the per-message arg cap.
        for chunk in self.topics.chunks(MAX_ARGS_PER_SUB) {
            let msg = OpMessage {
                op: "subscribe",
                args: Some(chunk),
            };
            let text = serde_json::to_string(&msg)?;
            sink.send(Message::Text(text)).await?;
        }
        Ok(())
    }

    async fn on_message(
        &self,
        msg: Message,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<Option<Frame>> {
        let text = match msg {
            Message::Text(t) => t,
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => return Ok(None),
        };
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                return Err(anyhow::anyhow!("bybit ws decode: {e}"));
            }
        };

        // Server may spontaneously send `{"op":"ping"}`; acknowledge with
        // `{"op":"pong"}`. Subscription acks are `{"op":"subscribe","success":true,...}`.
        if let Some(op) = value.get("op").and_then(|v| v.as_str()) {
            match op {
                "ping" => {
                    let pong = OpMessage {
                        op: "pong",
                        args: None,
                    };
                    let text = serde_json::to_string(&pong)?;
                    sink.send(Message::Text(text)).await?;
                    return Ok(None);
                }
                "pong" | "subscribe" | "unsubscribe" | "auth" => {
                    // Control ack; swallow.
                    return Ok(None);
                }
                _ => {}
            }
        }

        // Data messages carry `"topic": "tickers.<symbol>"`, `"type":
        // "snapshot" | "delta"`, and a `"data"` object. We surface them
        // unchanged so the collector-side merger owns state.
        if value.get("topic").is_some() {
            return Ok(Some(Frame::Payload(value)));
        }

        // Unknown shape — surface as raw so callers can decide.
        Ok(Some(Frame::Raw(text.into_bytes())))
    }

    fn ping_interval(&self) -> Duration {
        // Bybit expects a heartbeat inside 20s.
        Duration::from_secs(20)
    }

    fn idle_timeout(&self) -> Duration {
        // At 20s ping cadence, we should see a pong promptly. 60s idle
        // without any server traffic is a dead line.
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_are_formatted_with_prefix() {
        let h = BybitTickersWs::new(["BTCUSDT", "ETHUSDT"]);
        assert_eq!(h.topics, vec!["tickers.BTCUSDT", "tickers.ETHUSDT"]);
    }

    #[test]
    fn default_url_is_linear_public_v5() {
        let h = BybitTickersWs::new(["BTCUSDT"]);
        assert_eq!(h.url(), "wss://stream.bybit.com/v5/public/linear");
    }

    #[test]
    fn topic_chunks_respect_arg_cap() {
        let syms: Vec<String> = (0..25).map(|i| format!("SYM{i}USDT")).collect();
        let h = BybitTickersWs::new(syms);
        let chunks: Vec<_> = h.topics.chunks(MAX_ARGS_PER_SUB).collect();
        assert_eq!(chunks.len(), 3);
        for c in &chunks[..2] {
            assert_eq!(c.len(), MAX_ARGS_PER_SUB);
        }
        assert_eq!(chunks[2].len(), 5);
    }
}
