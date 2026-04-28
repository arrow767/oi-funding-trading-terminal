//! Hyperliquid WebSocket — `activeAssetCtx` per-coin subscriptions.
//!
//! Stream: `wss://api.hyperliquid.xyz/ws`.
//!
//! Subscribe shape (one message per coin):
//! ```json
//! {"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}
//! ```
//! The per-coin subscription model means ~200 separate subscribe
//! messages for the full universe, all over a single connection.
//!
//! Heartbeat: `{"method":"ping"}` as JSON every 50s; server replies
//! `{"channel":"pong"}`. (Unlike OKX/Bitget — Hyperliquid uses JSON for
//! heartbeats, not plain text.)
//!
//! Docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>

use crate::common::ws::{Frame, WsConn, WsHandler};
use async_trait::async_trait;
use futures::{stream::SplitSink, SinkExt};
use serde::Serialize;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const DEFAULT_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

#[derive(Debug, Clone)]
pub struct HyperliquidActiveAssetCtxWs {
    url: String,
    coins: Vec<String>,
}

impl HyperliquidActiveAssetCtxWs {
    pub fn new(coins: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            url: DEFAULT_WS_URL.into(),
            coins: coins.into_iter().map(Into::into).collect(),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[derive(Serialize)]
struct SubscriptionBody<'a> {
    #[serde(rename = "type")]
    sub_type: &'a str,
    coin: &'a str,
}

#[derive(Serialize)]
struct SubscribeMessage<'a> {
    method: &'a str,
    subscription: SubscriptionBody<'a>,
}

#[derive(Serialize)]
struct PingMessage<'a> {
    method: &'a str,
}

#[async_trait]
impl WsHandler for HyperliquidActiveAssetCtxWs {
    fn url(&self) -> &str {
        &self.url
    }
    fn name(&self) -> &'static str {
        "hyperliquid-activeassetctx"
    }

    async fn on_connect(
        &self,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()> {
        for coin in &self.coins {
            let msg = SubscribeMessage {
                method: "subscribe",
                subscription: SubscriptionBody {
                    sub_type: "activeAssetCtx",
                    coin,
                },
            };
            let text = serde_json::to_string(&msg)?;
            sink.send(Message::Text(text)).await?;
        }
        // Send an initial ping so the server's idle clock starts now
        // and doesn't trip before our first data frame.
        let ping = PingMessage { method: "ping" };
        sink.send(Message::Text(serde_json::to_string(&ping)?))
            .await?;
        Ok(())
    }

    async fn on_message(
        &self,
        msg: Message,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<Option<Frame>> {
        let Message::Text(t) = msg else {
            return Ok(None);
        };
        let value: serde_json::Value = serde_json::from_str(&t)?;

        // Channel dispatch.
        let channel = value.get("channel").and_then(|v| v.as_str());
        match channel {
            Some("pong") => return Ok(None),
            Some("subscriptionResponse") => return Ok(None),
            Some("error") => {
                // Log but don't kill the connection — sometimes only
                // one subscription is rejected (e.g. delisted coin).
                tracing::warn!(body = %value, "hyperliquid ws error frame");
                return Ok(None);
            }
            _ => {}
        }

        // Server doesn't push its own `ping`, but handle defensively
        // if Hyperliquid adds it later.
        if value.get("method").and_then(|v| v.as_str()) == Some("ping") {
            let pong = serde_json::json!({"method": "pong"});
            sink.send(Message::Text(pong.to_string())).await?;
            return Ok(None);
        }

        // Data frame — let the stream parser own the shape check.
        if channel.is_some() {
            return Ok(Some(Frame::Payload(value)));
        }
        Ok(None)
    }

    fn ping_interval(&self) -> Duration {
        // Well inside Hyperliquid's 50s idle window.
        Duration::from_secs(20)
    }

    fn idle_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_url_is_hyperliquid_ws() {
        let h = HyperliquidActiveAssetCtxWs::new(["BTC".to_owned()]);
        assert_eq!(h.url(), "wss://api.hyperliquid.xyz/ws");
    }

    #[test]
    fn stores_coins_verbatim() {
        let h = HyperliquidActiveAssetCtxWs::new(["BTC", "ETH", "SOL"]);
        assert_eq!(h.coins, vec!["BTC", "ETH", "SOL"]);
    }
}
