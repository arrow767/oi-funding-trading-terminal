//! OKX v5 public WebSocket handler for the `open-interest` channel.
//!
//! Stream: `wss://ws.okx.com:8443/ws/v5/public`.
//!
//! Subscribe shape:
//! ```json
//! {"op":"subscribe","args":[{"channel":"open-interest","instId":"BTC-USDT-SWAP"}]}
//! ```
//!
//! Heartbeat: OKX expects a **plain-text** `ping` (not JSON!) every
//! 25–30s; the server responds with plain-text `pong`. Anything else
//! at the protocol level (ws ping frames) is also pong'd by the server
//! but the app-level heartbeat is the documented path.
//!
//! Docs: <https://www.okx.com/docs-v5/en/#overview-websocket-connect>
//!        <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-open-interest-channel>

use crate::common::ws::{Frame, WsConn, WsHandler};
use async_trait::async_trait;
use futures::{stream::SplitSink, SinkExt};
use serde::Serialize;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const DEFAULT_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// Max channels per subscription message. OKX has no hard per-message
/// cap but batches of ~40 keep frames small and acks quick.
const MAX_ARGS_PER_SUB: usize = 40;

#[derive(Debug, Clone)]
pub struct OkxOpenInterestWs {
    url: String,
    inst_ids: Vec<String>,
}

impl OkxOpenInterestWs {
    pub fn new(inst_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            url: DEFAULT_WS_URL.into(),
            inst_ids: inst_ids.into_iter().map(Into::into).collect(),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[derive(Serialize)]
struct SubscribeArg<'a> {
    channel: &'a str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
}

#[derive(Serialize)]
struct SubscribeMessage<'a> {
    op: &'a str,
    args: Vec<SubscribeArg<'a>>,
}

#[async_trait]
impl WsHandler for OkxOpenInterestWs {
    fn url(&self) -> &str {
        &self.url
    }

    fn name(&self) -> &'static str {
        "okx-open-interest"
    }

    async fn on_connect(
        &self,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()> {
        for chunk in self.inst_ids.chunks(MAX_ARGS_PER_SUB) {
            let args: Vec<SubscribeArg<'_>> = chunk
                .iter()
                .map(|id| SubscribeArg {
                    channel: "open-interest",
                    inst_id: id,
                })
                .collect();
            let msg = SubscribeMessage {
                op: "subscribe",
                args,
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
        match msg {
            Message::Text(t) if t == "pong" => Ok(None),
            Message::Text(t) if t == "ping" => {
                sink.send(Message::Text("pong".into())).await?;
                Ok(None)
            }
            Message::Text(t) => {
                let value: serde_json::Value = serde_json::from_str(&t)?;
                // Subscribe acks have `{"event":"subscribe",...}`.
                if value.get("event").is_some() {
                    return Ok(None);
                }
                if value.get("arg").is_some() {
                    return Ok(Some(Frame::Payload(value)));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn ping_interval(&self) -> Duration {
        // Within OKX's 25–30s window. The `spawn_ws` supervisor also
        // sends protocol-level pings; this handler emits the OKX
        // app-level heartbeat which matters when intermediaries strip
        // protocol pings.
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
    fn default_url_is_public_v5() {
        let h = OkxOpenInterestWs::new(["BTC-USDT-SWAP".to_owned()]);
        assert_eq!(h.url(), "wss://ws.okx.com:8443/ws/v5/public");
    }

    #[test]
    fn chunking_respects_arg_cap() {
        let ids: Vec<String> = (0..100).map(|i| format!("SYM{i}-USDT-SWAP")).collect();
        let h = OkxOpenInterestWs::new(ids);
        let chunks: Vec<_> = h.inst_ids.chunks(MAX_ARGS_PER_SUB).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), MAX_ARGS_PER_SUB);
        assert_eq!(chunks[2].len(), 20);
    }
}
