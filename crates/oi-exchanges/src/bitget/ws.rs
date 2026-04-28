//! Bitget v2 public WebSocket handler — `ticker` channel.
//!
//! Stream: `wss://ws.bitget.com/v2/ws/public`.
//!
//! Subscribe shape:
//! ```json
//! {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"}]}
//! ```
//! Max 50 args per subscribe message per docs; 200 channel subscriptions
//! per connection.
//!
//! Heartbeat: **plain-text** `ping` every 30s; server responds with
//! plain-text `pong`. Server will also occasionally send its own
//! `ping`, which we echo back. Same quirk as OKX.
//!
//! Docs: <https://www.bitget.com/api-doc/contract/websocket/public/Tickers-Channel>
//!        <https://www.bitget.com/api-doc/common/websocket/intro>

use crate::common::ws::{Frame, WsConn, WsHandler};
use async_trait::async_trait;
use futures::{stream::SplitSink, SinkExt};
use serde::Serialize;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

pub const DEFAULT_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const MAX_ARGS_PER_SUB: usize = 50;

#[derive(Debug, Clone)]
pub struct BitgetTickerWs {
    url: String,
    inst_ids: Vec<String>,
    inst_type: String,
}

impl BitgetTickerWs {
    pub fn new(inst_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            url: DEFAULT_WS_URL.into(),
            inst_ids: inst_ids.into_iter().map(Into::into).collect(),
            inst_type: "USDT-FUTURES".into(),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }
}

#[derive(Serialize)]
struct SubArg<'a> {
    #[serde(rename = "instType")]
    inst_type: &'a str,
    channel: &'a str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
}

#[derive(Serialize)]
struct SubMessage<'a> {
    op: &'a str,
    args: Vec<SubArg<'a>>,
}

#[async_trait]
impl WsHandler for BitgetTickerWs {
    fn url(&self) -> &str {
        &self.url
    }
    fn name(&self) -> &'static str {
        "bitget-ticker"
    }

    async fn on_connect(
        &self,
        sink: &mut SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()> {
        for chunk in self.inst_ids.chunks(MAX_ARGS_PER_SUB) {
            let args: Vec<SubArg<'_>> = chunk
                .iter()
                .map(|id| SubArg {
                    inst_type: &self.inst_type,
                    channel: "ticker",
                    inst_id: id,
                })
                .collect();
            let msg = SubMessage {
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
                // Subscribe acks: `{"event":"subscribe","code":0,...}`.
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
        // Bitget wants heartbeat within 30s. 25s leaves room for RTT.
        Duration::from_secs(25)
    }

    fn idle_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_url_is_v2_public() {
        let h = BitgetTickerWs::new(["BTCUSDT".to_owned()]);
        assert_eq!(h.url(), "wss://ws.bitget.com/v2/ws/public");
    }

    #[test]
    fn arg_chunks_respect_cap() {
        let ids: Vec<String> = (0..120).map(|i| format!("SYM{i}USDT")).collect();
        let h = BitgetTickerWs::new(ids);
        let chunks: Vec<_> = h.inst_ids.chunks(MAX_ARGS_PER_SUB).collect();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), MAX_ARGS_PER_SUB);
        assert_eq!(chunks[2].len(), 20);
    }
}
