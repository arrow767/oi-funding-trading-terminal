//! Supervised WebSocket wrapper.
//!
//! Responsibilities:
//! * connect with TLS to any `wss://` endpoint,
//! * send a configurable subscription message on connect,
//! * ping/pong heartbeat (interval configurable per exchange — defaults to
//!   20s, inside the most-aggressive venue window),
//! * detect dead connections by absence of traffic over `idle_timeout`,
//! * reconnect with exponential backoff + jitter (`base=500ms`, `factor=2`,
//!   `cap=30s`), unbounded attempts — only user-initiated shutdown stops,
//! * surface decoded messages on an `mpsc::Receiver<Frame>`.
//!
//! Every exchange has its own quirks (text vs. binary pings, server-initiated
//! pings that MUST be pong'd, authenticated subscription), so the caller
//! supplies a [`WsHandler`] that plugs into the supervisor's lifecycle.

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, warn};

pub type WsConn = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Application-level frame emitted by the handler after decoding.
#[derive(Debug)]
pub enum Frame {
    /// Structured payload decoded by the handler.
    Payload(serde_json::Value),
    /// Raw text/binary when the handler wants to defer decode.
    Raw(Vec<u8>),
}

/// Per-exchange WS lifecycle hooks. Stateless between connects (the supervisor
/// re-runs `on_connect` each time).
#[async_trait]
pub trait WsHandler: Send + Sync + std::fmt::Debug {
    /// WebSocket URL.
    fn url(&self) -> &str;

    /// Identifier for metrics & logs. Example: `"binance-fstream"`.
    fn name(&self) -> &'static str;

    /// Called exactly once per successful handshake. The handler sends its
    /// subscription messages here.
    async fn on_connect(
        &self,
        sink: &mut futures::stream::SplitSink<WsConn, Message>,
    ) -> anyhow::Result<()>;

    /// Decide what to do with an inbound message. Return `Ok(Some(frame))` to
    /// surface it to the application; `Ok(None)` swallows it (heartbeat,
    /// ack). Errors cause the supervisor to reconnect.
    async fn on_message(
        &self,
        msg: Message,
        sink: &mut futures::stream::SplitSink<WsConn, Message>,
    ) -> anyhow::Result<Option<Frame>>;

    /// How often to emit an application-level ping if the exchange doesn't
    /// send one (otherwise we rely on tungstenite's automatic pong).
    fn ping_interval(&self) -> Duration {
        Duration::from_secs(20)
    }

    /// If no traffic is received for this duration, assume dead and reconnect.
    fn idle_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

/// Start the supervisor; returns a receiver of decoded frames.
///
/// Dropping the receiver or the returned `JoinHandle` shuts down the task.
pub fn spawn_ws<H: WsHandler + 'static>(handler: H) -> (mpsc::Receiver<Frame>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1024);
    let handler = Arc::new(handler);
    let jh = tokio::spawn(run_supervisor(handler, tx));
    (rx, jh)
}

async fn run_supervisor<H: WsHandler + 'static>(handler: Arc<H>, tx: mpsc::Sender<Frame>) {
    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(30))
        .with_factor(2.0)
        .with_jitter()
        .without_max_times()
        .build();

    loop {
        if tx.is_closed() {
            info!(ws = handler.name(), "consumer dropped; shutting down");
            return;
        }
        match connect_once(&handler, &tx).await {
            Ok(reason) => {
                warn!(ws = handler.name(), %reason, "ws connection closed; reconnecting");
            }
            Err(e) => {
                error!(ws = handler.name(), error=%e, "ws error; reconnecting");
            }
        }
        // No-op when no metrics recorder is installed (e.g. unit tests).
        metrics::counter!("oi_ws_reconnects_total", "handler" => handler.name())
            .increment(1);
        let delay = backoff.next().unwrap_or(Duration::from_secs(30));
        tokio::time::sleep(delay).await;
    }
}

async fn connect_once<H: WsHandler>(
    handler: &Arc<H>,
    tx: &mpsc::Sender<Frame>,
) -> anyhow::Result<String> {
    let name = handler.name();
    let url = handler.url().to_owned();
    debug!(ws = name, %url, "connecting");

    let (stream, _resp) = timeout(Duration::from_secs(10), connect_async(&url))
        .await
        .map_err(|_| anyhow::anyhow!("ws connect timeout"))??;
    info!(ws = name, %url, "ws connected");

    let (mut sink, mut src) = stream.split();
    handler.on_connect(&mut sink).await?;

    let mut ping_tick = interval(handler.ping_interval());
    ping_tick.tick().await; // consume the immediate first tick

    let idle = handler.idle_timeout();

    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                if let Err(e) = sink.send(Message::Ping(Vec::new())).await {
                    return Ok(format!("ping send failed: {e}"));
                }
            }
            next = timeout(idle, src.next()) => {
                let msg = match next {
                    Err(_) => return Ok("idle timeout".into()),
                    Ok(None) => return Ok("stream ended".into()),
                    Ok(Some(Err(e))) => return Err(anyhow::anyhow!("ws read error: {e}")),
                    Ok(Some(Ok(m))) => m,
                };
                match msg {
                    Message::Ping(p) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Message::Pong(_) => { /* ignore */ }
                    Message::Close(c) => return Ok(format!("close frame: {c:?}")),
                    other => {
                        if let Some(frame) = handler.on_message(other, &mut sink).await? {
                            if tx.send(frame).await.is_err() {
                                return Ok("consumer dropped".into());
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestHandler;

    #[async_trait]
    impl WsHandler for TestHandler {
        fn url(&self) -> &str {
            "wss://example.invalid/"
        }
        fn name(&self) -> &'static str {
            "test"
        }
        async fn on_connect(
            &self,
            _sink: &mut futures::stream::SplitSink<WsConn, Message>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_message(
            &self,
            _msg: Message,
            _sink: &mut futures::stream::SplitSink<WsConn, Message>,
        ) -> anyhow::Result<Option<Frame>> {
            Ok(None)
        }
    }

    #[test]
    fn handler_defaults_are_reasonable() {
        let h = TestHandler;
        assert_eq!(h.ping_interval(), Duration::from_secs(20));
        assert_eq!(h.idle_timeout(), Duration::from_secs(60));
    }
}
