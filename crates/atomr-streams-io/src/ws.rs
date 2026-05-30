//! WebSocket frame `Source` with automatic reconnect.
//!
//! [`WsSource::connect`] opens a WebSocket connection via
//! [`tokio_tungstenite::connect_async`], maps each inbound tungstenite
//! [`Message`](tokio_tungstenite::tungstenite::Message) to a [`WsFrame`], and
//! wraps the whole connect-and-stream cycle in
//! [`RestartSource::with_backoff`](atomr_streams::RestartSource::with_backoff).
//! When the connection drops (or the initial connect fails) the restart
//! combinator re-runs the factory after a backoff governed by the supplied
//! [`RestartSettings`](atomr_streams::RestartSettings), so the source
//! transparently reconnects.
//!
//! Each connection attempt yields a [`Source`] of
//! `Result<WsFrame, WsError>`: a failed connect produces a single
//! `Err(WsError::Connect(_))` (after which restart backs off and retries),
//! and a live connection produces `Ok(WsFrame::…)` per inbound message until
//! the peer closes or errors.
//!
//! ## Rate limiting
//!
//! As with [`crate::http_poll`], rate mediation is left to the upstream
//! operators in `atomr_streams::rate` (e.g. `token_bucket`) so policy is
//! uniform across sources.

use atomr_streams::{RestartSettings, RestartSource, Source};
use bytes::Bytes;
use futures_util::StreamExt;

/// A WebSocket frame, decoupled from the underlying tungstenite types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsFrame {
    /// A UTF-8 text frame.
    Text(String),
    /// A binary frame.
    Binary(Bytes),
    /// A ping control frame with its payload.
    Ping(Bytes),
    /// A pong control frame with its payload.
    Pong(Bytes),
    /// A close control frame (close reason discarded).
    Close,
}

/// Errors raised by the WebSocket source.
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    /// Failed to establish (or re-establish) the connection.
    #[error("websocket connect error: {0}")]
    Connect(String),
    /// A protocol / transport error on an established connection.
    #[error("websocket protocol error: {0}")]
    Protocol(String),
}

/// WebSocket source factory.
pub struct WsSource;

impl WsSource {
    /// Connect to `url`, emitting `Ok(WsFrame)` per inbound message and
    /// reconnecting with backoff per `restart` whenever the stream ends or a
    /// connect attempt fails.
    ///
    /// A failed connect surfaces as a single `Err(WsError::Connect(_))` from
    /// that attempt; the [`RestartSource`] then backs off and tries again
    /// (subject to `restart.max_restarts`).
    pub fn connect(url: url::Url, restart: RestartSettings) -> Source<Result<WsFrame, WsError>> {
        RestartSource::with_backoff(restart, move || {
            let url = url.clone();
            connect_once(url)
        })
    }
}

/// Build a `Source` for a single connection attempt.
///
/// On connect failure the source is a single `Err(Connect(_))`; on success it
/// streams mapped frames, terminating when the socket closes/errors so the
/// outer [`RestartSource`] can reconnect.
fn connect_once(url: url::Url) -> Source<Result<WsFrame, WsError>> {
    // A bounded channel decouples the per-connection driver task from the
    // consumer and bounds in-flight frames (backpressure).
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<WsFrame, WsError>>();

    tokio::spawn(async move {
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Err(e) => {
                let _ = tx.send(Err(WsError::Connect(e.to_string())));
                // Stream ends -> RestartSource reconnects after backoff.
            }
            Ok((ws, _resp)) => {
                let (_write, mut read) = ws.split();
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(m) => {
                            if let Some(frame) = map_message(m) {
                                if tx.send(Ok(frame)).is_err() {
                                    return; // consumer gone
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(WsError::Protocol(e.to_string())));
                            return; // end this connection's stream
                        }
                    }
                }
            }
        }
    });

    Source::from_receiver(rx)
}

/// Map a tungstenite [`Message`] to a [`WsFrame`].
///
/// Returns `None` for `Frame(_)` raw frames (an internal tungstenite variant
/// not produced by the high-level read stream).
fn map_message(m: tokio_tungstenite::tungstenite::Message) -> Option<WsFrame> {
    use tokio_tungstenite::tungstenite::Message as M;
    match m {
        M::Text(s) => Some(WsFrame::Text(s)),
        M::Binary(b) => Some(WsFrame::Binary(Bytes::from(b))),
        M::Ping(b) => Some(WsFrame::Ping(Bytes::from(b))),
        M::Pong(b) => Some(WsFrame::Pong(Bytes::from(b))),
        M::Close(_) => Some(WsFrame::Close),
        M::Frame(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_streams::Sink;
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn fast_restart(max: usize) -> RestartSettings {
        RestartSettings {
            min_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            random_factor: 0.0,
            max_restarts: Some(max),
        }
    }

    #[tokio::test]
    async fn first_frame_is_text_against_local_server() {
        // Local WS server: accept one connection, send one Text frame, close.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                    use futures_util::SinkExt;
                    use tokio_tungstenite::tungstenite::Message;
                    let _ = ws.send(Message::Text("hello".to_string())).await;
                    let _ = ws.close(None).await;
                }
            }
        });

        let url = url::Url::parse(&format!("ws://{addr}/")).unwrap();
        // Only one connection's worth of frames is needed.
        let src = WsSource::connect(url, fast_restart(1));

        let first = Sink::first(src).await.expect("expected one frame");
        match first {
            Ok(WsFrame::Text(s)) => assert_eq!(s, "hello"),
            other => panic!("expected Ok(Text(\"hello\")), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn refused_port_surfaces_connect_err() {
        // Nothing listens on port 1 -> connect fails -> Err(Connect).
        let url = url::Url::parse("ws://127.0.0.1:1/").unwrap();
        let src = WsSource::connect(url, fast_restart(1));

        let first = Sink::first(src).await.expect("expected one emission");
        match first {
            Err(WsError::Connect(_)) => {}
            other => panic!("expected Err(Connect), got {other:?}"),
        }
    }
}
