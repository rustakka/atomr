//! # atomr-streams-io
//!
//! Real-world I/O `Source`/`Sink` adapters for `atomr-streams`, behind feature
//! flags so the core streams crate stays dependency-light:
//!
//! * `http` — [`HttpPollSource`](http_poll::HttpPollSource): periodic / conditional GET polling.
//! * `ws` — [`WsSource`](ws::WsSource): WebSocket frame source with reconnect.
//!
//! Token-bucket rate limiting lives upstream in `atomr_streams::rate`
//! (`token_bucket`, `token_bucket_keyed`, `respect_retry_after`) so backpressure
//! and limiting are uniform across all sources.

#![forbid(unsafe_code)]

#[cfg(feature = "http")]
pub mod http_poll;

#[cfg(feature = "ws")]
pub mod ws;
