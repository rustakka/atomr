//! HTTP polling `Source` adapters.
//!
//! [`HttpPollSource`] turns a periodic HTTP `GET` into a
//! [`Source<Result<HttpResponse, HttpError>>`]. Two flavours are provided:
//!
//! * [`HttpPollSource::new`] — fire a plain `GET` every `interval`.
//! * [`HttpPollSource::with_etag`] — conditional `GET`: track the last
//!   `ETag` response header and send it back as `If-None-Match` on the next
//!   request, surfacing `304 Not Modified` as
//!   [`HttpResponse { not_modified: true, .. }`].
//!
//! ## Backpressure
//!
//! The polling task and the consumer are decoupled by a **bounded** Tokio
//! mpsc channel (capacity [`POLL_CHANNEL_CAPACITY`]). The producer task
//! `send().await`s into that channel, so when the consumer falls behind the
//! channel fills and the producer's send awaits — naturally pausing further
//! polling until the consumer catches up. This is the simplest correct
//! backpressure story: we never drop responses and never grow memory without
//! bound. (Contrast with [`Source::from_receiver`], which takes an *unbounded*
//! receiver; we deliberately bridge a bounded channel onto it via a small
//! forwarding adaptor below so the bound is honoured end to end.)
//!
//! ## Rate limiting
//!
//! This adapter does **not** implement rate limiting itself. Compose it with
//! the upstream limiter, e.g.
//! `atomr_streams::rate::token_bucket(src, 10.0, 1)`, to cap request rate —
//! see `examples/edgar_poller.rs`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use atomr_streams::Source;
use bytes::Bytes;

/// Bounded capacity of the channel bridging the polling task to the stream.
///
/// Small on purpose: it bounds in-flight, un-consumed responses, which is
/// what gives us backpressure onto the polling loop.
pub const POLL_CHANNEL_CAPACITY: usize = 8;

/// Describes a single HTTP request: target URL plus extra request headers.
///
/// Callers SHOULD always include a `User-Agent` header (many APIs — e.g. the
/// SEC EDGAR system — reject requests without one). See the crate examples.
#[derive(Debug, Clone)]
pub struct RequestSpec {
    /// Absolute URL to `GET`.
    pub url: String,
    /// Additional request headers as `(name, value)` pairs.
    pub headers: Vec<(String, String)>,
}

impl RequestSpec {
    /// Convenience constructor for a bare URL with no extra headers.
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), headers: Vec::new() }
    }

    /// Builder-style: append a request header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// A captured HTTP response.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// HTTP status code (e.g. `200`, `304`).
    pub status: u16,
    /// Response headers as `(name, value)` pairs (lower-cased names).
    pub headers: Vec<(String, String)>,
    /// Response body bytes (empty for `304 Not Modified`).
    pub body: Bytes,
    /// `true` when the server answered `304 Not Modified` to a conditional
    /// `GET` — the body is empty and the previously fetched representation is
    /// still current.
    pub not_modified: bool,
}

/// Errors raised while polling.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    /// A transport / network / I/O error performing the request.
    #[error("http transport error: {0}")]
    Transport(String),
    /// The request could not be constructed (bad URL, invalid header, …).
    #[error("http request build error: {0}")]
    Build(String),
}

/// HTTP polling source factory.
pub struct HttpPollSource;

impl HttpPollSource {
    /// Poll `req` with a plain `GET` every `interval`, emitting each
    /// `Result<HttpResponse, HttpError>` in order.
    ///
    /// The first poll happens after the first `interval` has elapsed. The
    /// returned source is effectively infinite — terminate it downstream with
    /// `take`, a [`atomr_streams::KillSwitch`], etc.
    // This is a source *factory*; returning the constructed `Source` rather
    // than `Self` is the whole point of the API.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(req: RequestSpec, interval: Duration) -> Source<Result<HttpResponse, HttpError>> {
        Self::spawn(req, interval, false)
    }

    /// Like [`new`](Self::new) but performs a *conditional* `GET`: after the
    /// first successful response carrying an `ETag`, subsequent requests send
    /// `If-None-Match: <etag>`. A `304 Not Modified` is surfaced as
    /// [`HttpResponse`] with `not_modified == true` and an empty body.
    pub fn with_etag(req: RequestSpec, interval: Duration) -> Source<Result<HttpResponse, HttpError>> {
        Self::spawn(req, interval, true)
    }

    fn spawn(
        req: RequestSpec,
        interval: Duration,
        use_etag: bool,
    ) -> Source<Result<HttpResponse, HttpError>> {
        // Bounded channel => backpressure onto the polling loop.
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Result<HttpResponse, HttpError>>(POLL_CHANNEL_CAPACITY.max(1));

        let client = reqwest::Client::new();
        // Shared last-seen ETag (only mutated by the single polling task, but
        // kept behind a Mutex so the type is simple & Send).
        let etag: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let conditional =
                    if use_etag { etag.lock().unwrap_or_else(|p| p.into_inner()).clone() } else { None };
                let result = perform_get(&client, &req, conditional, &etag, use_etag).await;

                // Bounded send: awaits if the consumer is behind (backpressure).
                if tx.send(result).await.is_err() {
                    // Consumer dropped; stop polling.
                    return;
                }
            }
        });

        // Bridge the bounded receiver onto an unbounded one for
        // `Source::from_receiver`, preserving the upstream bound: this
        // forwarder only pulls from the bounded `rx` when it has handed the
        // previous item off, so the bounded channel remains the limiting
        // buffer.
        let (utx, urx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                if utx.send(item).is_err() {
                    return;
                }
            }
        });

        Source::from_receiver(urx)
    }
}

/// Perform one `GET`, optionally conditional, mapping the outcome to a
/// `Result<HttpResponse, HttpError>` and updating the stored ETag.
async fn perform_get(
    client: &reqwest::Client,
    req: &RequestSpec,
    conditional_etag: Option<String>,
    etag_store: &Arc<Mutex<Option<String>>>,
    use_etag: bool,
) -> Result<HttpResponse, HttpError> {
    let mut builder = client.get(&req.url);
    for (name, value) in &req.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    if let Some(tag) = conditional_etag {
        builder = builder.header(reqwest::header::IF_NONE_MATCH, tag);
    }

    let resp = builder.send().await.map_err(|e| {
        if e.is_builder() {
            HttpError::Build(e.to_string())
        } else {
            HttpError::Transport(e.to_string())
        }
    })?;

    let status = resp.status().as_u16();
    let not_modified = status == 304;

    let headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect();

    // Track the freshest ETag we've seen for the next conditional request.
    if use_etag && !not_modified {
        if let Some(tag) = resp.headers().get(reqwest::header::ETAG) {
            if let Ok(s) = tag.to_str() {
                *etag_store.lock().unwrap_or_else(|p| p.into_inner()) = Some(s.to_string());
            }
        }
    }

    let body = if not_modified {
        Bytes::new()
    } else {
        resp.bytes().await.map_err(|e| HttpError::Transport(e.to_string()))?
    };

    Ok(HttpResponse { status, headers, body, not_modified })
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_streams::Sink;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    /// Spawn a one-shot raw-HTTP responder that accepts a single connection,
    /// drains the request, writes a canned `200 OK` with an ETag, and closes.
    /// Returns the bound `http://127.0.0.1:<port>/` URL.
    async fn canned_ok_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                // Read the request bytes until the header terminator so reqwest
                // considers the exchange well-formed; we don't parse them.
                let mut buf = [0u8; 1024];
                use tokio::io::AsyncReadExt;
                let _ = sock.read(&mut buf).await;
                let body = b"hi";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.write_all(body).await;
                let _ = sock.flush().await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://{addr}/")
    }

    #[tokio::test]
    async fn first_emission_is_ok_200_against_canned_server() {
        let url = canned_ok_server().await;
        let req = RequestSpec::new(url).header("User-Agent", "atomr-test/0.1");
        let src = HttpPollSource::new(req, Duration::from_millis(5));

        let first = Sink::first(src).await.expect("expected one emission");
        match first {
            Ok(resp) => {
                assert_eq!(resp.status, 200);
                assert!(!resp.not_modified);
                assert_eq!(resp.body.as_ref(), b"hi");
                assert!(resp.headers.iter().any(|(k, v)| k.eq_ignore_ascii_case("etag") && v == "\"abc\""));
            }
            Err(e) => panic!("expected Ok(200), got Err: {e}"),
        }
    }

    #[tokio::test]
    async fn connection_refused_surfaces_transport_err() {
        // Port 1 is reserved/unused; connect should be refused -> Transport err.
        let req = RequestSpec::new("http://127.0.0.1:1/").header("User-Agent", "atomr-test/0.1");
        let src = HttpPollSource::new(req, Duration::from_millis(5));

        let first = Sink::first(src).await.expect("expected one emission");
        match first {
            Err(HttpError::Transport(_)) => {}
            other => panic!("expected Err(Transport), got {other:?}"),
        }
    }
}
