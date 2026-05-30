//! EDGAR-style HTTP poller, rate-limited to 10 requests/second.
//!
//! The SEC EDGAR system requires a descriptive `User-Agent` on every request
//! and asks clients to stay at or below 10 requests per second. This example
//! wires [`HttpPollSource`] (which polls on a fixed interval) through the
//! upstream [`token_bucket`](atomr_streams::rate::token_bucket) limiter so the
//! emitted request rate provably never breaches 10 req/s — the bucket refills
//! at 10 tokens/sec with a burst of 1, and every poll must spend a token.
//!
//! Run with:
//!
//! ```text
//! cargo run -p atomr-streams-io --features http --example edgar_poller
//! ```
//!
//! By default this example does **not** hit the network: it constructs the
//! rate-limited pipeline and demonstrates the wiring. Set the environment
//! variable `EDGAR_LIVE=1` to actually poll the (illustrative) endpoint.

use std::time::Duration;

use atomr_streams::token_bucket;
use atomr_streams::Sink;
use atomr_streams_io::http_poll::{HttpPollSource, RequestSpec};

#[tokio::main]
async fn main() {
    // EDGAR mandates a contact-bearing User-Agent. Replace with your own.
    let req = RequestSpec::new("https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany")
        .header("User-Agent", "atomr-example admin@example.com")
        .header("Accept-Encoding", "gzip, deflate");

    // Poll fairly aggressively; the token bucket — not the poll interval — is
    // what enforces the hard ceiling.
    let polls = HttpPollSource::with_etag(req, Duration::from_millis(50));

    // Hard cap: 10 requests/sec, burst 1. The bucket refills at 10/s and each
    // emission spends a token, so over any window the count never exceeds
    // `1 + 10 * window_seconds` — i.e. the 10 req/s limit is respected.
    let limited = token_bucket(polls, 10.0, 1);

    if std::env::var("EDGAR_LIVE").as_deref() != Ok("1") {
        println!(
            "Pipeline constructed: HttpPollSource::with_etag -> token_bucket(10.0, 1).\n\
             Set EDGAR_LIVE=1 to actually poll the network."
        );
        return;
    }

    // Take a handful of responses then stop (the source is otherwise endless).
    let responses = Sink::collect(limited.take(3)).await;
    for r in responses {
        match r {
            Ok(resp) if resp.not_modified => println!("304 Not Modified (etag still fresh)"),
            Ok(resp) => println!("{} — {} bytes", resp.status, resp.body.len()),
            Err(e) => eprintln!("poll error: {e}"),
        }
    }
}
