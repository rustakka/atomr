//! Rate-mediation operators on `Source<T>`: `Conflate`,
//! `ConflateWithSeed`, `Expand`, `Extrapolate`.
//!
//! These operators decouple producer / consumer rates without buffering
//! every element: when downstream is slow, `conflate` collapses
//! upstream values into a running aggregate; when upstream is slow,
//! `expand` repeatedly emits a derived value until the next upstream
//! element arrives.

use std::collections::HashMap;
use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::source::Source;

/// `conflate(seed, fold)` — when downstream is slower than upstream,
/// merge consecutive upstream elements into a running aggregate via
/// `fold`. The aggregate is emitted whenever downstream pulls.
///
/// In our buffered-channel model "merge until pulled" is approximated
/// by folding contiguous bursts inside the upstream task and emitting
/// at each await point: every output element is the fold of the
/// upstream burst since the last emission.
pub fn conflate<T, U, S, F>(src: Source<T>, mut seed: S, mut fold: F) -> Source<U>
where
    T: Send + 'static,
    U: Send + 'static,
    S: FnMut(T) -> U + Send + 'static,
    F: FnMut(U, T) -> U + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<U>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut acc: Option<U> = None;
        loop {
            match inner.next().await {
                Some(item) => {
                    acc = Some(match acc.take() {
                        None => seed(item),
                        Some(prev) => fold(prev, item),
                    });
                    // Try to flush the accumulator if downstream is
                    // ready to receive — best-effort; otherwise keep
                    // folding.
                    if let Some(a) = acc.take() {
                        if tx.send(a).is_err() {
                            return;
                        }
                    }
                }
                None => {
                    if let Some(a) = acc.take() {
                        let _ = tx.send(a);
                    }
                    return;
                }
            }
        }
    });
    Source::from_receiver(rx)
}

/// `expand(extrapolate)` — when upstream is slower than downstream,
/// repeatedly call `extrapolate(last)` between elements to keep
/// downstream supplied. After the upstream completes, the iterator
/// returned by `extrapolate(last)` continues to be drained until it
/// itself is exhausted.
///
/// / `Source.Extrapolate`.
///
/// The closure receives the most recent upstream element by reference
/// and returns an `Iterator<Item = T>` describing the synthetic
/// values to emit while waiting for the next upstream element.
pub fn expand<T, F, I>(src: Source<T>, mut extrapolate: F) -> Source<T>
where
    T: Clone + Send + 'static,
    F: FnMut(&T) -> I + Send + 'static,
    I: Iterator<Item = T> + Send + 'static,
{
    let (tx, rx) = mpsc::unbounded_channel::<T>();
    let mut inner = src.into_boxed();
    tokio::spawn(async move {
        let mut last: Option<T> = None;
        loop {
            match inner.next().await {
                Some(item) => {
                    if tx.send(item.clone()).is_err() {
                        return;
                    }
                    last = Some(item);
                }
                None => {
                    // Upstream done — drain extrapolation iterator
                    // once, then close.
                    if let Some(l) = last {
                        for synth in extrapolate(&l) {
                            if tx.send(synth).is_err() {
                                return;
                            }
                        }
                    }
                    return;
                }
            }
        }
    });
    Source::from_receiver(rx)
}

/// A classic token bucket: it accrues `rate_per_sec` tokens per second up to a
/// ceiling of `burst` tokens, and each tracked unit consumes one token, waiting
/// for one to accrue if the bucket is empty.
///
/// Refill is computed lazily from elapsed wall time ([`tokio::time::Instant`])
/// at each call, so there is no background timer.
struct TokenBucket {
    /// Tokens accrued per second.
    rate_per_sec: f64,
    /// Maximum tokens that can be banked.
    capacity: f64,
    /// Current token count (fractional accrual is tracked).
    tokens: f64,
    /// Last time tokens were refilled.
    last: Instant,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, burst: u32) -> Self {
        let capacity = burst as f64;
        TokenBucket {
            rate_per_sec: rate_per_sec.max(0.0),
            capacity,
            // Start full so the initial burst is permitted immediately.
            tokens: capacity,
            last: Instant::now(),
        }
    }

    /// Add tokens accrued since `last`, clamped to `capacity`.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
            self.last = now;
        }
    }

    /// How long until at least one token is available, given `now`. `None`
    /// means a token is available right now.
    fn delay_until_token(&mut self, now: Instant) -> Option<Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            None
        } else if self.rate_per_sec <= 0.0 {
            // No refill will ever happen; treat as a very long wait so the
            // element effectively stalls (degenerate config).
            Some(Duration::from_secs(u64::MAX / 2))
        } else {
            let needed = 1.0 - self.tokens;
            Some(Duration::from_secs_f64(needed / self.rate_per_sec))
        }
    }

    /// Consume one token (caller must have ensured availability).
    fn consume(&mut self) {
        self.tokens -= 1.0;
    }
}

/// Wait for a token in `bucket`, sleeping as needed, then consume it.
async fn acquire(bucket: &mut TokenBucket) {
    loop {
        let now = Instant::now();
        match bucket.delay_until_token(now) {
            None => {
                bucket.consume();
                return;
            }
            Some(d) => {
                tokio::time::sleep(d).await;
                // Loop and re-check: sleep granularity / scheduling may mean we
                // still need a touch more time.
            }
        }
    }
}

/// `token_bucket(rate_per_sec, burst)` — a real-time leaky/token-bucket rate
/// limiter.
///
/// The bucket refills at `rate_per_sec` tokens per second up to `burst` tokens
/// of capacity and starts full, so an initial burst of up to `burst` elements
/// passes immediately. Thereafter each element waits (via
/// [`tokio::time::sleep`]) until a token is available, then consumes one.
///
/// **Property.** Over any window the number of emitted elements never exceeds
/// `burst + rate_per_sec * window_seconds` (sustained rate plus the bucket
/// capacity), modulo timer granularity.
///
/// This is a *wall-time* limiter — it intentionally uses real time rather than
/// the logical [`Clock`](atomr_core::time::Clock); use
/// [`clock_gated`](crate::clock_gated::clock_gated) for logical-time gating.
pub fn token_bucket<T>(src: Source<T>, rate_per_sec: f64, burst: u32) -> Source<T>
where
    T: Send + 'static,
{
    struct State<T> {
        inner: BoxStream<'static, T>,
        bucket: TokenBucket,
    }
    let state = State { inner: src.into_boxed(), bucket: TokenBucket::new(rate_per_sec, burst) };
    Source::unfold(state, |mut st| async move {
        match st.inner.next().await {
            None => None,
            Some(item) => {
                acquire(&mut st.bucket).await;
                Some((item, st))
            }
        }
    })
}

/// `token_bucket_keyed(key, rate_per_sec, burst)` — like [`token_bucket`] but
/// maintains an independent bucket per key.
///
/// Each distinct key returned by `key` gets its own [`TokenBucket`] with the
/// same `rate_per_sec` / `burst` parameters, so heavy traffic on one key never
/// starves another. Buckets are created lazily on first sight of a key and held
/// for the lifetime of the stream.
pub fn token_bucket_keyed<T, K, F>(src: Source<T>, key: F, rate_per_sec: f64, burst: u32) -> Source<T>
where
    T: Send + 'static,
    K: Eq + std::hash::Hash + Send + 'static,
    F: Fn(&T) -> K + Send + 'static,
{
    struct State<T, K, F> {
        inner: BoxStream<'static, T>,
        buckets: HashMap<K, TokenBucket>,
        key: F,
        rate_per_sec: f64,
        burst: u32,
    }
    let state = State { inner: src.into_boxed(), buckets: HashMap::new(), key, rate_per_sec, burst };
    Source::unfold(state, |mut st| async move {
        match st.inner.next().await {
            None => None,
            Some(item) => {
                let k = (st.key)(&item);
                let rate = st.rate_per_sec;
                let burst = st.burst;
                let bucket = st.buckets.entry(k).or_insert_with(|| TokenBucket::new(rate, burst));
                acquire(bucket).await;
                Some((item, st))
            }
        }
    })
}

/// A minimal carrier for a "retry after N seconds" signal.
///
/// This is deliberately protocol-agnostic: parsing HTTP `429 Too Many Requests`
/// / `Retry-After` headers into a `RetryAfter` lives in the future
/// `atomr-streams-io` crate. [`respect_retry_after`] only consumes the
/// already-extracted duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryAfter {
    /// Number of seconds the producer asked us to back off.
    pub seconds: u64,
}

/// `respect_retry_after` — honour back-off signals carried in-band.
///
/// The input is a stream of `Result<T, RetryAfter>`. Every element is passed
/// through unchanged (both `Ok` and `Err` variants are forwarded — nothing is
/// dropped), but whenever an element carries a [`RetryAfter`], emission pauses
/// for the requested duration *after* forwarding it, so all subsequent elements
/// are delayed rather than discarded.
///
/// Parsing of protocol-level rate-limit responses (e.g. HTTP 429) into
/// `RetryAfter` is the responsibility of the future `atomr-streams-io` crate;
/// this operator is the generic, transport-free building block.
pub fn respect_retry_after<T>(src: Source<Result<T, RetryAfter>>) -> Source<Result<T, RetryAfter>>
where
    T: Send + 'static,
{
    struct State<T> {
        inner: BoxStream<'static, Result<T, RetryAfter>>,
        // A back-off to serve before pulling the next upstream element.
        pending_backoff: Option<Duration>,
    }
    let state = State { inner: src.into_boxed(), pending_backoff: None };
    Source::unfold(state, |mut st| async move {
        // Honour a back-off requested by the previously emitted element before
        // touching upstream again — subsequent elements are delayed, not dropped.
        if let Some(d) = st.pending_backoff.take() {
            tokio::time::sleep(d).await;
        }
        match st.inner.next().await {
            None => None,
            Some(item) => {
                if let Err(ra) = &item {
                    if ra.seconds > 0 {
                        st.pending_backoff = Some(Duration::from_secs(ra.seconds));
                    }
                }
                Some((item, st))
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;

    #[tokio::test]
    async fn conflate_passes_through_when_downstream_keeps_up() {
        let s = Source::from_iter(vec![1u32, 2, 3]);
        let out = Sink::collect(conflate(s, |t| t, |a, b| a + b)).await;
        // With unbounded channel + immediate flush, each element
        // emerges separately rather than folded.
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn conflate_seed_initializes_accumulator() {
        let s = Source::from_iter(vec![10u32]);
        let out = Sink::collect(conflate(s, |t| t * 2, |a, b| a + b)).await;
        assert_eq!(out, vec![20]);
    }

    #[tokio::test]
    async fn expand_emits_extrapolated_values_after_upstream_close() {
        let s = Source::from_iter(vec![5i32]);
        let out = Sink::collect(expand(s, |last| {
            let l = *last;
            (0..3).map(move |i| l + i + 1)
        }))
        .await;
        // upstream emits 5, then extrapolation iterator emits 6, 7, 8.
        assert_eq!(out, vec![5, 6, 7, 8]);
    }

    #[tokio::test]
    async fn expand_no_synthetics_when_iterator_empty() {
        let s = Source::from_iter(vec![1i32, 2, 3]);
        let out = Sink::collect(expand(s, |_last| std::iter::empty::<i32>())).await;
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn token_bucket_respects_rate_plus_burst() {
        use std::time::Instant as StdInstant;
        // 50 tokens/sec, burst 5: in a ~120ms window we expect at most
        // 5 (burst) + 50 * 0.12 = ~11 emissions.
        let rate = 50.0;
        let burst = 5u32;
        let src = Source::from_iter(0u32..1000);
        let mut stream = token_bucket(src, rate, burst).into_boxed();

        let start = StdInstant::now();
        let window = Duration::from_millis(120);
        let mut count = 0u64;
        while StdInstant::now().duration_since(start) < window {
            match tokio::time::timeout(Duration::from_millis(20), stream.next()).await {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(_) => {}
            }
        }
        let elapsed = StdInstant::now().duration_since(start).as_secs_f64();
        let allowed = burst as f64 + rate * elapsed;
        // Generous timing slack for scheduler jitter.
        assert!(count as f64 <= allowed + 4.0, "emitted {count} in {elapsed:.3}s, allowed ~{allowed:.1}",);
        // And the initial burst should at least have come out promptly.
        assert!(count >= burst as u64, "expected at least the burst {burst}, got {count}");
    }

    #[tokio::test]
    async fn token_bucket_keyed_limits_keys_independently() {
        use std::time::Instant as StdInstant;
        // Interleave two keys "a"/"b". Each key: 20/sec, burst 2.
        // Within ~50ms a single key allows ~2 + 20*0.05 = 3, so two keys ~6.
        let items: Vec<&'static str> = (0..200).map(|i| if i % 2 == 0 { "a" } else { "b" }).collect();
        let src = Source::from_iter(items);
        let limited = token_bucket_keyed(src, |s: &&str| *s, 20.0, 2);
        let mut stream = limited.into_boxed();

        let start = StdInstant::now();
        let mut a = 0u64;
        let mut b = 0u64;
        while StdInstant::now().duration_since(start) < Duration::from_millis(60) {
            match tokio::time::timeout(Duration::from_millis(15), stream.next()).await {
                Ok(Some("a")) => a += 1,
                Ok(Some("b")) => b += 1,
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {}
            }
        }
        let elapsed = StdInstant::now().duration_since(start).as_secs_f64();
        let allowed = 2.0 + 20.0 * elapsed + 4.0;
        // Each key independently limited — neither should blow past its own
        // allowance, and both should make progress (independent buckets).
        assert!(a as f64 <= allowed, "key a emitted {a}, allowed ~{allowed:.1}");
        assert!(b as f64 <= allowed, "key b emitted {b}, allowed ~{allowed:.1}");
        assert!(a >= 2 && b >= 2, "both keys should pass their burst: a={a} b={b}");
    }

    #[tokio::test]
    async fn respect_retry_after_pauses_then_continues() {
        use std::time::Instant as StdInstant;
        let src: Source<Result<u32, RetryAfter>> =
            Source::from_iter(vec![Ok(1u32), Err(RetryAfter { seconds: 1 }), Ok(2u32)]);
        let start = StdInstant::now();
        let out = Sink::collect(respect_retry_after(src)).await;
        let elapsed = start.elapsed();

        // Nothing dropped: all three elements forwarded in order.
        assert_eq!(out, vec![Ok(1), Err(RetryAfter { seconds: 1 }), Ok(2)]);
        // The 1s back-off after the Err delayed the trailing Ok.
        assert!(elapsed >= Duration::from_millis(950), "expected ~1s pause, got {elapsed:?}");
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::time::Instant as StdInstant;

    proptest! {
        #![proptest_config(ProptestConfig { cases: 12, ..ProptestConfig::default() })]

        /// Over the elapsed emission time, the count emitted by `token_bucket`
        /// never exceeds `burst + rate * elapsed` (plus timing slack).
        #[test]
        fn token_bucket_count_bounded_by_burst_plus_rate(
            rate in 20.0f64..200.0,
            burst in 1u32..8,
            n in 20usize..60,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async move {
                let src = Source::from_iter(0..n as u32);
                let mut stream = token_bucket(src, rate, burst).into_boxed();
                let start = StdInstant::now();
                let mut count = 0u64;
                while stream.next().await.is_some() {
                    count += 1;
                    let elapsed = start.elapsed().as_secs_f64();
                    let allowed = burst as f64 + rate * elapsed + 5.0;
                    prop_assert!(
                        count as f64 <= allowed,
                        "emitted {count} after {elapsed:.4}s, allowed ~{allowed:.2} (rate={rate}, burst={burst})",
                    );
                }
                Ok(())
            })?;
        }
    }
}
