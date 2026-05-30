//! Clock-gated emission (FR-2).
//!
//! A clock-gated [`Source`] couples element emission to a logical [`Clock`]
//! watermark rather than to wall-clock latency: an element `e` is released to
//! downstream only once `clock.now() >= event_time(e)`. This is the foundation
//! for deterministic replay and point-in-time backtests, where a consumer must
//! never observe data "ahead" of the simulation watermark — regardless of how
//! fast or slow the downstream async pipeline happens to run.
//!
//! Two flavours are provided:
//!
//! * [`clock_gated`] — gate against any [`Clock`]. With a
//!   [`SystemClock`](atomr_core::time::SystemClock) it behaves like a real-time
//!   release valve; with a [`ManualClock`] the harness advances the watermark
//!   explicitly to release elements step by step.
//! * [`step_locked`] — a stricter, fully back-pressured variant: the source
//!   hands out an [`InstantToken`] with every element and refuses to pull the
//!   next upstream element until the consumer has acked the previous one via the
//!   returned [`AckSink`]. This lets a replay harness keep the [`ManualClock`]
//!   and the stream in lock-step.

use std::sync::Arc;
use std::time::Duration;

use atomr_core::time::{Clock, LogicalTime, ManualClock};
use futures::stream::{BoxStream, StreamExt};
use tokio::sync::mpsc;

use crate::source::Source;

/// Poll interval used while waiting for the watermark to reach a peeked
/// element's event time. Small enough to feel responsive, large enough to avoid
/// a hot spin loop.
const GATE_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Gate `src` against `clock`: emit element `e` only once
/// `clock.now() >= event_time(e)`.
///
/// `event_time` **must be monotonic non-decreasing** over the stream — i.e. for
/// elements emitted in order `e0, e1, …`, `event_time(e0) <= event_time(e1) <= …`.
/// The gate peeks exactly one element ahead and holds it until the watermark
/// catches up; because event times never regress, holding the head element also
/// holds every element behind it.
///
/// # Key guarantee
///
/// No element whose `event_time` exceeds the current watermark is *ever* emitted,
/// no matter how slow the downstream consumer is. The gate re-checks the
/// watermark only at the moment it is about to release an element, so a slow
/// consumer can only delay emission further — never let an element slip out
/// early. While waiting it sleeps for a short fixed `GATE_POLL_INTERVAL` between checks
/// rather than busy-spinning.
pub fn clock_gated<T, F>(src: Source<T>, clock: Arc<dyn Clock>, event_time: F) -> Source<T>
where
    T: Send + 'static,
    F: Fn(&T) -> LogicalTime + Send + 'static,
{
    struct State<T> {
        inner: BoxStream<'static, T>,
        peeked: Option<T>,
        clock: Arc<dyn Clock>,
        event_time: Box<dyn Fn(&T) -> LogicalTime + Send + 'static>,
        started: bool,
    }

    let state = State {
        inner: src.into_boxed(),
        peeked: None,
        clock,
        event_time: Box::new(event_time),
        started: false,
    };

    Source::unfold(state, |mut st| async move {
        // Prime the pipeline with the first element on the first poll.
        if !st.started {
            st.started = true;
            st.peeked = st.inner.next().await;
        }

        loop {
            match st.peeked.take() {
                None => return None, // upstream exhausted
                Some(item) => {
                    let due = (st.event_time)(&item);
                    if st.clock.now() >= due {
                        // Release this element and peek the next one so the
                        // following poll can gate against it.
                        st.peeked = st.inner.next().await;
                        return Some((item, st));
                    } else {
                        // Not yet due: put the element back and wait. We must
                        // re-check at release time, so the watermark can never
                        // be raced past by a slow downstream.
                        st.peeked = Some(item);
                        tokio::time::sleep(GATE_POLL_INTERVAL).await;
                    }
                }
            }
        }
    })
}

/// Opaque, monotonically increasing token identifying an emitted instant from
/// [`step_locked`]. Consumers ack an element by sending its token back through
/// the [`AckSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstantToken(pub u64);

/// The ack side of a [`step_locked`] source. A consumer must call [`ack`] (or
/// drop the sink, which releases the source) after handling each emitted
/// element; the source will not pull element `N + 1` from upstream until the
/// ack for element `N` has been received.
///
/// [`ack`]: AckSink::ack
#[derive(Debug, Clone)]
pub struct AckSink {
    tx: mpsc::UnboundedSender<InstantToken>,
}

impl AckSink {
    /// Acknowledge a previously emitted [`InstantToken`], permitting the source
    /// to advance to the next element. Returns `false` if the source half has
    /// already been dropped.
    pub fn ack(&self, token: InstantToken) -> bool {
        self.tx.send(token).is_ok()
    }
}

/// A fully back-pressured, lock-stepped source.
///
/// Each upstream element `e` is emitted paired with an [`InstantToken`]. The
/// source then *blocks* — it will not pull the next upstream element — until the
/// consumer acks that token through the returned [`AckSink`]. This guarantees
/// the consumer and a driving [`ManualClock`] never drift: the harness can
/// advance the clock, observe exactly one element, ack it, advance again, and so
/// on.
///
/// The `clock` handle is retained so callers can share a single watermark
/// between the harness and the stream; the source itself does not gate on it
/// (that is [`clock_gated`]'s job) but holding it here keeps the two APIs
/// symmetric and lets a caller advance the same clone it passed in.
pub fn step_locked<T>(src: Source<T>, clock: Arc<ManualClock>) -> (Source<(T, InstantToken)>, AckSink)
where
    T: Send + 'static,
{
    let (ack_tx, ack_rx) = mpsc::unbounded_channel::<InstantToken>();
    let ack_sink = AckSink { tx: ack_tx };

    struct State<T> {
        inner: BoxStream<'static, T>,
        ack_rx: mpsc::UnboundedReceiver<InstantToken>,
        next_token: u64,
        // Token of the element we are still awaiting an ack for, if any.
        pending: Option<InstantToken>,
        // Retained to keep the shared watermark alive for the caller.
        _clock: Arc<ManualClock>,
    }

    let state = State { inner: src.into_boxed(), ack_rx, next_token: 0, pending: None, _clock: clock };

    let source = Source::unfold(state, |mut st| async move {
        // Wait for the ack of the previously emitted element before pulling the
        // next one. If the ack channel is closed (consumer gone) we stop.
        if st.pending.is_some() {
            match st.ack_rx.recv().await {
                Some(_token) => {
                    st.pending = None;
                }
                None => return None,
            }
        }

        match st.inner.next().await {
            None => None,
            Some(item) => {
                let token = InstantToken(st.next_token);
                st.next_token += 1;
                st.pending = Some(token);
                Some(((item, token), st))
            }
        }
    });

    (source, ack_sink)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use atomr_core::time::ManualClock;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn et_ms(t: &(u64, u64)) -> LogicalTime {
        LogicalTime::from_millis(t.1)
    }

    #[tokio::test]
    async fn clock_gated_never_emits_ahead_of_watermark_even_with_slow_consumer() {
        // Elements (id, event_time_ms), monotonic non-decreasing event time.
        let elems = vec![(0u64, 0u64), (1, 10), (2, 20), (3, 30), (4, 40)];
        let clock = ManualClock::new();
        let clock_dyn: Arc<dyn Clock> = Arc::new(clock.clone());

        let observed = Arc::new(parking_lot::Mutex::new(Vec::<(u64, u64)>::new()));
        // Largest watermark violation seen at emission (must stay 0).
        let violations = Arc::new(AtomicU64::new(0));

        let clock_obs = clock.clone();
        let observed_c = observed.clone();
        let violations_c = violations.clone();

        let gated = clock_gated(Source::from_iter(elems.clone()), clock_dyn, et_ms);

        // Drive in a background task with an artificially SLOW downstream
        // (sleep inside map_async) so emission timing is decoupled from pulls.
        let handle = tokio::spawn(async move {
            let slow = gated.map_async(1, move |item| {
                let clock = clock_obs.clone();
                let observed = observed_c.clone();
                let violations = violations_c.clone();
                async move {
                    // Observe the watermark AT the moment the element reaches
                    // the consumer.
                    let wm = clock.watermark();
                    let due = LogicalTime::from_millis(item.1);
                    if due > wm {
                        violations.fetch_max(due.as_nanos() - wm.as_nanos(), Ordering::SeqCst);
                    }
                    observed.lock().push(item);
                    // Slow consumer.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    item
                }
            });
            Sink::collect(slow).await
        });

        // Initially watermark is 0: only element (0,0) may be released.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(observed.lock().clone(), vec![(0, 0)], "only the t=0 element should be out");

        // Advance to 20ms: releases (1,10) and (2,20).
        clock.advance_to(LogicalTime::from_millis(20));
        tokio::time::sleep(Duration::from_millis(40)).await;
        {
            let got = observed.lock().clone();
            assert_eq!(got, vec![(0, 0), (1, 10), (2, 20)], "watermark=20 releases through t=20");
        }

        // Advance fully: releases the rest.
        clock.advance_to(LogicalTime::from_millis(100));
        let out = handle.await.unwrap();
        assert_eq!(out, elems);

        // The invariant: nothing was ever observed ahead of the watermark.
        assert_eq!(violations.load(Ordering::SeqCst), 0, "an element was emitted ahead of the watermark");
    }

    #[tokio::test]
    async fn clock_gated_empty_source_completes() {
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new());
        let gated = clock_gated(Source::<(u64, u64)>::empty(), clock, et_ms);
        let out = Sink::collect(gated).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn step_locked_blocks_until_acked() {
        let clock = Arc::new(ManualClock::new());
        let (src, ack) = step_locked(Source::from_iter(vec![10u32, 20, 30]), clock);
        let mut stream = src.into_boxed();

        // First element is immediately available (no prior ack required).
        let (v0, t0) = stream.next().await.unwrap();
        assert_eq!(v0, 10);
        assert_eq!(t0, InstantToken(0));

        // Without acking, the next pull must not resolve. Race it against a
        // timeout — the timeout should win.
        let pending = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(pending.is_err(), "source advanced before ack");

        // Ack t0 -> next element flows.
        assert!(ack.ack(t0));
        let (v1, t1) = stream.next().await.unwrap();
        assert_eq!(v1, 20);
        assert_eq!(t1, InstantToken(1));

        assert!(ack.ack(t1));
        let (v2, t2) = stream.next().await.unwrap();
        assert_eq!(v2, 30);
        assert_eq!(t2, InstantToken(2));

        assert!(ack.ack(t2));
        assert!(stream.next().await.is_none());
    }
}
