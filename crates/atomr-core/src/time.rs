//! Logical-clock abstraction shared across the substrate.
//!
//! Time-based behaviour in atomr (stream throttling, replay gating, rate
//! limiting) historically reached for `tokio::time` directly, which couples
//! correctness to the wall clock. That is fatal for deterministic replay and
//! point-in-time backtests, where consumers must not observe data "ahead" of a
//! simulation watermark regardless of async latency.
//!
//! This module introduces a pluggable [`Clock`] so production code runs on
//! [`SystemClock`] while replay / backtest harnesses drive a [`ManualClock`]
//! they advance explicitly. It is the foundation for the clock-gated stream
//! source (FR-2), the token-bucket rate limiter (FR-3), and record-and-replay
//! determinism (FR-13).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Monotonic logical time, measured in nanoseconds since an arbitrary epoch.
///
/// For [`SystemClock`] the epoch is the Unix epoch; for [`ManualClock`] it is
/// whatever the harness defines. Only ordering and differences are meaningful
/// across clocks of the same kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogicalTime(pub u64);

impl LogicalTime {
    /// The zero instant.
    pub const ZERO: LogicalTime = LogicalTime(0);

    /// Construct from a raw nanosecond count.
    pub const fn from_nanos(nanos: u64) -> Self {
        LogicalTime(nanos)
    }

    /// Construct from a millisecond count (saturating).
    pub const fn from_millis(millis: u64) -> Self {
        LogicalTime(millis.saturating_mul(1_000_000))
    }

    /// Raw nanosecond count.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

/// A source of [`LogicalTime`].
///
/// Implementations must be cheap to call and thread-safe; emission/gating logic
/// may poll `now()` frequently.
pub trait Clock: Send + Sync {
    /// The current logical instant.
    fn now(&self) -> LogicalTime;
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> LogicalTime {
        (**self).now()
    }
}

/// Wall-clock [`Clock`] backed by `SystemTime`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> LogicalTime {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        LogicalTime(nanos)
    }
}

/// An explicitly-advanced logical clock for deterministic replay and backtests.
///
/// The watermark only ever moves forward: [`advance_to`](Self::advance_to)
/// ignores regressions, guaranteeing monotonicity even under concurrent
/// advances. Clones share the same underlying watermark, so a harness can hold
/// one handle while a stream operator holds another.
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    watermark: Arc<AtomicU64>,
}

impl ManualClock {
    /// A new clock at [`LogicalTime::ZERO`].
    pub fn new() -> Self {
        Self { watermark: Arc::new(AtomicU64::new(0)) }
    }

    /// A new clock starting at `start`.
    pub fn at(start: LogicalTime) -> Self {
        Self { watermark: Arc::new(AtomicU64::new(start.0)) }
    }

    /// Advance the watermark to `target`. Monotonic — a `target` at or below the
    /// current watermark is a no-op.
    pub fn advance_to(&self, target: LogicalTime) {
        self.watermark.fetch_max(target.0, Ordering::SeqCst);
    }

    /// Advance the watermark by `delta`.
    pub fn advance_by(&self, delta: LogicalTime) {
        self.watermark.fetch_add(delta.0, Ordering::SeqCst);
    }

    /// The current watermark.
    pub fn watermark(&self) -> LogicalTime {
        LogicalTime(self.watermark.load(Ordering::SeqCst))
    }
}

impl Clock for ManualClock {
    fn now(&self) -> LogicalTime {
        self.watermark()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_is_monotonic() {
        let c = ManualClock::new();
        assert_eq!(c.watermark(), LogicalTime::ZERO);
        c.advance_to(LogicalTime::from_millis(100));
        assert_eq!(c.now(), LogicalTime::from_millis(100));
        // Regression is ignored.
        c.advance_to(LogicalTime::from_millis(50));
        assert_eq!(c.now(), LogicalTime::from_millis(100));
        c.advance_by(LogicalTime::from_millis(25));
        assert_eq!(c.now(), LogicalTime::from_millis(125));
    }

    #[test]
    fn clones_share_watermark() {
        let a = ManualClock::new();
        let b = a.clone();
        a.advance_to(LogicalTime::from_nanos(42));
        assert_eq!(b.now(), LogicalTime::from_nanos(42));
    }

    #[test]
    fn system_clock_advances() {
        let c = SystemClock;
        let t1 = c.now();
        assert!(t1.as_nanos() > 0);
    }

    #[test]
    fn arc_dyn_clock_dispatches() {
        let c: Arc<dyn Clock> = Arc::new(ManualClock::at(LogicalTime::from_nanos(7)));
        assert_eq!(c.now(), LogicalTime::from_nanos(7));
    }
}
