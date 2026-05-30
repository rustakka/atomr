//! Per-message [`Metadata`] — trace context + baggage that propagates across
//! actor hops without polluting domain message types (FR-10).
//!
//! End-to-end tracing needs causal context (W3C TraceContext, plus arbitrary
//! baggage) to ride along every `tell`/`ask`, through mailboxes, and across
//! stream / `JoinSet` boundaries. Wrapping every domain message in a
//! `Traced<M>` by hand is invasive and easy to drop at a boundary. Instead the
//! envelope carries an extensible [`Metadata`] map that the runtime threads
//! automatically, and a [`MessageInterceptor`] (installed via
//! [`Props::with_interceptor`](super::Props::with_interceptor)) opens a span on
//! the way in and injects child context on the way out.

use std::any::Any;
use std::collections::BTreeMap;

/// Extensible message metadata: W3C-style trace context plus a small string
/// baggage map. Empty by default and cheap to clone (the baggage map does not
/// allocate until the first insert).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    trace_id: Option<String>,
    span_id: Option<String>,
    baggage: BTreeMap<String, String>,
}

impl Metadata {
    /// An empty metadata map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a trace + span id.
    pub fn with_trace(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        Self { trace_id: Some(trace_id.into()), span_id: Some(span_id.into()), baggage: BTreeMap::new() }
    }

    /// The W3C trace id, if set.
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    /// The current span id, if set.
    pub fn span_id(&self) -> Option<&str> {
        self.span_id.as_deref()
    }

    /// Set the trace id.
    pub fn set_trace_id(&mut self, trace_id: impl Into<String>) {
        self.trace_id = Some(trace_id.into());
    }

    /// Set the span id.
    pub fn set_span_id(&mut self, span_id: impl Into<String>) {
        self.span_id = Some(span_id.into());
    }

    /// Insert a baggage key/value.
    pub fn set_baggage(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.baggage.insert(key.into(), value.into());
    }

    /// Read a baggage value.
    pub fn baggage(&self, key: &str) -> Option<&str> {
        self.baggage.get(key).map(String::as_str)
    }

    /// Iterate baggage entries (e.g. to serialize on a remote hop).
    pub fn baggage_iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.baggage.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// True if no trace context and no baggage are present.
    pub fn is_empty(&self) -> bool {
        self.trace_id.is_none() && self.span_id.is_none() && self.baggage.is_empty()
    }
}

/// RAII guard returned by [`MessageInterceptor::before_handle`]. Held for the
/// duration of `handle` and dropped afterward, so an implementation can keep a
/// `tracing::span::Entered` (or any other scope guard) alive across the call.
#[must_use = "the span guard must be held for the duration of message handling"]
pub struct SpanGuard(#[allow(dead_code)] Option<Box<dyn Any + Send>>);

impl SpanGuard {
    /// A no-op guard.
    pub fn none() -> Self {
        SpanGuard(None)
    }

    /// Wrap an arbitrary scope guard so it lives for the handle duration.
    pub fn holding(guard: impl Any + Send) -> Self {
        SpanGuard(Some(Box::new(guard)))
    }
}

impl Default for SpanGuard {
    fn default() -> Self {
        SpanGuard::none()
    }
}

/// A Props-level hook that observes message handling for cross-cutting concerns
/// such as distributed tracing. Default methods are no-ops, so an interceptor
/// only overrides what it needs.
///
/// The default [`TraceContextInterceptor`]-style behaviour (linking parent →
/// child spans) is provided by `atomr-telemetry` (FR-11); core only defines the
/// hook so domain message types stay clean.
pub trait MessageInterceptor: Send + Sync {
    /// Called immediately before [`Actor::handle`](super::Actor::handle) with
    /// the incoming message's metadata. The returned [`SpanGuard`] is dropped
    /// once handling completes.
    fn before_handle(&self, meta: &Metadata) -> SpanGuard {
        let _ = meta;
        SpanGuard::none()
    }

    /// Derive the metadata to attach to messages sent *while* handling the
    /// current message. Defaults to propagating the parent context unchanged.
    fn outgoing(&self, parent: &Metadata) -> Metadata {
        parent.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_by_default() {
        let m = Metadata::new();
        assert!(m.is_empty());
        assert_eq!(m.trace_id(), None);
    }

    #[test]
    fn carries_trace_and_baggage() {
        let mut m = Metadata::with_trace("trace-1", "span-1");
        m.set_baggage("tenant", "acme");
        assert_eq!(m.trace_id(), Some("trace-1"));
        assert_eq!(m.span_id(), Some("span-1"));
        assert_eq!(m.baggage("tenant"), Some("acme"));
        assert!(!m.is_empty());
    }

    struct Noop;
    impl MessageInterceptor for Noop {}

    #[test]
    fn default_interceptor_propagates() {
        let i = Noop;
        let mut m = Metadata::with_trace("t", "s");
        m.set_baggage("k", "v");
        let child = i.outgoing(&m);
        assert_eq!(child, m);
        let _guard = i.before_handle(&m);
    }
}
