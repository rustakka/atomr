//! Pluggable exporters for external observability backends.
//!
//! The [`Exporter`] trait is synchronous so probes can call exporters
//! from hot paths without `.await`ing. Exporters that need to batch or
//! send over the network can buffer internally and flush on an interval
//! (see [`otel`]) or on scrape (see [`prometheus`]).

use crate::bus::TelemetryEvent;

pub mod config;

#[cfg(feature = "prometheus")]
pub mod prometheus;

#[cfg(feature = "otel")]
pub mod otel;

#[cfg(feature = "otel")]
pub mod otel_tracer;

/// Synchronous exporter callback surface. Implementers are stored as
/// `Arc<dyn Exporter>` on the `TelemetryBus`.
pub trait Exporter: Send + Sync + 'static {
    fn on_event(&self, event: &TelemetryEvent);

    /// Called by the dashboard on shutdown. Implementations can flush
    /// buffered metrics/spans here.
    fn shutdown(&self) {}
}
