//! OpenTelemetry *tracing* (span) exporter for `atomr-telemetry` (FR-11).
//!
//! The metrics-only [`OtelExporter`](super::otel::OtelExporter) covers
//! counters/gauges; this module adds the span model so a single trace can
//! follow a message across actor hops. It reuses the same
//! [`OtlpConfig`](super::config::OtlpConfig) as the metrics exporter and the
//! same OTLP/stdout transport selection.
//!
//! [`OtelTracerExporter`] owns an [`SdkTracerProvider`](opentelemetry_sdk::trace::TracerProvider)
//! plus a [`Tracer`] and emits a handful of well-known spans:
//!
//! - `actor.handle`   — one span per message handled by an actor
//! - `actor.restart`  — emitted when supervision restarts an actor
//! - `stream.element` — optional, sampled span per stream element
//!
//! [`TraceContextInterceptor`] bridges this exporter to the FR-10
//! [`MessageInterceptor`] hook: it opens an `actor.handle` span on the way in
//! (parented by the incoming [`Metadata`] trace context) and injects child
//! trace context on the way out so the same `trace_id` flows parent → child.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use atomr_core::actor::{MessageInterceptor, Metadata, SpanGuard};
use opentelemetry::trace::{
    SpanBuilder, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer,
    TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::{Config, Sampler, TracerProvider};
use opentelemetry_sdk::Resource;

use super::config::OtlpConfig;

/// Sampling / probe configuration for the span model.
#[derive(Debug, Clone, Copy)]
pub struct SpanProbeConfig {
    /// Head-based sampling ratio in `[0.0, 1.0]`. `1.0` records every trace,
    /// `0.0` records none. Applied via a `TraceIdRatioBased` sampler.
    pub sample_ratio: f64,
    /// When `true`, the per-element `stream.element` span is emitted (subject
    /// to `sample_ratio`). Off by default — element spans are high-cardinality.
    pub stream_elements: bool,
}

impl Default for SpanProbeConfig {
    fn default() -> Self {
        Self { sample_ratio: 1.0, stream_elements: false }
    }
}

/// Span (trace) exporter. Holds the SDK tracer provider alive — dropping it
/// flushes pending spans — and exposes typed helpers to record the atomr
/// span model.
pub struct OtelTracerExporter {
    provider: TracerProvider,
    tracer: opentelemetry_sdk::trace::Tracer,
}

impl OtelTracerExporter {
    /// Build a new tracer exporter. The node name is attached as the
    /// `service.instance.id` resource attribute, mirroring the metrics
    /// exporter.
    pub fn new(cfg: OtlpConfig) -> Result<Self, String> {
        Self::new_with_node(cfg, "atomr-node")
    }

    /// Convenience constructor that also sets the node label.
    pub fn new_with_node(cfg: OtlpConfig, node: impl Into<String>) -> Result<Self, String> {
        Self::with_probe(cfg, node, SpanProbeConfig::default())
    }

    /// Build with an explicit [`SpanProbeConfig`] (controls the sampler).
    pub fn with_probe(
        cfg: OtlpConfig,
        node: impl Into<String>,
        probe: SpanProbeConfig,
    ) -> Result<Self, String> {
        let node = node.into();
        let service_name = cfg.service_name.clone().unwrap_or_else(|| "atomr".to_string());

        let mut kvs =
            vec![KeyValue::new("service.name", service_name), KeyValue::new("service.instance.id", node)];
        for (k, v) in cfg.resource_attributes.iter() {
            kvs.push(KeyValue::new(k.clone(), v.clone()));
        }
        let resource = Resource::new(kvs);

        let sampler =
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(probe.sample_ratio.clamp(0.0, 1.0))));
        let trace_config = Config::default().with_sampler(sampler).with_resource(resource);

        let provider = build_provider(&cfg, trace_config)?;
        let tracer = provider.tracer("atomr-telemetry");
        Ok(Self { provider, tracer })
    }

    /// Record an `actor.handle` span. `mailbox_wait_ms` is the time the
    /// message waited in the mailbox; `handle_ms` is the time spent in
    /// `Actor::handle`. When `parent` carries a trace context the span is
    /// parented by it (so the same `trace_id` continues across the hop).
    pub fn record_handle_span(
        &self,
        actor_path: &str,
        msg_type: &str,
        mailbox_wait_ms: f64,
        handle_ms: f64,
        parent: Option<&Metadata>,
    ) {
        let attrs = vec![
            KeyValue::new("actor.path", actor_path.to_string()),
            KeyValue::new("message.type", msg_type.to_string()),
            KeyValue::new("mailbox.wait_ms", mailbox_wait_ms),
            KeyValue::new("handle.duration_ms", handle_ms),
        ];
        let now = SystemTime::now();
        let end = now + Duration::from_secs_f64((handle_ms.max(0.0)) / 1000.0);
        self.emit("actor.handle", SpanKind::Server, attrs, now, end, parent);
    }

    /// Record an `actor.restart` span — supervision restarted `actor_path`
    /// with the given `directive` after `error`.
    pub fn record_restart_span(&self, actor_path: &str, directive: &str, error: &str) {
        let attrs = vec![
            KeyValue::new("actor.path", actor_path.to_string()),
            KeyValue::new("supervision.directive", directive.to_string()),
            KeyValue::new("error.message", error.to_string()),
        ];
        let now = SystemTime::now();
        self.emit("actor.restart", SpanKind::Internal, attrs, now, now, None);
    }

    /// Record an optional, sampled `stream.element` span. `backpressure_ms`
    /// is the time the element spent blocked on downstream demand.
    pub fn record_stream_element_span(
        &self,
        stream: &str,
        stage: &str,
        backpressure_ms: f64,
        parent: Option<&Metadata>,
    ) {
        let attrs = vec![
            KeyValue::new("stream.name", stream.to_string()),
            KeyValue::new("stream.stage", stage.to_string()),
            KeyValue::new("stream.backpressure_ms", backpressure_ms),
        ];
        let now = SystemTime::now();
        let end = now + Duration::from_secs_f64((backpressure_ms.max(0.0)) / 1000.0);
        self.emit("stream.element", SpanKind::Internal, attrs, now, end, parent);
    }

    /// Common span-emission path. Builds a span, optionally parents it by an
    /// incoming `Metadata` trace context, then immediately ends it (these are
    /// "completed work" spans, recorded after the fact).
    fn emit(
        &self,
        name: &'static str,
        kind: SpanKind,
        attrs: Vec<KeyValue>,
        start: SystemTime,
        end: SystemTime,
        parent: Option<&Metadata>,
    ) {
        let builder = SpanBuilder::from_name(name)
            .with_kind(kind)
            .with_attributes(attrs)
            .with_start_time(start)
            .with_end_time(end);

        match parent.and_then(metadata_to_span_context) {
            Some(parent_ctx) => {
                let cx = Context::current().with_remote_span_context(parent_ctx);
                let _span = self.tracer.build_with_context(builder, &cx);
            }
            None => {
                let _span = self.tracer.build(builder);
            }
        }
        // `_span` is dropped here, ending it and queueing it for export.
    }

    /// The underlying tracer — used by [`TraceContextInterceptor`] to open a
    /// live (held-open) span for the duration of `handle`.
    pub(crate) fn tracer(&self) -> &opentelemetry_sdk::trace::Tracer {
        &self.tracer
    }

    /// Flush pending spans. Mirrors `OtelExporter::flush`.
    pub fn flush(&self) {
        let _ = self.provider.force_flush();
    }

    /// Flush and shut down the provider.
    pub fn shutdown(&self) {
        let _ = self.provider.force_flush();
        let _ = self.provider.shutdown();
    }
}

/// Convert an atomr [`Metadata`] trace context to an OTel [`SpanContext`] so a
/// new span can be parented by it. Returns `None` when ids are absent or
/// unparseable. The context is marked `is_remote = true` and `SAMPLED`.
fn metadata_to_span_context(meta: &Metadata) -> Option<SpanContext> {
    let trace_id = TraceId::from_hex(meta.trace_id()?).ok()?;
    let span_id = SpanId::from_hex(meta.span_id()?).ok()?;
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }
    Some(SpanContext::new(trace_id, span_id, TraceFlags::SAMPLED, true, TraceState::NONE))
}

/// A [`MessageInterceptor`] that ties the FR-10 envelope metadata to FR-11
/// spans. On the way in it opens an `actor.handle` span parented by the
/// incoming trace context; on the way out it injects child context so the
/// same `trace_id` flows to messages sent while handling.
pub struct TraceContextInterceptor {
    tracer: Arc<OtelTracerExporter>,
    #[allow(dead_code)]
    probe: SpanProbeConfig,
}

impl TraceContextInterceptor {
    pub fn new(tracer: Arc<OtelTracerExporter>, probe: SpanProbeConfig) -> Self {
        Self { tracer, probe }
    }
}

impl MessageInterceptor for TraceContextInterceptor {
    fn before_handle(&self, meta: &Metadata) -> SpanGuard {
        let builder = SpanBuilder::from_name("actor.handle").with_kind(SpanKind::Server);
        let span = match metadata_to_span_context(meta) {
            Some(parent_ctx) => {
                let cx = Context::current().with_remote_span_context(parent_ctx);
                self.tracer.tracer().build_with_context(builder, &cx)
            }
            None => self.tracer.tracer().build(builder),
        };
        // Hold the span open for the duration of `handle`; dropping the guard
        // ends the span.
        SpanGuard::holding(span)
    }

    fn outgoing(&self, parent: &Metadata) -> Metadata {
        // Propagate the parent trace_id (or mint a fresh one if absent) and
        // assign a brand-new span_id for the child hop, so parent → child
        // share a trace_id but get distinct span_ids.
        let trace_id = match parent.trace_id().and_then(|t| TraceId::from_hex(t).ok()) {
            Some(t) if t != TraceId::INVALID => t,
            _ => random_trace_id(),
        };
        let span_id = random_span_id();

        let mut child = parent.clone();
        child.set_trace_id(format!("{trace_id:032x}"));
        child.set_span_id(format!("{span_id:016x}"));
        child
    }
}

/// Mint a random non-zero trace id without pulling in an RNG dependency — we
/// seed off the high-resolution clock plus this crate's address space. Trace
/// ids only need to be unique, not cryptographically random.
fn random_trace_id() -> TraceId {
    let hi = nanos_seed();
    let lo = nanos_seed().rotate_left(31) ^ 0x9E37_79B9_7F4A_7C15;
    let v = ((hi as u128) << 64) | (lo as u128 | 1);
    TraceId::from_bytes(v.to_be_bytes())
}

fn random_span_id() -> SpanId {
    let v = nanos_seed() | 1;
    SpanId::from_bytes(v.to_be_bytes())
}

fn nanos_seed() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        .wrapping_mul(0x2545_F491_4F6C_DD1D)
        ^ (&random_span_id as *const _ as u64)
}

// ---- transport / provider construction (mirrors otel.rs) -------------------

#[cfg(feature = "otel-stdout")]
fn build_provider(cfg: &OtlpConfig, trace_config: Config) -> Result<TracerProvider, String> {
    if cfg.stdout || !otlp_transport_enabled() {
        let exporter = opentelemetry_stdout::SpanExporter::default();
        return Ok(TracerProvider::builder()
            .with_simple_exporter(exporter)
            .with_config(trace_config)
            .build());
    }
    build_otlp_provider(cfg, trace_config)
}

#[cfg(not(feature = "otel-stdout"))]
fn build_provider(cfg: &OtlpConfig, trace_config: Config) -> Result<TracerProvider, String> {
    if cfg.stdout {
        return Err("stdout OTel tracer requested but `otel-stdout` feature is not enabled".to_string());
    }
    build_otlp_provider(cfg, trace_config)
}

#[cfg(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http"))]
fn build_otlp_provider(cfg: &OtlpConfig, trace_config: Config) -> Result<TracerProvider, String> {
    use opentelemetry_otlp::WithExportConfig;

    match cfg.protocol.as_str() {
        #[cfg(feature = "otel-otlp-grpc")]
        "grpc" => opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(cfg.endpoint.clone()))
            .with_trace_config(trace_config)
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .map_err(|e| format!("otlp grpc tracer init: {e}")),
        #[cfg(feature = "otel-otlp-http")]
        "http" => opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(opentelemetry_otlp::new_exporter().http().with_endpoint(cfg.endpoint.clone()))
            .with_trace_config(trace_config)
            .install_batch(opentelemetry_sdk::runtime::Tokio)
            .map_err(|e| format!("otlp http tracer init: {e}")),
        other => Err(format!(
            "unsupported or un-enabled OTLP protocol `{other}`; enable the matching cargo feature"
        )),
    }
}

#[cfg(not(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http")))]
fn build_otlp_provider(_cfg: &OtlpConfig, _trace_config: Config) -> Result<TracerProvider, String> {
    Err("no OTLP transport compiled in; enable `otel-otlp-grpc` or `otel-otlp-http`".to_string())
}

#[allow(dead_code)]
fn otlp_transport_enabled() -> bool {
    cfg!(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config that builds an in-process (stdout) tracer with no network.
    fn test_cfg() -> OtlpConfig {
        OtlpConfig {
            enabled: true,
            endpoint: "http://localhost:4317".into(),
            protocol: "grpc".into(),
            service_name: Some("atomr-test".into()),
            interval_secs: 1,
            headers: Default::default(),
            resource_attributes: Default::default(),
            traces: true,
            stdout: true,
        }
    }

    #[cfg(feature = "otel-stdout")]
    fn build_tracer() -> Arc<OtelTracerExporter> {
        Arc::new(OtelTracerExporter::new_with_node(test_cfg(), "node-a").expect("build tracer"))
    }

    #[cfg(feature = "otel-stdout")]
    #[test]
    fn constructs_with_stdout_config() {
        let _t = build_tracer();
    }

    #[cfg(feature = "otel-stdout")]
    #[test]
    fn record_spans_do_not_panic() {
        let t = build_tracer();
        let parent = Metadata::with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        t.record_handle_span("/user/worker", "Ping", 1.5, 4.2, Some(&parent));
        t.record_handle_span("/user/worker", "Ping", 0.0, 0.0, None);
        t.record_restart_span("/user/worker", "restart", "boom");
        t.record_stream_element_span("ingest", "map", 0.3, Some(&parent));
        t.flush();
    }

    #[cfg(feature = "otel-stdout")]
    #[test]
    fn before_handle_returns_guard_empty_and_populated() {
        let t = build_tracer();
        let interceptor = TraceContextInterceptor::new(t, SpanProbeConfig::default());

        // Empty metadata: must not panic, returns a guard (root span).
        let empty = Metadata::new();
        let _g1 = interceptor.before_handle(&empty);

        // Populated metadata: parented span, must not panic.
        let populated = Metadata::with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        let _g2 = interceptor.before_handle(&populated);
    }

    #[cfg(feature = "otel-stdout")]
    #[test]
    fn outgoing_propagates_trace_id_and_mints_fresh_span_id() {
        let t = build_tracer();
        let interceptor = TraceContextInterceptor::new(t, SpanProbeConfig::default());

        let parent = Metadata::with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        let child = interceptor.outgoing(&parent);

        // Same trace id flows parent -> child...
        assert_eq!(child.trace_id(), parent.trace_id(), "trace_id must propagate");
        // ...but the span id is freshly assigned (distinct from parent's).
        assert!(child.span_id().is_some(), "child must have a span id");
        assert_ne!(child.span_id(), parent.span_id(), "span_id must be fresh");
    }

    #[cfg(feature = "otel-stdout")]
    #[test]
    fn outgoing_mints_trace_when_parent_empty() {
        let t = build_tracer();
        let interceptor = TraceContextInterceptor::new(t, SpanProbeConfig::default());

        let parent = Metadata::new();
        let child = interceptor.outgoing(&parent);

        assert!(child.trace_id().is_some(), "a trace id should be minted");
        assert!(child.span_id().is_some(), "a span id should be minted");
    }

    #[test]
    fn span_probe_config_defaults() {
        let p = SpanProbeConfig::default();
        assert_eq!(p.sample_ratio, 1.0);
        assert!(!p.stream_elements);
    }

    #[test]
    fn metadata_to_span_context_rejects_bad_ids() {
        // No ids -> None.
        assert!(metadata_to_span_context(&Metadata::new()).is_none());
        // Non-hex -> None.
        let bad = Metadata::with_trace("not-hex", "also-not-hex");
        assert!(metadata_to_span_context(&bad).is_none());
        // Good -> Some.
        let good = Metadata::with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        assert!(metadata_to_span_context(&good).is_some());
    }
}
