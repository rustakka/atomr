//! OpenTelemetry exporter for `atomr-telemetry`.
//!
//! Maps incoming [`TelemetryEvent`]s to OTel instruments. Transport is
//! selected at compile time via sub-features on the `otel` feature:
//!
//! - `otel-otlp-grpc` — OTLP over gRPC (tonic)
//! - `otel-otlp-http` — OTLP over HTTP/Protobuf (reqwest)
//! - `otel-stdout` — pretty-print to stdout for dev/tests
//!
//! The `config::OtlpConfig::stdout` flag forces stdout even when an OTLP
//! sub-feature is compiled in — useful for integration tests that don't
//! want to actually hit a collector.

use std::sync::Arc;
#[cfg(any(feature = "otel-stdout", feature = "otel-otlp-grpc", feature = "otel-otlp-http"))]
use std::time::Duration;

use opentelemetry::metrics::{Counter, Meter, UpDownCounter};
use opentelemetry::KeyValue;
#[cfg(feature = "otel-stdout")]
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use parking_lot::Mutex;

use super::config::OtlpConfig;
use super::Exporter;
use crate::bus::TelemetryEvent;

/// Handle returned after installing the OTel exporter. Keeps the
/// `SdkMeterProvider` alive (drops flush) and exposes meter-backed
/// counters.
pub struct OtelExporter {
    provider: Arc<SdkMeterProvider>,
    #[allow(dead_code)]
    meter: Meter,
    instruments: Instruments,
    node: String,
}

struct Instruments {
    actors_spawned: Counter<u64>,
    actors_stopped: Counter<u64>,
    actors_live: UpDownCounter<i64>,
    dead_letters: Counter<u64>,
    cluster_events: Counter<u64>,
    sharding_events: Counter<u64>,
    persistence_writes: Counter<u64>,
    remote_events: Counter<u64>,
    streams_started: Counter<u64>,
    streams_finished: Counter<u64>,
    streams_running: UpDownCounter<i64>,
    ddata_updates: Counter<u64>,
}

impl OtelExporter {
    /// Build a new exporter. The node name is attached as the
    /// `service.instance.id` resource attribute.
    pub fn new(cfg: OtlpConfig) -> Result<Self, String> {
        Self::new_with_node(cfg, "atomr-node")
    }

    /// Convenience constructor that also sets the node label.
    pub fn new_with_node(cfg: OtlpConfig, node: impl Into<String>) -> Result<Self, String> {
        let node = node.into();
        let service_name = cfg.service_name.clone().unwrap_or_else(|| "atomr".to_string());

        let mut kvs = vec![
            KeyValue::new("service.name", service_name.clone()),
            KeyValue::new("service.instance.id", node.clone()),
        ];
        for (k, v) in cfg.resource_attributes.iter() {
            kvs.push(KeyValue::new(k.clone(), v.clone()));
        }
        let resource = Resource::new(kvs);

        let provider = build_provider(&cfg, resource)?;
        let meter = opentelemetry::global::meter_with_version(
            "atomr-telemetry",
            Some(env!("CARGO_PKG_VERSION")),
            None::<&str>,
            None,
        );
        let _ = opentelemetry::global::set_meter_provider(provider.clone());

        let instruments = Instruments {
            actors_spawned: meter
                .u64_counter("atomr.actors.spawned")
                .with_description("Actors spawned")
                .init(),
            actors_stopped: meter
                .u64_counter("atomr.actors.stopped")
                .with_description("Actors stopped")
                .init(),
            actors_live: meter
                .i64_up_down_counter("atomr.actors.live")
                .with_description("Live actor count")
                .init(),
            dead_letters: meter
                .u64_counter("atomr.dead_letters")
                .with_description("Dead lettered messages")
                .init(),
            cluster_events: meter
                .u64_counter("atomr.cluster.member_events")
                .with_description("Cluster membership events")
                .init(),
            sharding_events: meter
                .u64_counter("atomr.sharding.events")
                .with_description("Sharding events")
                .init(),
            persistence_writes: meter
                .u64_counter("atomr.persistence.events_written")
                .with_description("Journal writes")
                .init(),
            remote_events: meter
                .u64_counter("atomr.remote.association_events")
                .with_description("Remote association state transitions")
                .init(),
            streams_started: meter
                .u64_counter("atomr.streams.started")
                .with_description("Stream graphs started")
                .init(),
            streams_finished: meter
                .u64_counter("atomr.streams.finished")
                .with_description("Stream graphs finished")
                .init(),
            streams_running: meter
                .i64_up_down_counter("atomr.streams.running")
                .with_description("Currently-running stream graphs")
                .init(),
            ddata_updates: meter
                .u64_counter("atomr.ddata.updates")
                .with_description("Distributed-data updates")
                .init(),
        };

        Ok(Self { provider: Arc::new(provider), meter, instruments, node })
    }

    fn node_attr(&self) -> KeyValue {
        KeyValue::new("node", self.node.clone())
    }

    /// Flush pending metrics. Returns Ok(()) if the provider is healthy.
    pub fn flush(&self) {
        let _ = self.provider.force_flush();
    }
}

impl Exporter for OtelExporter {
    fn on_event(&self, event: &TelemetryEvent) {
        let node = self.node_attr();
        match event {
            TelemetryEvent::ActorSpawned(_) => {
                self.instruments.actors_spawned.add(1, &[node.clone()]);
                self.instruments.actors_live.add(1, &[node]);
            }
            TelemetryEvent::ActorStopped { .. } => {
                self.instruments.actors_stopped.add(1, &[node.clone()]);
                self.instruments.actors_live.add(-1, &[node]);
            }
            TelemetryEvent::MailboxSampled { .. } => {}
            TelemetryEvent::DeadLetter(_) => {
                self.instruments.dead_letters.add(1, &[node]);
            }
            TelemetryEvent::ClusterChanged(diff) => {
                self.instruments
                    .cluster_events
                    .add(diff.added.len() as u64, &[node.clone(), KeyValue::new("kind", "added")]);
                self.instruments
                    .cluster_events
                    .add(diff.updated.len() as u64, &[node.clone(), KeyValue::new("kind", "updated")]);
                self.instruments
                    .cluster_events
                    .add(diff.removed.len() as u64, &[node.clone(), KeyValue::new("kind", "removed")]);
                self.instruments.cluster_events.add(
                    diff.became_unreachable.len() as u64,
                    &[node.clone(), KeyValue::new("kind", "unreachable")],
                );
                self.instruments
                    .cluster_events
                    .add(diff.became_reachable.len() as u64, &[node, KeyValue::new("kind", "reachable")]);
            }
            TelemetryEvent::ShardingChanged(ev) => {
                self.instruments.sharding_events.add(
                    1,
                    &[
                        node,
                        KeyValue::new("region", ev.region_id.clone()),
                        KeyValue::new("event", ev.event.clone()),
                    ],
                );
            }
            TelemetryEvent::JournalWrite(info) => {
                self.instruments
                    .persistence_writes
                    .add(1, &[node, KeyValue::new("journal", info.journal.clone())]);
            }
            TelemetryEvent::RemoteAssociation(info) => {
                self.instruments.remote_events.add(1, &[node, KeyValue::new("state", info.state.clone())]);
            }
            TelemetryEvent::StreamsGraphStarted { .. } => {
                self.instruments.streams_started.add(1, &[node.clone()]);
                self.instruments.streams_running.add(1, &[node]);
            }
            TelemetryEvent::StreamsGraphFinished { .. } => {
                self.instruments.streams_finished.add(1, &[node.clone()]);
                self.instruments.streams_running.add(-1, &[node]);
            }
            TelemetryEvent::DDataUpdated { key } => {
                self.instruments.ddata_updates.add(1, &[node, KeyValue::new("key", key.clone())]);
            }
        }
    }

    fn shutdown(&self) {
        let _ = self.provider.force_flush();
        let _ = self.provider.shutdown();
    }
}

#[cfg(feature = "otel-stdout")]
fn build_provider(cfg: &OtlpConfig, resource: Resource) -> Result<SdkMeterProvider, String> {
    if cfg.stdout || !otlp_transport_enabled() {
        let exporter = opentelemetry_stdout::MetricsExporter::default();
        let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_secs(cfg.interval_secs.max(1)))
            .build();
        return Ok(SdkMeterProvider::builder().with_reader(reader).with_resource(resource).build());
    }
    build_otlp_provider(cfg, resource)
}

#[cfg(not(feature = "otel-stdout"))]
fn build_provider(cfg: &OtlpConfig, resource: Resource) -> Result<SdkMeterProvider, String> {
    if cfg.stdout {
        return Err("stdout OTel exporter requested but `otel-stdout` feature is not enabled".to_string());
    }
    build_otlp_provider(cfg, resource)
}

#[cfg(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http"))]
fn build_otlp_provider(cfg: &OtlpConfig, resource: Resource) -> Result<SdkMeterProvider, String> {
    use opentelemetry_otlp::WithExportConfig;

    let interval = Duration::from_secs(cfg.interval_secs.max(1));

    match cfg.protocol.as_str() {
        #[cfg(feature = "otel-otlp-grpc")]
        "grpc" => opentelemetry_otlp::new_pipeline()
            .metrics(opentelemetry_sdk::runtime::Tokio)
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(cfg.endpoint.clone()))
            .with_resource(resource)
            .with_period(interval)
            .build()
            .map_err(|e| format!("otlp grpc init: {e}")),
        #[cfg(feature = "otel-otlp-http")]
        "http" => opentelemetry_otlp::new_pipeline()
            .metrics(opentelemetry_sdk::runtime::Tokio)
            .with_exporter(opentelemetry_otlp::new_exporter().http().with_endpoint(cfg.endpoint.clone()))
            .with_resource(resource)
            .with_period(interval)
            .build()
            .map_err(|e| format!("otlp http init: {e}")),
        other => Err(format!(
            "unsupported or un-enabled OTLP protocol `{other}`; enable the matching cargo feature"
        )),
    }
}

#[cfg(not(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http")))]
fn build_otlp_provider(_cfg: &OtlpConfig, _resource: Resource) -> Result<SdkMeterProvider, String> {
    Err("no OTLP transport compiled in; enable `otel-otlp-grpc` or `otel-otlp-http`".to_string())
}

#[allow(dead_code)]
fn otlp_transport_enabled() -> bool {
    cfg!(any(feature = "otel-otlp-grpc", feature = "otel-otlp-http"))
}

/// Testing helper — collect events so assertions are easy without a real
/// OTel collector. Not intended for production use.
pub struct CapturingExporter {
    pub events: Arc<Mutex<Vec<TelemetryEvent>>>,
}

impl CapturingExporter {
    pub fn new() -> Self {
        Self { events: Arc::new(Mutex::new(Vec::new())) }
    }
}

impl Default for CapturingExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl Exporter for CapturingExporter {
    fn on_event(&self, event: &TelemetryEvent) {
        self.events.lock().push(event.clone());
    }
}

#[cfg(test)]
mod capture_tests {
    use super::*;

    #[test]
    fn capturing_exporter_records_events() {
        let cap = CapturingExporter::new();
        cap.on_event(&TelemetryEvent::ActorStopped { path: "/user/z".into() });
        let events = cap.events.lock().clone();
        assert_eq!(events.len(), 1);
    }
}
