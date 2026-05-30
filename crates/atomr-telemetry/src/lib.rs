//! # atomr-telemetry
//!
//! Optional probe surface for observing a running `atomr` node.
//!
//! The crate is passive and opt-in. Construct a [`TelemetryExtension`] and
//! register it on an [`atomr_core::actor::ActorSystem`] via
//! `atomr_core::actor::Extensions`. Subsystems check for the extension
//! at runtime (cheap `Arc<T>` lookup) and, when present, emit snapshots +
//! events into the telemetry [`bus::TelemetryBus`]. When absent, there is
//! no cost beyond a single `DashMap` lookup.
//!
//! See [`crate::exporters`] for the Prometheus / OpenTelemetry exporters
//! gated behind cargo features.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod actor_registry;
pub mod bus;
pub mod cluster;
pub mod ddata;
pub mod dead_letters;
pub mod dto;
pub mod exporters;
pub mod persistence;
pub mod remote;
pub mod sharding;
pub mod streams;

use std::sync::Arc;

use atomr_core::actor::ActorSystem;
use parking_lot::RwLock;

use crate::actor_registry::ActorRegistry;
use crate::bus::TelemetryBus;
use crate::dead_letters::DeadLetterFeed;

/// The telemetry extension. Construct once per actor system, register via
/// the actor system's extensions, and all other probes will pick it up.
pub struct TelemetryExtension {
    pub node: String,
    pub bus: TelemetryBus,
    pub actors: Arc<ActorRegistry>,
    pub dead_letters: Arc<DeadLetterFeed>,
    pub cluster: Arc<cluster::ClusterProbe>,
    pub sharding: Arc<sharding::ShardingProbe>,
    pub persistence: Arc<persistence::PersistenceProbe>,
    pub remote: Arc<remote::RemoteProbe>,
    pub streams: Arc<streams::StreamsProbe>,
    pub ddata: Arc<ddata::DDataProbe>,
    pub(crate) exporters: RwLock<Vec<Arc<dyn exporters::Exporter>>>,
}

impl TelemetryExtension {
    /// Build a telemetry extension for the given node name. Channel
    /// capacity controls how many in-flight `TelemetryEvent`s the broadcast
    /// bus will buffer per subscriber.
    pub fn new(node: impl Into<String>, channel_capacity: usize) -> Arc<Self> {
        let bus = TelemetryBus::new(channel_capacity);
        Arc::new(Self {
            node: node.into(),
            actors: Arc::new(ActorRegistry::new(bus.clone())),
            dead_letters: Arc::new(DeadLetterFeed::new(bus.clone(), 1024)),
            cluster: Arc::new(cluster::ClusterProbe::new(bus.clone())),
            sharding: Arc::new(sharding::ShardingProbe::new(bus.clone())),
            persistence: Arc::new(persistence::PersistenceProbe::new(bus.clone())),
            remote: Arc::new(remote::RemoteProbe::new(bus.clone())),
            streams: Arc::new(streams::StreamsProbe::new(bus.clone())),
            ddata: Arc::new(ddata::DDataProbe::new(bus.clone())),
            bus,
            exporters: RwLock::new(Vec::new()),
        })
    }

    /// Install this extension on the given `ActorSystem`. Returns a clone
    /// of the shared `Arc<TelemetryExtension>`; the caller may keep it to
    /// feed probes directly from application code.
    pub fn install(self: Arc<Self>, system: &ActorSystem) -> Arc<Self> {
        system.extensions().register(TelemetryHandle(self.clone()));
        system.set_spawn_observer(self.actors.clone());
        system.set_dead_letter_observer(self.dead_letters.clone());
        self
    }

    /// Look up an installed extension on an `ActorSystem`.
    pub fn from_system(system: &ActorSystem) -> Option<Arc<Self>> {
        system.extensions().get::<TelemetryHandle>().map(|h| h.0.clone())
    }

    /// Register an exporter. Exporters receive every event published to
    /// the bus and may poll probes for snapshots on their own cadence.
    pub fn add_exporter(&self, exporter: Arc<dyn exporters::Exporter>) {
        self.bus.attach_exporter(exporter.clone());
        self.exporters.write().push(exporter);
    }

    /// Install the FR-11 OTel span exporter and return the
    /// [`TraceContextInterceptor`]
    /// to register on actor `Props` via `Props::with_interceptor`. The
    /// metrics exporter path is independent and untouched.
    #[cfg(feature = "otel")]
    pub fn add_span_exporter(
        &self,
        exporter: Arc<exporters::otel_tracer::OtelTracerExporter>,
        probe: exporters::otel_tracer::SpanProbeConfig,
    ) -> Arc<exporters::otel_tracer::TraceContextInterceptor> {
        self.bus.attach_span_exporter(exporter, probe)
    }

    /// Snapshot the full telemetry state of this node (one JSON payload).
    pub fn snapshot(&self) -> dto::NodeSnapshot {
        dto::NodeSnapshot {
            node: self.node.clone(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            actors: self.actors.snapshot(),
            dead_letters: self.dead_letters.recent(100),
            cluster: self.cluster.snapshot(),
            sharding: self.sharding.snapshot(),
            persistence: self.persistence.snapshot(),
            remote: self.remote.snapshot(),
            streams: self.streams.snapshot(),
            ddata: self.ddata.snapshot(),
        }
    }
}

/// Shim so we can register `Arc<TelemetryExtension>` in the typed
/// `Extensions` bag under a stable handle type.
pub struct TelemetryHandle(pub Arc<TelemetryExtension>);

/// Re-exports of the FR-11 OTel span model (gated behind the `otel` feature).
#[cfg(feature = "otel")]
pub use crate::exporters::otel_tracer::{OtelTracerExporter, SpanProbeConfig, TraceContextInterceptor};
