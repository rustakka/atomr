//! Cluster-tools submodule: DistributedPubSub, ClusterClient, Singleton,
//! the FR-5 cluster-wide kill switch, and the FR-7 role-weighted quorum.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use atomr_cluster::RoleWeightedQuorum;
use atomr_cluster_tools::{
    ClusterClientSettings, ClusterKillSwitch, ClusterReceptionist, ClusterSingletonManager, HaltReason,
    ResetAuthorization, ResetError, SingletonState,
};
use atomr_distributed_data::Replicator;

use crate::runtime::runtime;

#[pyclass(name = "DistributedPubSub", module = "atomr._native.cluster_tools")]
pub struct PyDistributedPubSub {
    topics: Arc<Mutex<std::collections::HashMap<String, Vec<Py<PyAny>>>>>,
}

#[pymethods]
impl PyDistributedPubSub {
    #[new]
    fn new() -> Self {
        Self { topics: Arc::new(Mutex::new(Default::default())) }
    }

    fn subscribe(&self, topic: String, callback: Py<PyAny>) {
        self.topics.lock().entry(topic).or_default().push(callback);
    }

    fn publish(&self, py: Python<'_>, topic: String, message: Py<PyAny>) -> PyResult<()> {
        let subs: Vec<Py<PyAny>> = {
            let g = self.topics.lock();
            g.get(&topic).map(|v| v.iter().map(|c| c.clone_ref(py)).collect()).unwrap_or_default()
        };
        for cb in subs {
            cb.call1(py, (message.clone_ref(py),))?;
        }
        Ok(())
    }

    fn topics(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for k in self.topics.lock().keys() {
            list.append(k)?;
        }
        Ok(list.unbind())
    }
}

/// Tracks cluster-singleton state independently of actor-ref binding.
///
/// PyO3 limitation: `UntypedActorRef` doesn't round-trip through Python
/// without a fully-typed actor binding (the inner ref carries type info
/// erased through `Box<dyn Any>` which isn't usable from Python). The
/// Python view exposes the state-machine surface — Inactive / Starting
/// / HandingOver / Active — and the buffered/drops counters; live
/// delivery still flows through Rust if the manager is shared via a
/// Rust-side cluster instance.
#[pyclass(name = "ClusterSingletonManager", module = "atomr._native.cluster_tools")]
pub struct PyClusterSingletonManager {
    inner: Arc<ClusterSingletonManager>,
}

#[pymethods]
impl PyClusterSingletonManager {
    #[new]
    #[pyo3(signature = (buffer_size=1000))]
    fn new(buffer_size: usize) -> Self {
        Self { inner: ClusterSingletonManager::with_buffer_size(buffer_size) }
    }

    /// Current state name: `inactive`, `starting`, `handing_over`,
    /// `active_here`, `active_remote`.
    #[getter]
    fn state(&self) -> String {
        match self.inner.state() {
            SingletonState::Inactive => "inactive".into(),
            SingletonState::Starting => "starting".into(),
            SingletonState::HandingOver => "handing_over".into(),
            SingletonState::Active { here: true, .. } => "active_here".into(),
            SingletonState::Active { here: false, .. } => "active_remote".into(),
            _ => "unknown".into(),
        }
    }

    fn begin_handover(&self) {
        self.inner.begin_handover();
    }
    fn begin_starting(&self) {
        self.inner.begin_starting();
    }
    fn clear(&self) {
        self.inner.clear();
    }

    #[getter]
    fn buffered(&self) -> usize {
        self.inner.buffered()
    }
    #[getter]
    fn drops(&self) -> u64 {
        self.inner.drops()
    }
}

/// Server-side registry mapping logical names to actor refs.
#[pyclass(name = "ClusterReceptionist", module = "atomr._native.cluster_tools")]
pub struct PyClusterReceptionist {
    inner: Arc<ClusterReceptionist>,
}

#[pymethods]
impl PyClusterReceptionist {
    #[new]
    fn new() -> Self {
        Self { inner: ClusterReceptionist::new() }
    }

    /// Names of services registered with this receptionist.
    fn registered(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for n in self.inner.registered() {
            list.append(n)?;
        }
        Ok(list.unbind())
    }

    /// Drop the named service.
    fn unregister(&self, name: String) {
        self.inner.unregister(&name);
    }

    /// True if `name` resolves to a registered actor ref.
    fn has(&self, name: String) -> bool {
        self.inner.lookup(&name).is_some()
    }
}

/// Client-side proxy settings (initial-contact list, retry limit).
#[pyclass(name = "ClusterClientSettings", module = "atomr._native.cluster_tools")]
#[derive(Clone)]
pub struct PyClusterClientSettings {
    pub(crate) inner: ClusterClientSettings,
}

#[pymethods]
impl PyClusterClientSettings {
    #[new]
    #[pyo3(signature = (initial_contacts=Vec::new(), max_attempts=5))]
    fn new(initial_contacts: Vec<String>, max_attempts: u32) -> Self {
        Self {
            inner: ClusterClientSettings::default()
                .with_initial_contacts(initial_contacts)
                .with_max_attempts(max_attempts),
        }
    }

    fn with_initial_contacts(&self, contacts: Vec<String>) -> Self {
        Self { inner: self.inner.clone().with_initial_contacts(contacts) }
    }

    fn with_max_attempts(&self, n: u32) -> Self {
        Self { inner: self.inner.clone().with_max_attempts(n) }
    }
}

/// FR-5 cluster-wide emergency-halt latch. A distributed, monotonic
/// "engaged / not-engaged" latch backed by a distributed-data `Flag`
/// CRDT: once any node engages, the engaged state survives merge, gossip
/// and rebalance. `reset` requires a two-person authorization (two
/// distinct, non-empty approvers) and advances to a fresh epoch.
///
/// This binding owns its own in-process `Replicator`, which is sufficient
/// for single-node operation and tests (the local-ack quiescence path the
/// Rust API exercises).
#[pyclass(name = "ClusterKillSwitch", module = "atomr._native.cluster_tools")]
pub struct PyClusterKillSwitch {
    inner: Arc<ClusterKillSwitch>,
}

#[pymethods]
impl PyClusterKillSwitch {
    /// Build a switch gating on the well-known `base_key`. `node` is this
    /// node's stable id (used for epoch-register tie-breaking).
    #[new]
    #[pyo3(signature = (base_key="atomr/killswitch".to_string(), node="local".to_string()))]
    fn new(base_key: String, node: String) -> Self {
        let replicator = Replicator::new();
        Self { inner: ClusterKillSwitch::new(replicator, base_key, node) }
    }

    /// Engage the latch for the current epoch and record a free-form
    /// `reason`. Idempotent (OR-merge). Returns the monotonic halt token.
    #[pyo3(signature = (reason="manual".to_string()))]
    fn engage(&self, reason: String) -> u64 {
        self.inner.engage(HaltReason::Manual(reason)).0
    }

    /// Read the merged latch state for the current epoch.
    fn is_engaged(&self) -> bool {
        self.inner.is_engaged()
    }

    /// Current epoch (advanced by `reset`).
    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    /// Fan the halt out to all registered guarded parties and collect
    /// their acks, up to `timeout_secs`. Async — returns an awaitable
    /// yielding `(acked, total, timed_out, remote_acked, remote_total)`.
    ///
    /// `acked` / `total` are the combined local + remote counts. This
    /// single-node binding owns an in-process replicator with no cross-node
    /// transport, so `remote_acked` / `remote_total` are always `0` here;
    /// they are surfaced for parity with the Rust `QuiescenceReport`.
    #[pyo3(signature = (timeout_secs=5.0))]
    fn await_quiescence<'py>(&self, py: Python<'py>, timeout_secs: f64) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let dur = Duration::from_secs_f64(timeout_secs.max(0.0));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = inner.await_quiescence(dur).await;
            Python::with_gil(|py| {
                let tup: Py<PyAny> =
                    (report.acked, report.total, report.timed_out, report.remote_acked, report.remote_total)
                        .into_py(py);
                Ok::<Py<PyAny>, PyErr>(tup)
            })
        })
    }

    /// Reset the switch by advancing to a fresh epoch. Requires two
    /// distinct, non-empty approvers. Returns the new epoch number;
    /// raises `ValueError` on invalid authorization.
    fn reset(&self, approver_a: String, approver_b: String) -> PyResult<u64> {
        self.inner
            .reset(ResetAuthorization { approver_a, approver_b })
            .map_err(|e: ResetError| PyValueError::new_err(e.to_string()))
    }
}

/// FR-7 role-weighted split-brain quorum. Weights cluster survival by
/// *role importance* rather than raw node count: each role carries a
/// weight, a member's weight is the max over its roles, and the reachable
/// side survives only if its summed weight meets `min_quorum_weight`.
#[pyclass(name = "RoleWeightedQuorum", module = "atomr._native.cluster_tools")]
#[derive(Clone)]
pub struct PyRoleWeightedQuorum {
    pub(crate) inner: RoleWeightedQuorum,
}

#[pymethods]
impl PyRoleWeightedQuorum {
    /// Build from a `{role: weight}` map and the minimum summed weight the
    /// reachable side must hold to keep quorum.
    #[new]
    fn new(weights: HashMap<String, u32>, min_quorum_weight: u32) -> Self {
        Self { inner: RoleWeightedQuorum::new(weights, min_quorum_weight) }
    }

    #[getter]
    fn min_quorum_weight(&self) -> u32 {
        self.inner.min_quorum_weight
    }

    /// The configured weight for `role` (`0` if the role is unweighted).
    fn weight_of(&self, role: String) -> u32 {
        self.inner.weights.get(&role).copied().unwrap_or(0)
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = runtime();
    let sub = PyModule::new_bound(py, "cluster_tools")?;
    sub.add_class::<PyDistributedPubSub>()?;
    sub.add_class::<PyClusterSingletonManager>()?;
    sub.add_class::<PyClusterReceptionist>()?;
    sub.add_class::<PyClusterClientSettings>()?;
    sub.add_class::<PyClusterKillSwitch>()?;
    sub.add_class::<PyRoleWeightedQuorum>()?;
    m.add_submodule(&sub)?;
    Ok(())
}
