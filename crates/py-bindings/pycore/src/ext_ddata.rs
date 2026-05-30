//! Distributed Data CRDTs exposed to Python. Uses the Rust crate's
//! native types directly for deterministic merge semantics.
//!
//! Phase 7 — `LwwRegister`, `Flag`, `ORMap`, `LWWMap`, `PNCounterMap`,
//! `ORMultiMap`, plus a `Replicator` actor handle and
//! `Read/WriteConsistency` Python types. Nested-CRDT values inside
//! `ORMap` are restricted to the built-in CRDTs we re-implement here:
//! Python user-defined CRDTs cannot ride along because merge runs in
//! Rust without the GIL.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tokio::sync::mpsc;

use atomr_distributed_data::{
    CrdtMerge, DurableStore, FileDurableStore, Flag, GCounter, GSet, LWWMap, LwwRegister, NoopDurableStore,
    ORMap, ORMultiMap, OrSet, PNCounter, PNCounterMap, PruningPhase, PruningState, ReadAggregator,
    ReadConsistency, ReplicatorAck, ReplicatorActor, SignedSum, SignedSumMap, SubscriptionToken,
    WriteAggregator, WriteConsistency,
};

use crate::actor_system::PyActorSystem;
use crate::errors;
use crate::runtime::runtime;

// =====================================================================
// Existing CRDTs (unchanged behaviour, kept verbatim)
// =====================================================================

#[pyclass(name = "GCounter", module = "atomr._native.ddata")]
pub struct PyGCounter {
    pub(crate) inner: Mutex<GCounter>,
}

#[pymethods]
impl PyGCounter {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(GCounter::new()) }
    }
    fn increment(&self, node: String, delta: u64) {
        self.inner.lock().increment(&node, delta);
    }
    fn value(&self) -> u64 {
        self.inner.lock().value()
    }
    fn merge(&self, other: &PyGCounter) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

#[pyclass(name = "PNCounter", module = "atomr._native.ddata")]
pub struct PyPNCounter {
    pub(crate) inner: Mutex<PNCounter>,
}

#[pymethods]
impl PyPNCounter {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(PNCounter::new()) }
    }
    fn increment(&self, node: String, delta: u64) {
        self.inner.lock().increment(&node, delta);
    }
    fn decrement(&self, node: String, delta: u64) {
        self.inner.lock().decrement(&node, delta);
    }
    fn value(&self) -> i64 {
        self.inner.lock().value()
    }
    fn merge(&self, other: &PyPNCounter) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

#[pyclass(name = "GSet", module = "atomr._native.ddata")]
pub struct PyGSet {
    pub(crate) inner: Mutex<GSet<String>>,
}

#[pymethods]
impl PyGSet {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(GSet::new()) }
    }
    fn add(&self, item: String) {
        self.inner.lock().add(item);
    }
    fn contains(&self, item: String) -> bool {
        self.inner.lock().contains(&item)
    }
    fn size(&self) -> usize {
        self.inner.lock().len()
    }
    fn merge(&self, other: &PyGSet) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
    fn elements(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for v in self.inner.lock().iter() {
            list.append(v.clone())?;
        }
        Ok(list.unbind())
    }
}

#[pyclass(name = "ORSet", module = "atomr._native.ddata")]
pub struct PyORSet {
    pub(crate) inner: Mutex<OrSet<String>>,
}

#[pymethods]
impl PyORSet {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(OrSet::new()) }
    }
    fn add(&self, item: String) {
        self.inner.lock().add(item);
    }
    fn remove(&self, item: String) {
        self.inner.lock().remove(&item);
    }
    fn contains(&self, item: String) -> bool {
        self.inner.lock().contains(&item)
    }
    fn merge(&self, other: &PyORSet) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
    /// Snapshot of current set elements.
    fn elements(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty_bound(py);
        for v in self.inner.lock().iter() {
            list.append(v.clone())?;
        }
        Ok(list.unbind())
    }
}

/// Pruning bookkeeping for CRDTs after a node leaves the cluster.
#[pyclass(name = "PruningState", module = "atomr._native.ddata")]
pub struct PyPruningState {
    inner: Mutex<PruningState>,
}

#[pymethods]
impl PyPruningState {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(PruningState::new()) }
    }

    fn initialize(&self, removed_node: String, owner: String) {
        self.inner.lock().initialize(removed_node, owner);
    }

    fn mark_performed(&self, removed_node: String, obsolete_at: u64) -> bool {
        self.inner.lock().mark_performed(&removed_node, obsolete_at)
    }

    fn is_pruned(&self, removed_node: String) -> bool {
        self.inner.lock().is_pruned(&removed_node)
    }

    fn owner(&self, removed_node: String) -> Option<String> {
        self.inner.lock().owner(&removed_node).map(|s| s.to_string())
    }

    fn gc(&self, current_round: u64) -> usize {
        self.inner.lock().gc(current_round)
    }

    fn merge(&self, other: &PyPruningState) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }

    /// Phase string for `removed_node`: `"initialized"`, `"performed"`, or
    /// `None` if the node isn't tracked.
    fn phase(&self, removed_node: String) -> Option<String> {
        let g = self.inner.lock();
        match g.markers.get(&removed_node)? {
            PruningPhase::Initialized { .. } => Some("initialized".into()),
            PruningPhase::Performed { .. } => Some("performed".into()),
        }
    }
}

#[pyfunction]
fn pruning_phases() -> Vec<String> {
    vec!["initialized".into(), "performed".into()]
}

/// Quorum-write tracker.
#[pyclass(name = "WriteAggregator", module = "atomr._native.ddata")]
pub struct PyWriteAggregator {
    inner: Mutex<WriteAggregator>,
}

#[pymethods]
impl PyWriteAggregator {
    #[new]
    fn new(target: usize) -> Self {
        Self { inner: Mutex::new(WriteAggregator::new(target)) }
    }
    fn ack(&self) {
        self.inner.lock().ack();
    }
    fn nack(&self) {
        self.inner.lock().nack();
    }
    fn is_satisfied(&self) -> bool {
        self.inner.lock().is_satisfied()
    }
    fn is_failed(&self, cluster_size: usize) -> bool {
        self.inner.lock().is_failed(cluster_size)
    }
    #[getter]
    fn received(&self) -> usize {
        self.inner.lock().received()
    }
    #[getter]
    fn target(&self) -> usize {
        self.inner.lock().target()
    }
}

/// Quorum-read tracker.
#[pyclass(name = "ReadAggregator", module = "atomr._native.ddata")]
pub struct PyReadAggregator {
    inner: Mutex<ReadAggregator>,
}

#[pymethods]
impl PyReadAggregator {
    #[new]
    fn new(target: usize) -> Self {
        Self { inner: Mutex::new(ReadAggregator::new(target)) }
    }
    fn reply(&self) {
        self.inner.lock().reply();
    }
    fn is_satisfied(&self) -> bool {
        self.inner.lock().is_satisfied()
    }
    #[getter]
    fn target(&self) -> usize {
        self.inner.lock().target()
    }
}

// =====================================================================
// Phase 7 — new CRDTs
// =====================================================================

/// `LwwRegister` whose value is opaque bytes — Python users encode
/// arbitrary objects (typically via `json.dumps`) before passing in.
#[pyclass(name = "LwwRegister", module = "atomr._native.ddata")]
pub struct PyLwwRegister {
    pub(crate) inner: Mutex<LwwRegister<Vec<u8>>>,
}

impl PyLwwRegister {
    fn snapshot(&self) -> LwwRegister<Vec<u8>> {
        self.inner.lock().clone()
    }
}

#[pymethods]
impl PyLwwRegister {
    #[new]
    #[pyo3(signature = (initial=None, node="local".to_string(), timestamp=0))]
    fn new(initial: Option<Vec<u8>>, node: String, timestamp: u64) -> Self {
        let value = initial.unwrap_or_default();
        Self { inner: Mutex::new(LwwRegister::new(node, value, timestamp)) }
    }

    /// Convenience: build a register with `value` already set, using
    /// `now()` (microseconds since UNIX epoch) as the timestamp and
    /// `"local"` as the originating node.
    #[staticmethod]
    fn with_value(value: Vec<u8>) -> Self {
        let ts = now_micros();
        Self { inner: Mutex::new(LwwRegister::new("local", value, ts)) }
    }

    /// Set `value` with the supplied `timestamp` (caller-provided —
    /// usually `time.time_ns() // 1000`). If `timestamp` is older than
    /// the current one, the write is ignored (LWW semantics).
    #[pyo3(signature = (value, timestamp=None, node="local".to_string()))]
    fn set(&self, value: Vec<u8>, timestamp: Option<u64>, node: String) {
        let ts = timestamp.unwrap_or_else(now_micros);
        self.inner.lock().set(value, ts, node);
    }

    fn value<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        let g = self.inner.lock();
        PyBytes::new_bound(py, g.value())
    }

    #[getter]
    fn timestamp(&self) -> u64 {
        self.inner.lock().timestamp()
    }

    fn merge(&self, other: &PyLwwRegister) {
        let o = other.snapshot();
        self.inner.lock().merge(&o);
    }
}

/// `Flag` — monotonic boolean (false → true).
#[pyclass(name = "Flag", module = "atomr._native.ddata")]
pub struct PyFlag {
    pub(crate) inner: Mutex<Flag>,
}

impl PyFlag {
    fn snapshot(&self) -> Flag {
        *self.inner.lock()
    }
}

#[pymethods]
impl PyFlag {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(Flag::new()) }
    }

    /// Switch the flag on (idempotent).
    fn enable(&self) {
        self.inner.lock().switch_on();
    }

    fn is_enabled(&self) -> bool {
        self.inner.lock().enabled()
    }

    fn merge(&self, other: &PyFlag) {
        let o = other.snapshot();
        self.inner.lock().merge(&o);
    }
}

/// Tagged-enum storage for `PyORMap`. ORMap is type-homogeneous in its
/// value: a single instance carries one of the variants below. `put`
/// rejects values whose CRDT class doesn't match the variant; `merge`
/// rejects across-variant merges with `ValueError`.
///
/// Supported value types: `LwwRegister<bytes>`, `PNCounter`, `Flag`,
/// `GSet<String>`, `LWWMap<String, bytes>`.
pub(crate) enum OrMapStorage {
    LwwReg(ORMap<String, LwwRegister<Vec<u8>>>),
    PnCounter(ORMap<String, PNCounter>),
    Flag(ORMap<String, Flag>),
    GSet(ORMap<String, GSet<String>>),
    LwwMap(ORMap<String, LWWMap<String, Vec<u8>>>),
}

impl OrMapStorage {
    fn variant_name(&self) -> &'static str {
        match self {
            Self::LwwReg(_) => "LwwRegister",
            Self::PnCounter(_) => "PNCounter",
            Self::Flag(_) => "Flag",
            Self::GSet(_) => "GSet",
            Self::LwwMap(_) => "LWWMap",
        }
    }
}

/// Observed-remove map of `String` → CRDT value. The value type is
/// fixed at construction via the `of_*` factory methods (or defaults
/// to `LwwRegister<bytes>` for the bare constructor — preserved for
/// backwards compat). Mixing variants in one ORMap raises
/// `ValueError`. For per-key counters use [`PyPNCounterMap`]; for
/// set-valued maps use [`PyORMultiMap`].
#[pyclass(name = "ORMap", module = "atomr._native.ddata")]
pub struct PyORMap {
    pub(crate) inner: Mutex<OrMapStorage>,
}

#[pymethods]
impl PyORMap {
    /// Default constructor — values are `LwwRegister<bytes>`. Equivalent
    /// to `ORMap.of_lww_register()`. Preserved for backwards compat.
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(OrMapStorage::LwwReg(ORMap::new())) }
    }

    /// Construct an ORMap whose values are `LwwRegister<bytes>`.
    #[staticmethod]
    fn of_lww_register() -> Self {
        Self { inner: Mutex::new(OrMapStorage::LwwReg(ORMap::new())) }
    }

    /// Construct an ORMap whose values are `PNCounter`.
    #[staticmethod]
    fn of_pn_counter() -> Self {
        Self { inner: Mutex::new(OrMapStorage::PnCounter(ORMap::new())) }
    }

    /// Construct an ORMap whose values are `Flag`.
    #[staticmethod]
    fn of_flag() -> Self {
        Self { inner: Mutex::new(OrMapStorage::Flag(ORMap::new())) }
    }

    /// Construct an ORMap whose values are `GSet<String>`.
    #[staticmethod]
    fn of_g_set() -> Self {
        Self { inner: Mutex::new(OrMapStorage::GSet(ORMap::new())) }
    }

    /// Construct an ORMap whose values are `LWWMap<String, bytes>`.
    #[staticmethod]
    fn of_lww_map() -> Self {
        Self { inner: Mutex::new(OrMapStorage::LwwMap(ORMap::new())) }
    }

    /// Returns the CRDT class name this ORMap holds as values:
    /// `"LwwRegister"`, `"PNCounter"`, `"Flag"`, `"GSet"`, or
    /// `"LWWMap"`.
    fn value_type(&self) -> String {
        self.inner.lock().variant_name().to_string()
    }

    /// Insert / replace a key. `value` must be an instance of the CRDT
    /// type fixed at construction — e.g. an ORMap built with
    /// `of_pn_counter()` only accepts `PNCounter` values. Mismatch
    /// raises `ValueError`.
    fn put(&self, key: String, value: Bound<'_, PyAny>) -> PyResult<()> {
        let mut g = self.inner.lock();
        match &mut *g {
            OrMapStorage::LwwReg(m) => {
                let cell: PyRef<PyLwwRegister> = value.extract().map_err(|_| {
                    PyErr::new::<PyValueError, _>(
                        "ORMap[LwwRegister]: value must be a `LwwRegister` instance",
                    )
                })?;
                let snapshot = cell.inner.lock().clone();
                m.put(key, snapshot);
            }
            OrMapStorage::PnCounter(m) => {
                let cell: PyRef<PyPNCounter> = value.extract().map_err(|_| {
                    PyErr::new::<PyValueError, _>("ORMap[PNCounter]: value must be a `PNCounter` instance")
                })?;
                let snapshot = cell.inner.lock().clone();
                m.put(key, snapshot);
            }
            OrMapStorage::Flag(m) => {
                let cell: PyRef<PyFlag> = value.extract().map_err(|_| {
                    PyErr::new::<PyValueError, _>("ORMap[Flag]: value must be a `Flag` instance")
                })?;
                let snapshot = *cell.inner.lock();
                m.put(key, snapshot);
            }
            OrMapStorage::GSet(m) => {
                let cell: PyRef<PyGSet> = value.extract().map_err(|_| {
                    PyErr::new::<PyValueError, _>("ORMap[GSet]: value must be a `GSet` instance")
                })?;
                let snapshot = cell.inner.lock().clone();
                m.put(key, snapshot);
            }
            OrMapStorage::LwwMap(m) => {
                let cell: PyRef<PyLWWMap> = value.extract().map_err(|_| {
                    PyErr::new::<PyValueError, _>("ORMap[LWWMap]: value must be a `LWWMap` instance")
                })?;
                let snapshot = cell.inner.lock().clone();
                m.put(key, snapshot);
            }
        }
        Ok(())
    }

    fn remove(&self, key: String) {
        let mut g = self.inner.lock();
        match &mut *g {
            OrMapStorage::LwwReg(m) => m.remove(&key),
            OrMapStorage::PnCounter(m) => m.remove(&key),
            OrMapStorage::Flag(m) => m.remove(&key),
            OrMapStorage::GSet(m) => m.remove(&key),
            OrMapStorage::LwwMap(m) => m.remove(&key),
        }
    }

    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Option<Py<PyAny>>> {
        let g = self.inner.lock();
        match &*g {
            OrMapStorage::LwwReg(m) => match m.get(&key).cloned() {
                Some(v) => {
                    let py_obj = Py::new(py, PyLwwRegister { inner: Mutex::new(v) })?;
                    Ok(Some(py_obj.into_any()))
                }
                None => Ok(None),
            },
            OrMapStorage::PnCounter(m) => match m.get(&key).cloned() {
                Some(v) => {
                    let py_obj = Py::new(py, PyPNCounter { inner: Mutex::new(v) })?;
                    Ok(Some(py_obj.into_any()))
                }
                None => Ok(None),
            },
            OrMapStorage::Flag(m) => match m.get(&key).copied() {
                Some(v) => {
                    let py_obj = Py::new(py, PyFlag { inner: Mutex::new(v) })?;
                    Ok(Some(py_obj.into_any()))
                }
                None => Ok(None),
            },
            OrMapStorage::GSet(m) => match m.get(&key).cloned() {
                Some(v) => {
                    let py_obj = Py::new(py, PyGSet { inner: Mutex::new(v) })?;
                    Ok(Some(py_obj.into_any()))
                }
                None => Ok(None),
            },
            OrMapStorage::LwwMap(m) => match m.get(&key).cloned() {
                Some(v) => {
                    let py_obj = Py::new(py, PyLWWMap { inner: Mutex::new(v) })?;
                    Ok(Some(py_obj.into_any()))
                }
                None => Ok(None),
            },
        }
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let g = self.inner.lock();
        let list = PyList::empty_bound(py);
        match &*g {
            OrMapStorage::LwwReg(m) => {
                for (k, _) in m.iter() {
                    list.append(k.clone())?;
                }
            }
            OrMapStorage::PnCounter(m) => {
                for (k, _) in m.iter() {
                    list.append(k.clone())?;
                }
            }
            OrMapStorage::Flag(m) => {
                for (k, _) in m.iter() {
                    list.append(k.clone())?;
                }
            }
            OrMapStorage::GSet(m) => {
                for (k, _) in m.iter() {
                    list.append(k.clone())?;
                }
            }
            OrMapStorage::LwwMap(m) => {
                for (k, _) in m.iter() {
                    list.append(k.clone())?;
                }
            }
        }
        Ok(list.unbind())
    }

    fn merge(&self, other: &PyORMap) -> PyResult<()> {
        // Snapshot the other side first to release its lock before we
        // grab ours (avoids deadlock on `a.merge(a)` though that's a
        // user error anyway).
        let other_clone = {
            let og = other.inner.lock();
            match &*og {
                OrMapStorage::LwwReg(m) => OrMapStorage::LwwReg(m.clone()),
                OrMapStorage::PnCounter(m) => OrMapStorage::PnCounter(m.clone()),
                OrMapStorage::Flag(m) => OrMapStorage::Flag(m.clone()),
                OrMapStorage::GSet(m) => OrMapStorage::GSet(m.clone()),
                OrMapStorage::LwwMap(m) => OrMapStorage::LwwMap(m.clone()),
            }
        };
        let mut g = self.inner.lock();
        match (&mut *g, other_clone) {
            (OrMapStorage::LwwReg(a), OrMapStorage::LwwReg(b)) => a.merge(&b),
            (OrMapStorage::PnCounter(a), OrMapStorage::PnCounter(b)) => a.merge(&b),
            (OrMapStorage::Flag(a), OrMapStorage::Flag(b)) => a.merge(&b),
            (OrMapStorage::GSet(a), OrMapStorage::GSet(b)) => a.merge(&b),
            (OrMapStorage::LwwMap(a), OrMapStorage::LwwMap(b)) => a.merge(&b),
            (a, b) => {
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "ORMap merge requires matching value types: self={}, other={}",
                    a.variant_name(),
                    b.variant_name(),
                )));
            }
        }
        Ok(())
    }
}

/// Last-writer-wins map of `String` → opaque bytes.
#[pyclass(name = "LWWMap", module = "atomr._native.ddata")]
pub struct PyLWWMap {
    pub(crate) inner: Mutex<LWWMap<String, Vec<u8>>>,
}

#[pymethods]
impl PyLWWMap {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(LWWMap::new()) }
    }

    /// Put `value` for `key`. `timestamp` defaults to "now in
    /// microseconds"; supply it manually only for deterministic tests.
    #[pyo3(signature = (key, value, timestamp=None))]
    fn put(&self, key: String, value: Vec<u8>, timestamp: Option<u128>) {
        let ts = timestamp.unwrap_or_else(|| now_micros() as u128);
        self.inner.lock().put(key, value, ts);
    }

    fn get<'py>(&self, py: Python<'py>, key: String) -> Option<Bound<'py, PyBytes>> {
        let g = self.inner.lock();
        g.get(&key).map(|v| PyBytes::new_bound(py, v))
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let g = self.inner.lock();
        let list = PyList::empty_bound(py);
        for (k, _) in g.iter() {
            list.append(k.clone())?;
        }
        Ok(list.unbind())
    }

    fn merge(&self, other: &PyLWWMap) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

/// Map of `String` → `PNCounter`.
#[pyclass(name = "PNCounterMap", module = "atomr._native.ddata")]
pub struct PyPNCounterMap {
    pub(crate) inner: Mutex<PNCounterMap<String>>,
}

#[pymethods]
impl PyPNCounterMap {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(PNCounterMap::new()) }
    }

    #[pyo3(signature = (key, delta=1, node="local".to_string()))]
    fn increment(&self, key: String, delta: u64, node: String) {
        self.inner.lock().increment(key, &node, delta);
    }

    #[pyo3(signature = (key, delta=1, node="local".to_string()))]
    fn decrement(&self, key: String, delta: u64, node: String) {
        self.inner.lock().decrement(key, &node, delta);
    }

    fn value(&self, key: String) -> i64 {
        self.inner.lock().value(&key)
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let g = self.inner.lock();
        let list = PyList::empty_bound(py);
        for (k, _) in g.iter() {
            list.append(k.clone())?;
        }
        Ok(list.unbind())
    }

    fn merge(&self, other: &PyPNCounterMap) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

/// Map of `String` → `OrSet<String>`.
#[pyclass(name = "ORMultiMap", module = "atomr._native.ddata")]
pub struct PyORMultiMap {
    pub(crate) inner: Mutex<ORMultiMap<String, String>>,
}

#[pymethods]
impl PyORMultiMap {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(ORMultiMap::new()) }
    }

    fn add(&self, key: String, value: String) {
        self.inner.lock().add(key, value);
    }

    fn remove(&self, key: String, value: String) {
        self.inner.lock().remove(&key, &value);
    }

    fn contains(&self, key: String, value: String) -> bool {
        self.inner.lock().contains(&key, &value)
    }

    fn key_count(&self) -> usize {
        self.inner.lock().key_count()
    }

    fn merge(&self, other: &PyORMultiMap) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

// =====================================================================
// SignedSum / SignedSumMap — FR-4 signed-exposure CRDTs (i128 payload)
// =====================================================================

/// Signed, `i128`-payload exposure CRDT. Tracks a net signed value that
/// converges across replicas: `value() = sum(pos) - sum(neg)`. Negative
/// deltas are first-class (routed into the per-node negative accumulator);
/// all arithmetic saturates rather than wrapping. `i128` maps to Python
/// `int`.
#[pyclass(name = "SignedSum", module = "atomr._native.ddata")]
pub struct PySignedSum {
    pub(crate) inner: Mutex<SignedSum>,
}

#[pymethods]
impl PySignedSum {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(SignedSum::new()) }
    }

    /// Apply a signed `delta` attributed to `node`. `delta >= 0` adds to
    /// the positive accumulator; `delta < 0` adds its magnitude to the
    /// negative accumulator.
    fn add(&self, node: String, delta: i128) {
        self.inner.lock().add(&node, delta);
    }

    /// Net signed exposure (`sum(pos) - sum(neg)`).
    fn value(&self) -> i128 {
        self.inner.lock().value()
    }

    fn merge(&self, other: &PySignedSum) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

/// Map of `String` key → [`PySignedSum`] — convergent per-key net signed
/// exposure (e.g. per instrument / desk / counterparty).
#[pyclass(name = "SignedSumMap", module = "atomr._native.ddata")]
pub struct PySignedSumMap {
    pub(crate) inner: Mutex<SignedSumMap<String>>,
}

#[pymethods]
impl PySignedSumMap {
    #[new]
    fn new() -> Self {
        Self { inner: Mutex::new(SignedSumMap::new()) }
    }

    /// Apply a signed `delta` attributed to `node` for `key`.
    fn add(&self, key: String, node: String, delta: i128) {
        self.inner.lock().add(key, &node, delta);
    }

    /// Net signed exposure for `key` (`0` if the key is absent).
    fn get(&self, key: String) -> i128 {
        self.inner.lock().get(&key)
    }

    /// `(key, net_value)` pairs in key order.
    fn entries(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let g = self.inner.lock();
        let list = PyList::empty_bound(py);
        for (k, v) in g.entries() {
            let tup: Py<PyAny> = (k.clone(), v).into_py(py);
            list.append(tup)?;
        }
        Ok(list.unbind())
    }

    fn merge(&self, other: &PySignedSumMap) {
        let o = other.inner.lock().clone();
        self.inner.lock().merge(&o);
    }
}

// =====================================================================
// Read / Write consistency Python types
// =====================================================================

#[pyclass(name = "WriteConsistency", module = "atomr._native.ddata", frozen)]
#[derive(Clone, Copy)]
pub struct PyWriteConsistency {
    pub(crate) inner: WriteConsistency,
}

#[pymethods]
impl PyWriteConsistency {
    #[staticmethod]
    fn local() -> Self {
        Self { inner: WriteConsistency::Local }
    }

    #[staticmethod]
    #[pyo3(signature = (timeout=2.0))]
    fn majority(timeout: f64) -> Self {
        Self { inner: WriteConsistency::Majority { timeout: Duration::from_secs_f64(timeout) } }
    }

    #[staticmethod]
    #[pyo3(signature = (timeout=2.0))]
    fn all(timeout: f64) -> Self {
        Self { inner: WriteConsistency::All { timeout: Duration::from_secs_f64(timeout) } }
    }

    #[staticmethod]
    #[pyo3(signature = (n, timeout=2.0))]
    fn write_to(n: usize, timeout: f64) -> Self {
        Self { inner: WriteConsistency::From { n, timeout: Duration::from_secs_f64(timeout) } }
    }

    fn __repr__(&self) -> String {
        format!("WriteConsistency({:?})", self.inner)
    }
}

#[pyclass(name = "ReadConsistency", module = "atomr._native.ddata", frozen)]
#[derive(Clone, Copy)]
pub struct PyReadConsistency {
    pub(crate) inner: ReadConsistency,
}

#[pymethods]
impl PyReadConsistency {
    #[staticmethod]
    fn local() -> Self {
        Self { inner: ReadConsistency::Local }
    }

    #[staticmethod]
    #[pyo3(signature = (timeout=2.0))]
    fn majority(timeout: f64) -> Self {
        Self { inner: ReadConsistency::Majority { timeout: Duration::from_secs_f64(timeout) } }
    }

    #[staticmethod]
    #[pyo3(signature = (timeout=2.0))]
    fn all(timeout: f64) -> Self {
        Self { inner: ReadConsistency::All { timeout: Duration::from_secs_f64(timeout) } }
    }

    #[staticmethod]
    #[pyo3(signature = (n, timeout=2.0))]
    fn read_from(n: usize, timeout: f64) -> Self {
        Self { inner: ReadConsistency::From { n, timeout: Duration::from_secs_f64(timeout) } }
    }

    fn __repr__(&self) -> String {
        format!("ReadConsistency({:?})", self.inner)
    }
}

// =====================================================================
// DurableStore
// =====================================================================

#[derive(Clone)]
enum DurableStoreKind {
    Noop,
    File(Arc<FileDurableStore>),
}

impl DurableStoreKind {
    fn as_dyn(&self) -> Arc<dyn DurableStore> {
        match self {
            Self::Noop => Arc::new(NoopDurableStore),
            Self::File(s) => s.clone() as Arc<dyn DurableStore>,
        }
    }
}

/// Durable backing store for the `Replicator`. The default is `noop`
/// (memory only). Use `DurableStore.file(path)` to persist updates to
/// disk; survives an actor-system restart.
#[pyclass(name = "DurableStore", module = "atomr._native.ddata")]
#[derive(Clone)]
pub struct PyDurableStore {
    kind: DurableStoreKind,
}

#[pymethods]
impl PyDurableStore {
    #[staticmethod]
    fn noop() -> Self {
        Self { kind: DurableStoreKind::Noop }
    }

    #[staticmethod]
    fn file(path: String) -> PyResult<Self> {
        let inner = FileDurableStore::open(PathBuf::from(path)).map_err(errors::map)?;
        Ok(Self { kind: DurableStoreKind::File(Arc::new(inner)) })
    }

    /// Whether `key` has a marker on disk (debug helper for tests).
    fn contains(&self, key: String) -> bool {
        match &self.kind {
            DurableStoreKind::File(s) => s.contains(&key),
            DurableStoreKind::Noop => false,
        }
    }

    /// Return all keys currently held (sorted).
    fn keys(&self) -> Vec<String> {
        match &self.kind {
            DurableStoreKind::File(s) => s.keys().unwrap_or_default(),
            DurableStoreKind::Noop => Vec::new(),
        }
    }
}

// =====================================================================
// Replicator
// =====================================================================

/// Tagged manifest for the CRDT family stored under a key. Used by the
/// replicator to round-trip the right wrapper type when calling
/// `modify_fn` / `get`.
#[derive(Clone, Copy, Debug)]
enum CrdtKind {
    GCounter,
    PNCounter,
    GSet,
    ORSet,
    LwwRegister,
    Flag,
    ORMap,
    LWWMap,
    PNCounterMap,
    ORMultiMap,
}

impl CrdtKind {
    fn from_class(name: &str) -> Option<Self> {
        Some(match name {
            "GCounter" => Self::GCounter,
            "PNCounter" => Self::PNCounter,
            "GSet" => Self::GSet,
            "ORSet" => Self::ORSet,
            "LwwRegister" => Self::LwwRegister,
            "Flag" => Self::Flag,
            "ORMap" => Self::ORMap,
            "LWWMap" => Self::LWWMap,
            "PNCounterMap" => Self::PNCounterMap,
            "ORMultiMap" => Self::ORMultiMap,
            _ => return None,
        })
    }
}

/// Per-system replicator handle. Contains the `ReplicatorActor`, the
/// system name (used as the singleton key), the durable store for
/// reload, and a per-key subscriber registry.
struct ReplicatorState {
    actor: ReplicatorActor,
    durable: PyDurableStore,
    /// Per-key broadcast list, one mpsc::Sender per active subscription.
    subscribers: Mutex<HashMap<String, Vec<mpsc::Sender<()>>>>,
    /// Keep `SubscriptionToken`s alive so the underlying `Replicator`
    /// retains the notify callbacks.
    _tokens: Mutex<HashMap<String, SubscriptionToken>>,
}

static REPLICATORS: Lazy<Mutex<HashMap<String, Arc<ReplicatorState>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn get_or_spawn_replicator(system: &PyActorSystem) -> Arc<ReplicatorState> {
    let key = system.inner.name().to_string();
    let mut g = REPLICATORS.lock();
    if let Some(existing) = g.get(&key) {
        return existing.clone();
    }
    let store = pick_durable_store(system);
    let _rt_guard = runtime().enter();
    let actor = ReplicatorActor::spawn_with(store.kind.as_dyn());
    let state = Arc::new(ReplicatorState {
        actor,
        durable: store,
        subscribers: Mutex::new(HashMap::new()),
        _tokens: Mutex::new(HashMap::new()),
    });
    g.insert(key, state.clone());
    state
}

fn pick_durable_store(system: &PyActorSystem) -> PyDurableStore {
    // Read `distributed-data.durable.store-actor-class` from the
    // system config; default to `noop`. `file` requires
    // `distributed-data.durable.path` to be set.
    let cfg = &system.inner.config();
    let kind = cfg.get_string("distributed-data.durable.store-actor-class").unwrap_or_else(|_| "noop".into());
    match kind.as_str() {
        "file" => {
            if let Ok(path) = cfg.get_string("distributed-data.durable.path") {
                if let Ok(store) = FileDurableStore::open(PathBuf::from(path)) {
                    return PyDurableStore { kind: DurableStoreKind::File(Arc::new(store)) };
                }
            }
            PyDurableStore { kind: DurableStoreKind::Noop }
        }
        _ => PyDurableStore { kind: DurableStoreKind::Noop },
    }
}

/// Async iterator yielded by `Replicator.subscribe(key)`. Each step
/// returns the key name when an update or delete fires.
#[pyclass(name = "ReplicatorSubscription", module = "atomr._native.ddata")]
pub struct PyReplicatorSubscription {
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<()>>>,
    key: String,
}

#[pymethods]
impl PyReplicatorSubscription {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let key = self.key.clone();
        let rx = self.rx.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut g = rx.lock().await;
            match g.recv().await {
                Some(()) => Python::with_gil(|py| {
                    let s: Py<PyAny> = key.into_py(py);
                    Ok::<Py<PyAny>, PyErr>(s)
                }),
                None => Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>(())),
            }
        })
    }
}

/// Lazy singleton replicator handle, one per `ActorSystem`.
#[pyclass(name = "Replicator", module = "atomr._native.ddata")]
pub struct PyReplicator {
    state: Arc<ReplicatorState>,
}

#[pymethods]
impl PyReplicator {
    /// `Replicator.get(system)` — lazy spawn the replicator actor.
    #[staticmethod]
    fn get(py: Python<'_>, system: Py<PyActorSystem>) -> PyResult<Py<Self>> {
        let state = {
            let sys_ref = system.borrow(py);
            get_or_spawn_replicator(&sys_ref)
        };
        Py::new(py, PyReplicator { state })
    }

    /// Replace the replicator's durable store. Use only at startup —
    /// spawns a fresh replicator and registers it under `system`. Any
    /// previously-stored values not on disk are lost.
    #[staticmethod]
    fn with_durable_store(
        py: Python<'_>,
        system: Py<PyActorSystem>,
        store: Py<PyDurableStore>,
    ) -> PyResult<Py<Self>> {
        let new_store = store.borrow(py).clone();
        let key = {
            let sys_ref = system.borrow(py);
            sys_ref.inner.name().to_string()
        };
        let _rt_guard = runtime().enter();
        let actor = ReplicatorActor::spawn_with(new_store.kind.as_dyn());
        let state = Arc::new(ReplicatorState {
            actor,
            durable: new_store,
            subscribers: Mutex::new(HashMap::new()),
            _tokens: Mutex::new(HashMap::new()),
        });
        REPLICATORS.lock().insert(key, state.clone());
        Py::new(py, PyReplicator { state })
    }

    #[getter]
    fn durable(&self, py: Python<'_>) -> PyResult<Py<PyDurableStore>> {
        Py::new(py, self.state.durable.clone())
    }

    /// `await update(key, initial, modify_fn, write_consistency)`.
    /// `initial` is one of the CRDT classes (`GCounter`, `LwwRegister`,
    /// …); `modify_fn` is a sync or async callable that receives the
    /// current CRDT wrapper, mutates it in place (or returns a new
    /// instance), and the resulting state is merged back.
    #[pyo3(signature = (key, initial, modify_fn, write_consistency=None))]
    fn update<'py>(
        &self,
        py: Python<'py>,
        key: String,
        initial: Py<PyAny>,
        modify_fn: Py<PyAny>,
        write_consistency: Option<Py<PyWriteConsistency>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kind = resolve_kind(py, &initial)?;
        let wc = write_consistency.map(|w| w.borrow(py).inner).unwrap_or(WriteConsistency::Local);
        let state = self.state.clone();
        let key_for_async = key.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // 1. Fetch current value (or fresh instance).
            let current_obj = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                fetch_current(py, &state, &key_for_async, kind)
            })?;

            // 2. Call user's modify_fn and `await` if needed.
            let modified = call_modify(modify_fn, current_obj.clone_ref_unchecked()).await?;

            // 3. Freeze (mark wrapper read-only) is best-effort: we
            //    simply don't reuse the wrapper. The user gets a fresh
            //    one next round.

            // 4. Submit to actor with the typed write path.
            let ack = submit_update(&state, &key_for_async, kind, modified, wc).await?;
            Python::with_gil(|py| {
                let s = match ack {
                    ReplicatorAck::Ok => "ok",
                    ReplicatorAck::KeyNotFound => "not-found",
                    ReplicatorAck::Timeout => "timeout",
                    _ => "unknown",
                };
                Ok::<Py<PyAny>, PyErr>(s.to_string().into_py(py))
            })
        })
    }

    /// `await get(key, read_consistency=ReadConsistency.local())`.
    /// Returns the typed CRDT wrapper, or `None` if absent. The CRDT
    /// type is inferred from the stored value.
    #[pyo3(signature = (key, kind, read_consistency=None))]
    fn get_value<'py>(
        &self,
        py: Python<'py>,
        key: String,
        kind: Py<PyAny>,
        read_consistency: Option<Py<PyReadConsistency>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let crdt_kind = resolve_kind(py, &kind)?;
        let _rc = read_consistency.map(|r| r.borrow(py).inner).unwrap_or(ReadConsistency::Local);
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = fetch_value_async(state.clone(), key, crdt_kind).await?;
            Python::with_gil(|py| match result {
                Some(obj) => Ok(obj),
                None => Ok(py.None()),
            })
        })
    }

    /// Delete a key.
    fn delete<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ack = state.actor.delete(key).await;
            Python::with_gil(|py| {
                let s = match ack {
                    ReplicatorAck::Ok => "ok",
                    _ => "timeout",
                };
                Ok::<Py<PyAny>, PyErr>(s.to_string().into_py(py))
            })
        })
    }

    /// All keys currently held.
    fn keys(&self) -> Vec<String> {
        self.state.actor.inner().keys()
    }

    /// Returns an async iterator yielding once per update / delete on
    /// `key`. Bounded mpsc with drop-oldest overflow.
    fn subscribe(&self, py: Python<'_>, key: String) -> PyResult<Py<PyReplicatorSubscription>> {
        let (tx, rx) = mpsc::channel::<()>(64);
        // Register the broadcast sender.
        self.state.subscribers.lock().entry(key.clone()).or_default().push(tx.clone());

        // Make sure we have a single underlying replicator-level
        // subscription per key that drives all our broadcast senders.
        let mut tokens = self.state._tokens.lock();
        if !tokens.contains_key(&key) {
            let state_w = Arc::downgrade(&self.state);
            let key_for_cb = key.clone();
            let token = self.state.actor.inner().subscribe(key.clone(), move |_k| {
                if let Some(state) = state_w.upgrade() {
                    let mut subs = state.subscribers.lock();
                    if let Some(list) = subs.get_mut(&key_for_cb) {
                        list.retain(|s| {
                            // drop-oldest: try_send; if the channel is
                            // full, drain one and try again.
                            match s.try_send(()) {
                                Ok(()) => true,
                                Err(mpsc::error::TrySendError::Full(_)) => true,
                                Err(mpsc::error::TrySendError::Closed(_)) => false,
                            }
                        });
                    }
                }
            });
            tokens.insert(key.clone(), token);
        }

        Py::new(py, PyReplicatorSubscription { rx: Arc::new(tokio::sync::Mutex::new(rx)), key })
    }
}

// -- helpers ---------------------------------------------------------

fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as u64).unwrap_or(0)
}

fn resolve_kind(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<CrdtKind> {
    let bound = obj.bind(py);
    let name: String = if let Ok(class_name) = bound.getattr("__name__") {
        class_name.extract()?
    } else if let Ok(cls) = bound.getattr("__class__") {
        cls.getattr("__name__")?.extract()?
    } else {
        return Err(PyErr::new::<PyValueError, _>("cannot determine CRDT kind from value"));
    };
    CrdtKind::from_class(&name).ok_or_else(|| {
        PyErr::new::<PyValueError, _>(format!(
            "unsupported CRDT type `{name}`; expected one of GCounter, PNCounter, GSet, \
             ORSet, LwwRegister, Flag, ORMap, LWWMap, PNCounterMap, ORMultiMap"
        ))
    })
}

fn fetch_current(py: Python<'_>, state: &ReplicatorState, key: &str, kind: CrdtKind) -> PyResult<Py<PyAny>> {
    let r = state.actor.inner();
    Ok(match kind {
        CrdtKind::GCounter => {
            let v: GCounter = r.get(key).unwrap_or_default();
            Py::new(py, PyGCounter { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::PNCounter => {
            let v: PNCounter = r.get(key).unwrap_or_default();
            Py::new(py, PyPNCounter { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::GSet => {
            let v: GSet<String> = r.get(key).unwrap_or_default();
            Py::new(py, PyGSet { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::ORSet => {
            let v: OrSet<String> = r.get(key).unwrap_or_default();
            Py::new(py, PyORSet { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::LwwRegister => {
            let v: LwwRegister<Vec<u8>> =
                r.get(key).unwrap_or_else(|| LwwRegister::new("local", Vec::new(), 0));
            Py::new(py, PyLwwRegister { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::Flag => {
            let v: Flag = r.get(key).unwrap_or_default();
            Py::new(py, PyFlag { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::ORMap => {
            let v: ORMap<String, LwwRegister<Vec<u8>>> = r.get(key).unwrap_or_default();
            Py::new(py, PyORMap { inner: Mutex::new(OrMapStorage::LwwReg(v)) })?.into_any()
        }
        CrdtKind::LWWMap => {
            let v: LWWMap<String, Vec<u8>> = r.get(key).unwrap_or_default();
            Py::new(py, PyLWWMap { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::PNCounterMap => {
            let v: PNCounterMap<String> = r.get(key).unwrap_or_default();
            Py::new(py, PyPNCounterMap { inner: Mutex::new(v) })?.into_any()
        }
        CrdtKind::ORMultiMap => {
            let v: ORMultiMap<String, String> = r.get(key).unwrap_or_default();
            Py::new(py, PyORMultiMap { inner: Mutex::new(v) })?.into_any()
        }
    })
}

trait CloneRefUnchecked {
    fn clone_ref_unchecked(&self) -> Self;
}

impl CloneRefUnchecked for Py<PyAny> {
    fn clone_ref_unchecked(&self) -> Self {
        Python::with_gil(|py| self.clone_ref(py))
    }
}

async fn call_modify(modify_fn: Py<PyAny>, current: Py<PyAny>) -> PyResult<Py<PyAny>> {
    // Call modify_fn(current); if it returns a coroutine, await it.
    let call_result: Py<PyAny> = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let bound = modify_fn.bind(py);
        let result = bound.call1((current.bind(py),))?;
        Ok(result.unbind())
    })?;

    let is_coro = Python::with_gil(|py| -> PyResult<bool> {
        let asyncio = py.import_bound("inspect")?;
        let is_co = asyncio.call_method1("iscoroutine", (call_result.bind(py),))?;
        is_co.extract::<bool>()
    })?;

    if is_coro {
        let fut = Python::with_gil(|py| -> PyResult<_> {
            let bound = call_result.bind(py);
            pyo3_async_runtimes::tokio::into_future(bound.clone())
        })?;
        let v = fut.await?;
        Ok(v)
    } else {
        Ok(call_result)
    }
}

async fn submit_update(
    state: &Arc<ReplicatorState>,
    key: &str,
    kind: CrdtKind,
    modified: Py<PyAny>,
    wc: WriteConsistency,
) -> PyResult<ReplicatorAck> {
    let key = key.to_string();
    Ok(match kind {
        CrdtKind::GCounter => {
            let value = Python::with_gil(|py| -> PyResult<GCounter> {
                let r = modified.bind(py);
                let cell: PyRef<PyGCounter> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::PNCounter => {
            let value = Python::with_gil(|py| -> PyResult<PNCounter> {
                let r = modified.bind(py);
                let cell: PyRef<PyPNCounter> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::GSet => {
            let value = Python::with_gil(|py| -> PyResult<GSet<String>> {
                let r = modified.bind(py);
                let cell: PyRef<PyGSet> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::ORSet => {
            let value = Python::with_gil(|py| -> PyResult<OrSet<String>> {
                let r = modified.bind(py);
                let cell: PyRef<PyORSet> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::LwwRegister => {
            let value = Python::with_gil(|py| -> PyResult<LwwRegister<Vec<u8>>> {
                let r = modified.bind(py);
                let cell: PyRef<PyLwwRegister> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::Flag => {
            let value = Python::with_gil(|py| -> PyResult<Flag> {
                let r = modified.bind(py);
                let cell: PyRef<PyFlag> = r.extract()?;
                let snapshot = *cell.inner.lock();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::ORMap => {
            let value = Python::with_gil(|py| -> PyResult<ORMap<String, LwwRegister<Vec<u8>>>> {
                let r = modified.bind(py);
                let cell: PyRef<PyORMap> = r.extract()?;
                let g = cell.inner.lock();
                match &*g {
                    OrMapStorage::LwwReg(m) => Ok(m.clone()),
                    other => Err(PyErr::new::<PyValueError, _>(format!(
                        "Replicator.update only supports ORMap[LwwRegister]; got ORMap[{}]. \
                         Use of_lww_register() to construct a Replicator-compatible ORMap.",
                        other.variant_name(),
                    ))),
                }
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::LWWMap => {
            let value = Python::with_gil(|py| -> PyResult<LWWMap<String, Vec<u8>>> {
                let r = modified.bind(py);
                let cell: PyRef<PyLWWMap> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::PNCounterMap => {
            let value = Python::with_gil(|py| -> PyResult<PNCounterMap<String>> {
                let r = modified.bind(py);
                let cell: PyRef<PyPNCounterMap> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
        CrdtKind::ORMultiMap => {
            let value = Python::with_gil(|py| -> PyResult<ORMultiMap<String, String>> {
                let r = modified.bind(py);
                let cell: PyRef<PyORMultiMap> = r.extract()?;
                let snapshot = cell.inner.lock().clone();
                Ok(snapshot)
            })?;
            persist(&state.durable, &key, &value);
            state.actor.update(key, value, wc).await
        }
    })
}

fn persist<T: serde::Serialize>(store: &PyDurableStore, key: &str, value: &T) {
    if let DurableStoreKind::File(s) = &store.kind {
        if let Ok(blob) = serde_json::to_vec(value) {
            let _ = s.persist(key, &blob);
        }
    }
}

async fn fetch_value_async(
    state: Arc<ReplicatorState>,
    key: String,
    kind: CrdtKind,
) -> PyResult<Option<Py<PyAny>>> {
    let inner = state.actor.inner().clone();
    match kind {
        CrdtKind::GCounter => inner.get::<GCounter>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyGCounter { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::PNCounter => inner.get::<PNCounter>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyPNCounter { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::GSet => inner.get::<GSet<String>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyGSet { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::ORSet => inner.get::<OrSet<String>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyORSet { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::LwwRegister => inner.get::<LwwRegister<Vec<u8>>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyLwwRegister { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::Flag => inner.get::<Flag>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyFlag { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::ORMap => inner.get::<ORMap<String, LwwRegister<Vec<u8>>>>(&key).map(|v| {
            Python::with_gil(|py| {
                Py::new(py, PyORMap { inner: Mutex::new(OrMapStorage::LwwReg(v)) }).map(|p| p.into_any())
            })
        }),
        CrdtKind::LWWMap => inner.get::<LWWMap<String, Vec<u8>>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyLWWMap { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::PNCounterMap => inner.get::<PNCounterMap<String>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyPNCounterMap { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
        CrdtKind::ORMultiMap => inner.get::<ORMultiMap<String, String>>(&key).map(|v| {
            Python::with_gil(|py| Py::new(py, PyORMultiMap { inner: Mutex::new(v) }).map(|p| p.into_any()))
        }),
    }
    .transpose()
}

// =====================================================================
// Module registration
// =====================================================================

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "ddata")?;
    sub.add_class::<PyGCounter>()?;
    sub.add_class::<PyPNCounter>()?;
    sub.add_class::<PyGSet>()?;
    sub.add_class::<PyORSet>()?;
    sub.add_class::<PyLwwRegister>()?;
    sub.add_class::<PyFlag>()?;
    sub.add_class::<PyORMap>()?;
    sub.add_class::<PyLWWMap>()?;
    sub.add_class::<PyPNCounterMap>()?;
    sub.add_class::<PyORMultiMap>()?;
    sub.add_class::<PySignedSum>()?;
    sub.add_class::<PySignedSumMap>()?;
    sub.add_class::<PyPruningState>()?;
    sub.add_class::<PyWriteAggregator>()?;
    sub.add_class::<PyReadAggregator>()?;
    sub.add_class::<PyReadConsistency>()?;
    sub.add_class::<PyWriteConsistency>()?;
    sub.add_class::<PyDurableStore>()?;
    sub.add_class::<PyReplicator>()?;
    sub.add_class::<PyReplicatorSubscription>()?;
    sub.add_function(wrap_pyfunction!(pruning_phases, &sub)?)?;
    m.add_submodule(&sub)?;
    Ok(())
}
