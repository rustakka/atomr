//! Python streams submodule. Exposes the `atomr-streams` DSL through two
//! layers:
//!
//! 1. **Legacy i64 helpers** — `map_reduce`, `run_collect`, `run_fold`,
//!    `via_keep_alive`, `via_initial_delay`, `via_conflate`, `via_expand`,
//!    `merge_sorted_`, `merge_prioritized_`, `via_split_after_count`,
//!    `via_prefix_and_tail`, `via_recover_with_retries`, `via_select_error`.
//!    Kept for backward compatibility.
//!
//! 2. **Typed DSL** on `Py<PyAny>` — `Source`, `Sink`, `Flow`, `RunnableGraph`,
//!    `KillSwitch`, `BroadcastHub`, `MergeHub`, `SourceQueue`, `SinkQueue`,
//!    plus `QueueOfferResult`. Stream callbacks acquire the GIL inline on the
//!    materializer dispatcher; element drops happen inside `Python::with_gil`
//!    via the `SendPyAny` newtype.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use atomr_core::time::{LogicalTime, ManualClock as RustManualClock};
use atomr_streams::{
    conflate, expand, initial_delay, keep_alive, merge_prioritized, merge_sorted, prefix_and_tail,
    recover_with_retries, select_error, split_after, token_bucket, ActorMaterializer,
    BidiFlow as RustBidiFlow, BroadcastHub as RustBroadcastHub, FileIO as RustFileIO, Flow,
    Framing as RustFraming, KillSwitch as RustKillSwitch, MergeHub as RustMergeHub,
    QueueOfferResult as RustOfferResult, RestartSettings as RustRestartSettings,
    RestartSource as RustRestartSource, Sink, SinkQueue as RustSinkQueue, Source, SupervisionDirective,
    Tcp as RustTcp,
};
use bytes::Bytes;
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3_async_runtimes::tokio as pyo3_tokio;

use crate::runtime::runtime;

// =============================================================================
// SendPyAny — Send/Sync wrapper around Py<PyAny> with GIL-safe Drop and Clone.
// =============================================================================

/// Newtype wrapping `Py<PyAny>` so it can travel through `Send + 'static`
/// channels (futures, tokio mpsc) safely. `Py<PyAny>` is `Send` already, but
/// dropping or cloning it without the GIL panics — every site that may drop
/// must acquire the GIL.
pub struct SendPyAny(pub Py<PyAny>);

// SAFETY: `Py<PyAny>` is `Send`. We are explicit here because some sites
// require `Sync` (broadcast::Sender bounds). Access is internally serialised
// by acquiring the GIL.
unsafe impl Send for SendPyAny {}
unsafe impl Sync for SendPyAny {}

impl SendPyAny {
    pub fn new(obj: Py<PyAny>) -> Self {
        Self(obj)
    }

    pub fn into_inner(self) -> Py<PyAny> {
        // We must avoid running our custom Drop after extracting; use
        // ManuallyDrop to skip the GIL re-acquire when the inner value
        // is already being moved into an owning context.
        let inner = unsafe { std::ptr::read(&self.0) };
        std::mem::forget(self);
        inner
    }

    pub fn as_py(&self) -> &Py<PyAny> {
        &self.0
    }
}

impl Drop for SendPyAny {
    fn drop(&mut self) {
        // Acquiring the GIL ensures that the inner Py<PyAny>'s Drop, which
        // runs inside this scope, can decrement the refcount safely. If the
        // GIL is already held by this thread, with_gil is reentrant.
        Python::with_gil(|_py| {
            // The compiler-generated drop of Py<PyAny> runs at end of scope.
        });
    }
}

impl Clone for SendPyAny {
    fn clone(&self) -> Self {
        Python::with_gil(|py| SendPyAny(self.0.clone_ref(py)))
    }
}

fn pylist_from_send<'py>(py: Python<'py>, items: Vec<SendPyAny>) -> Bound<'py, PyList> {
    let list = PyList::empty_bound(py);
    for item in items {
        let obj = item.into_inner();
        let _ = list.append(obj.bind(py));
    }
    list
}

// =============================================================================
// Builder enums — pipeline parts are constructed lazily and consumed once.
// =============================================================================

/// Source-side stages applied to an upstream `Source<SendPyAny>`.
enum SourceStage {
    /// Composition with a (already-materialized) `Flow<SendPyAny, SendPyAny>`
    /// builder, applied as a one-shot.
    Flow(FlowBuilder),
}

/// Lazy Source builder. We keep the build closure boxed because
/// `Source<T>` itself owns a `BoxStream` which is not `Clone`.
struct SourceBuilder {
    build: Box<dyn FnOnce() -> Source<SendPyAny> + Send + 'static>,
}

impl SourceBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> Source<SendPyAny> + Send + 'static,
    {
        Self { build: Box::new(f) }
    }

    fn build(self) -> Source<SendPyAny> {
        (self.build)()
    }

    fn apply(self, stage: SourceStage) -> Self {
        match stage {
            SourceStage::Flow(flow) => SourceBuilder::new(move || {
                let src = (self.build)();
                let f: Flow<SendPyAny, SendPyAny> = flow.build();
                src.via(f)
            }),
        }
    }
}

/// Lazy Flow builder. Composing flows in Rust requires consuming both —
/// we represent composition by stacking transforms.
struct FlowBuilder {
    build: Box<dyn FnOnce() -> Flow<SendPyAny, SendPyAny> + Send + 'static>,
}

impl FlowBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce() -> Flow<SendPyAny, SendPyAny> + Send + 'static,
    {
        Self { build: Box::new(f) }
    }

    fn build(self) -> Flow<SendPyAny, SendPyAny> {
        (self.build)()
    }

    fn via(self, next: FlowBuilder) -> FlowBuilder {
        FlowBuilder::new(move || {
            let a = (self.build)();
            let b = (next.build)();
            a.via(b)
        })
    }
}

/// A Sink builder reduces a built `Source<SendPyAny>` into a future that
/// returns a Python materialized value.
type SinkRunner = Box<
    dyn FnOnce(Source<SendPyAny>) -> futures::future::BoxFuture<'static, PyResult<Py<PyAny>>>
        + Send
        + 'static,
>;

struct SinkBuilder {
    run: SinkRunner,
}

impl SinkBuilder {
    fn new<F>(f: F) -> Self
    where
        F: FnOnce(Source<SendPyAny>) -> futures::future::BoxFuture<'static, PyResult<Py<PyAny>>>
            + Send
            + 'static,
    {
        Self { run: Box::new(f) }
    }
}

// =============================================================================
// PySource
// =============================================================================

#[pyclass(name = "Source", module = "atomr._native.streams")]
pub struct PySource {
    builder: Mutex<Option<SourceBuilder>>,
}

impl PySource {
    fn from_builder(builder: SourceBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<SourceBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Source has already been consumed"))
    }
}

#[pymethods]
impl PySource {
    /// `Source.from_iter(iterable)` — materialize a Python iterable into
    /// a Vec<SendPyAny>. Materialization is eager so the iterable is
    /// drained while we still hold the GIL.
    #[staticmethod]
    fn from_iter(py: Python<'_>, iterable: Bound<'_, PyAny>) -> PyResult<Self> {
        let mut items: Vec<SendPyAny> = Vec::new();
        let it = iterable.iter()?;
        for v in it {
            let v = v?;
            items.push(SendPyAny::new(v.unbind()));
        }
        let _ = py;
        Ok(PySource::from_builder(SourceBuilder::new(move || Source::from_iter(items))))
    }

    /// `Source.empty()` — empty source.
    #[staticmethod]
    fn empty() -> Self {
        PySource::from_builder(SourceBuilder::new(Source::empty))
    }

    /// `Source.single(value)` — single-element source.
    #[staticmethod]
    fn single(value: Bound<'_, PyAny>) -> Self {
        let val = SendPyAny::new(value.unbind());
        PySource::from_builder(SourceBuilder::new(move || Source::single(val)))
    }

    /// `Source.from_queue()` — returns `(Source, SourceQueue)` with an
    /// unbounded mpsc backing.
    #[staticmethod]
    fn from_queue(py: Python<'_>) -> PyResult<(Py<PySource>, Py<PySourceQueue>)> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SendPyAny>();
        let receiver_slot: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<SendPyAny>>>> =
            Arc::new(Mutex::new(Some(rx)));
        let receiver_for_build = Arc::clone(&receiver_slot);
        let source = PySource::from_builder(SourceBuilder::new(move || {
            let rx_opt = receiver_for_build.lock().take();
            match rx_opt {
                Some(rx) => Source::from_receiver(rx),
                None => Source::empty(),
            }
        }));
        let queue = PySourceQueue { tx: Mutex::new(Some(tx)) };
        Ok((Py::new(py, source)?, Py::new(py, queue)?))
    }

    /// `source.via(flow)` — append a flow stage. Consumes both `self`
    /// and `flow`, returning a new `Source`.
    fn via(&self, py: Python<'_>, flow: Py<PyFlow>) -> PyResult<Py<PySource>> {
        let src_b = self.take_builder()?;
        let flow_b = flow.borrow_mut(py).take_builder()?;
        let new = PySource::from_builder(src_b.apply(SourceStage::Flow(flow_b)));
        Py::new(py, new)
    }

    /// `source.to(sink)` — build a `RunnableGraph`. Consumes both sides.
    fn to(&self, py: Python<'_>, sink: Py<PySink>) -> PyResult<Py<PyRunnableGraph>> {
        let src_b = self.take_builder()?;
        let sink_b = sink.borrow_mut(py).take_builder()?;
        let graph = PyRunnableGraph::new(src_b, sink_b);
        Py::new(py, graph)
    }

    /// `source.run_collect()` — convenience: run with a collecting Sink.
    /// Returns awaitable yielding a `list`.
    fn run_collect_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let src_b = self.take_builder()?;
        pyo3_tokio::future_into_py(py, async move {
            let src = src_b.build();
            let collected = Sink::collect(src).await;
            Python::with_gil(|py| Ok(pylist_from_send(py, collected).unbind().into_any()))
        })
    }

    /// `source.token_bucket(rate_per_sec, burst)` — rate-limit emission to
    /// `rate_per_sec` elements/second, allowing short bursts up to `burst`
    /// tokens. Consumes and returns a new `Source`.
    #[pyo3(signature = (rate_per_sec, burst=1))]
    fn token_bucket(&self, py: Python<'_>, rate_per_sec: f64, burst: u32) -> PyResult<Py<PySource>> {
        let src_b = self.take_builder()?;
        let rate = rate_per_sec.max(0.0);
        let new_b = SourceBuilder::new(move || token_bucket(src_b.build(), rate, burst));
        Py::new(py, PySource::from_builder(new_b))
    }

    /// `source.kill_switch()` — wrap with a fresh `KillSwitch`. Returns
    /// `(Source, KillSwitch)`.
    fn kill_switch(&self, py: Python<'_>) -> PyResult<(Py<PySource>, Py<PyKillSwitch>)> {
        let src_b = self.take_builder()?;
        let ks = RustKillSwitch::new();
        let ks_clone = ks.clone();
        let new_b = SourceBuilder::new(move || ks_clone.flow(src_b.build()));
        let py_ks = PyKillSwitch { inner: ks };
        Ok((Py::new(py, PySource::from_builder(new_b))?, Py::new(py, py_ks)?))
    }
}

// =============================================================================
// PyFlow
// =============================================================================

#[pyclass(name = "Flow", module = "atomr._native.streams")]
pub struct PyFlow {
    builder: Mutex<Option<FlowBuilder>>,
}

impl PyFlow {
    fn from_builder(builder: FlowBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<FlowBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Flow has already been consumed"))
    }
}

#[pymethods]
impl PyFlow {
    /// Identity flow.
    #[staticmethod]
    fn identity() -> Self {
        PyFlow::from_builder(FlowBuilder::new(Flow::identity))
    }

    /// `Flow.from_fn(fn)` — synchronous mapping callable.
    #[staticmethod]
    fn from_fn(py: Python<'_>, func: Py<PyAny>) -> Self {
        let _ = py;
        PyFlow::from_builder(FlowBuilder::new(move || {
            let func = func;
            Flow::from_fn(move |x: SendPyAny| -> SendPyAny {
                Python::with_gil(|py| {
                    let inner = x.into_inner();
                    let arg = inner.bind(py);
                    match func.call1(py, (arg,)) {
                        Ok(out) => SendPyAny::new(out),
                        Err(e) => {
                            // Surface as None on error; preserve consistency with
                            // i64 wrappers which fall back on errors. Tests
                            // exercise the happy path.
                            e.restore(py);
                            SendPyAny::new(py.None())
                        }
                    }
                })
            })
        }))
    }

    /// `Flow.map(fn)` — alias of `from_fn` for symmetry with Source.via.
    #[staticmethod]
    fn map(py: Python<'_>, func: Py<PyAny>) -> Self {
        Self::from_fn(py, func)
    }

    /// `Flow.filter(pred)` — drop elements where `pred(x)` is falsy.
    /// Drops happen inside `Python::with_gil` because the predicate runs
    /// inside the GIL and the dropped `SendPyAny`'s own Drop also takes
    /// the GIL — defensive but cheap (with_gil is reentrant).
    #[staticmethod]
    fn filter(py: Python<'_>, pred: Py<PyAny>) -> Self {
        let _ = py;
        PyFlow::from_builder(FlowBuilder::new(move || {
            let pred = pred;
            Flow::<SendPyAny, SendPyAny>::filter(move |v: &SendPyAny| -> bool {
                Python::with_gil(|py| match pred.call1(py, (v.0.bind(py),)) {
                    Ok(b) => b.is_truthy(py).unwrap_or(false),
                    Err(e) => {
                        e.restore(py);
                        false
                    }
                })
            })
        }))
    }

    /// `Flow.take(n)` — limit to first `n` elements.
    #[staticmethod]
    fn take(n: usize) -> Self {
        PyFlow::from_builder(FlowBuilder::new(move || Flow::<SendPyAny, SendPyAny>::take(n)))
    }

    /// `Flow.skip(n)` — drop first `n` elements.
    #[staticmethod]
    fn skip(n: usize) -> Self {
        PyFlow::from_builder(FlowBuilder::new(move || Flow::<SendPyAny, SendPyAny>::skip(n)))
    }

    /// `Flow.map_async(fn, parallelism=1)` — fn returns an awaitable.
    /// Parallelism is bounded; outputs preserve upstream order.
    #[staticmethod]
    #[pyo3(signature = (func, parallelism=1))]
    fn map_async(py: Python<'_>, func: Py<PyAny>, parallelism: usize) -> Self {
        let _ = py;
        let p = parallelism.max(1);
        PyFlow::from_builder(FlowBuilder::new(move || {
            let func = func;
            Flow::<SendPyAny, SendPyAny>::map_async(p, move |x: SendPyAny| {
                let func = Python::with_gil(|py| func.clone_ref(py));
                async move {
                    // Call into Python under the GIL, convert to an asyncio
                    // future via pyo3-async-runtimes, then await it.
                    let py_fut = Python::with_gil(|py| -> PyResult<_> {
                        let coro = func.call1(py, (x.0.bind(py),))?;
                        pyo3_tokio::into_future(coro.bind(py).clone())
                    });
                    let result: PyResult<Py<PyAny>> = match py_fut {
                        Ok(fut) => fut.await,
                        Err(e) => Err(e),
                    };
                    match result {
                        Ok(v) => SendPyAny::new(v),
                        Err(e) => Python::with_gil(|py| {
                            e.restore(py);
                            SendPyAny::new(py.None())
                        }),
                    }
                }
            })
        }))
    }

    /// `Flow.via(other)` — compose two flows.
    fn via(&self, py: Python<'_>, other: Py<PyFlow>) -> PyResult<Py<PyFlow>> {
        let a = self.take_builder()?;
        let b = other.borrow_mut(py).take_builder()?;
        Py::new(py, PyFlow::from_builder(a.via(b)))
    }

    /// `Flow.try_map(fn)` — like `Flow.from_fn` but propagates Python
    /// exceptions into the supervised stream. Companion of
    /// `with_supervision`. Without supervision installed downstream, a
    /// raised exception substitutes `None` (matching `Flow.from_fn`).
    ///
    /// On error, the substituted output is a special "error sentinel"
    /// pair `(None, py_err_class_path)` that `with_supervision` recognises
    /// and applies the decider to. Without `with_supervision`, the
    /// sentinel is observable as a 2-tuple downstream.
    #[staticmethod]
    fn try_map(py: Python<'_>, func: Py<PyAny>) -> Self {
        let _ = py;
        PyFlow::from_builder(FlowBuilder::new(move || {
            let func = func;
            Flow::<SendPyAny, SendPyAny>::from_fn(move |x: SendPyAny| -> SendPyAny {
                Python::with_gil(|py| {
                    let inner = x.into_inner();
                    let arg = inner.bind(py);
                    match func.call1(py, (arg,)) {
                        Ok(out) => SendPyAny::new(out),
                        Err(e) => {
                            // Build an error sentinel: a 2-tuple
                            // `("__atomr_stream_error__", class_path)`
                            // recognisable by `with_supervision` below.
                            let ty = e.get_type_bound(py);
                            let module: String = ty
                                .getattr("__module__")
                                .and_then(|m| m.extract::<String>())
                                .unwrap_or_default();
                            let qual: String = ty
                                .getattr("__qualname__")
                                .and_then(|q| q.extract::<String>())
                                .or_else(|_| ty.getattr("__name__").and_then(|n| n.extract::<String>()))
                                .unwrap_or_default();
                            let class_path = if module.is_empty() || module == "builtins" {
                                qual
                            } else {
                                format!("{module}.{qual}")
                            };
                            let tup = pyo3::types::PyTuple::new_bound(
                                py,
                                ["__atomr_stream_error__".into_py(py), class_path.into_py(py)],
                            );
                            // Clear the error so it doesn't leak; the
                            // sentinel encodes the type for the supervision
                            // decider.
                            e.restore(py);
                            let _ = PyErr::take(py);
                            SendPyAny::new(tup.unbind().into_any())
                        }
                    }
                })
            })
        }))
    }

    /// `flow.with_supervision(decider, default="stop")` — wrap a flow so
    /// any Python exception raised inside an upstream `try_map` callable is
    /// routed through the decider. Errors mapped to `resume` or `restart`
    /// drop the failing element; `stop` terminates the stream.
    ///
    /// `decider` is a list of `(exception_class_path, "resume" | "restart" |
    /// "stop")` pairs. `class_path` is matched against
    /// `<module>.<qualname>` and falls back to the bare class name.
    #[pyo3(signature = (decider=None, default=None))]
    fn with_supervision(
        &self,
        py: Python<'_>,
        decider: Option<Vec<(String, String)>>,
        default: Option<String>,
    ) -> PyResult<Py<PyFlow>> {
        let mut rules: HashMap<String, SupervisionDirective> = HashMap::new();
        if let Some(rs) = decider {
            for (k, v) in rs {
                rules.insert(k, parse_stream_directive(&v)?);
            }
        }
        let default_dir = match default.as_deref() {
            Some(s) => parse_stream_directive(s)?,
            None => SupervisionDirective::Stop,
        };
        let prev = self.take_builder()?;
        // Use a shared bool to signal "stop" — once set, downstream elements
        // are dropped. We deliberately keep this simple: we use Source
        // operations on the materialised Flow so the implementation only
        // depends on public API.
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let new_builder = FlowBuilder::new(move || {
            let inner = prev.build();
            let stopped = Arc::clone(&stopped);
            // Compose: inner_flow -> filter_map_flow that interprets the
            // try_map error sentinel and applies the decider.
            let supervisor = Flow::<SendPyAny, Vec<SendPyAny>>::from_fn(move |item: SendPyAny| {
                if stopped.load(std::sync::atomic::Ordering::SeqCst) {
                    // Drop subsequent elements.
                    return Vec::new();
                }
                Python::with_gil(|py| {
                    let bound = item.0.bind(py);
                    // Detect the try_map error sentinel.
                    let is_err =
                        bound.extract::<(String, String)>().ok().map(|(t, _)| t == "__atomr_stream_error__");
                    if let Some(true) = is_err {
                        // Extract class_path and apply decider.
                        let (_, class_path) = bound.extract::<(String, String)>().unwrap();
                        let directive = rules
                            .get(&class_path)
                            .copied()
                            .or_else(|| {
                                // Bare classname fallback.
                                let bare = class_path.rsplit('.').next().unwrap_or(&class_path);
                                rules.get(bare).copied()
                            })
                            .unwrap_or(default_dir);
                        match directive {
                            SupervisionDirective::Stop => {
                                stopped.store(true, std::sync::atomic::Ordering::SeqCst);
                                Vec::new()
                            }
                            // Both Resume and Restart: drop the failing
                            // element. (Restart for stateless ops behaves
                            // like Resume.)
                            _ => Vec::new(),
                        }
                    } else {
                        vec![item]
                    }
                })
            });
            // Convert Vec<SendPyAny> back to SendPyAny via flat_map_concat.
            let flatten =
                Flow::<Vec<SendPyAny>, SendPyAny>::flat_map_concat(|v: Vec<SendPyAny>| -> Vec<SendPyAny> {
                    v
                });
            inner.via(supervisor).via(flatten)
        });
        Py::new(py, PyFlow::from_builder(new_builder))
    }
}

// =============================================================================
// PySink
// =============================================================================

#[pyclass(name = "Sink", module = "atomr._native.streams")]
pub struct PySink {
    builder: Mutex<Option<SinkBuilder>>,
}

impl PySink {
    fn from_builder(builder: SinkBuilder) -> Self {
        Self { builder: Mutex::new(Some(builder)) }
    }

    fn take_builder(&self) -> PyResult<SinkBuilder> {
        self.builder
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Sink has already been consumed"))
    }
}

#[pymethods]
impl PySink {
    /// `Sink.collect()` — gather elements into a list.
    #[staticmethod]
    fn collect() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                let v = Sink::collect(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    Ok(pylist_from_send(py, v).unbind().into_any())
                })
            }
            .boxed()
        }))
    }

    /// `Sink.fold(zero, fn)` — accumulate with a Python reducer.
    #[staticmethod]
    fn fold(_py: Python<'_>, zero: Bound<'_, PyAny>, func: Py<PyAny>) -> Self {
        use futures::FutureExt;
        let zero = SendPyAny::new(zero.unbind());
        PySink::from_builder(SinkBuilder::new(move |src: Source<SendPyAny>| {
            async move {
                let func = func;
                let acc = Sink::fold(src, zero, move |acc: SendPyAny, v: SendPyAny| -> SendPyAny {
                    Python::with_gil(|py| {
                        let acc_obj = acc.into_inner();
                        let v_obj = v.into_inner();
                        match func.call1(py, (acc_obj.bind(py), v_obj.bind(py))) {
                            Ok(r) => SendPyAny::new(r),
                            Err(e) => {
                                e.restore(py);
                                SendPyAny::new(py.None())
                            }
                        }
                    })
                })
                .await;
                Python::with_gil(|_py| -> PyResult<Py<PyAny>> { Ok(acc.into_inner()) })
            }
            .boxed()
        }))
    }

    /// `Sink.foreach(fn)` — call `fn(x)` for each element.
    /// Materialized value is `None`.
    #[staticmethod]
    fn foreach(_py: Python<'_>, func: Py<PyAny>) -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(move |src: Source<SendPyAny>| {
            async move {
                let func = func;
                Sink::for_each(src, move |x: SendPyAny| {
                    Python::with_gil(|py| {
                        if let Err(e) = func.call1(py, (x.0.bind(py),)) {
                            e.restore(py);
                        }
                        // `x` drops here, with the GIL held.
                    })
                })
                .await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(py.None()) })
            }
            .boxed()
        }))
    }

    /// `Sink.head_option()` — produce the first element (or None).
    #[staticmethod]
    fn head_option() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                let head = Sink::first(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    match head {
                        Some(v) => Ok(v.into_inner()),
                        None => Ok(py.None()),
                    }
                })
            }
            .boxed()
        }))
    }

    /// `Sink.ignore()` — drain and produce None.
    #[staticmethod]
    fn ignore() -> Self {
        use futures::FutureExt;
        PySink::from_builder(SinkBuilder::new(|src: Source<SendPyAny>| {
            async move {
                Sink::ignore(src).await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(py.None()) })
            }
            .boxed()
        }))
    }

    /// `Sink.queue(source)` — drive a source through a pull-based queue.
    /// Static method that takes a Source and returns a SinkQueue handle
    /// (this is the dual of `Source.from_queue`, exposing pull semantics).
    #[staticmethod]
    fn queue(py: Python<'_>, source: Py<PySource>) -> PyResult<Py<PySinkQueue>> {
        let src_b = source.borrow_mut(py).take_builder()?;
        // We need to build the source on a Tokio runtime entry; do it now,
        // since SinkQueue spawns its own task internally.
        let rt = runtime();
        let _g = rt.enter();
        let q = Sink::queue(src_b.build());
        Py::new(py, PySinkQueue { inner: Arc::new(q) })
    }
}

// =============================================================================
// PyRunnableGraph
// =============================================================================

#[pyclass(name = "RunnableGraph", module = "atomr._native.streams")]
pub struct PyRunnableGraph {
    state: Mutex<Option<(SourceBuilder, SinkBuilder)>>,
}

impl PyRunnableGraph {
    fn new(src: SourceBuilder, sink: SinkBuilder) -> Self {
        Self { state: Mutex::new(Some((src, sink))) }
    }
}

#[pymethods]
impl PyRunnableGraph {
    /// `graph.run()` — return an awaitable that yields the materialized value.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pair = self
            .state
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("RunnableGraph already run"))?;
        let (src_b, sink_b) = pair;
        pyo3_tokio::future_into_py(py, async move {
            let src = src_b.build();
            (sink_b.run)(src).await
        })
    }

    /// `graph.run_blocking()` — block the calling thread; convenient for
    /// scripts and tests.
    fn run_blocking(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let pair = self
            .state
            .lock()
            .take()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("RunnableGraph already run"))?;
        let (src_b, sink_b) = pair;
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let src = src_b.build();
                (sink_b.run)(src).await
            })
        })
    }
}

// =============================================================================
// KillSwitch
// =============================================================================

#[pyclass(name = "KillSwitch", module = "atomr._native.streams")]
pub struct PyKillSwitch {
    inner: RustKillSwitch,
}

#[pymethods]
impl PyKillSwitch {
    #[new]
    fn new() -> Self {
        Self { inner: RustKillSwitch::new() }
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }

    #[pyo3(signature = (error_msg=None))]
    fn abort(&self, error_msg: Option<String>) {
        self.inner.abort(error_msg.unwrap_or_else(|| "aborted".to_string()));
    }

    fn is_shut_down(&self) -> bool {
        self.inner.is_shut_down()
    }

    fn error(&self) -> Option<String> {
        self.inner.error()
    }
}

// =============================================================================
// SourceQueue / SinkQueue
// =============================================================================

#[pyclass(name = "SourceQueue", module = "atomr._native.streams")]
pub struct PySourceQueue {
    tx: Mutex<Option<tokio::sync::mpsc::UnboundedSender<SendPyAny>>>,
}

#[pymethods]
impl PySourceQueue {
    /// `offer(value) -> str` — return one of `"Enqueued"`, `"QueueClosed"`.
    fn offer(&self, value: Bound<'_, PyAny>) -> PyResult<String> {
        let v = SendPyAny::new(value.unbind());
        let guard = self.tx.lock();
        match guard.as_ref() {
            Some(tx) => match tx.send(v) {
                Ok(()) => Ok(format!("{:?}", RustOfferResult::Enqueued)),
                Err(_) => Ok(format!("{:?}", RustOfferResult::QueueClosed)),
            },
            None => Ok(format!("{:?}", RustOfferResult::QueueClosed)),
        }
    }

    /// Close the queue so the source completes normally.
    fn complete(&self) {
        let _ = self.tx.lock().take();
    }

    fn is_closed(&self) -> bool {
        let guard = self.tx.lock();
        match guard.as_ref() {
            Some(tx) => tx.is_closed(),
            None => true,
        }
    }
}

#[pyclass(name = "SinkQueue", module = "atomr._native.streams")]
pub struct PySinkQueue {
    inner: Arc<RustSinkQueue<SendPyAny>>,
}

#[pymethods]
impl PySinkQueue {
    /// `pull()` — await the next element. Returns `None` once exhausted.
    fn pull<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_tokio::future_into_py(py, async move {
            let v = inner.pull().await;
            Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                match v {
                    Some(v) => Ok(v.into_inner()),
                    None => Ok(py.None()),
                }
            })
        })
    }

    /// Blocking pull — convenient for tests.
    fn pull_blocking(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = Arc::clone(&self.inner);
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let v = inner.pull().await;
                Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                    match v {
                        Some(v) => Ok(v.into_inner()),
                        None => Ok(py.None()),
                    }
                })
            })
        })
    }
}

// =============================================================================
// BroadcastHub / MergeHub
// =============================================================================

#[pyclass(name = "BroadcastHub", module = "atomr._native.streams")]
pub struct PyBroadcastHub {
    inner: Arc<RustBroadcastHub<SendPyAny>>,
}

#[pymethods]
impl PyBroadcastHub {
    /// `BroadcastHub.attach(source, buffer_size)` — drives `source`'s
    /// elements into a fan-out hub. Returns the hub.
    #[staticmethod]
    #[pyo3(signature = (source, buffer_size=16))]
    fn attach(py: Python<'_>, source: Py<PySource>, buffer_size: usize) -> PyResult<Py<PyBroadcastHub>> {
        let src_b = source.borrow_mut(py).take_builder()?;
        let hub = Arc::new(RustBroadcastHub::<SendPyAny>::new(buffer_size.max(1)));
        let rt = runtime();
        let _g = rt.enter();
        hub.attach(src_b.build());
        Py::new(py, PyBroadcastHub { inner: hub })
    }

    /// Acquire a fresh consumer source.
    fn consumer(&self, py: Python<'_>) -> PyResult<Py<PySource>> {
        let hub = Arc::clone(&self.inner);
        // `consumer()` requires being on a Tokio runtime; subscribe is sync
        // but the unfold runs lazily. Build closure must own the receiver.
        let src = hub.consumer();
        // `src` is a Source<SendPyAny>; we wrap it directly.
        let src_holder = Arc::new(Mutex::new(Some(src)));
        let src_holder_for_build = Arc::clone(&src_holder);
        let builder =
            SourceBuilder::new(move || src_holder_for_build.lock().take().unwrap_or_else(Source::empty));
        Py::new(py, PySource::from_builder(builder))
    }

    fn consumer_count(&self) -> usize {
        self.inner.consumer_count()
    }
}

#[pyclass(name = "MergeHub", module = "atomr._native.streams")]
pub struct PyMergeHub {
    inner: Arc<RustMergeHub<SendPyAny>>,
}

#[pymethods]
impl PyMergeHub {
    #[new]
    fn new() -> Self {
        Self { inner: Arc::new(RustMergeHub::new()) }
    }

    /// Attach a producer source.
    fn attach(&self, py: Python<'_>, source: Py<PySource>) -> PyResult<()> {
        let src_b = source.borrow_mut(py).take_builder()?;
        let rt = runtime();
        let _g = rt.enter();
        self.inner.attach(src_b.build());
        Ok(())
    }

    /// Take the merged consumer source.
    fn source(&self, py: Python<'_>) -> PyResult<Py<PySource>> {
        let s = self.inner.source();
        let holder = Arc::new(Mutex::new(Some(s)));
        let holder_b = Arc::clone(&holder);
        let builder = SourceBuilder::new(move || holder_b.lock().take().unwrap_or_else(Source::empty));
        Py::new(py, PySource::from_builder(builder))
    }
}

// =============================================================================
// RestartSource / RestartSettings
// =============================================================================

/// `RestartSource(min_backoff, max_backoff, random_factor, max_restarts)`.
///
/// Wraps `atomr_streams::RestartSource::with_backoff`. The factory callback is
/// a Python callable returning a fresh `Source` each time it is invoked. The
/// produced `Source` re-subscribes to the factory until completion or until
/// the configured `max_restarts` is reached.
#[pyclass(name = "RestartSource", module = "atomr._native.streams")]
pub struct PyRestartSource {
    settings: RustRestartSettings,
}

#[pymethods]
impl PyRestartSource {
    /// Build a `RestartSource` configuration. Times are in seconds (float).
    /// If `max_restarts` is `None`, the source restarts indefinitely.
    #[new]
    #[pyo3(signature = (min_backoff=0.1, max_backoff=30.0, random_factor=0.0, max_restarts=Some(5)))]
    fn new(min_backoff: f64, max_backoff: f64, random_factor: f64, max_restarts: Option<usize>) -> Self {
        Self {
            settings: RustRestartSettings {
                min_backoff: Duration::from_secs_f64(min_backoff.max(0.0)),
                max_backoff: Duration::from_secs_f64(max_backoff.max(0.0)),
                random_factor,
                max_restarts,
            },
        }
    }

    /// `via_source(source_factory)` — `source_factory` is a Python callable
    /// returning a fresh `Source`. Returns a `Source` that resubscribes to
    /// the factory according to the configured backoff policy.
    fn via_source(&self, py: Python<'_>, source_factory: Py<PyAny>) -> PyResult<Py<PySource>> {
        let settings = self.settings;
        let factory_holder = Arc::new(Mutex::new(source_factory));
        let builder = SourceBuilder::new(move || {
            let factory_holder = Arc::clone(&factory_holder);
            // The factory closure may be called multiple times; each call
            // invokes the Python callable to get a fresh Source.
            RustRestartSource::with_backoff(settings, move || -> Source<SendPyAny> {
                let factory = factory_holder.lock();
                Python::with_gil(|py| -> Source<SendPyAny> {
                    match factory.call0(py) {
                        Ok(obj) => match obj.extract::<Py<PySource>>(py) {
                            Ok(py_src) => match py_src.borrow_mut(py).take_builder() {
                                Ok(b) => b.build(),
                                Err(_) => Source::empty(),
                            },
                            Err(e) => {
                                e.restore(py);
                                Source::empty()
                            }
                        },
                        Err(e) => {
                            e.restore(py);
                            Source::empty()
                        }
                    }
                })
            })
        });
        Py::new(py, PySource::from_builder(builder))
    }
}

/// `RestartSettings` — read-only view onto the settings struct (mostly for
/// introspection from Python).
#[pyclass(name = "RestartSettings", module = "atomr._native.streams")]
pub struct PyRestartSettings {
    inner: RustRestartSettings,
}

#[pymethods]
impl PyRestartSettings {
    #[new]
    #[pyo3(signature = (min_backoff=0.1, max_backoff=30.0, random_factor=0.0, max_restarts=Some(5)))]
    fn new(min_backoff: f64, max_backoff: f64, random_factor: f64, max_restarts: Option<usize>) -> Self {
        Self {
            inner: RustRestartSettings {
                min_backoff: Duration::from_secs_f64(min_backoff.max(0.0)),
                max_backoff: Duration::from_secs_f64(max_backoff.max(0.0)),
                random_factor,
                max_restarts,
            },
        }
    }

    #[getter]
    fn min_backoff(&self) -> f64 {
        self.inner.min_backoff.as_secs_f64()
    }

    #[getter]
    fn max_backoff(&self) -> f64 {
        self.inner.max_backoff.as_secs_f64()
    }

    #[getter]
    fn random_factor(&self) -> f64 {
        self.inner.random_factor
    }

    #[getter]
    fn max_restarts(&self) -> Option<usize> {
        self.inner.max_restarts
    }
}

// =============================================================================
// Stream Decider (supervision)
// =============================================================================

/// Compile a `[(class_path, "resume" | "restart" | "stop")]` list (plus an
/// optional default) into a `Decider<PyErr>` that inspects the Python error
/// type via its `__module__` + `.` + `__qualname__`.
fn parse_stream_directive(s: &str) -> PyResult<SupervisionDirective> {
    match s {
        "resume" => Ok(SupervisionDirective::Resume),
        "restart" => Ok(SupervisionDirective::Restart),
        "stop" => Ok(SupervisionDirective::Stop),
        other => Err(PyValueError::new_err(format!(
            "unknown stream directive `{other}`; expected one of resume | restart | stop"
        ))),
    }
}

// =============================================================================
// GraphDsl
// =============================================================================

/// `GraphDsl` — minimal builder for fan-in / fan-out stream graphs.
///
/// The Python surface is intentionally linear-plus-junctions: `add()`
/// returns a port handle; `edge(from, to)` connects two handles; `run()`
/// materialises the assembled graph. Currently supports the linear pattern:
/// `Source -> Flow* -> Sink`.
#[pyclass(name = "GraphDsl", module = "atomr._native.streams")]
pub struct PyGraphDsl {
    nodes: Mutex<Vec<GraphNode>>,
    edges: Mutex<Vec<(usize, usize)>>,
}

enum GraphNode {
    Source(SourceBuilder),
    Flow(FlowBuilder),
    Sink(SinkBuilder),
    Consumed,
}

#[pymethods]
impl PyGraphDsl {
    #[new]
    fn new() -> Self {
        Self { nodes: Mutex::new(Vec::new()), edges: Mutex::new(Vec::new()) }
    }

    /// Add a `Source`, `Flow`, or `Sink` node and return its index.
    fn add(&self, py: Python<'_>, item: Py<PyAny>) -> PyResult<usize> {
        let bound = item.bind(py);
        let node = if let Ok(s) = bound.extract::<Py<PySource>>() {
            let b = s.borrow_mut(py).take_builder()?;
            GraphNode::Source(b)
        } else if let Ok(f) = bound.extract::<Py<PyFlow>>() {
            let b = f.borrow_mut(py).take_builder()?;
            GraphNode::Flow(b)
        } else if let Ok(k) = bound.extract::<Py<PySink>>() {
            let b = k.borrow_mut(py).take_builder()?;
            GraphNode::Sink(b)
        } else {
            return Err(PyValueError::new_err("GraphDsl.add expects a Source, Flow, or Sink"));
        };
        let mut nodes = self.nodes.lock();
        let idx = nodes.len();
        nodes.push(node);
        Ok(idx)
    }

    /// Connect node `from` to node `to`.
    fn edge(&self, from: usize, to: usize) -> PyResult<()> {
        let nodes = self.nodes.lock();
        if from >= nodes.len() || to >= nodes.len() {
            return Err(PyValueError::new_err("edge port index out of range"));
        }
        drop(nodes);
        self.edges.lock().push((from, to));
        Ok(())
    }

    /// Materialise and run the graph. Returns a Python awaitable that
    /// resolves to the materialised value of the (single) sink.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (src_b, flow_chain, sink_b) = self.compile()?;
        pyo3_tokio::future_into_py(py, async move {
            let mut src = src_b.build();
            for f in flow_chain {
                src = src.via(f.build());
            }
            (sink_b.run)(src).await
        })
    }

    /// Blocking `run()`.
    fn run_blocking(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let (src_b, flow_chain, sink_b) = self.compile()?;
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                let mut src = src_b.build();
                for f in flow_chain {
                    src = src.via(f.build());
                }
                (sink_b.run)(src).await
            })
        })
    }
}

impl PyGraphDsl {
    fn compile(&self) -> PyResult<(SourceBuilder, Vec<FlowBuilder>, SinkBuilder)> {
        let mut nodes = self.nodes.lock();
        let edges = self.edges.lock().clone();
        let n = nodes.len();
        let mut out_neighbors: Vec<Option<usize>> = vec![None; n];
        let mut in_neighbors: Vec<Option<usize>> = vec![None; n];
        for (a, b) in &edges {
            if out_neighbors[*a].is_some() || in_neighbors[*b].is_some() {
                return Err(PyValueError::new_err(
                    "GraphDsl currently supports only linear graphs (single in/out per node)",
                ));
            }
            out_neighbors[*a] = Some(*b);
            in_neighbors[*b] = Some(*a);
        }
        let src_idx = nodes
            .iter()
            .enumerate()
            .find_map(|(i, n)| matches!(n, GraphNode::Source(_)).then_some(i))
            .ok_or_else(|| PyValueError::new_err("GraphDsl requires at least one Source"))?;
        let sink_idx = nodes
            .iter()
            .enumerate()
            .find_map(|(i, n)| matches!(n, GraphNode::Sink(_)).then_some(i))
            .ok_or_else(|| PyValueError::new_err("GraphDsl requires at least one Sink"))?;
        let mut chain: Vec<usize> = Vec::new();
        let mut cur = out_neighbors[src_idx];
        while let Some(idx) = cur {
            if idx == sink_idx {
                break;
            }
            chain.push(idx);
            cur = out_neighbors[idx];
        }
        if cur != Some(sink_idx) {
            return Err(PyValueError::new_err("GraphDsl chain does not reach the Sink"));
        }
        let src_b = match std::mem::replace(&mut nodes[src_idx], GraphNode::Consumed) {
            GraphNode::Source(b) => b,
            _ => unreachable!(),
        };
        let mut flows = Vec::with_capacity(chain.len());
        for idx in chain {
            let b = match std::mem::replace(&mut nodes[idx], GraphNode::Consumed) {
                GraphNode::Flow(b) => b,
                _ => return Err(PyValueError::new_err("expected Flow node in chain")),
            };
            flows.push(b);
        }
        let sink_b = match std::mem::replace(&mut nodes[sink_idx], GraphNode::Consumed) {
            GraphNode::Sink(b) => b,
            _ => unreachable!(),
        };
        Ok((src_b, flows, sink_b))
    }
}

// =============================================================================
// BidiFlow
// =============================================================================

/// `BidiFlow.from_flows(forward, backward)` — wraps `atomr_streams::BidiFlow`
/// over two `Flow[PyAny -> PyAny]` directions. Both directions are kept as
/// `Py<PyAny>` to avoid the typing complexity of distinct In/Out types.
#[pyclass(name = "BidiFlow", module = "atomr._native.streams")]
pub struct PyBidiFlow {
    state: Mutex<Option<BidiState>>,
}

struct BidiState {
    forward: FlowBuilder,
    backward: FlowBuilder,
}

#[pymethods]
impl PyBidiFlow {
    #[staticmethod]
    fn from_flows(py: Python<'_>, forward: Py<PyFlow>, backward: Py<PyFlow>) -> PyResult<Self> {
        let f = forward.borrow_mut(py).take_builder()?;
        let b = backward.borrow_mut(py).take_builder()?;
        Ok(Self { state: Mutex::new(Some(BidiState { forward: f, backward: b })) })
    }

    /// Project the forward direction as a standalone `Flow`.
    fn forward(&self, py: Python<'_>) -> PyResult<Py<PyFlow>> {
        let mut g = self.state.lock();
        let s = g.as_mut().ok_or_else(|| PyRuntimeError::new_err("BidiFlow already projected"))?;
        let forward = std::mem::replace(
            &mut s.forward,
            FlowBuilder::new(|| Flow::<SendPyAny, SendPyAny>::from_fn(|x| x)),
        );
        Py::new(py, PyFlow::from_builder(forward))
    }

    /// Project the backward direction as a standalone `Flow`.
    fn backward(&self, py: Python<'_>) -> PyResult<Py<PyFlow>> {
        let mut g = self.state.lock();
        let s = g.as_mut().ok_or_else(|| PyRuntimeError::new_err("BidiFlow already projected"))?;
        let backward = std::mem::replace(
            &mut s.backward,
            FlowBuilder::new(|| Flow::<SendPyAny, SendPyAny>::from_fn(|x| x)),
        );
        Py::new(py, PyFlow::from_builder(backward))
    }

    /// Materialise the underlying Rust `BidiFlow`. Currently exposed for
    /// symmetry with the Rust API; consume forward/backward separately
    /// for ordinary streaming uses.
    fn join(&self) -> PyResult<()> {
        let mut g = self.state.lock();
        let s = g.take().ok_or_else(|| PyRuntimeError::new_err("BidiFlow already consumed"))?;
        let f = s.forward.build();
        let b = s.backward.build();
        let _bidi: RustBidiFlow<SendPyAny, SendPyAny, SendPyAny, SendPyAny> = RustBidiFlow::from_flows(f, b);
        Ok(())
    }
}

// =============================================================================
// Framing
// =============================================================================

/// `Framing` — codec-level binary framing utilities. Each `Framing.*`
/// factory returns a `Flow` over Python `bytes` objects: input chunks are
/// joined and split into framed messages.
#[pyclass(name = "Framing", module = "atomr._native.streams")]
pub struct PyFraming;

fn bytes_flow_from<F>(make: F) -> PyFlow
where
    F: FnOnce() -> Flow<Bytes, Result<Bytes, atomr_streams::FramingError>> + Send + 'static,
{
    PyFlow::from_builder(FlowBuilder::new(move || {
        // Decode SendPyAny -> Vec<Bytes> (zero or one item per element); a
        // non-bytes value is silently dropped.
        let to_bytes_vec = Flow::<SendPyAny, Vec<Bytes>>::from_fn(move |item: SendPyAny| {
            Python::with_gil(|py| {
                let inner = item.into_inner();
                let bound = inner.bind(py);
                match bound.extract::<&[u8]>() {
                    Ok(b) => vec![Bytes::copy_from_slice(b)],
                    Err(_) => Vec::new(),
                }
            })
        });
        let flatten_bytes = Flow::<Vec<Bytes>, Bytes>::flat_map_concat(|v: Vec<Bytes>| -> Vec<Bytes> { v });
        let framing_flow = make();
        // Drop framing errors; keep only ok'd frames.
        let drop_errs = Flow::<Result<Bytes, atomr_streams::FramingError>, Vec<Bytes>>::from_fn(
            |r: Result<Bytes, atomr_streams::FramingError>| -> Vec<Bytes> { r.ok().into_iter().collect() },
        );
        let flatten_ok = Flow::<Vec<Bytes>, Bytes>::flat_map_concat(|v: Vec<Bytes>| -> Vec<Bytes> { v });
        // Encode Bytes -> SendPyAny.
        let to_pybytes = Flow::<Bytes, SendPyAny>::from_fn(|b: Bytes| {
            Python::with_gil(|py| {
                let pb = PyBytes::new_bound(py, &b);
                SendPyAny::new(pb.unbind().into_any())
            })
        });
        to_bytes_vec.via(flatten_bytes).via(framing_flow).via(drop_errs).via(flatten_ok).via(to_pybytes)
    }))
}

#[pymethods]
impl PyFraming {
    /// `Framing.delimiter(delimiter, max_frame_length)` — split a bytes
    /// stream on a single-byte delimiter. `delimiter` must be a `bytes`
    /// object of length 1.
    #[staticmethod]
    fn delimiter(delimiter: &[u8], max_frame_length: usize) -> PyResult<PyFlow> {
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err("Framing.delimiter requires a single-byte delimiter"));
        }
        let d = delimiter[0];
        Ok(bytes_flow_from(move || RustFraming::delimiter(d, max_frame_length)))
    }

    /// `Framing.length_field(max_frame_length, field_length=4)` — split by
    /// a little-endian u32 length-prefix. `field_length` is documented for
    /// symmetry with the Akka API and is currently fixed at 4 bytes by the
    /// underlying framing codec.
    #[staticmethod]
    #[pyo3(signature = (max_frame_length, field_length=4))]
    fn length_field(max_frame_length: usize, field_length: usize) -> PyResult<PyFlow> {
        if field_length != 4 {
            return Err(PyValueError::new_err(
                "Framing.length_field currently supports a 4-byte little-endian length prefix only",
            ));
        }
        Ok(bytes_flow_from(move || RustFraming::length_field(max_frame_length)))
    }
}

// =============================================================================
// Tcp
// =============================================================================

/// `Tcp` — streaming socket adapters. Element type is `bytes` for both
/// directions.
#[pyclass(name = "Tcp", module = "atomr._native.streams")]
pub struct PyTcp;

#[pymethods]
impl PyTcp {
    /// `Tcp.outgoing(host, port)` — connect to `host:port`. Returns a
    /// `TcpOutgoing` handle exposing the read-side `Source[bytes]` and a
    /// `send(data)` method for the write side.
    #[staticmethod]
    fn outgoing(py: Python<'_>, host: String, port: u16) -> PyResult<Py<PyTcpOutgoing>> {
        let addr_str = format!("{host}:{port}");
        let addr: SocketAddr = addr_str.parse().map_err(|e: std::net::AddrParseError| {
            PyValueError::new_err(format!("invalid host:port `{addr_str}`: {e}"))
        })?;
        let rt = runtime();
        let conn = py
            .allow_threads(|| rt.block_on(async move { RustTcp::outgoing_connection(addr).await }))
            .map_err(|e| PyRuntimeError::new_err(format!("connect failed: {e}")))?;
        let writer = conn.writer.clone();
        // Bridge reader (Source<io::Result<Bytes>>) → mpsc<Bytes> via a
        // background tokio task; the channel becomes the source for Python.
        let (tx_bytes, rx_bytes) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
        let reader_src = conn.reader.filter_map(|r| r.ok());
        rt.spawn(async move {
            // Drain reader_src into tx_bytes via Sink::for_each.
            Sink::for_each(reader_src, move |b: Bytes| {
                let _ = tx_bytes.send(b);
            })
            .await;
        });
        let rx_holder = Arc::new(Mutex::new(Some(rx_bytes)));
        let rx_for_build = Arc::clone(&rx_holder);
        let reader_builder = SourceBuilder::new(move || {
            let rx = rx_for_build.lock().take();
            match rx {
                Some(rx) => Source::from_receiver(rx).map(|b: Bytes| {
                    Python::with_gil(|py| {
                        let pb = PyBytes::new_bound(py, &b);
                        SendPyAny::new(pb.unbind().into_any())
                    })
                }),
                None => Source::empty(),
            }
        });
        Py::new(
            py,
            PyTcpOutgoing {
                reader: Mutex::new(Some(reader_builder)),
                writer: Mutex::new(Some(writer)),
                remote_addr: conn.remote_addr.to_string(),
            },
        )
    }

    /// `Tcp.incoming(bind_addr)` — bind a listener and return a `Source` of
    /// `(remote_addr: str, data: bytes)` tuples for every chunk received
    /// from any client.
    #[staticmethod]
    fn incoming(py: Python<'_>, bind_addr: String) -> PyResult<Py<PySource>> {
        let addr: SocketAddr = bind_addr.parse().map_err(|e: std::net::AddrParseError| {
            PyValueError::new_err(format!("invalid bind_addr `{bind_addr}`: {e}"))
        })?;
        let rt = runtime();
        let incoming = py
            .allow_threads(|| rt.block_on(async move { RustTcp::bind(addr).await }))
            .map_err(|e| PyRuntimeError::new_err(format!("bind failed: {e}")))?;
        // Spawn a task that accepts connections and forwards
        // `(remote_addr, bytes)` to a single channel.
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(String, Bytes)>();
        let incoming_filtered = incoming.filter_map(|r| r.ok());
        rt.spawn(async move {
            // For each accepted connection, spawn a sub-task to drain its
            // reader; we use Sink::for_each on the outer source so we keep
            // accepting while sub-tasks are running.
            Sink::for_each(incoming_filtered, move |conn| {
                let remote = conn.remote_addr.to_string();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let reader_src = conn.reader.filter_map(|r| r.ok());
                    Sink::for_each(reader_src, move |b: Bytes| {
                        let _ = tx.send((remote.clone(), b));
                    })
                    .await;
                });
            })
            .await;
        });
        let rx_holder = Arc::new(Mutex::new(Some(rx)));
        let rx_for_build = Arc::clone(&rx_holder);
        let builder = SourceBuilder::new(move || {
            let rx = rx_for_build.lock().take();
            match rx {
                Some(rx) => Source::from_receiver(rx).map(|(remote, b): (String, Bytes)| {
                    Python::with_gil(|py| {
                        let pb = PyBytes::new_bound(py, &b);
                        let tup =
                            pyo3::types::PyTuple::new_bound(py, [remote.into_py(py), pb.unbind().into_any()]);
                        SendPyAny::new(tup.unbind().into_any())
                    })
                }),
                None => Source::empty(),
            }
        });
        Py::new(py, PySource::from_builder(builder))
    }
}

/// Handle returned by `Tcp.outgoing` — exposes the read-side `Source[bytes]`
/// and a write-side `send(data)` method.
#[pyclass(name = "TcpOutgoing", module = "atomr._native.streams")]
pub struct PyTcpOutgoing {
    reader: Mutex<Option<SourceBuilder>>,
    writer: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Bytes>>>,
    remote_addr: String,
}

#[pymethods]
impl PyTcpOutgoing {
    /// Take the read-side `Source[bytes]`. Can only be called once.
    fn source(&self, py: Python<'_>) -> PyResult<Py<PySource>> {
        let b = self
            .reader
            .lock()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("TcpOutgoing source already taken"))?;
        Py::new(py, PySource::from_builder(b))
    }

    /// `outgoing.send(data)` — push `bytes` to the write side. Returns
    /// `True` if the bytes were enqueued.
    fn send(&self, data: &[u8]) -> bool {
        let g = self.writer.lock();
        match g.as_ref() {
            Some(tx) => tx.send(Bytes::copy_from_slice(data)).is_ok(),
            None => false,
        }
    }

    /// Close the write side. The reader side completes once the remote
    /// peer closes its socket.
    fn close(&self) {
        let _ = self.writer.lock().take();
    }

    #[getter]
    fn remote_addr(&self) -> &str {
        &self.remote_addr
    }
}

// =============================================================================
// FileIO
// =============================================================================

/// `FileIO` — read/write files as `bytes` streams.
#[pyclass(name = "FileIO", module = "atomr._native.streams")]
pub struct PyFileIO;

#[pymethods]
impl PyFileIO {
    /// `FileIO.from_path(path, chunk_size=8192)` — open `path` and stream
    /// it as `Source[bytes]`. IO errors terminate the stream.
    #[staticmethod]
    #[pyo3(signature = (path, chunk_size=8192))]
    fn from_path(py: Python<'_>, path: String, chunk_size: usize) -> PyResult<Py<PySource>> {
        let p = PathBuf::from(path);
        let cs = chunk_size.max(1);
        let builder = SourceBuilder::new(move || {
            // Source<io::Result<Bytes>> -> Source<Bytes> via filter_map.
            let src = RustFileIO::from_path(p, cs).filter_map(|r| r.ok());
            // Bytes -> SendPyAny via map.
            src.map(|b: Bytes| {
                Python::with_gil(|py| {
                    let pb = PyBytes::new_bound(py, &b);
                    SendPyAny::new(pb.unbind().into_any())
                })
            })
        });
        Py::new(py, PySource::from_builder(builder))
    }

    /// `FileIO.to_path(path)` — return a `Sink` that writes every `bytes`
    /// chunk it receives to `path`. The materialised value is the number
    /// of bytes written (Python `int`).
    #[staticmethod]
    fn to_path(py: Python<'_>, path: String) -> PyResult<Py<PySink>> {
        use futures::FutureExt;
        let p = PathBuf::from(path);
        let sink_b = SinkBuilder::new(move |src: Source<SendPyAny>| {
            // SendPyAny -> Bytes via filter_map; non-bytes entries are silently
            // dropped.
            let bytes_src = src.filter_map(|item: SendPyAny| {
                Python::with_gil(|py| {
                    let inner = item.into_inner();
                    let bound = inner.bind(py);
                    match bound.extract::<&[u8]>() {
                        Ok(b) => Some(Bytes::copy_from_slice(b)),
                        Err(_) => None,
                    }
                })
            });
            async move {
                match RustFileIO::to_path(bytes_src, &p).await {
                    Ok(n) => Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(n.into_py(py)) }),
                    Err(e) => Err(PyRuntimeError::new_err(format!("FileIO.to_path: {e}"))),
                }
            }
            .boxed()
        });
        Py::new(py, PySink::from_builder(sink_b))
    }
}

// =============================================================================
// Legacy i64 helpers (unchanged)
// =============================================================================

/// `map_reduce(iter, transform, reducer, zero)` — synchronous one-shot
/// pipeline. Runs entirely in Python; kept for backward compatibility
/// with the previous Python binding.
#[pyfunction]
fn map_reduce(
    py: Python<'_>,
    source: Py<PyAny>,
    transform: Py<PyAny>,
    reducer: Py<PyAny>,
    zero: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let iter = source.bind(py).iter()?;
    let mut acc: Py<PyAny> = zero;
    for item in iter {
        let item = item?;
        let mapped = transform.call1(py, (item,))?;
        acc = reducer.call1(py, (acc, mapped))?;
    }
    Ok(acc)
}

/// `run_collect(items, mapper)` — drives the native materializer. Items
/// are materialized once into a `Vec<i64>`; the Python mapper is called
/// synchronously per element (released GIL around the blocking future).
#[pyfunction]
fn run_collect(py: Python<'_>, items: Vec<i64>, mapper: Py<PyAny>) -> PyResult<Vec<i64>> {
    let mapper_ref = mapper.clone_ref(py);
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rt.block_on(async move {
            let source = Source::from_iter(items);
            let flow: Flow<i64, i64> = Flow::from_fn(move |x| {
                Python::with_gil(|py| {
                    let v = mapper_ref.call1(py, (x,))?;
                    v.extract::<i64>(py)
                })
                .unwrap_or(0)
            });
            let mat = ActorMaterializer::new();
            Ok::<_, PyErr>(mat.run_collect(source.via(flow)).await)
        })
    })
}

/// `run_fold(items, mapper, zero, reducer)` — drives the materializer and
/// accumulates an `i64` fold on top of the mapped elements.
#[pyfunction]
fn run_fold(
    py: Python<'_>,
    items: Vec<i64>,
    mapper: Py<PyAny>,
    zero: i64,
    reducer: Py<PyAny>,
) -> PyResult<i64> {
    let mapper_ref = mapper.clone_ref(py);
    let reducer_ref = reducer.clone_ref(py);
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        rt.block_on(async move {
            let source = Source::from_iter(items).map(move |x| {
                Python::with_gil(|py| -> PyResult<i64> {
                    let v = mapper_ref.call1(py, (x,))?;
                    v.extract::<i64>(py)
                })
                .unwrap_or(0)
            });
            Ok::<_, PyErr>(
                Sink::fold(source, zero, move |acc, v| {
                    Python::with_gil(|py| -> PyResult<i64> {
                        let res = reducer_ref.call1(py, (acc, v))?;
                        res.extract::<i64>(py)
                    })
                    .unwrap_or(acc)
                })
                .await,
            )
        })
    })
}

fn run_to_vec<F>(py: Python<'_>, build: F) -> Vec<i64>
where
    F: FnOnce() -> Source<i64> + Send,
{
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mat = ActorMaterializer::new();
            mat.run_collect(build()).await
        })
    })
}

/// `keep_alive(items, idle_secs, filler)` — emit `filler` after each
/// quiet interval of `idle_secs`..
#[pyfunction]
fn via_keep_alive(py: Python<'_>, items: Vec<i64>, idle_secs: f64, filler: i64) -> Vec<i64> {
    run_to_vec(py, move || {
        keep_alive(Source::from_iter(items), Duration::from_secs_f64(idle_secs), move || filler)
    })
}

/// `initial_delay(items, delay_secs)` — delay the first element.
#[pyfunction]
fn via_initial_delay(py: Python<'_>, items: Vec<i64>, delay_secs: f64) -> Vec<i64> {
    run_to_vec(py, move || initial_delay(Source::from_iter(items), Duration::from_secs_f64(delay_secs)))
}

/// `conflate(items, fold)` — coalesce backed-up upstream elements.
#[pyfunction]
fn via_conflate(py: Python<'_>, items: Vec<i64>, fold: Py<PyAny>) -> Vec<i64> {
    let fold_ref = fold;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = conflate(
                Source::from_iter(items),
                |x: i64| x,
                move |acc, v| {
                    Python::with_gil(|gil| -> PyResult<i64> {
                        let r = fold_ref.call1(gil, (acc, v))?;
                        r.extract::<i64>(gil)
                    })
                    .unwrap_or(acc)
                },
            );
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `expand(items, extrapolate)` — replace each element with an iterator
/// of values via `extrapolate(x) -> List[int]`..
#[pyfunction]
fn via_expand(py: Python<'_>, items: Vec<i64>, extrapolate: Py<PyAny>) -> Vec<i64> {
    let cb = extrapolate;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = expand(Source::from_iter(items), move |x: &i64| {
                let xv = *x;
                let v: Vec<i64> = Python::with_gil(|gil| -> PyResult<Vec<i64>> {
                    let r = cb.call1(gil, (xv,))?;
                    r.extract::<Vec<i64>>(gil)
                })
                .unwrap_or_else(|_| vec![xv]);
                v.into_iter()
            });
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `merge_sorted(left, right)` — merge two sorted streams.
#[pyfunction]
fn merge_sorted_(py: Python<'_>, left: Vec<i64>, right: Vec<i64>) -> Vec<i64> {
    run_to_vec(py, move || merge_sorted(Source::from_iter(left), Source::from_iter(right)))
}

/// `merge_prioritized(left, left_weight, right, right_weight)`.
#[pyfunction]
fn merge_prioritized_(
    py: Python<'_>,
    left: Vec<i64>,
    left_weight: u32,
    right: Vec<i64>,
    right_weight: u32,
) -> Vec<i64> {
    run_to_vec(py, move || {
        merge_prioritized(Source::from_iter(left), left_weight, Source::from_iter(right), right_weight)
    })
}

/// `split_after(items, predicate)` — count of substreams emitted.
#[pyfunction]
fn via_split_after_count(py: Python<'_>, items: Vec<i64>, pred: Py<PyAny>) -> usize {
    let cb = pred;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = split_after(Source::from_iter(items), move |x: &i64| {
                Python::with_gil(|gil| {
                    cb.call1(gil, (*x,)).and_then(|r| r.extract::<bool>(gil)).unwrap_or(false)
                })
            });
            ActorMaterializer::new().run_collect(s).await.len()
        })
    })
}

/// `prefix_and_tail(items, n)`.
#[pyfunction]
fn via_prefix_and_tail(py: Python<'_>, items: Vec<i64>, n: usize) -> (Vec<i64>, usize) {
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let s = prefix_and_tail(Source::from_iter(items), n);
            let collected = ActorMaterializer::new().run_collect(s).await;
            if let Some((prefix, tail)) = collected.into_iter().next() {
                let tail_count = ActorMaterializer::new().run_collect(tail).await.len();
                (prefix, tail_count)
            } else {
                (Vec::new(), 0)
            }
        })
    })
}

/// `recover_with_retries(items_with_errors, replacement, attempts)`.
#[pyfunction]
fn via_recover_with_retries(
    py: Python<'_>,
    items_with_errors: Vec<(i64, bool)>,
    replacement: Vec<i64>,
    attempts: usize,
) -> Vec<i64> {
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mapped: Vec<Result<i64, ()>> =
                items_with_errors.into_iter().map(|(v, e)| if e { Err(()) } else { Ok(v) }).collect();
            let src = Source::from_iter(mapped);
            let s = recover_with_retries(src, attempts, move || Source::from_iter(replacement.clone()));
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

/// `select_error(items, mapper)`.
#[pyfunction]
fn via_select_error(
    py: Python<'_>,
    items_with_errors: Vec<(i64, Option<String>)>,
    mapper: Py<PyAny>,
) -> Vec<i64> {
    let cb = mapper;
    py.allow_threads(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(async move {
            let mapped: Vec<Result<i64, String>> = items_with_errors
                .into_iter()
                .map(|(v, e)| match e {
                    None => Ok(v),
                    Some(label) => Err(label),
                })
                .collect();
            let src = Source::from_iter(mapped);
            let mapped_src = select_error(src, move |label: String| -> String {
                Python::with_gil(|gil| {
                    cb.call1(gil, (label.clone(),)).and_then(|r| r.extract::<String>(gil)).unwrap_or(label)
                })
            });
            let s: Source<i64> = mapped_src.filter_map(|r| r.ok());
            ActorMaterializer::new().run_collect(s).await
        })
    })
}

// =============================================================================
// ManualClock — explicitly-advanced logical clock (FR-2)
// =============================================================================

/// An explicitly-advanced logical clock for deterministic replay and
/// backtests. The watermark only ever moves forward. Time is expressed in
/// nanoseconds (Python `int`); use `advance_to_millis` for a millisecond
/// convenience. Clones share the same underlying watermark.
#[pyclass(name = "ManualClock", module = "atomr._native.streams")]
#[derive(Clone)]
pub struct PyManualClock {
    inner: RustManualClock,
}

#[pymethods]
impl PyManualClock {
    /// A new clock at logical-time zero, or at `start_nanos` if given.
    #[new]
    #[pyo3(signature = (start_nanos=0))]
    fn new(start_nanos: u64) -> Self {
        Self { inner: RustManualClock::at(LogicalTime::from_nanos(start_nanos)) }
    }

    /// Advance the watermark to `target_nanos` (monotonic — a target at or
    /// below the current watermark is a no-op).
    fn advance_to(&self, target_nanos: u64) {
        self.inner.advance_to(LogicalTime::from_nanos(target_nanos));
    }

    /// Advance the watermark to `target_millis`, expressed in milliseconds.
    fn advance_to_millis(&self, target_millis: u64) {
        self.inner.advance_to(LogicalTime::from_millis(target_millis));
    }

    /// Advance the watermark by `delta_nanos`.
    fn advance_by(&self, delta_nanos: u64) {
        self.inner.advance_by(LogicalTime::from_nanos(delta_nanos));
    }

    /// The current watermark in nanoseconds.
    fn watermark(&self) -> u64 {
        self.inner.watermark().as_nanos()
    }

    fn __repr__(&self) -> String {
        format!("ManualClock(watermark_nanos={})", self.inner.watermark().as_nanos())
    }
}

// =============================================================================
// Module registration
// =============================================================================

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new_bound(py, "streams")?;
    // legacy i64 helpers
    sub.add_function(wrap_pyfunction!(map_reduce, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_collect, &sub)?)?;
    sub.add_function(wrap_pyfunction!(run_fold, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_keep_alive, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_initial_delay, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_conflate, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_expand, &sub)?)?;
    sub.add_function(wrap_pyfunction!(merge_sorted_, &sub)?)?;
    sub.add_function(wrap_pyfunction!(merge_prioritized_, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_split_after_count, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_prefix_and_tail, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_recover_with_retries, &sub)?)?;
    sub.add_function(wrap_pyfunction!(via_select_error, &sub)?)?;
    // typed DSL
    sub.add_class::<PySource>()?;
    sub.add_class::<PyFlow>()?;
    sub.add_class::<PySink>()?;
    sub.add_class::<PyRunnableGraph>()?;
    sub.add_class::<PyKillSwitch>()?;
    sub.add_class::<PySourceQueue>()?;
    sub.add_class::<PySinkQueue>()?;
    sub.add_class::<PyBroadcastHub>()?;
    sub.add_class::<PyMergeHub>()?;
    // Epic F additions: closing the deferred Phase 8 streams items.
    sub.add_class::<PyRestartSource>()?;
    sub.add_class::<PyRestartSettings>()?;
    sub.add_class::<PyGraphDsl>()?;
    sub.add_class::<PyBidiFlow>()?;
    sub.add_class::<PyFraming>()?;
    sub.add_class::<PyTcp>()?;
    sub.add_class::<PyTcpOutgoing>()?;
    sub.add_class::<PyFileIO>()?;
    sub.add_class::<PyManualClock>()?;
    let _ = pyo3_tokio::get_runtime;
    m.add_submodule(&sub)?;
    Ok(())
}
