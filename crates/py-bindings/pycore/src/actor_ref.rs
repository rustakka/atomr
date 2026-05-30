//! `ActorRef` — untyped Python-facing handle. We always send
//! `Py<PyAny>` messages across the boundary; typed stubs live in the
//! Python facade.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;

use atomr_core::actor::ActorRef as RustRef;
use atomr_core::actor::Metadata as RustMetadata;
use atomr_core::supervision::{Directive as RustDirective, SuspendMode};

use crate::py_actor::PyMessage;
use crate::runtime::runtime;

/// Restricted operating mode a [`PyDirective`] suspend places an actor
/// into. Parsed from a string: `flat_only`, `risk_reducing_only`,
/// `full_halt`.
fn parse_suspend_mode(mode: &str) -> PyResult<SuspendMode> {
    Ok(match mode {
        "flat_only" => SuspendMode::FlatOnly,
        "risk_reducing_only" | "risk_reducing" => SuspendMode::RiskReducingOnly,
        "full_halt" => SuspendMode::FullHalt,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown suspend mode `{other}`; expected one of \
                 flat_only | risk_reducing_only | full_halt"
            )))
        }
    })
}

fn suspend_mode_name(mode: SuspendMode) -> &'static str {
    match mode {
        SuspendMode::FlatOnly => "flat_only",
        SuspendMode::RiskReducingOnly => "risk_reducing_only",
        SuspendMode::FullHalt => "full_halt",
        _ => "unknown",
    }
}

/// Per-message metadata: W3C-style trace context + a string baggage map
/// (FR-10). Rides every `tell_with_meta` send and is surfaced to the
/// receiving actor via its context metadata.
#[pyclass(name = "Metadata", module = "atomr._native")]
#[derive(Clone, Default)]
pub struct PyMetadata {
    pub(crate) inner: RustMetadata,
}

#[pymethods]
impl PyMetadata {
    #[new]
    #[pyo3(signature = (trace_id=None, span_id=None))]
    fn new(trace_id: Option<String>, span_id: Option<String>) -> Self {
        let mut inner = RustMetadata::new();
        if let Some(t) = trace_id {
            inner.set_trace_id(t);
        }
        if let Some(s) = span_id {
            inner.set_span_id(s);
        }
        Self { inner }
    }

    #[getter]
    fn trace_id(&self) -> Option<String> {
        self.inner.trace_id().map(str::to_string)
    }

    #[getter]
    fn span_id(&self) -> Option<String> {
        self.inner.span_id().map(str::to_string)
    }

    fn set_baggage(&mut self, key: String, value: String) {
        self.inner.set_baggage(key, value);
    }

    fn baggage(&self, key: String) -> Option<String> {
        self.inner.baggage(&key).map(str::to_string)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn __repr__(&self) -> String {
        format!("Metadata(trace_id={:?}, span_id={:?})", self.inner.trace_id(), self.inner.span_id())
    }
}

/// A graded supervisor directive (FR-6) pushed into a *running* actor
/// without restarting it. `Throttle`/`Suspend`/`ResumeFrom` change the
/// actor's operating mode; the actor observes them via its `on_directive`
/// hook.
#[pyclass(name = "Directive", module = "atomr._native", frozen)]
#[derive(Clone, Copy)]
pub struct PyDirective {
    pub(crate) inner: RustDirective,
}

#[pymethods]
impl PyDirective {
    /// Reduce the actor's effective rate/size by `factor` (e.g. `0.25` =
    /// quarter) for at least `window_secs`. Applied without restart.
    #[staticmethod]
    fn throttle(factor: f32, window_secs: f64) -> Self {
        Self {
            inner: RustDirective::Throttle { factor, window: Duration::from_secs_f64(window_secs.max(0.0)) },
        }
    }

    /// Move the actor into a restricted operating `mode` (`flat_only`,
    /// `risk_reducing_only`, `full_halt`) without restart.
    #[staticmethod]
    fn suspend(mode: &str) -> PyResult<Self> {
        Ok(Self { inner: RustDirective::Suspend { mode: parse_suspend_mode(mode)? } })
    }

    /// Step the actor back up the ladder to a less-restrictive `mode`.
    #[staticmethod]
    fn resume_from(mode: &str) -> PyResult<Self> {
        Ok(Self { inner: RustDirective::ResumeFrom(parse_suspend_mode(mode)?) })
    }

    fn __repr__(&self) -> String {
        match self.inner {
            RustDirective::Throttle { factor, window } => {
                format!("Directive.throttle(factor={factor}, window_secs={})", window.as_secs_f64())
            }
            RustDirective::Suspend { mode } => {
                format!("Directive.suspend({})", suspend_mode_name(mode))
            }
            RustDirective::ResumeFrom(mode) => {
                format!("Directive.resume_from({})", suspend_mode_name(mode))
            }
            ref other => format!("Directive({other:?})"),
        }
    }
}

#[pyclass(name = "ActorRef", module = "atomr._native")]
pub struct PyActorRef {
    pub(crate) inner: Arc<RustRef<PyMessage>>,
    pub(crate) path: String,
}

impl PyActorRef {
    pub fn new(inner: RustRef<PyMessage>, path: String) -> Self {
        Self { inner: Arc::new(inner), path }
    }

    /// Construct from a pre-shared Arc — avoids cloning the underlying
    /// `RustRef` when we already have it `Arc`-wrapped (used by the
    /// Phase 1 context plumbing where the same ref is exposed to
    /// Python multiple times per dispatch).
    pub fn from_arc(inner: Arc<RustRef<PyMessage>>, path: String) -> Self {
        Self { inner, path }
    }
}

#[pymethods]
impl PyActorRef {
    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Fire-and-forget send.
    fn tell(&self, msg: Bound<'_, PyAny>) -> PyResult<()> {
        let payload = msg.unbind();
        self.inner.tell(PyMessage::new(payload));
        Ok(())
    }

    /// Fire-and-forget send with explicit `sender`. The receiver's
    /// `ctx.sender` will resolve to `sender`.
    fn tell_with_sender(&self, msg: Bound<'_, PyAny>, sender: Py<PyActorRef>) -> PyResult<()> {
        let payload = msg.unbind();
        let sender_inner = Python::with_gil(|py| sender.borrow(py).inner.clone());
        self.inner.tell(PyMessage::with_sender(payload, sender_inner));
        Ok(())
    }

    /// Fire-and-forget send with an explicit consistent-hash routing
    /// key. Required when sending through a `Props.consistent_hash`
    /// router; otherwise the router has no stable basis for picking a
    /// routee.
    fn tell_with_key(&self, msg: Bound<'_, PyAny>, key: u64) -> PyResult<()> {
        let payload = msg.unbind();
        self.inner.tell(PyMessage::with_hash(payload, key));
        Ok(())
    }

    /// Async ask — returns an `asyncio`-compatible awaitable.
    #[pyo3(signature = (msg, timeout=5.0))]
    fn ask<'py>(&self, py: Python<'py>, msg: Bound<'py, PyAny>, timeout: f64) -> PyResult<Bound<'py, PyAny>> {
        let payload = msg.unbind();
        let (env, rx) = PyMessage::ask(payload);
        self.inner.tell(env);
        let dur = std::time::Duration::from_secs_f64(timeout);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = match tokio::time::timeout(dur, rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => Err(PyErr::new::<crate::errors::AskError, _>("reply channel dropped")),
                Err(_) => Err(PyErr::new::<crate::errors::AskError, _>("ask timed out")),
            };
            match result {
                Ok(obj) => Ok(obj),
                Err(e) => Err(e),
            }
        })
    }

    /// Blocking ask, for sync CLI-style code. Spawns on the shared runtime.
    #[pyo3(signature = (msg, timeout=5.0))]
    fn ask_blocking(&self, py: Python<'_>, msg: Bound<'_, PyAny>, timeout: f64) -> PyResult<Py<PyAny>> {
        let payload = msg.unbind();
        let (env, rx) = PyMessage::ask(payload);
        self.inner.tell(env);
        let dur = std::time::Duration::from_secs_f64(timeout);
        let rt = runtime();
        py.allow_threads(|| {
            rt.block_on(async move {
                match tokio::time::timeout(dur, rx).await {
                    Ok(Ok(Ok(v))) => Ok(v),
                    Ok(Ok(Err(e))) => Err(e),
                    Ok(Err(_)) => Err(PyErr::new::<crate::errors::AskError, _>("reply channel dropped")),
                    Err(_) => Err(PyErr::new::<crate::errors::AskError, _>("ask timed out")),
                }
            })
        })
    }

    /// Fire-and-forget send with attached [`Metadata`](PyMetadata) (trace
    /// context + baggage). The metadata rides the envelope and is exposed
    /// to the receiving actor via its context metadata.
    fn tell_with_meta(&self, msg: Bound<'_, PyAny>, metadata: &PyMetadata) -> PyResult<()> {
        let payload = msg.unbind();
        self.inner.tell_with_meta(PyMessage::new(payload), metadata.inner.clone());
        Ok(())
    }

    /// Push a graded supervisor [`Directive`](PyDirective)
    /// (`throttle`/`suspend`/`resume_from`) into this running actor without
    /// restarting it (FR-6). Delivered on the system channel; the actor
    /// observes it via its `on_directive` hook. No-op for remote refs.
    fn tell_directive(&self, directive: &PyDirective) -> PyResult<()> {
        self.inner.tell_directive(directive.inner);
        Ok(())
    }

    /// Send a `SystemMsg::Stop` to the target. The actor finishes the
    /// current message (if any), runs `post_stop`, and notifies any
    /// watchers via `Terminated`.
    fn stop(&self) {
        self.inner.stop();
    }

    /// Best-effort: returns `True` once the actor cell has shut down.
    /// For remote refs we cannot inspect the far-end mailbox so this
    /// always returns `False`.
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }

    fn __repr__(&self) -> String {
        format!("<ActorRef path={}>", self.path)
    }

    /// Return a sibling `ActorRef` with the same underlying mailbox
    /// channel but a rewritten path. Used by Epic A's remote-tell
    /// tests to mint a "remote-shaped" ref pointing at another
    /// system's TCP-resolved address — `tell_remote` consults the
    /// path string when deciding local-vs-remote routing.
    ///
    /// The `inner` channel is only relevant for the local fast-path;
    /// for true remote sends the transport delivers via path lookup
    /// on the receiving side, so the original `inner` is harmless.
    fn with_path(slf: PyRef<'_, Self>, path: String) -> PyActorRef {
        PyActorRef { inner: slf.inner.clone(), path }
    }
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyActorRef>()?;
    m.add_class::<PyMetadata>()?;
    m.add_class::<PyDirective>()?;
    Ok(())
}
