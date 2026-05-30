//! USB / serial remote actors from Python.
//!
//! Exposes the minimum slice of `atomr-remote` and `atomr-remote-serial`
//! that the `examples/usb-link-probe/python/` demo needs: a serial
//! transport, a `RemoteSystem` wired on top of it, codec registration
//! that lets Python ship pre-encoded bytes (typically `json.dumps(obj)
//! .encode()`), `expose_actor` so a local Python actor can receive
//! those bytes, and `actor_selection` so a Python script can tell a
//! remote peer.
//!
//! Wire model: pre-encoded bytes. Python is responsible for serializing
//! its own messages (the demo uses JSON). The Rust side treats payloads
//! as opaque `Vec<u8>` and only routes them — `register_bytes_codec`
//! installs an identity codec under a chosen manifest, which is enough
//! for the receiver to look up the dispatcher and downcast back to
//! `Vec<u8>` before delivery.

use std::any::{Any, TypeId};
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use atomr_core::actor::{ActorPath, ActorRef as RustRef};
use atomr_remote::serialization::{SerializeError, TypeCodec, JSON_SERIALIZER_ID};
use atomr_remote::system_daemon::LocalDispatch;
use atomr_remote::transport::Transport;
use atomr_remote::{RemoteSettings, RemoteSystem};
use atomr_remote_serial::SerialTransport;

use crate::actor_ref::PyActorRef;
use crate::py_actor::PyMessage;
use crate::runtime::runtime;

/// Python-facing wrapper around `SerialTransport`.
///
/// Construct via `SerialTransport(system_name, device, baud=115200,
/// max_frame_size=4*1024*1024)` and pass into `RemoteSystem.start_serial(...)`.
#[pyclass(name = "SerialTransport", module = "atomr._native")]
pub struct PySerialTransport {
    inner: Option<Arc<dyn Transport>>,
    system_name: String,
    device: PathBuf,
}

#[pymethods]
impl PySerialTransport {
    #[new]
    #[pyo3(signature = (system_name, device, baud=115_200, max_frame_size=4*1024*1024))]
    fn new(system_name: String, device: String, baud: u32, max_frame_size: usize) -> PyResult<Self> {
        let transport = SerialTransport::with_options(
            system_name.clone(),
            device.clone(),
            baud,
            max_frame_size,
            atomr_remote_serial::ReconnectPolicy::default(),
        );
        Ok(Self { inner: Some(Arc::new(transport)), system_name, device: PathBuf::from(device) })
    }

    #[getter]
    fn system_name(&self) -> &str {
        &self.system_name
    }

    #[getter]
    fn device(&self) -> String {
        self.device.to_string_lossy().into_owned()
    }

    /// Cross-platform serial-port enumeration. Returns a list of
    /// `(name, type_description)` tuples — see
    /// `tokio_serial::available_ports()`.
    #[staticmethod]
    fn list_devices() -> PyResult<Vec<(String, String)>> {
        match tokio_serial::available_ports() {
            Ok(ports) => Ok(ports.into_iter().map(|p| (p.port_name, format!("{:?}", p.port_type))).collect()),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("enumerate: {e}"))),
        }
    }

    /// Test helper: build two `SerialTransport`s wired together via
    /// an in-memory `tokio::io::duplex` pair. Lets Python loopback
    /// tests exercise the full RemoteSystem pipeline without real
    /// serial hardware. Returns `(transport_a, transport_b)`.
    #[staticmethod]
    #[pyo3(signature = (system_a, system_b, buffer=8192, max_frame=64*1024))]
    fn duplex_pair(
        system_a: String,
        system_b: String,
        buffer: usize,
        max_frame: usize,
    ) -> PyResult<(Self, Self)> {
        // `with_streams` calls `tokio::spawn` for the link runner — we
        // need an active Tokio runtime context. The pyo3-async-runtimes
        // shared runtime serves both sync and async PyO3 entry points.
        let _guard = runtime().enter();
        let (a_io, b_io) = tokio::io::duplex(buffer);
        let (a_reader, a_writer) = tokio::io::split(a_io);
        let (b_reader, b_writer) = tokio::io::split(b_io);
        let a = SerialTransport::with_streams(system_a.clone(), a_reader, a_writer, max_frame);
        let b = SerialTransport::with_streams(system_b.clone(), b_reader, b_writer, max_frame);
        Ok((
            Self { inner: Some(Arc::new(a)), system_name: system_a, device: PathBuf::from("<duplex>") },
            Self { inner: Some(Arc::new(b)), system_name: system_b, device: PathBuf::from("<duplex>") },
        ))
    }
}

/// Python-facing wrapper around `RemoteSystem`. Construct via the
/// async `RemoteSystem.start_serial(...)` classmethod (see
/// `python/atomr/remote_serial.py` for the convenience wrapper).
#[pyclass(name = "RemoteSystem", module = "atomr._native")]
pub struct PyRemoteSystem {
    inner: Arc<RemoteSystem>,
}

#[pymethods]
impl PyRemoteSystem {
    /// Bind a `SerialTransport` to a `PyActorSystem` and return the
    /// wired `RemoteSystem`. Async — returns an asyncio-compatible
    /// awaitable.
    #[staticmethod]
    fn start_serial<'py>(
        py: Python<'py>,
        actor_system: Py<crate::actor_system::PyActorSystem>,
        transport: Py<PySerialTransport>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (sys, transport_arc) = Python::with_gil(|py| {
            let sys = actor_system.borrow(py).inner.clone();
            let mut t = transport.borrow_mut(py);
            let arc = t.inner.take().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "SerialTransport already consumed (each transport may bind to one RemoteSystem)",
                )
            })?;
            Ok::<_, PyErr>((sys, arc))
        })?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let remote = RemoteSystem::start_with_transport(sys, transport_arc, RemoteSettings::default())
                .await
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("start_with_transport: {e:?}"))
                })?;
            Python::with_gil(|py| {
                Py::new(py, PyRemoteSystem { inner: Arc::new(remote) }).map(|p| p.into_any())
            })
        })
    }

    #[getter]
    fn local_address(&self) -> String {
        self.inner.local_address.to_string()
    }

    /// Register an identity codec for `manifest`. After this,
    /// pre-encoded `bytes` payloads tagged with `manifest` round-trip
    /// through the wire unchanged. Most callers use one manifest per
    /// logical message family (e.g. "LinkMsg") and JSON-encode their
    /// dict on top.
    fn register_bytes_codec(&self, manifest: String) {
        register_identity_codec(&self.inner, manifest);
    }

    /// Wire `local_actor` to receive inbound bytes for `manifest`.
    /// Inbound payloads are decoded back to `Vec<u8>` and delivered to
    /// the actor as a Python `bytes` object.
    fn expose_actor(&self, local_actor: Py<PyActorRef>, manifest: String) -> PyResult<()> {
        let target_arc: Arc<RustRef<PyMessage>> = Python::with_gil(|py| local_actor.borrow(py).inner.clone());
        let target_path: ActorPath = target_arc.path().clone();
        let manifest_for_warn = manifest.clone();

        // Make sure the manifest has a codec registered, otherwise the
        // daemon would have nothing to decode the inbound bytes with.
        register_identity_codec(&self.inner, manifest);

        let dispatch: LocalDispatch = Arc::new(move |_p, _manifest, value: Box<dyn Any + Send>| {
            let bytes = match value.downcast::<Vec<u8>>() {
                Ok(b) => *b,
                Err(_) => {
                    tracing::warn!(manifest = %manifest_for_warn, "expected Vec<u8>, got other Any payload");
                    return;
                }
            };
            // Wrap the raw bytes as a Python `bytes` object and tell the
            // local actor. Acquiring the GIL inside the dispatch closure
            // is fine — Tokio worker threads can hold the GIL briefly.
            Python::with_gil(|py| {
                let py_bytes: Py<PyAny> = PyBytes::new_bound(py, &bytes).unbind().into_any();
                target_arc.tell(PyMessage::new(py_bytes));
            });
        });
        self.inner.daemon.register(target_path, dispatch);
        Ok(())
    }

    /// Resolve a remote actor path and return a `RemoteActorRef` whose
    /// `.tell(payload)` ships `payload` (raw `bytes`) under `manifest`.
    /// Returns `None` if the path is malformed or local.
    fn actor_selection(
        &self,
        py: Python<'_>,
        path: String,
        manifest: String,
    ) -> PyResult<Option<Py<PyRemoteActorRef>>> {
        let _guard = runtime().enter();
        // Custom serialize closure: don't bincode-encode the payload —
        // it's already-encoded bytes. Wrap them in a SerializedMessage
        // with the chosen manifest + serializer_id (we use the JSON
        // serializer id since this is the canonical Python wire format
        // for the demo; the receiver doesn't actually run JSON, since
        // the codec is identity, but the id has to match a codec entry
        // and JSON is the natural choice).
        let manifest_for_send = manifest.clone();
        let serialize: Arc<
            dyn Fn(Vec<u8>, Option<ActorPath>) -> atomr_core::actor::SerializedMessage + Send + Sync,
        > = Arc::new(move |bytes: Vec<u8>, sender: Option<ActorPath>| atomr_core::actor::SerializedMessage {
            serializer_id: JSON_SERIALIZER_ID,
            manifest: manifest_for_send.clone(),
            payload: bytes,
            sender,
        });
        let Some(rust_ref) = self.inner.system.actor_selection_with::<Vec<u8>>(&path, serialize) else {
            return Ok(None);
        };
        let py_ref = PyRemoteActorRef { inner: Arc::new(rust_ref), path: path.clone() };
        Ok(Some(Py::new(py, py_ref)?))
    }

    /// Async — close the underlying endpoint manager and clear daemon
    /// routes. Safe to call multiple times.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.shutdown().await;
            Ok(())
        })
    }

    /// Blocking shutdown for sync callers / Ctrl-C handlers.
    fn shutdown_blocking(&self, py: Python<'_>) {
        let inner = self.inner.clone();
        let rt = runtime();
        py.allow_threads(|| rt.block_on(async move { inner.shutdown().await }));
    }
}

/// Returned by `RemoteSystem.actor_selection(...)`. Holds an
/// `Arc<ActorRef<Vec<u8>>>` whose `tell` ships pre-encoded bytes.
#[pyclass(name = "RemoteActorRef", module = "atomr._native")]
pub struct PyRemoteActorRef {
    inner: Arc<RustRef<Vec<u8>>>,
    path: String,
}

#[pymethods]
impl PyRemoteActorRef {
    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    /// Fire-and-forget. `payload` must be a Python `bytes` object —
    /// the caller is responsible for encoding (typically
    /// `json.dumps(msg).encode()`).
    fn tell(&self, payload: &Bound<'_, PyBytes>) -> PyResult<()> {
        let bytes = payload.as_bytes().to_vec();
        // Remote refs spawn a tokio task under the hood to push through
        // the endpoint. Make sure the shared runtime is the active
        // context for the duration of the call so sync Python callers
        // (no async wrapping) still work.
        let _guard = runtime().enter();
        self.inner.tell(bytes);
        Ok(())
    }
}

/// Registers (idempotently) an identity codec on `remote`'s registry
/// for `manifest`: encode = pass bytes through, decode = re-box as
/// `Vec<u8>`. Used by both `register_bytes_codec` and `expose_actor`.
fn register_identity_codec(remote: &RemoteSystem, manifest: String) {
    let manifest_for_codec = manifest.clone();
    remote.registry().register_codec(TypeCodec {
        serializer_id: JSON_SERIALIZER_ID,
        manifest: manifest.clone(),
        type_id: TypeId::of::<Vec<u8>>(),
        encode: Arc::new(|v: &dyn Any| {
            let bytes =
                v.downcast_ref::<Vec<u8>>().ok_or_else(|| SerializeError::Downcast("Vec<u8>".into()))?;
            Ok(bytes.clone())
        }),
        decode: Arc::new(move |b: &[u8]| {
            let _ = &manifest_for_codec; // captured for diagnostics if needed
            Ok(Box::new(b.to_vec()) as Box<dyn Any + Send>)
        }),
    });
}

pub fn register(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySerialTransport>()?;
    m.add_class::<PyRemoteSystem>()?;
    m.add_class::<PyRemoteActorRef>()?;
    Ok(())
}
