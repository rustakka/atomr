//! PyO3 bindings for atomr-core. Exposes the native extension module
//! `atomr._native` whose submodules mirror the Rust crate layout.
//!
//! Public Python API surface lives in `python/atomr/`; this crate only
//! provides compiled types.

// PyO3 macros emit code that trips a handful of clippy false positives.
#![allow(
    clippy::useless_conversion,
    clippy::too_many_arguments,
    clippy::needless_lifetimes,
    clippy::new_without_default,
    clippy::type_complexity,
    dead_code,
    unexpected_cfgs
)]

use pyo3::prelude::*;

mod actor_ref;
mod actor_system;
mod cluster_transport;
mod compat;
mod config;
mod context;
mod dispatcher;
mod errors;
mod interpreter;
mod metrics;
mod props;
mod py_actor;
mod runtime;
mod supervision;

mod ext_cluster;
mod ext_cluster_metrics;
mod ext_cluster_sharding;
mod ext_cluster_tools;
mod ext_coordination;
mod ext_core_extras;
mod ext_dashboard;
mod ext_ddata;
mod ext_ddata_lmdb;
mod ext_di;
mod ext_discovery;
mod ext_hosting;
mod ext_money;
mod ext_pattern;
mod ext_persistence;
mod ext_remote;
mod ext_remote_serial;
mod ext_routing;
mod ext_streams;
mod ext_telemetry;
mod ext_testkit;

/// Entry point registered with `#[pymodule]` — exposes `atomr._native`.
#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    actor_system::register(py, m)?;
    actor_ref::register(py, m)?;
    config::register(py, m)?;
    context::register(py, m)?;
    dispatcher::register(py, m)?;
    errors::register(py, m)?;
    interpreter::register(py, m)?;
    metrics::register(py, m)?;
    props::register(py, m)?;
    supervision::register(py, m)?;
    compat::register(py, m)?;

    ext_testkit::register(py, m)?;
    ext_cluster::register(py, m)?;
    ext_cluster_metrics::register(py, m)?;
    ext_cluster_tools::register(py, m)?;
    ext_cluster_sharding::register(py, m)?;
    ext_core_extras::register(py, m)?;
    ext_money::register(py, m)?;
    ext_ddata::register(py, m)?;
    ext_ddata_lmdb::register(py, m)?;
    ext_persistence::register(py, m)?;
    ext_remote::register(py, m)?;
    ext_remote_serial::register(py, m)?;
    ext_streams::register(py, m)?;
    ext_coordination::register(py, m)?;
    ext_discovery::register(py, m)?;
    ext_di::register(py, m)?;
    ext_hosting::register(py, m)?;
    ext_dashboard::register(py, m)?;
    ext_telemetry::register(py, m)?;
    ext_pattern::register(py, m)?;
    // Routing must register *after* ext_pattern and props because it
    // attaches classmethods to the `Props` class object.
    ext_routing::register(py, m)?;

    Ok(())
}
