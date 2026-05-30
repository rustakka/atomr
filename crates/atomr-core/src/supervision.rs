//! Supervision.

use std::sync::Arc;
use std::time::Duration;

/// Structured panic payload carried via `std::panic::panic_any` so that
/// language-binding actors (Python, etc.) can attach typed failure
/// metadata that the decider can inspect by class name. The
/// `actor_cell` panic-catch path renders this as
/// `"<module>.<qualname>: <repr>"` for the legacy `Decider: Fn(&str)`
/// surface; deciders wishing to match by class can split on the first
/// `": "` boundary.
#[derive(Debug, Clone)]
pub struct PanicPayload {
    pub module: String,
    pub qualname: String,
    pub repr: String,
}

impl PanicPayload {
    pub fn new(module: impl Into<String>, qualname: impl Into<String>, repr: impl Into<String>) -> Self {
        Self { module: module.into(), qualname: qualname.into(), repr: repr.into() }
    }

    /// Fully-qualified class path, e.g. `"builtins.ValueError"`.
    pub fn class_path(&self) -> String {
        if self.module.is_empty() {
            self.qualname.clone()
        } else {
            format!("{}.{}", self.module, self.qualname)
        }
    }

    /// Wire format used by `actor_cell::panic_payload_to_string` so
    /// that legacy `Decider: Fn(&str) -> Directive` deciders can still
    /// inspect the payload.
    pub fn to_wire(&self) -> String {
        format!("{}: {}", self.class_path(), self.repr)
    }
}

/// Operating mode a [`Directive::Suspend`] places a child into. Ordered from
/// least to most restrictive; an actor's [`on_directive`](crate::actor::Actor::on_directive)
/// hook is expected to gate its outbound effects accordingly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SuspendMode {
    /// Accept only actions that move toward a flat / zero position.
    FlatOnly,
    /// Accept only risk-reducing actions (a superset of flat-only intent).
    RiskReducingOnly,
    /// Reject all outbound effects until resumed.
    FullHalt,
}

/// What the supervisor decides when a child fails — or, for the graded
/// variants, how it should degrade a *still-running* child.
///
/// `Resume`/`Restart`/`Stop`/`Escalate` are the classic crash-recovery
/// directives. `Throttle`/`Suspend`/`ResumeFrom` are graded operating-mode
/// changes (FR-6): the supervisor pushes them into a running child via
/// [`Actor::on_directive`] *without* a
/// restart, so the child's state is preserved. This lets a risk circuit
/// breaker reduce order rate/size or move to flat-only instead of bouncing the
/// actor.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum Directive {
    Resume,
    Restart,
    Stop,
    Escalate,
    /// Reduce the child's effective rate/size by `factor` (e.g. `0.25` = quarter)
    /// for at least `window`. Applied without restart.
    Throttle {
        factor: f32,
        window: Duration,
    },
    /// Move the child into a restricted operating `mode` without restart.
    Suspend {
        mode: SuspendMode,
    },
    /// Step the child back up the ladder to a less-restrictive `mode`.
    ResumeFrom(SuspendMode),
}

impl Eq for Directive {}

pub type Decider = Arc<dyn Fn(&str) -> Directive + Send + Sync>;

/// Strategy applied to children of a supervising actor. Splits into
/// `OneForOne` (each child handled independently) and `AllForOne`
/// (one child's failure restarts all siblings).
#[derive(Clone)]
pub struct SupervisorStrategy {
    pub kind: StrategyKind,
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StrategyKind {
    OneForOne,
    AllForOne,
}

impl std::fmt::Debug for SupervisorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SupervisorStrategy")
            .field("kind", &self.kind)
            .field("max_retries", &self.max_retries)
            .field("within", &self.within)
            .finish_non_exhaustive()
    }
}

impl Default for SupervisorStrategy {
    fn default() -> Self {
        OneForOneStrategy::default().into()
    }
}

impl SupervisorStrategy {
    pub fn decide(&self, err: &str) -> Directive {
        (self.decider)(err)
    }

    /// Directive applied when [`Self::max_retries`] within
    /// [`Self::within`] is exceeded.
    ///
    /// Defaults to [`Directive::Escalate`] — matching Akka's behaviour:
    /// the parent supervisor decides the next step, and at the user-guardian
    /// root that is equivalent to stopping the cell. Override via a future
    /// builder method when per-strategy on-overflow behaviour is needed.
    pub fn on_max_retries(&self) -> Directive {
        Directive::Escalate
    }
}

/// Builder for `OneForOne` — the default.
pub struct OneForOneStrategy {
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

impl Default for OneForOneStrategy {
    fn default() -> Self {
        Self {
            max_retries: Some(10),
            within: Some(Duration::from_secs(60)),
            decider: Arc::new(|_| Directive::Restart),
        }
    }
}

impl OneForOneStrategy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = Some(n);
        self
    }

    pub fn with_within(mut self, d: Duration) -> Self {
        self.within = Some(d);
        self
    }

    pub fn with_decider(mut self, f: impl Fn(&str) -> Directive + Send + Sync + 'static) -> Self {
        self.decider = Arc::new(f);
        self
    }
}

impl From<OneForOneStrategy> for SupervisorStrategy {
    fn from(o: OneForOneStrategy) -> Self {
        Self {
            kind: StrategyKind::OneForOne,
            max_retries: o.max_retries,
            within: o.within,
            decider: o.decider,
        }
    }
}

/// Builder for `AllForOne`.
pub struct AllForOneStrategy {
    pub max_retries: Option<u32>,
    pub within: Option<Duration>,
    pub decider: Decider,
}

impl Default for AllForOneStrategy {
    fn default() -> Self {
        Self {
            max_retries: Some(10),
            within: Some(Duration::from_secs(60)),
            decider: Arc::new(|_| Directive::Restart),
        }
    }
}

impl From<AllForOneStrategy> for SupervisorStrategy {
    fn from(o: AllForOneStrategy) -> Self {
        Self {
            kind: StrategyKind::AllForOne,
            max_retries: o.max_retries,
            within: o.within,
            decider: o.decider,
        }
    }
}

// -- Phase 1.D: typed `SupervisorOf<C>` trait ---------------------------
//
// Compile-time supervision contract. The legacy `SupervisorStrategy`
// value above (a closure-based decider held on `Props`) stays in
// place as the default runtime policy. `SupervisorOf<C>` layers an
// **opt-in** per-(parent, child) typed policy on top — see P-8 of
// `docs/idiomatic-rust.md` and Phase 1.D of
// `docs/full-port-plan.md`.
//
// Rust's coherence rules forbid a blanket-with-override pattern (no
// stable specialization), so `SupervisorOf<C>` is **not** auto-impl'd
// for every `(P, C)` pair. Actors that want compile-time-typed child
// supervision implement it explicitly:
//
// ```ignore
// impl SupervisorOf<Worker> for Boss {
//     type ChildError = WorkerError;
//     fn decide(&self, err: &WorkerError) -> Directive { … }
// }
// ```
//
// Actors without an explicit impl fall through to the legacy
// closure-based `Props::supervisor_strategy` at runtime — exactly
// the pre-Phase-1 behaviour. The forthcoming `Context::
// spawn_supervised::<C>(…)` (Phase 1.D follow-on) will require
// `Self: SupervisorOf<C>` so that opting into typed supervision is
// enforced at the call site.

use crate::actor::Actor;

/// A parent actor's typed supervision policy for a specific child
/// type `C`. Opt-in only — see module docs.
pub trait SupervisorOf<C: Actor> {
    /// The child's error type. Implementations choose this; the
    /// recommended pattern is one error enum per supervised child
    /// type.
    type ChildError: std::error::Error + Send + 'static;

    /// Decide what to do when the child fails with `err`. Defaults to
    /// `Restart`.
    fn decide(&self, _err: &Self::ChildError) -> Directive {
        Directive::Restart
    }
}

/// Generic boxed-string error suitable for `SupervisorOf` impls that
/// don't yet have a typed error story (e.g. wrapping a panic
/// payload). New code should prefer a domain-specific
/// `#[derive(thiserror::Error)]` enum instead.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SupervisionError {
    pub message: String,
}

impl SupervisionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, Context};

    #[test]
    fn default_is_one_for_one_restart() {
        let s = SupervisorStrategy::default();
        assert_eq!(s.kind, StrategyKind::OneForOne);
        assert_eq!(s.decide("boom"), Directive::Restart);
    }

    #[test]
    fn custom_decider_runs() {
        let s: SupervisorStrategy = OneForOneStrategy::new()
            .with_decider(|e| if e == "stop" { Directive::Stop } else { Directive::Resume })
            .into();
        assert_eq!(s.decide("stop"), Directive::Stop);
        assert_eq!(s.decide("keep"), Directive::Resume);
    }

    // ---- SupervisorOf<C> --------------------------------------

    #[derive(Default)]
    struct Boss;
    #[derive(Default)]
    struct Worker;

    #[async_trait::async_trait]
    impl Actor for Boss {
        type Msg = ();
        async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
    }

    #[async_trait::async_trait]
    impl Actor for Worker {
        type Msg = ();
        async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
    }

    #[derive(Debug, thiserror::Error)]
    #[error("worker died: {0}")]
    struct WorkerError(String);

    impl SupervisorOf<Worker> for Boss {
        type ChildError = WorkerError;
        fn decide(&self, _err: &WorkerError) -> Directive {
            Directive::Stop
        }
    }

    #[test]
    fn explicit_impl_is_resolvable_with_typed_error() {
        fn assert_typed_decider<P: SupervisorOf<C, ChildError = WorkerError>, C: Actor>() {}
        assert_typed_decider::<Boss, Worker>();
    }

    #[test]
    fn typed_decider_runs() {
        let boss = Boss;
        let err = WorkerError("oops".into());
        let d = SupervisorOf::<Worker>::decide(&boss, &err);
        assert_eq!(d, Directive::Stop);
    }

    /// Demonstrates the recommended pattern of using
    /// [`SupervisionError`] for actors that don't yet have a typed
    /// child-error enum. Future PRs replace this with the domain
    /// error.
    #[test]
    fn supervision_error_works_as_default_child_error() {
        struct Default42;
        #[async_trait::async_trait]
        impl Actor for Default42 {
            type Msg = ();
            async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
        }
        struct AnyParent;
        #[async_trait::async_trait]
        impl Actor for AnyParent {
            type Msg = ();
            async fn handle(&mut self, _ctx: &mut Context<Self>, _msg: ()) {}
        }
        impl SupervisorOf<Default42> for AnyParent {
            type ChildError = SupervisionError;
        }
        let p = AnyParent;
        let err = SupervisionError::new("crash");
        assert_eq!(SupervisorOf::<Default42>::decide(&p, &err), Directive::Restart);
    }

    #[test]
    fn supervision_error_displays_message() {
        let e = SupervisionError::new("halt");
        assert_eq!(e.to_string(), "halt");
    }
}
