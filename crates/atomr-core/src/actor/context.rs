//! `Context<A>` — the actor's window into the system.
//! (partial).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use std::marker::PhantomData;

use super::actor_cell::{ChildEntry, SystemMsg};
use super::actor_ref::{ActorRef, UntypedActorRef};
use super::path::ActorPath;
use super::props::Props;
use super::scheduler::Scheduler;
use super::sender::Sender;
use super::traits::Actor;

/// Public, opaque handle to the running [`super::ActorSystem`] that an actor
/// can use to reach a small, curated subset of the system surface — currently
/// just [`Self::scheduler`]. The handle holds a `Weak` reference so it does
/// not keep the system alive; callers must check [`Self::is_alive`] (or
/// receive `None` from [`Self::scheduler`]) before relying on it.
///
/// This type is the canonical way for binding layers (e.g. `pycore`) to
/// register cancellable timers from inside `Actor::handle` without spawning
/// detached `tokio::spawn` tasks. The internal `ActorSystemInner` type is
/// crate-private, so this thin wrapper is the only stable public path.
#[derive(Clone)]
pub struct SystemHandle {
    inner: Weak<super::actor_system::ActorSystemInner>,
}

impl SystemHandle {
    pub(crate) fn new(inner: Weak<super::actor_system::ActorSystemInner>) -> Self {
        Self { inner }
    }

    /// True if the underlying [`super::ActorSystem`] has not been dropped.
    pub fn is_alive(&self) -> bool {
        self.inner.strong_count() > 0
    }

    /// Borrow the system's [`Scheduler`]. Returns `None` if the system has
    /// been dropped.
    pub fn scheduler(&self) -> Option<Arc<dyn Scheduler>> {
        self.inner.upgrade().map(|s| s.scheduler.clone())
    }
}

/// Lifecycle phase exposed via [`Context::phase`]. Phase 1.C of
/// `docs/full-port-plan.md` — runtime precursor to the phantom-typed
/// `Context<A, Phase>` (kept additive so it doesn't break existing
/// signatures). Stage-only APIs assert against this in debug builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecyclePhase {
    Starting,
    Running,
    Stopping,
}

/// Passed to every `Actor::handle` call.
pub struct Context<A: Actor> {
    pub(crate) self_ref: ActorRef<A::Msg>,
    pub(crate) path: ActorPath,
    pub(crate) system: Weak<super::actor_system::ActorSystemInner>,
    pub(crate) children: HashMap<String, ChildEntry>,
    pub(crate) watching: HashSet<ActorPath>,
    pub(crate) watched_by: HashSet<UntypedActorRef>,
    pub(crate) stash: VecDeque<A::Msg>,
    pub(crate) receive_timeout: Option<Duration>,
    pub(crate) current_sender: Sender,
    pub(crate) current_metadata: super::metadata::Metadata,
    pub(crate) stopping: bool,
    pub(crate) phase: LifecyclePhase,
}

impl<A: Actor> Context<A> {
    pub(crate) fn new(
        self_ref: ActorRef<A::Msg>,
        path: ActorPath,
        system: Weak<super::actor_system::ActorSystemInner>,
    ) -> Self {
        Self {
            self_ref,
            path,
            system,
            children: HashMap::new(),
            watching: HashSet::new(),
            watched_by: HashSet::new(),
            stash: VecDeque::new(),
            receive_timeout: None,
            current_sender: Sender::None,
            current_metadata: super::metadata::Metadata::new(),
            stopping: false,
            phase: LifecyclePhase::Starting,
        }
    }

    /// Metadata (trace context + baggage) of the message currently being
    /// processed (FR-10). Empty unless the sender attached it. Handlers can
    /// read it to extend trace context onto outgoing sends.
    pub fn metadata(&self) -> &super::metadata::Metadata {
        &self.current_metadata
    }

    /// Current lifecycle phase. Phase 1.C marker — useful in
    /// generic helpers that need to gate calls (e.g. `become`,
    /// `unstash_all`) without taking a typed-`Context<A, P>`
    /// parameter.
    pub fn phase(&self) -> LifecyclePhase {
        self.phase
    }

    pub fn self_ref(&self) -> &ActorRef<A::Msg> {
        &self.self_ref
    }

    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Opaque handle to the running [`super::ActorSystem`]. Useful in
    /// binding layers that need to register cancellable timers via the
    /// system [`Scheduler`] without spawning detached `tokio` tasks.
    /// Holds a `Weak` reference; see [`SystemHandle`].
    pub fn system_handle(&self) -> SystemHandle {
        SystemHandle::new(self.system.clone())
    }

    /// Spawn a child actor under this context.
    pub fn spawn<B: Actor>(&mut self, props: Props<B>, name: &str) -> Result<ActorRef<B::Msg>, SpawnError> {
        if self.children.contains_key(name) {
            return Err(SpawnError::NameTaken(name.into()));
        }
        let system = self.system.upgrade().ok_or(SpawnError::SystemTerminated)?;
        let child_path = self.path.child(name);
        let r = super::actor_cell::spawn_cell::<B>(system.clone(), props, child_path.clone())?;
        if let Some(obs) = system.spawn_observer.read().as_ref() {
            obs.on_spawn(&child_path, Some(&self.path), std::any::type_name::<B>());
        }
        self.children.insert(
            name.to_string(),
            ChildEntry { path: child_path, untyped: r.as_untyped(), system_tx: r.system_sender() },
        );
        Ok(r)
    }

    /// Stop a specific child.
    pub fn stop_child(&mut self, name: &str) {
        if let Some(c) = self.children.get(name) {
            let _ = c.system_tx.send(SystemMsg::Stop);
        }
    }

    /// Watch another actor. The sender is notified with a `SystemMsg::Terminated`
    /// when the watched actor stops.
    pub fn watch<M: Send + 'static>(&mut self, target: &ActorRef<M>) {
        if self.watching.insert(target.path().clone()) {
            let _ = target.system_sender().send(SystemMsg::Watch(self.self_ref.as_untyped()));
        }
    }

    /// Stop watching.
    pub fn unwatch<M: Send + 'static>(&mut self, target: &ActorRef<M>) {
        if self.watching.remove(target.path()) {
            let _ = target.system_sender().send(SystemMsg::Unwatch(self.path.clone()));
        }
    }

    /// Stash the currently-processed message for later unstash.
    pub fn stash(&mut self, msg: A::Msg) {
        self.stash.push_back(msg);
    }

    /// Put all stashed messages back at the front of the mailbox.
    pub fn unstash_all(&mut self) -> Vec<A::Msg> {
        let mut out = Vec::with_capacity(self.stash.len());
        while let Some(m) = self.stash.pop_front() {
            out.push(m);
        }
        out
    }

    /// Stop self.
    pub fn stop_self(&mut self) {
        self.stopping = true;
    }

    /// Set idle-receive timeout (like).
    pub fn set_receive_timeout(&mut self, d: Option<Duration>) {
        self.receive_timeout = d;
    }

    /// Typed sender of the message currently being processed.
    ///
    /// Returns [`Sender::None`] if the sender slot was empty (the
    /// analog of `Sender == NoSender`).
    pub fn sender(&self) -> &Sender {
        &self.current_sender
    }

    /// Backwards-compatible alias for [`Context::sender`].
    #[doc(hidden)]
    pub fn sender_typed(&self) -> &Sender {
        &self.current_sender
    }

    /// Borrow this context as a phase-typed view. The phase parameter is a
    /// phantom witness only — call sites typically use one of
    /// [`Context::starting`], [`Context::running`], or
    /// [`Context::stopping_view`] to get a view whose method surface
    /// matches the phase.
    pub fn phased<P: PhaseMarker>(&mut self) -> Option<TypedContext<'_, A, P>> {
        if P::PHASE == self.phase {
            Some(TypedContext { inner: self, _phase: PhantomData })
        } else {
            None
        }
    }

    /// Phase-typed view valid only while the actor is in `Starting`.
    pub fn starting(&mut self) -> Option<TypedContext<'_, A, Starting>> {
        self.phased::<Starting>()
    }

    /// Phase-typed view valid only while the actor is in `Running`.
    pub fn running(&mut self) -> Option<TypedContext<'_, A, Running>> {
        self.phased::<Running>()
    }

    /// Phase-typed view valid only while the actor is in `Stopping`.
    pub fn stopping_view(&mut self) -> Option<TypedContext<'_, A, Stopping>> {
        self.phased::<Stopping>()
    }
}

/// Phase markers for [`TypedContext`]. Each marker type implements
/// [`PhaseMarker`] with a const [`LifecyclePhase`] discriminant; the
/// `PhasedContext` view inspects this at runtime to gate phase-only APIs.
pub trait PhaseMarker: sealed::Sealed {
    const PHASE: LifecyclePhase;
}

/// Marker for the `Starting` lifecycle phase.
pub struct Starting;
/// Marker for the `Running` lifecycle phase.
pub struct Running;
/// Marker for the `Stopping` lifecycle phase.
pub struct Stopping;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Starting {}
    impl Sealed for super::Running {}
    impl Sealed for super::Stopping {}
}

impl PhaseMarker for Starting {
    const PHASE: LifecyclePhase = LifecyclePhase::Starting;
}
impl PhaseMarker for Running {
    const PHASE: LifecyclePhase = LifecyclePhase::Running;
}
impl PhaseMarker for Stopping {
    const PHASE: LifecyclePhase = LifecyclePhase::Stopping;
}

/// Phase-typed view over a [`Context`]. The phase parameter is a phantom
/// witness; only methods valid in that phase are exposed.
///
/// `Starting`-only: nothing yet (constructor surface).
/// `Running` exposes `become_`, `unstash_all`, `set_receive_timeout`.
/// `Stopping` exposes only inspection (no new state changes).
pub struct TypedContext<'a, A: Actor, P: PhaseMarker> {
    inner: &'a mut Context<A>,
    _phase: PhantomData<P>,
}

impl<'a, A: Actor, P: PhaseMarker> TypedContext<'a, A, P> {
    pub fn ctx(&self) -> &Context<A> {
        self.inner
    }
    pub fn ctx_mut(&mut self) -> &mut Context<A> {
        self.inner
    }
    pub fn self_ref(&self) -> &ActorRef<A::Msg> {
        &self.inner.self_ref
    }
}

impl<'a, A: Actor> TypedContext<'a, A, Running> {
    /// Set the receive-idle timeout. Only callable from a `Running` view.
    pub fn set_receive_timeout(&mut self, d: Option<Duration>) {
        self.inner.set_receive_timeout(d);
    }

    /// Drain stashed messages. Only callable from a `Running` view.
    pub fn unstash_all(&mut self) -> Vec<A::Msg> {
        self.inner.unstash_all()
    }

    /// Begin self-stop. Transitions the underlying context to `Stopping`
    /// once the cell observes the request.
    pub fn stop_self(&mut self) {
        self.inner.stop_self();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("child name `{0}` already taken")]
    NameTaken(String),
    #[error("actor system has terminated")]
    SystemTerminated,
}
