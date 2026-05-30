//! ClusterSingletonManager / Proxy — one logical actor across the cluster.
//!
//! Phase 7.C of `docs/full-port-plan.md`. Handover protocol:
//!
//! ```text
//! Active(here) ── oldest changed ──► HandingOver ── handover ack ──► Inactive
//! Inactive ────── elected oldest ──► Starting     ── started   ──► Active(here)
//! Inactive ────── observed remote ──► Active(remote)
//! ```
//!
//! While in `HandingOver` or `Starting`, messages submitted via the
//! [`ClusterSingletonProxy`] are buffered (up to `buffer_size`) and
//! flushed once the singleton is `Active` again.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use atomr_core::actor::UntypedActorRef;

/// Monotonic fencing token (FR-7c). Bumped every time this node assumes
/// the singleton, so stale writers from a prior incarnation can be
/// rejected by comparing tokens. `Ord` so the highest token wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(pub u64);

/// Opaque serialized external session carried across a handover.
///
/// The bytes are produced by the outgoing singleton's
/// [`SingletonHandoff::prepare_handoff`] and handed to the incoming
/// singleton's [`SingletonHandoff::assume`]. The manager never inspects
/// them.
pub struct HandoffState(pub Vec<u8>);

/// Hook for migrating *external* state (e.g. a broker session, a lease,
/// an open file) when the singleton moves between nodes (FR-7c).
///
/// `prepare_handoff` runs on the outgoing node when it enters
/// `HandingOver`; `assume` runs on the incoming node when it becomes
/// `Active(here)`, receiving the prior state (if any was captured
/// locally) and the freshly-bumped [`FenceToken`].
#[async_trait::async_trait]
pub trait SingletonHandoff: Send + 'static {
    async fn prepare_handoff(&mut self) -> HandoffState;
    async fn assume(&mut self, prior: Option<HandoffState>, fence: FenceToken);
}

/// Singleton lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SingletonState {
    /// No singleton known yet.
    Inactive,
    /// We are about to become the singleton, but haven't started it.
    Starting,
    /// The singleton lives at this ref.
    Active { ref_: UntypedActorRef, here: bool },
    /// We were the singleton; new oldest member is taking over.
    HandingOver,
}

/// Buffered envelope used during handover. The payload is type-erased
/// (Box<dyn Any>) because the proxy doesn't know the concrete message
/// type at the API boundary; recipients downcast on flush. Phase 13
/// will replace this with typed-per-(M) buffering.
type BufferedMsg = Box<dyn FnOnce(&UntypedActorRef) + Send + 'static>;

/// Decides which node hosts the singleton based on oldest up-member —
/// a hook is provided so tests can simulate handover without wiring
/// the full cluster.
pub struct ClusterSingletonManager {
    state: RwLock<SingletonState>,
    buffer: parking_lot::Mutex<VecDeque<BufferedMsg>>,
    buffer_size: usize,
    /// Count of messages dropped because the buffer was full.
    drops: parking_lot::Mutex<u64>,
    /// Monotonic fencing token; bumped on every `set_active_here`.
    fence: AtomicU64,
    /// Optional external-state migration hook (FR-7c).
    handoff: tokio::sync::Mutex<Option<Box<dyn SingletonHandoff>>>,
    /// State captured by the last `prepare_handoff`, consumed by the
    /// next local `assume`. For a single-node hand-back this carries the
    /// session across; cross-node carriage is a transport concern.
    prior: parking_lot::Mutex<Option<HandoffState>>,
}

impl Default for ClusterSingletonManager {
    fn default() -> Self {
        Self {
            state: RwLock::new(SingletonState::Inactive),
            buffer: parking_lot::Mutex::new(VecDeque::new()),
            buffer_size: 1_000,
            drops: parking_lot::Mutex::new(0),
            fence: AtomicU64::new(0),
            handoff: tokio::sync::Mutex::new(None),
            prior: parking_lot::Mutex::new(None),
        }
    }
}

impl ClusterSingletonManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Construct with a custom proxy buffer size.
    pub fn with_buffer_size(size: usize) -> Arc<Self> {
        Arc::new(Self { buffer_size: size, ..Self::default() })
    }

    /// Construct with an external-state migration hook (FR-7c).
    pub fn with_handoff(handoff: Box<dyn SingletonHandoff>) -> Arc<Self> {
        Arc::new(Self { handoff: tokio::sync::Mutex::new(Some(handoff)), ..Self::default() })
    }

    pub fn state(&self) -> SingletonState {
        self.state.read().clone()
    }

    /// Current fencing token (FR-7c). Monotonically increases each time
    /// this node assumes the singleton via [`Self::set_active_here`].
    pub fn fence(&self) -> FenceToken {
        FenceToken(self.fence.load(Ordering::SeqCst))
    }

    /// Mark `r` as the local singleton (we won the election).
    ///
    /// Bumps the fencing token, runs the [`SingletonHandoff::assume`]
    /// hook (if configured) with any locally-captured prior state, then
    /// flushes any messages buffered during handover.
    pub fn set_active_here(&self, r: UntypedActorRef) {
        // Bump fence first so `assume` sees the incarnation it owns.
        let token = FenceToken(self.fence.fetch_add(1, Ordering::SeqCst) + 1);
        self.run_assume(token);
        *self.state.write() = SingletonState::Active { ref_: r.clone(), here: true };
        self.flush(&r);
    }

    /// Drive the `assume` hook (if any), consuming the prior captured
    /// state. Runs the async hook to completion on the current thread.
    fn run_assume(&self, token: FenceToken) {
        // `try_lock` cannot contend: the singleton lifecycle is driven from a
        // single task. Skip the hook rather than panic if that ever changes.
        if let Ok(mut guard) = self.handoff.try_lock() {
            if let Some(h) = guard.as_mut() {
                let prior = self.prior.lock().take();
                futures::executor::block_on(h.assume(prior, token));
            }
        }
    }

    /// Mark `r` as the remote singleton (some other node is hosting it).
    pub fn set_active_remote(&self, r: UntypedActorRef) {
        *self.state.write() = SingletonState::Active { ref_: r.clone(), here: false };
        self.flush(&r);
    }

    /// Begin handover (we used to be Active(here)).
    ///
    /// Runs the [`SingletonHandoff::prepare_handoff`] hook (if
    /// configured) and stashes the captured state locally so a
    /// subsequent local `assume` can pick it up.
    pub fn begin_handover(&self) {
        // See `run_assume`: uncontended by design; skip the hook rather than
        // panic on the impossible contention path. The state transition below
        // must still run regardless.
        if let Ok(mut guard) = self.handoff.try_lock() {
            if let Some(h) = guard.as_mut() {
                let captured = futures::executor::block_on(h.prepare_handoff());
                *self.prior.lock() = Some(captured);
            }
        }
        *self.state.write() = SingletonState::HandingOver;
    }

    /// Begin starting (we were elected as the new oldest).
    pub fn begin_starting(&self) {
        *self.state.write() = SingletonState::Starting;
    }

    /// Forget the current singleton entirely.
    pub fn clear(&self) {
        *self.state.write() = SingletonState::Inactive;
    }

    pub fn current(&self) -> Option<UntypedActorRef> {
        match &*self.state.read() {
            SingletonState::Active { ref_, .. } => Some(ref_.clone()),
            _ => None,
        }
    }

    /// Buffer `deliver` for replay once the singleton becomes
    /// `Active`. Used by the proxy when the singleton isn't yet
    /// reachable. Returns `true` if buffered, `false` if the buffer
    /// was full (in which case the caller can route to DeadLetters).
    fn buffer_or_deliver<F>(&self, deliver: F) -> bool
    where
        F: FnOnce(&UntypedActorRef) + Send + 'static,
    {
        if let Some(r) = self.current() {
            deliver(&r);
            return true;
        }
        let mut q = self.buffer.lock();
        if q.len() >= self.buffer_size {
            *self.drops.lock() += 1;
            return false;
        }
        q.push_back(Box::new(deliver));
        true
    }

    fn flush(&self, target: &UntypedActorRef) {
        let mut q = self.buffer.lock();
        while let Some(deliver) = q.pop_front() {
            deliver(target);
        }
    }

    /// Number of currently-buffered messages (waiting for handover to
    /// complete).
    pub fn buffered(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Total number of messages dropped due to buffer-full overflow.
    pub fn drops(&self) -> u64 {
        *self.drops.lock()
    }
}

/// Proxy that routes messages to the current singleton, buffering
/// during handover.
pub struct ClusterSingletonProxy {
    pub manager: Arc<ClusterSingletonManager>,
}

impl ClusterSingletonProxy {
    pub fn new(manager: Arc<ClusterSingletonManager>) -> Self {
        Self { manager }
    }

    pub fn singleton(&self) -> Option<UntypedActorRef> {
        self.manager.current()
    }

    /// Schedule `deliver` against the singleton. If `Active`, runs
    /// immediately; if `Inactive`/`Starting`/`HandingOver`, buffers
    /// for replay. Returns `false` if the buffer was full.
    pub fn send<F>(&self, deliver: F) -> bool
    where
        F: FnOnce(&UntypedActorRef) + Send + 'static,
    {
        self.manager.buffer_or_deliver(deliver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_core::actor::Inbox;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn proxy_routes_to_current_singleton() {
        let mgr = ClusterSingletonManager::new();
        let inbox = Inbox::<u32>::new("singleton");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        let proxy = ClusterSingletonProxy::new(mgr);
        assert!(proxy.singleton().is_some());
    }

    #[test]
    fn handover_state_transitions() {
        let mgr = ClusterSingletonManager::new();
        assert!(matches!(mgr.state(), SingletonState::Inactive));
        mgr.begin_starting();
        assert!(matches!(mgr.state(), SingletonState::Starting));
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert!(matches!(mgr.state(), SingletonState::Active { here: true, .. }));
        mgr.begin_handover();
        assert!(matches!(mgr.state(), SingletonState::HandingOver));
    }

    #[tokio::test]
    async fn proxy_buffers_during_handover_and_flushes_after() {
        let mgr = ClusterSingletonManager::new();
        let proxy = ClusterSingletonProxy::new(mgr.clone());

        let calls = Arc::new(AtomicU32::new(0));
        // Send 3 messages while inactive — all buffered.
        for _ in 0..3 {
            let c = calls.clone();
            assert!(proxy.send(move |_r| {
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }
        assert_eq!(mgr.buffered(), 3);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // Become active → buffer flushes.
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert_eq!(mgr.buffered(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // After active, send delivers immediately.
        let c2 = calls.clone();
        proxy.send(move |_| {
            c2.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn full_buffer_drops_and_counts_overflow() {
        let mgr = ClusterSingletonManager::with_buffer_size(2);
        let proxy = ClusterSingletonProxy::new(mgr.clone());
        assert!(proxy.send(|_| {}));
        assert!(proxy.send(|_| {}));
        // Third should overflow.
        assert!(!proxy.send(|_| {}));
        assert_eq!(mgr.drops(), 1);
        assert_eq!(mgr.buffered(), 2);
    }

    #[test]
    fn fence_token_is_monotonic() {
        let mgr = ClusterSingletonManager::new();
        assert_eq!(mgr.fence(), FenceToken(0));
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        let f1 = mgr.fence();
        mgr.begin_handover();
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        let f2 = mgr.fence();
        assert!(f2 > f1, "fence must advance: {f1:?} -> {f2:?}");
        assert_eq!(f1, FenceToken(1));
        assert_eq!(f2, FenceToken(2));
    }

    #[test]
    fn handoff_prepare_then_assume_carries_state_and_fence() {
        use std::sync::atomic::AtomicU64;

        struct Session {
            // Records the fence seen by the most recent `assume`.
            assumed_fence: Arc<AtomicU64>,
            // Records the prior bytes seen by the most recent `assume`.
            assumed_prior: Arc<parking_lot::Mutex<Option<Vec<u8>>>>,
        }
        #[async_trait::async_trait]
        impl SingletonHandoff for Session {
            async fn prepare_handoff(&mut self) -> HandoffState {
                HandoffState(b"session-42".to_vec())
            }
            async fn assume(&mut self, prior: Option<HandoffState>, fence: FenceToken) {
                self.assumed_fence.store(fence.0, Ordering::SeqCst);
                *self.assumed_prior.lock() = prior.map(|p| p.0);
            }
        }

        let seen_fence = Arc::new(AtomicU64::new(0));
        let seen_prior = Arc::new(parking_lot::Mutex::new(None));
        let mgr = ClusterSingletonManager::with_handoff(Box::new(Session {
            assumed_fence: seen_fence.clone(),
            assumed_prior: seen_prior.clone(),
        }));

        let inbox = Inbox::<u32>::new("s");
        // First activation: no prior state, fence == 1.
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert_eq!(seen_fence.load(Ordering::SeqCst), 1);
        assert!(seen_prior.lock().is_none());

        // Hand over (captures session bytes) then re-assume locally.
        mgr.begin_handover();
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert_eq!(seen_fence.load(Ordering::SeqCst), 2);
        assert_eq!(seen_prior.lock().clone(), Some(b"session-42".to_vec()));
    }

    #[test]
    fn handoff_buffering_still_works() {
        // With a handoff configured, buffering during inactive/handover
        // must be unchanged.
        struct Noop;
        #[async_trait::async_trait]
        impl SingletonHandoff for Noop {
            async fn prepare_handoff(&mut self) -> HandoffState {
                HandoffState(vec![])
            }
            async fn assume(&mut self, _prior: Option<HandoffState>, _fence: FenceToken) {}
        }
        let mgr = ClusterSingletonManager::with_handoff(Box::new(Noop));
        let proxy = ClusterSingletonProxy::new(mgr.clone());
        let calls = Arc::new(AtomicU32::new(0));
        for _ in 0..2 {
            let c = calls.clone();
            assert!(proxy.send(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
            }));
        }
        assert_eq!(mgr.buffered(), 2);
        let inbox = Inbox::<u32>::new("s");
        mgr.set_active_here(inbox.actor_ref().as_untyped());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(mgr.buffered(), 0);
    }

    #[test]
    fn set_active_remote_marks_here_false() {
        let mgr = ClusterSingletonManager::new();
        let inbox = Inbox::<u32>::new("remote-host");
        mgr.set_active_remote(inbox.actor_ref().as_untyped());
        match mgr.state() {
            SingletonState::Active { here, .. } => assert!(!here),
            _ => panic!("expected active-remote"),
        }
    }
}
