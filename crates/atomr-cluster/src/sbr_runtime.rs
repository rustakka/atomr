//! SBR runtime — wires a [`crate::DowningStrategy`] into a
//! [`crate::MembershipState`] and emits the resulting downing
//! actions.
//!
//! Runs on a tick with a stability deadline; if the partition has been
//! observed for at least `stable_after`, it consults the configured
//! strategy and returns the actions the leader should apply.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::member::{Member, MemberStatus};
use crate::membership::MembershipState;
use crate::sbr::{DowningDecision, DowningStrategy, QuorumObserver};

/// Action emitted by [`SbrRuntime::tick`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SbrAction {
    /// No change; partition not yet stable, or no decision.
    None,
    /// Down each address (the unreachable side).
    DownUnreachable(Vec<String>),
    /// Down every member (catastrophic — the strategy chose `DownAll`).
    DownAll(Vec<String>),
    /// Down this node (we lost; voluntarily exit).
    DownSelf,
}

/// Runtime that pairs a strategy with a stability deadline.
pub struct SbrRuntime<S: DowningStrategy> {
    strategy: S,
    stable_after: Duration,
    /// When did we first observe a non-empty unreachable set?
    /// Reset to `None` when the unreachable set is empty.
    unstable_since: Option<Instant>,
    /// Optional quorum observer (FR-7b). Notified once on the loss
    /// transition (action becomes `DownSelf`) and once on the
    /// subsequent regain (partition heals after a prior loss).
    observer: Option<Arc<dyn QuorumObserver>>,
    /// `true` once we have fired `on_quorum_lost` and not yet fired the
    /// matching `on_quorum_regained`. Drives edge-triggered semantics.
    quorum_lost: bool,
}

impl<S: DowningStrategy> SbrRuntime<S> {
    pub fn new(strategy: S, stable_after: Duration) -> Self {
        Self { strategy, stable_after, unstable_since: None, observer: None, quorum_lost: false }
    }

    /// Attach a [`QuorumObserver`] (FR-7b). Builder-style; consumes and
    /// returns `self`.
    pub fn with_observer(mut self, observer: Arc<dyn QuorumObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Fire `on_quorum_lost` exactly once per loss episode.
    fn note_quorum_lost(&mut self) {
        if !self.quorum_lost {
            self.quorum_lost = true;
            if let Some(o) = &self.observer {
                o.on_quorum_lost();
            }
        }
    }

    /// Fire `on_quorum_regained` exactly once, and only if we had
    /// previously lost quorum.
    fn note_quorum_regained(&mut self) {
        if self.quorum_lost {
            self.quorum_lost = false;
            if let Some(o) = &self.observer {
                o.on_quorum_regained();
            }
        }
    }

    /// One scheduling tick. Returns the action the leader should
    /// apply — typically nothing, sometimes a downing list.
    pub fn tick(&mut self, state: &MembershipState, now: Instant) -> SbrAction {
        // Partition the members by reachability.
        let mut reachable: Vec<&Member> = Vec::new();
        let mut unreachable: Vec<&Member> = Vec::new();
        for m in &state.members {
            if matches!(m.status, MemberStatus::Down | MemberStatus::Removed) {
                continue;
            }
            if state.reachability.is_reachable(&m.address) {
                reachable.push(m);
            } else {
                unreachable.push(m);
            }
        }

        if unreachable.is_empty() {
            // Healthy — reset the stability clock and, if we had
            // previously declared quorum lost, announce the regain.
            self.unstable_since = None;
            self.note_quorum_regained();
            return SbrAction::None;
        }

        // First observation of a partition.
        let since = *self.unstable_since.get_or_insert(now);
        if now.duration_since(since) < self.stable_after {
            return SbrAction::None;
        }

        match self.strategy.decide(&reachable, &unreachable) {
            DowningDecision::Stay => SbrAction::None,
            DowningDecision::DownUnreachable => {
                SbrAction::DownUnreachable(unreachable.iter().map(|m| m.address.to_string()).collect())
            }
            DowningDecision::DownAll => {
                SbrAction::DownAll(state.members.iter().map(|m| m.address.to_string()).collect())
            }
            DowningDecision::DownSelf => {
                // We are on the losing side — quorum is lost. Fire the
                // observer once for this episode.
                self.note_quorum_lost();
                SbrAction::DownSelf
            }
        }
    }

    /// `true` once the partition has been observed for at least
    /// `stable_after`. Useful for telemetry.
    pub fn is_stable(&self, now: Instant) -> bool {
        match self.unstable_since {
            Some(t) => now.duration_since(t) >= self.stable_after,
            None => true, // healthy → trivially stable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sbr::KeepMajorityStrategy;
    use atomr_core::actor::Address;

    fn member(addr: &str, status: MemberStatus) -> Member {
        let mut m = Member::new(Address::local(addr), vec![]);
        m.status = status;
        m
    }

    #[test]
    fn none_when_no_partition() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        let mut rt = SbrRuntime::new(KeepMajorityStrategy, Duration::from_millis(0));
        let r = rt.tick(&s, Instant::now());
        assert_eq!(r, SbrAction::None);
        assert!(rt.is_stable(Instant::now()));
    }

    #[test]
    fn waits_for_stability_window() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        s.add_or_update(member("c", MemberStatus::Up));
        s.reachability.unreachable(Address::local("b"), Address::local("c"));
        let mut rt = SbrRuntime::new(KeepMajorityStrategy, Duration::from_secs(60));
        let now = Instant::now();
        // First tick records the deadline; returns None.
        assert_eq!(rt.tick(&s, now), SbrAction::None);
        assert!(!rt.is_stable(now));
    }

    #[test]
    fn fires_after_stability_window_with_majority() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        s.add_or_update(member("c", MemberStatus::Up));
        // c is unreachable.
        s.reachability.unreachable(Address::local("b"), Address::local("c"));
        let mut rt = SbrRuntime::new(KeepMajorityStrategy, Duration::from_millis(0));
        let r = rt.tick(&s, Instant::now());
        match r {
            SbrAction::DownUnreachable(addrs) => {
                assert_eq!(addrs, vec!["akka://c".to_string()]);
            }
            other => panic!("expected DownUnreachable, got {other:?}"),
        }
    }

    #[test]
    fn quorum_observer_fires_on_loss_and_regain() {
        use crate::sbr::{QuorumObserver, StaticQuorumStrategy};
        use std::sync::atomic::{AtomicU32, Ordering};

        struct Counter {
            lost: AtomicU32,
            regained: AtomicU32,
        }
        impl QuorumObserver for Counter {
            fn on_quorum_lost(&self) {
                self.lost.fetch_add(1, Ordering::SeqCst);
            }
            fn on_quorum_regained(&self) {
                self.regained.fetch_add(1, Ordering::SeqCst);
            }
        }

        let obs = Arc::new(Counter { lost: AtomicU32::new(0), regained: AtomicU32::new(0) });

        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        s.add_or_update(member("c", MemberStatus::Up));
        // a is the local survivor candidate but quorum_size=3 means a
        // single-reachable partition loses → DownSelf.
        s.reachability.unreachable(Address::local("a"), Address::local("b"));
        s.reachability.unreachable(Address::local("a"), Address::local("c"));

        // quorum_size 3, only `a` reachable → DownSelf → loss fires.
        let mut rt = SbrRuntime::new(StaticQuorumStrategy { quorum_size: 3 }, Duration::from_millis(0))
            .with_observer(obs.clone());
        let now = Instant::now();
        assert_eq!(rt.tick(&s, now), SbrAction::DownSelf);
        assert_eq!(obs.lost.load(Ordering::SeqCst), 1);
        // Idempotent within the same loss episode.
        assert_eq!(rt.tick(&s, now), SbrAction::DownSelf);
        assert_eq!(obs.lost.load(Ordering::SeqCst), 1);
        assert_eq!(obs.regained.load(Ordering::SeqCst), 0);

        // Heal the partition → regain fires once.
        s.reachability.reachable(Address::local("a"), Address::local("b"));
        s.reachability.reachable(Address::local("a"), Address::local("c"));
        assert_eq!(rt.tick(&s, now + Duration::from_secs(1)), SbrAction::None);
        assert_eq!(obs.regained.load(Ordering::SeqCst), 1);
        // Idempotent — stays healthy.
        assert_eq!(rt.tick(&s, now + Duration::from_secs(2)), SbrAction::None);
        assert_eq!(obs.regained.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn resets_clock_when_partition_heals() {
        let mut s = MembershipState::new();
        s.add_or_update(member("a", MemberStatus::Up));
        s.add_or_update(member("b", MemberStatus::Up));
        s.reachability.unreachable(Address::local("a"), Address::local("b"));
        let mut rt = SbrRuntime::new(KeepMajorityStrategy, Duration::from_secs(60));
        let now = Instant::now();
        let _ = rt.tick(&s, now);
        // Heal partition.
        s.reachability.reachable(Address::local("a"), Address::local("b"));
        let r = rt.tick(&s, now + Duration::from_secs(1));
        assert_eq!(r, SbrAction::None);
        assert!(rt.is_stable(now + Duration::from_secs(1)));
    }
}
