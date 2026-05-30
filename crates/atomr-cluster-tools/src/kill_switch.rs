//! FR-5 — cluster-wide kill switch (emergency halt latch).
//!
//! A [`ClusterKillSwitch`] is a distributed, monotonic "engaged / not
//! engaged" latch backed by a distributed-data [`Flag`] CRDT stored in a
//! [`Replicator`] under a well-known key. Because `Flag` OR-merges, once
//! *any* node engages the switch the engaged state **survives merge,
//! gossip, and rebalance** — there is no way to accidentally un-latch by
//! losing a delta or by a node rejoining with a stale (off) copy. This
//! is exactly the property an emergency halt needs.
//!
//! # Epochs (how "reset" works)
//!
//! A `Flag` is monotonic: it can only go `false -> true`, never back.
//! That is the right safety property for *engaging*, but it means we
//! cannot simply flip the same flag off to recover. Instead the switch
//! is **epoch-versioned**: the current epoch is held in an
//! [`LwwRegister<u64>`], and the latch flag lives under a key derived
//! from `(base_key, epoch)`. [`ClusterKillSwitch::reset`] advances the
//! epoch (LWW with a higher timestamp), which moves the gate to a
//! *fresh* flag that starts `false`. The old epoch's flag stays latched
//! forever (audit trail), but no longer gates the system.
//!
//! Reset is deliberately guarded by a **two-person rule**
//! ([`ResetAuthorization`]): two distinct, non-empty approvers.
//!
//! # Quiescence
//!
//! [`ClusterKillSwitch::engage`] only flips the latch. Bringing the
//! system to a safe stop is a separate step: registered
//! [`HaltGuarded`] parties are asked to halt and acknowledge via
//! [`ClusterKillSwitch::await_quiescence`]. Quiescence has two tiers:
//!
//! * **Local** — registered [`HaltGuarded`] parties on this node are
//!   asked to halt and acknowledge through the ack channel they were
//!   handed at registration.
//! * **Cross-node** — when a [`HaltTransport`] is attached (via
//!   [`ClusterKillSwitch::with_transport`]), `await_quiescence` also
//!   broadcasts a [`HaltPdu::Halt`] to every peer and awaits a
//!   [`HaltPdu::Ack`] back from each. Peers feed inbound PDUs in via
//!   [`ClusterKillSwitch::apply_pdu`], which engages their local latch,
//!   quiesces their own guarded parties, and acks. With no transport the
//!   switch is purely local — sufficient for single-node operation and
//!   tests.
//!
//! A `link_stream` adapter (so an atomr-streams `KillSwitch` could
//! derive from this latch) is intentionally **out of scope** here to
//! avoid taking an `atomr-streams` dependency; the streams-side switch
//! would subscribe to this latch's flag key and complete its stream
//! when engaged.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;

use atomr_cluster::QuorumObserver;
use atomr_distributed_data::{Flag, LwwRegister, Replicator};

/// Why the kill switch was engaged. Recorded alongside the latch for
/// post-incident analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HaltReason {
    /// Operator pulled the switch manually; carries a free-form note.
    Manual(String),
    /// SBR / quorum machinery reported quorum loss (see
    /// [`KillSwitchQuorumObserver`]).
    QuorumLost,
    /// A risk/pre-trade guard tripped; carries a description.
    RiskBreach(String),
}

/// Token returned by [`ClusterKillSwitch::engage`], identifying the
/// engage operation (monotonic, per-process).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HaltToken(pub u64);

/// A party that must be brought to a safe stop when the switch engages.
/// `on_halt` should stop accepting new work and prepare to acknowledge
/// quiescence; it must not block for long.
pub trait HaltGuarded: Send {
    fn on_halt(&mut self, reason: &HaltReason);
}

/// Two-person authorization required to [`ClusterKillSwitch::reset`].
#[derive(Debug, Clone)]
pub struct ResetAuthorization {
    pub approver_a: String,
    pub approver_b: String,
}

/// Result of [`ClusterKillSwitch::await_quiescence`].
///
/// `acked` / `total` are the **combined** local + remote counts so existing
/// callers keep working; the `remote_*` fields break that down for clusters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuiescenceReport {
    /// How many parties acknowledged within the timeout (local + remote).
    pub acked: usize,
    /// How many parties were expected to ack (local guarded + remote peers).
    pub total: usize,
    /// `true` if not all parties acked before the deadline.
    pub timed_out: bool,
    /// How many remote peers acked their halt within the timeout.
    pub remote_acked: usize,
    /// How many remote peers a halt was broadcast to (0 with no transport).
    pub remote_total: usize,
}

/// Error returned by [`ClusterKillSwitch::reset`] when authorization is
/// invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResetError {
    #[error("reset requires two non-empty approvers")]
    MissingApprover,
    #[error("reset requires two distinct approvers (got the same identity twice)")]
    DuplicateApprover,
}

/// A registered guarded party: its halt callback plus an ack channel.
/// On halt we invoke `on_halt`, then await its ack on the receiver.
struct Guarded {
    party: Mutex<Box<dyn HaltGuarded>>,
    ack_rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

/// Handle handed back from [`ClusterKillSwitch::register_guarded`] that a
/// party uses to acknowledge it has quiesced.
pub struct AckHandle {
    tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AckHandle {
    /// Acknowledge that this party has reached a safe stopped state.
    pub fn ack(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Wire shape of a cross-node kill-switch exchange. Mirrors the
/// `MediatorPdu` pattern used by [`ClusterPubSub`](crate::ClusterPubSub):
/// an opaque, serializable message that a [`HaltTransport`] carries to a
/// peer and that the peer feeds back via [`ClusterKillSwitch::apply_pdu`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HaltPdu {
    /// Ask the receiving node to engage its latch for `epoch`, bring its
    /// local guarded parties to a safe stop, and ack. Carries the
    /// originator's node id and the engaging [`HaltReason`] so the remote
    /// audit trail matches the coordinator's.
    Halt { from: String, epoch: u64, reason: HaltReason },
    /// Acknowledge that `from` has quiesced its local parties for `epoch`.
    Ack { from: String, epoch: u64 },
}

/// Pluggable transport for cross-node kill-switch fan-out. Sends a
/// [`HaltPdu`] to a peer node identified by an opaque string node id
/// (typically `Address::to_string()`); the receiver feeds inbound PDUs
/// back into its local switch via [`ClusterKillSwitch::apply_pdu`].
///
/// This is the kill-switch analogue of
/// [`MediatorTransport`](crate::MediatorTransport): atomr does not bind
/// the safety latch to a concrete wire, so the cluster layer supplies the
/// transport.
pub trait HaltTransport: Send + Sync + 'static {
    /// Node ids of all known peers, **excluding** this node.
    fn peers(&self) -> Vec<String>;
    /// Send `pdu` to `target_node`.
    fn send(&self, target_node: &str, pdu: HaltPdu);
}

/// Distributed emergency-halt latch. Cheap to clone via `Arc`.
pub struct ClusterKillSwitch {
    replicator: Arc<Replicator>,
    /// Base well-known key; the per-epoch flag key is derived from it.
    base_key: String,
    /// Stable node id used for LWW register writes.
    node: String,
    /// Monotonic source for [`HaltToken`]s and LWW timestamps.
    seq: AtomicU64,
    /// Reason recorded by the most recent local `engage`.
    last_reason: Mutex<Option<HaltReason>>,
    /// Locally-registered guarded parties for quiescence.
    guarded: Mutex<Vec<Guarded>>,
    /// Optional cross-node fan-out transport. `None` ⇒ purely local.
    transport: Option<Arc<dyn HaltTransport>>,
    /// Per-epoch set of peer node ids that have acked their halt.
    remote_acks: Mutex<HashMap<u64, HashSet<String>>>,
    /// Wakes `await_quiescence` when a remote ack lands in `remote_acks`.
    ack_notify: Notify,
}

impl ClusterKillSwitch {
    /// Create a switch over `replicator`, gating on the well-known
    /// `base_key`. `node` is this node's stable id (used for LWW
    /// register tie-breaking on the epoch counter).
    pub fn new(
        replicator: Arc<Replicator>,
        base_key: impl Into<String>,
        node: impl Into<String>,
    ) -> Arc<Self> {
        Self::build(replicator, base_key, node, None)
    }

    /// Like [`new`](Self::new) but attaches a [`HaltTransport`] so
    /// [`await_quiescence`](Self::await_quiescence) also fans the halt out
    /// to remote peers and collects their acks.
    pub fn with_transport(
        replicator: Arc<Replicator>,
        base_key: impl Into<String>,
        node: impl Into<String>,
        transport: Arc<dyn HaltTransport>,
    ) -> Arc<Self> {
        Self::build(replicator, base_key, node, Some(transport))
    }

    fn build(
        replicator: Arc<Replicator>,
        base_key: impl Into<String>,
        node: impl Into<String>,
        transport: Option<Arc<dyn HaltTransport>>,
    ) -> Arc<Self> {
        let base_key = base_key.into();
        let this = Arc::new(Self {
            replicator,
            base_key,
            node: node.into(),
            seq: AtomicU64::new(0),
            last_reason: Mutex::new(None),
            guarded: Mutex::new(Vec::new()),
            transport,
            remote_acks: Mutex::new(HashMap::new()),
            ack_notify: Notify::new(),
        });
        // Seed the epoch register at 0 if absent so reads are stable.
        if this.epoch_register().is_none() {
            this.replicator.update(&this.epoch_key(), LwwRegister::new(&this.node, 0u64, this.next_ts()));
        }
        this
    }

    /// Access the backing replicator (telemetry / testing).
    pub fn replicator(&self) -> &Arc<Replicator> {
        &self.replicator
    }

    fn epoch_key(&self) -> String {
        format!("{}::epoch", self.base_key)
    }

    /// Per-epoch flag key. The gate flag for epoch N lives here.
    fn flag_key(&self, epoch: u64) -> String {
        format!("{}::flag::{}", self.base_key, epoch)
    }

    fn next_ts(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn epoch_register(&self) -> Option<LwwRegister<u64>> {
        self.replicator.get::<LwwRegister<u64>>(&self.epoch_key())
    }

    /// Current epoch (0 if unset).
    pub fn epoch(&self) -> u64 {
        self.epoch_register().map(|r| *r.value()).unwrap_or(0)
    }

    /// Engage the latch for the current epoch and record `reason`.
    /// Idempotent at the CRDT level (OR-merge); calling twice is safe.
    pub fn engage(&self, reason: HaltReason) -> HaltToken {
        self.engage_epoch(self.epoch(), reason);
        HaltToken(self.next_ts())
    }

    /// Engage (OR-merge → `true`) the latch flag for a specific `epoch` and
    /// record `reason`. Shared by [`engage`](Self::engage) and the remote
    /// [`HaltPdu::Halt`] path so both produce the same latched state.
    fn engage_epoch(&self, epoch: u64, reason: HaltReason) {
        let mut flag = self.replicator.get::<Flag>(&self.flag_key(epoch)).unwrap_or_default();
        flag.switch_on();
        self.replicator.update(&self.flag_key(epoch), flag);
        *self.last_reason.lock() = Some(reason);
    }

    /// Read the merged latch state for the current epoch. Survives
    /// merge/rebalance because the backing `Flag` only OR-merges.
    pub fn is_engaged(&self) -> bool {
        let epoch = self.epoch();
        self.replicator.get::<Flag>(&self.flag_key(epoch)).map(|f| f.enabled()).unwrap_or(false)
    }

    /// Reason recorded by the most recent local `engage`, if any.
    pub fn last_reason(&self) -> Option<HaltReason> {
        self.last_reason.lock().clone()
    }

    /// Register a guarded party. Returns an [`AckHandle`] the party
    /// keeps; when its `on_halt` runs (during `await_quiescence`) it
    /// signals quiescence by calling [`AckHandle::ack`].
    pub fn register_guarded(&self, party: Box<dyn HaltGuarded>) -> AckHandle {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.guarded.lock().push(Guarded { party: Mutex::new(party), ack_rx: Mutex::new(Some(rx)) });
        AckHandle { tx: Some(tx) }
    }

    /// Bring the system to a safe stop and report how many parties
    /// acknowledged within `timeout`.
    ///
    /// Always quiesces this node's local [`HaltGuarded`] parties. When a
    /// [`HaltTransport`] is attached it *also* broadcasts a
    /// [`HaltPdu::Halt`] to every peer and waits for a [`HaltPdu::Ack`]
    /// from each, so the returned [`QuiescenceReport`] reflects the whole
    /// cluster. `timeout` bounds the combined wait.
    pub async fn await_quiescence(&self, timeout: Duration) -> QuiescenceReport {
        let deadline = tokio::time::Instant::now() + timeout;
        let epoch = self.epoch();
        let reason = self.last_reason().unwrap_or(HaltReason::Manual(String::new()));

        // --- Local tier: halt + collect our own guarded parties. ---
        let (local_acked, local_total) = self.collect_local(deadline, &reason).await;

        // --- Remote tier: broadcast the halt and await peer acks. ---
        let (remote_acked, remote_total) = match &self.transport {
            Some(transport) => {
                let peers = transport.peers();
                let remote_total = peers.len();
                if remote_total == 0 {
                    (0, 0)
                } else {
                    // Start each epoch's remote-ack accounting from empty.
                    self.remote_acks.lock().entry(epoch).or_default().clear();
                    for peer in &peers {
                        transport.send(
                            peer,
                            HaltPdu::Halt { from: self.node.clone(), epoch, reason: reason.clone() },
                        );
                    }
                    let acked = self.collect_remote(epoch, peers, deadline).await;
                    (acked, remote_total)
                }
            }
            None => (0, 0),
        };

        let acked = local_acked + remote_acked;
        let total = local_total + remote_total;
        QuiescenceReport { acked, total, timed_out: acked < total, remote_acked, remote_total }
    }

    /// Fire `on_halt` for every local guarded party and count how many ack
    /// before `deadline`. Returns `(acked, total)`.
    async fn collect_local(&self, deadline: tokio::time::Instant, reason: &HaltReason) -> (usize, usize) {
        let mut receivers = Vec::new();
        {
            let guard = self.guarded.lock();
            for g in guard.iter() {
                g.party.lock().on_halt(reason);
                if let Some(rx) = g.ack_rx.lock().take() {
                    receivers.push(rx);
                }
            }
        }
        let total = receivers.len();
        let mut acked = 0usize;
        // Shared absolute deadline: once it passes, remaining waits return
        // immediately, so partial acks are still counted accurately.
        for rx in receivers {
            if let Ok(Ok(())) = tokio::time::timeout_at(deadline, rx).await {
                acked += 1;
            }
        }
        (acked, total)
    }

    /// Wait until every peer in `peers` has acked `epoch` (or `deadline`
    /// passes), returning the number that acked.
    async fn collect_remote(&self, epoch: u64, peers: Vec<String>, deadline: tokio::time::Instant) -> usize {
        let target = peers.len();
        loop {
            // Register for notification *before* reading the count so an ack
            // landing in the gap still wakes us (tokio's race-free idiom).
            let notified = self.ack_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let have = self.remote_ack_count(epoch, &peers);
            if have >= target {
                return have;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.remote_ack_count(epoch, &peers);
            }
        }
    }

    /// How many of `peers` have acked `epoch`.
    fn remote_ack_count(&self, epoch: u64, peers: &[String]) -> usize {
        let acks = self.remote_acks.lock();
        match acks.get(&epoch) {
            Some(set) => peers.iter().filter(|p| set.contains(p.as_str())).count(),
            None => 0,
        }
    }

    /// Apply an inbound [`HaltPdu`] delivered by a [`HaltTransport`].
    ///
    /// * [`HaltPdu::Halt`] — engage this node's latch for the named epoch,
    ///   quiesce its local guarded parties (bounded by `local_timeout`),
    ///   then ack back to the originator.
    /// * [`HaltPdu::Ack`] — record a peer's ack and wake any in-flight
    ///   [`await_quiescence`](Self::await_quiescence).
    pub async fn apply_pdu(&self, pdu: HaltPdu, local_timeout: Duration) {
        match pdu {
            HaltPdu::Halt { from, epoch, reason } => {
                self.engage_epoch(epoch, reason.clone());
                let deadline = tokio::time::Instant::now() + local_timeout;
                let _ = self.collect_local(deadline, &reason).await;
                if let Some(transport) = &self.transport {
                    transport.send(&from, HaltPdu::Ack { from: self.node.clone(), epoch });
                }
            }
            HaltPdu::Ack { from, epoch } => {
                self.remote_acks.lock().entry(epoch).or_default().insert(from);
                self.ack_notify.notify_waiters();
            }
        }
    }

    /// Reset the switch by advancing to a fresh epoch (the old epoch's
    /// latched flag is left as an audit record). Requires a valid
    /// two-person [`ResetAuthorization`].
    ///
    /// See the module docs for why this advances an epoch instead of
    /// flipping the flag: a `Flag` cannot un-latch.
    pub fn reset(&self, authz: ResetAuthorization) -> Result<u64, ResetError> {
        if authz.approver_a.trim().is_empty() || authz.approver_b.trim().is_empty() {
            return Err(ResetError::MissingApprover);
        }
        if authz.approver_a == authz.approver_b {
            return Err(ResetError::DuplicateApprover);
        }
        let new_epoch = self.epoch() + 1;
        self.replicator.update(&self.epoch_key(), LwwRegister::new(&self.node, new_epoch, self.next_ts()));
        // New epoch's flag starts unset → is_engaged() is false again.
        *self.last_reason.lock() = None;
        Ok(new_epoch)
    }
}

/// Adapter wiring FR-7 → FR-5: a [`QuorumObserver`] that engages the
/// kill switch when quorum is lost. Drop this into
/// `SbrRuntime::with_observer` to halt the node on partition loss.
pub struct KillSwitchQuorumObserver(pub Arc<ClusterKillSwitch>);

impl QuorumObserver for KillSwitchQuorumObserver {
    fn on_quorum_lost(&self) {
        self.0.engage(HaltReason::QuorumLost);
    }
    fn on_quorum_regained(&self) {
        // Recovery is an explicit, audited two-person `reset` — we do
        // not auto-clear the latch on regain. Intentionally a no-op.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    fn switch() -> Arc<ClusterKillSwitch> {
        ClusterKillSwitch::new(Replicator::new(), "atomr/killswitch", "node-1")
    }

    #[test]
    fn engage_latches_and_is_engaged() {
        let ks = switch();
        assert!(!ks.is_engaged());
        let t = ks.engage(HaltReason::Manual("drill".into()));
        assert!(ks.is_engaged());
        assert_eq!(ks.last_reason(), Some(HaltReason::Manual("drill".into())));
        assert!(t.0 > 0);
        // Engaging again is idempotent.
        ks.engage(HaltReason::Manual("again".into()));
        assert!(ks.is_engaged());
    }

    #[test]
    fn engaged_survives_independent_merge_of_off_copy() {
        // Simulate a stale (off) copy merging in: OR-merge keeps it on.
        let ks = switch();
        ks.engage(HaltReason::QuorumLost);
        let epoch = ks.epoch();
        // Merge a fresh default (false) Flag — must not un-latch.
        ks.replicator().update(&format!("atomr/killswitch::flag::{epoch}"), Flag::new());
        assert!(ks.is_engaged());
    }

    #[test]
    fn reset_requires_two_distinct_nonempty_approvers() {
        let ks = switch();
        ks.engage(HaltReason::Manual("x".into()));
        assert!(ks.is_engaged());

        assert_eq!(
            ks.reset(ResetAuthorization { approver_a: "".into(), approver_b: "bob".into() }),
            Err(ResetError::MissingApprover)
        );
        assert_eq!(
            ks.reset(ResetAuthorization { approver_a: "amy".into(), approver_b: "amy".into() }),
            Err(ResetError::DuplicateApprover)
        );
        // Still engaged — no valid reset happened.
        assert!(ks.is_engaged());

        // Valid two-person reset advances the epoch and re-arms.
        let new_epoch = ks
            .reset(ResetAuthorization { approver_a: "amy".into(), approver_b: "bob".into() })
            .expect("valid authz");
        assert_eq!(new_epoch, 1);
        assert!(!ks.is_engaged(), "new epoch flag must start unset");
        assert!(ks.last_reason().is_none());

        // The switch can be engaged again in the new epoch.
        ks.engage(HaltReason::RiskBreach("limit".into()));
        assert!(ks.is_engaged());
    }

    #[tokio::test]
    async fn await_quiescence_collects_acks() {
        // A guarded party that, on halt, records the fact and acks
        // quiescence through the handle it was given at registration.
        struct Party {
            halted: Arc<AtomicBool>,
            ack: Arc<Mutex<Option<AckHandle>>>,
        }
        impl HaltGuarded for Party {
            fn on_halt(&mut self, _reason: &HaltReason) {
                self.halted.store(true, Ordering::SeqCst);
                if let Some(h) = self.ack.lock().take() {
                    h.ack();
                }
            }
        }

        let ks = switch();
        let halted = Arc::new(AtomicBool::new(false));
        let ack_slot = Arc::new(Mutex::new(None));
        let party = Box::new(Party { halted: halted.clone(), ack: ack_slot.clone() });
        // Register, then hand the party its own ack handle via the slot.
        let handle = ks.register_guarded(party);
        *ack_slot.lock() = Some(handle);

        ks.engage(HaltReason::Manual("drill".into()));
        let report = ks.await_quiescence(Duration::from_millis(500)).await;
        assert!(halted.load(Ordering::SeqCst));
        assert_eq!(report.total, 1);
        assert_eq!(report.acked, 1);
        assert!(!report.timed_out);
    }

    #[tokio::test]
    async fn await_quiescence_times_out_without_ack() {
        struct Silent;
        impl HaltGuarded for Silent {
            fn on_halt(&mut self, _reason: &HaltReason) {}
        }
        let ks = switch();
        // Keep the handle alive but never ack.
        let _handle = ks.register_guarded(Box::new(Silent));
        let report = ks.await_quiescence(Duration::from_millis(50)).await;
        assert_eq!(report.total, 1);
        assert!(report.timed_out);
    }

    #[test]
    fn quorum_observer_engages_on_loss() {
        let ks = switch();
        let obs = KillSwitchQuorumObserver(ks.clone());
        assert!(!ks.is_engaged());
        obs.on_quorum_lost();
        assert!(ks.is_engaged());
        assert_eq!(ks.last_reason(), Some(HaltReason::QuorumLost));
        // Regain is a no-op (recovery is an explicit reset).
        obs.on_quorum_regained();
        assert!(ks.is_engaged());
    }

    #[test]
    fn no_transport_reports_zero_remote() {
        // A switch without a transport never has remote parties.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ks = switch();
            ks.engage(HaltReason::Manual("x".into()));
            let report = ks.await_quiescence(Duration::from_millis(50)).await;
            assert_eq!(report.remote_total, 0);
            assert_eq!(report.remote_acked, 0);
            assert_eq!(report.total, 0); // no local parties either
            assert!(!report.timed_out);
        });
    }

    /// In-memory [`HaltTransport`] that routes PDUs between switches held in
    /// a shared registry, spawning the receiver's `apply_pdu` on send.
    #[derive(Clone)]
    struct MemNet {
        self_node: String,
        reg: Arc<Mutex<HashMap<String, Arc<ClusterKillSwitch>>>>,
        ack_timeout: Duration,
    }

    impl HaltTransport for MemNet {
        fn peers(&self) -> Vec<String> {
            self.reg.lock().keys().filter(|k| *k != &self.self_node).cloned().collect()
        }
        fn send(&self, target: &str, pdu: HaltPdu) {
            let sw = self.reg.lock().get(target).cloned();
            if let Some(sw) = sw {
                let to = self.ack_timeout;
                tokio::spawn(async move {
                    sw.apply_pdu(pdu, to).await;
                });
            }
        }
    }

    // A guarded party that records the halt and acks via its handle.
    struct Party {
        halted: Arc<AtomicBool>,
        ack: Arc<Mutex<Option<AckHandle>>>,
    }
    impl HaltGuarded for Party {
        fn on_halt(&mut self, _reason: &HaltReason) {
            self.halted.store(true, Ordering::SeqCst);
            if let Some(h) = self.ack.lock().take() {
                h.ack();
            }
        }
    }

    #[tokio::test]
    async fn cross_node_halt_fans_out_and_collects_remote_acks() {
        let reg: Arc<Mutex<HashMap<String, Arc<ClusterKillSwitch>>>> = Arc::new(Mutex::new(HashMap::new()));
        let net_a = Arc::new(MemNet {
            self_node: "A".into(),
            reg: reg.clone(),
            ack_timeout: Duration::from_millis(500),
        });
        let net_b = Arc::new(MemNet {
            self_node: "B".into(),
            reg: reg.clone(),
            ack_timeout: Duration::from_millis(500),
        });
        let a = ClusterKillSwitch::with_transport(Replicator::new(), "atomr/ks", "A", net_a);
        let b = ClusterKillSwitch::with_transport(Replicator::new(), "atomr/ks", "B", net_b);
        reg.lock().insert("A".into(), a.clone());
        reg.lock().insert("B".into(), b.clone());

        // B has a local guarded party that acks when halted.
        let b_halted = Arc::new(AtomicBool::new(false));
        let b_ack = Arc::new(Mutex::new(None));
        let handle = b.register_guarded(Box::new(Party { halted: b_halted.clone(), ack: b_ack.clone() }));
        *b_ack.lock() = Some(handle);

        // Engage on A, then quiesce: A fans the halt to B and awaits B's ack.
        a.engage(HaltReason::RiskBreach("limit breach".into()));
        let report = a.await_quiescence(Duration::from_millis(2000)).await;

        assert_eq!(report.remote_total, 1, "A should broadcast to its one peer (B)");
        assert_eq!(report.remote_acked, 1, "B should ack its halt");
        assert_eq!(report.total, 1);
        assert_eq!(report.acked, 1);
        assert!(!report.timed_out);

        // The halt actually reached B: its latch engaged and its party halted.
        assert!(b.is_engaged(), "B's latch must engage from the Halt PDU");
        assert!(b_halted.load(Ordering::SeqCst), "B's guarded party must be halted");
        // The reason rode along in the PDU.
        assert_eq!(b.last_reason(), Some(HaltReason::RiskBreach("limit breach".into())));
    }

    #[tokio::test]
    async fn cross_node_times_out_when_peer_silent() {
        // B is registered as a peer but has NO transport of its own, so it
        // can never ack — A must report a timeout against the missing remote.
        let reg: Arc<Mutex<HashMap<String, Arc<ClusterKillSwitch>>>> = Arc::new(Mutex::new(HashMap::new()));
        let net_a = Arc::new(MemNet {
            self_node: "A".into(),
            reg: reg.clone(),
            ack_timeout: Duration::from_millis(100),
        });
        let a = ClusterKillSwitch::with_transport(Replicator::new(), "atomr/ks", "A", net_a);
        // A bare switch for B with no transport: receiving a Halt cannot ack.
        let b = ClusterKillSwitch::new(Replicator::new(), "atomr/ks", "B");
        reg.lock().insert("A".into(), a.clone());
        reg.lock().insert("B".into(), b.clone());

        a.engage(HaltReason::Manual("drill".into()));
        let report = a.await_quiescence(Duration::from_millis(150)).await;

        assert_eq!(report.remote_total, 1);
        assert_eq!(report.remote_acked, 0);
        assert!(report.timed_out, "missing remote ack must surface as a timeout");
        // B still engaged locally even though it could not ack back.
        assert!(b.is_engaged());
    }
}
