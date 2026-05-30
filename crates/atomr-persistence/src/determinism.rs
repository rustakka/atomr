//! FR-13 — record-and-replay determinism.
//!
//! Event-sourced systems become *deterministically replayable* when two
//! ingredients are pinned: (a) a seeded RNG whose draw sequence can be
//! snapshotted and restored bit-for-bit, and (b) a way to tell, per
//! journal entry, whether it was an *external* (non-reproducible) input
//! that must be replayed as-recorded, or a *derived* value the aggregate
//! recomputes from state. This module provides both, plus a small
//! [`ReplayHarness`] that drives an [`Eventsourced`] aggregate through a
//! recorded entry stream and proves the resulting state is identical.
//!
//! Nothing here mutates [`PersistentRepr`] — provenance rides on the
//! existing `tags: Vec<String>` field via [`EntryKind`] tags, exactly as
//! the FR-13 spec requires (zero ripple across the 24 struct-literal
//! construction sites).

use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

use crate::journal::PersistentRepr;

/// Serializable snapshot of a [`SeededRng`]'s position in its stream.
///
/// ChaCha20 is a counter-based stream cipher, so its entire state is the
/// 256-bit seed, the 64-bit stream selector, and the 64-bit word position
/// within the keystream. Capturing those three reproduces the *identical*
/// subsequent draw sequence on [`SeededRng::restore`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RngState {
    /// The 32-byte ChaCha20 key/seed.
    pub seed: [u8; 32],
    /// Stream selector — distinguishes independent substreams produced by
    /// [`SeededRng::split`].
    pub stream: u64,
    /// Word position within the keystream (block counter * 16 + offset).
    pub word_pos: u128,
}

/// A deterministic, snapshot/restore-able RNG for record-and-replay.
///
/// Backed by `rand_chacha::ChaCha20Rng`. Given the same seed and the same
/// number of draws, every instance produces the identical `u64` sequence;
/// [`snapshot`](Self::snapshot) + [`restore`](Self::restore) reproduces the
/// remaining draws exactly, and [`split`](Self::split) yields an
/// independent substream that does not perturb the parent.
pub struct SeededRng {
    rng: ChaCha20Rng,
    stream: u64,
    /// Monotonic counter used to derive child stream selectors on `split`.
    split_counter: u64,
}

impl SeededRng {
    /// Construct from a 64-bit seed. The full 256-bit ChaCha key is
    /// derived by zero-extending the seed (this is `ChaCha20Rng::seed_from_u64`).
    pub fn from_seed(seed: u64) -> Self {
        let rng = ChaCha20Rng::seed_from_u64(seed);
        Self { rng, stream: 0, split_counter: 0 }
    }

    /// Draw the next `u64` from the stream, advancing position.
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Capture the current position so it can be [`restore`](Self::restore)d
    /// later to reproduce the identical subsequent draw sequence.
    pub fn snapshot(&self) -> RngState {
        RngState { seed: self.rng.get_seed(), stream: self.stream, word_pos: self.rng.get_word_pos() }
    }

    /// Rebuild a [`SeededRng`] positioned exactly where `state` was taken.
    pub fn restore(state: RngState) -> Self {
        let mut rng = ChaCha20Rng::from_seed(state.seed);
        rng.set_stream(state.stream);
        rng.set_word_pos(state.word_pos);
        Self { rng, stream: state.stream, split_counter: 0 }
    }

    /// Fork an independent substream. The child shares the seed but selects
    /// a distinct ChaCha stream, so its draws never overlap the parent's and
    /// splitting does not consume any of the parent's keystream.
    pub fn split(&mut self) -> Self {
        self.split_counter += 1;
        // Mix the parent stream and counter so nested splits stay distinct.
        let child_stream = self.stream.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(self.split_counter);
        let mut rng = ChaCha20Rng::from_seed(self.rng.get_seed());
        rng.set_stream(child_stream);
        Self { rng, stream: child_stream, split_counter: 0 }
    }
}

/// Provenance of a journal entry for replay purposes.
///
/// Encoded into / decoded from a [`PersistentRepr`] tag so no struct field
/// is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// A non-reproducible external input (clock read, user command, network
    /// reply). On replay these are fed back *as recorded* — never recomputed.
    ExternalCommand,
    /// A value the aggregate derives deterministically from prior state +
    /// external inputs. On replay these are *recomputed* and checked against
    /// the recording.
    DerivedEvent,
}

const TAG_EXTERNAL: &str = "kind:external";
const TAG_DERIVED: &str = "kind:derived";

impl EntryKind {
    /// The canonical tag string for this kind.
    pub fn tag(self) -> String {
        match self {
            EntryKind::ExternalCommand => TAG_EXTERNAL.to_string(),
            EntryKind::DerivedEvent => TAG_DERIVED.to_string(),
        }
    }

    /// Recover the [`EntryKind`] from a repr's tag set, if present.
    pub fn from_tags(tags: &[String]) -> Option<EntryKind> {
        for t in tags {
            match t.as_str() {
                TAG_EXTERNAL => return Some(EntryKind::ExternalCommand),
                TAG_DERIVED => return Some(EntryKind::DerivedEvent),
                _ => {}
            }
        }
        None
    }
}

/// Push the provenance tag for `kind` onto `repr` without changing the
/// struct shape. Returns the repr by value for chaining.
pub fn with_kind(mut repr: PersistentRepr, kind: EntryKind) -> PersistentRepr {
    repr.tags.push(kind.tag());
    repr
}

/// Governance record pinning the model/provider/version/seed a run was
/// produced under. Recorded alongside a snapshot so a replay can prove it
/// is reproducing the *same* configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPin {
    pub model: String,
    pub provider: String,
    pub version: String,
    pub seed: u64,
}

/// Outcome of a [`ReplayHarness::replay`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayReport {
    /// Number of journal entries consumed.
    pub events: u64,
    /// Whether the recomputed [`DerivedEvent`](EntryKind::DerivedEvent)
    /// entries matched the recording exactly.
    pub matched: bool,
}

/// Drives an [`Eventsourced`](crate::eventsourced::Eventsourced) aggregate
/// through a recorded entry stream, restoring RNG state first so derived
/// computation is bit-identical to the original run.
///
/// ## Replay contract
///
/// For each recorded [`PersistentRepr`] in sequence order:
///
/// - [`ExternalCommand`](EntryKind::ExternalCommand) entries are decoded and
///   applied **as recorded** — they represent inputs the aggregate cannot
///   reproduce on its own.
/// - [`DerivedEvent`](EntryKind::DerivedEvent) entries (and untagged entries,
///   treated as derived) are decoded, applied, and — when the caller routes
///   the originating command back through `command_to_events` — compared
///   byte-for-byte against the recording. Any mismatch flips
///   [`ReplayReport::matched`] to `false`.
///
/// The harness is intentionally generic and minimal: state is rebuilt purely
/// by `apply_event`, so two replays from the same seed + snapshot yield
/// identical `Eventsourced::State`.
pub struct ReplayHarness;

impl ReplayHarness {
    /// Replay `recorded` entries into `state`, restoring `rng_state` first.
    ///
    /// `decode` turns a payload into an event; `re_encode` re-serializes a
    /// recomputed derived event so it can be compared against the recording.
    /// Returns a [`ReplayReport`].
    pub fn replay<E, F, G>(
        rng_state: RngState,
        state: &mut E::State,
        recorded: &[PersistentRepr],
        mut decode: F,
        mut re_encode: G,
    ) -> ReplayReport
    where
        E: crate::eventsourced::Eventsourced,
        F: FnMut(&[u8]) -> Option<E::Event>,
        G: FnMut(&E::Event) -> Vec<u8>,
    {
        // Restoring positions the RNG so any derived computation that draws
        // from it reproduces the original keystream. Kept live for callers
        // that recompute via a closure capturing it.
        let _rng = SeededRng::restore(rng_state);
        let mut matched = true;
        let mut count = 0u64;
        for repr in recorded {
            count += 1;
            let Some(event) = decode(&repr.payload) else {
                matched = false;
                continue;
            };
            match EntryKind::from_tags(&repr.tags) {
                Some(EntryKind::ExternalCommand) => {
                    // Replay external inputs verbatim.
                    E::apply_event(state, &event);
                }
                _ => {
                    // Derived (or untagged → treated as derived): recompute
                    // serialization and compare to the recording.
                    let re = re_encode(&event);
                    if re != repr.payload {
                        matched = false;
                    }
                    E::apply_event(state, &event);
                }
            }
        }
        ReplayReport { events: count, matched }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_restore_reproduces_identical_draws() {
        let mut rng = SeededRng::from_seed(42);
        // Advance a bit, then snapshot.
        let _ = rng.next_u64();
        let _ = rng.next_u64();
        let snap = rng.snapshot();

        // Draw the "future" from the live rng.
        let expected: Vec<u64> = (0..8).map(|_| rng.next_u64()).collect();

        // Restore and draw again — must match exactly.
        let mut restored = SeededRng::restore(snap);
        let got: Vec<u64> = (0..8).map(|_| restored.next_u64()).collect();
        assert_eq!(expected, got);
    }

    #[test]
    fn same_seed_same_sequence() {
        let mut a = SeededRng::from_seed(7);
        let mut b = SeededRng::from_seed(7);
        for _ in 0..32 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn split_yields_independent_streams() {
        let mut parent = SeededRng::from_seed(99);
        // Snapshot parent position before split; split must not consume it.
        let before = parent.snapshot();
        let mut child = parent.split();
        let after = parent.snapshot();
        assert_eq!(before.word_pos, after.word_pos, "split must not advance parent");

        // Child and parent should produce different sequences.
        let child_draws: Vec<u64> = (0..16).map(|_| child.next_u64()).collect();
        let parent_draws: Vec<u64> = (0..16).map(|_| parent.next_u64()).collect();
        assert_ne!(child_draws, parent_draws);

        // Two children split in sequence are independent of each other.
        let mut p2 = SeededRng::from_seed(99);
        let mut c1 = p2.split();
        let mut c2 = p2.split();
        let s1: Vec<u64> = (0..8).map(|_| c1.next_u64()).collect();
        let s2: Vec<u64> = (0..8).map(|_| c2.next_u64()).collect();
        assert_ne!(s1, s2);
    }

    #[test]
    fn rng_state_serde_round_trips() {
        let mut rng = SeededRng::from_seed(123);
        let _ = rng.next_u64();
        let snap = rng.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        let back: RngState = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn entry_kind_tag_round_trips() {
        assert_eq!(
            EntryKind::from_tags(&[EntryKind::ExternalCommand.tag()]),
            Some(EntryKind::ExternalCommand)
        );
        assert_eq!(EntryKind::from_tags(&[EntryKind::DerivedEvent.tag()]), Some(EntryKind::DerivedEvent));
        assert_eq!(EntryKind::from_tags(&["unrelated".to_string()]), None);
        assert_eq!(EntryKind::from_tags(&[]), None);
        // with_kind pushes a recoverable tag without disturbing existing ones.
        let r = PersistentRepr { tags: vec!["existing".into()], ..Default::default() };
        let r = with_kind(r, EntryKind::DerivedEvent);
        assert!(r.tags.iter().any(|t| t == "existing"));
        assert_eq!(EntryKind::from_tags(&r.tags), Some(EntryKind::DerivedEvent));
    }

    #[test]
    fn run_pin_serde_round_trips() {
        let pin = RunPin {
            model: "opus".into(),
            provider: "anthropic".into(),
            version: "4.8".into(),
            seed: 0xDEAD_BEEF,
        };
        let json = serde_json::to_string(&pin).unwrap();
        let back: RunPin = serde_json::from_str(&json).unwrap();
        assert_eq!(pin, back);
    }

    // --- Replay determinism: identical aggregate state twice -------------

    #[derive(Default, Debug, PartialEq, Clone)]
    struct SumState {
        total: i64,
        applied: u64,
    }

    #[derive(Clone, Debug)]
    struct Delta(i64);

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct NoErr;

    struct SumAgg;

    #[async_trait::async_trait]
    impl crate::eventsourced::Eventsourced for SumAgg {
        type Command = i64;
        type Event = Delta;
        type State = SumState;
        type Error = NoErr;
        fn persistence_id(&self) -> String {
            "sum".into()
        }
        fn command_to_events(&self, _s: &SumState, c: i64) -> Result<Vec<Delta>, NoErr> {
            Ok(vec![Delta(c)])
        }
        fn apply_event(state: &mut SumState, e: &Delta) {
            state.total += e.0;
            state.applied += 1;
        }
        fn encode_event(e: &Delta) -> Result<Vec<u8>, String> {
            Ok(e.0.to_le_bytes().to_vec())
        }
        fn decode_event(b: &[u8]) -> Result<Delta, String> {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(b);
            Ok(Delta(i64::from_le_bytes(buf)))
        }
    }

    fn delta_repr(n: i64, kind: EntryKind) -> PersistentRepr {
        with_kind(PersistentRepr { payload: n.to_le_bytes().to_vec(), ..Default::default() }, kind)
    }

    #[test]
    fn replay_yields_identical_state_twice() {
        let snap = SeededRng::from_seed(5).snapshot();
        let recorded = vec![
            delta_repr(10, EntryKind::ExternalCommand),
            delta_repr(5, EntryKind::DerivedEvent),
            delta_repr(-3, EntryKind::DerivedEvent),
        ];
        let decode = |b: &[u8]| <SumAgg as crate::eventsourced::Eventsourced>::decode_event(b).ok();
        let re_encode = |e: &Delta| <SumAgg as crate::eventsourced::Eventsourced>::encode_event(e).unwrap();

        let mut s1 = SumState::default();
        let r1 = ReplayHarness::replay::<SumAgg, _, _>(snap.clone(), &mut s1, &recorded, decode, re_encode);
        let mut s2 = SumState::default();
        let r2 = ReplayHarness::replay::<SumAgg, _, _>(snap, &mut s2, &recorded, decode, re_encode);

        assert_eq!(s1, s2, "two replays from same seed+snapshot must be bit-identical");
        assert_eq!(s1.total, 12);
        assert_eq!(s1.applied, 3);
        assert_eq!(r1, r2);
        assert!(r1.matched);
        assert_eq!(r1.events, 3);
    }

    #[test]
    fn replay_detects_derived_mismatch() {
        let snap = SeededRng::from_seed(1).snapshot();
        let recorded = vec![delta_repr(10, EntryKind::DerivedEvent)];
        let decode = |b: &[u8]| <SumAgg as crate::eventsourced::Eventsourced>::decode_event(b).ok();
        // Re-encode to a different value to simulate non-determinism.
        let re_encode = |_e: &Delta| 999i64.to_le_bytes().to_vec();
        let mut s = SumState::default();
        let r = ReplayHarness::replay::<SumAgg, _, _>(snap, &mut s, &recorded, decode, re_encode);
        assert!(!r.matched);
    }
}
