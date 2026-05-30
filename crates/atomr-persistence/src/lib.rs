//! atomr-persistence.
//!
//! Event-sourced persistent actor model with pluggable journal and snapshot stores.

mod alod;
mod async_snapshot;
mod determinism;
mod eventsourced;
mod journal;
mod persistent_actor;
mod persistent_fsm;
mod receive_persistent;
mod recovery;
mod recovery_permitter;
mod snapshot;

pub use alod::{AtLeastOnceDelivery, UnconfirmedDelivery};
pub use async_snapshot::{AsyncSnapshotter, SnapshotPolicy};
pub use determinism::{with_kind, EntryKind, ReplayHarness, ReplayReport, RngState, RunPin, SeededRng};
pub use eventsourced::{Eventsourced, EventsourcedError};
pub use journal::{InMemoryJournal, Journal, JournalError, PersistentRepr};
pub use persistent_actor::PersistentActor;
pub use persistent_fsm::PersistentFSM;
pub use receive_persistent::ReceivePersistent;
pub use recovery::{Recovery, RecoveryState};
pub use recovery_permitter::RecoveryPermitter;
pub use snapshot::{InMemorySnapshotStore, SnapshotMetadata, SnapshotStore};
