//! atomr-distributed-data.
//!
//! Provides CRDTs (`GCounter`, `PNCounter`, `GSet`, `ORSet`, `LWWRegister`,
//! `Flag`, `ORMap`, `LWWMap`, `PNCounterMap`) and a `Replicator` that
//! stores them in-memory and merges on request.

mod counters;
mod durable;
mod flag;
mod maps;
mod pruning;
mod register;
mod replicator;
mod replicator_actor;
mod sets;
mod signed_sum;
mod traits;

pub use counters::{GCounter, PNCounter};
pub use durable::{DurableStore, FileDurableStore, NoopDurableStore};
pub use flag::Flag;
pub use maps::{LWWMap, ORMap, ORMultiMap, PNCounterMap};
pub use pruning::{PruningPhase, PruningState, ReadAggregator, WriteAggregator};
pub use register::LwwRegister;
pub use replicator::{ReadConsistency, Replicator, SubscriptionToken, WriteConsistency};
pub use replicator_actor::{ReplicatorAck, ReplicatorActor, ReplicatorError};
pub use sets::{GSet, OrSet};
pub use signed_sum::{SignedSum, SignedSumDelta, SignedSumMap};
pub use traits::{CrdtMerge, DeltaCrdt};
