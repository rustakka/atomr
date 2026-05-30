//! # atomr-patterns
//!
//! DDD/CQRS pattern library for atomr. Provides opinionated,
//! ready-to-instantiate scaffolding on top of the primitives shipped by
//! `atomr-core`, `atomr-persistence`, `atomr-persistence-query`,
//! and `atomr-streams`.
//!
//! ## What's in the box
//!
//! | Pattern | Purpose |
//! |---|---|
//! | [`cqrs::CqrsPattern`] | Write-side aggregate + read-side projections (CQRS+ES) |
//! | [`saga::SagaPattern`] | Long-running orchestration across aggregates |
//! | [`bus::DomainEventBus`] | In-process event fan-out (cluster-wide behind `bus-cluster`) |
//! | [`outbox::OutboxPattern`] | Reliable publish-after-persist |
//! | [`acl::AntiCorruption`] | Translate between bounded contexts |
//!
//! ## Shape of a pattern instance
//!
//! Every pattern is configured with a fluent builder, returns a
//! [`Topology`] fragment, and is materialized on an [`atomr_core::actor::ActorSystem`]
//! to spawn its actors and start its streams. Typed handles come back
//! to you for further interaction.
//!
//! ```ignore
//! use atomr_patterns::prelude::*;
//!
//! let (builder, totals) = CqrsPattern::<Order>::builder()
//!     .name("orders")
//!     .factory(|id| Order::new(id))
//!     .journal(journal)
//!     .read_journal(read_journal)
//!     .with_reader(TotalsReader::default());
//!
//! let topology = builder.build()?;
//! let h = topology.materialize(&system).await?;
//! h.repository().send(PlaceOrder { id, sku }).await?;
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]

pub mod acl;
pub mod bus;
pub mod cqrs;
pub mod ddd;
pub mod error;
pub mod extensions;
pub mod inbox;
pub mod outbox;
pub mod prelude;
pub mod process_manager;
pub mod reactor;
pub mod saga;
pub mod specification;
pub mod topology;
pub mod tx_outbox;

pub use error::PatternError;
pub use topology::Topology;
pub use tx_outbox::{EventRow, OutboxError, OutboxRow, TxOutbox};

pub use ddd::{AggregateRoot, Command, DomainEvent, Entity, Repository, ValueObject};
