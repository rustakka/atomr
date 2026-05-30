//! Core `Actor` trait and message envelope.

use async_trait::async_trait;

use super::context::Context;
use super::metadata::Metadata;
use super::sender::Sender;
use crate::supervision::{Directive, SupervisorStrategy};

/// Envelope that carries a user message plus a typed [`Sender`].
///
/// `M` is the actor's user message type. The [`Sender`] preserves the
/// origin's identity end-to-end (no `Any::downcast` on reply paths) —
/// see `docs/idiomatic-rust.md` (P-1) and Phase 1 of
/// `docs/full-port-plan.md`.
pub struct MessageEnvelope<M> {
    pub message: M,
    pub sender: Sender,
    /// Trace context + baggage propagated across hops (FR-10). Empty unless a
    /// sender attached it via [`tell_with_meta`](super::ActorRef::tell_with_meta)
    /// or an interceptor injected it.
    pub metadata: Metadata,
}

impl<M> MessageEnvelope<M> {
    pub fn new(message: M) -> Self {
        Self { message, sender: Sender::None, metadata: Metadata::new() }
    }

    /// Construct with a typed [`Sender`].
    pub fn with_typed_sender(message: M, sender: Sender) -> Self {
        Self { message, sender, metadata: Metadata::new() }
    }

    /// Construct with a typed [`Sender`] and [`Metadata`].
    pub fn with_meta(message: M, sender: Sender, metadata: Metadata) -> Self {
        Self { message, sender, metadata }
    }
}

/// The user-facing `Actor` trait.
///
/// is expressed here as: each actor has an
/// associated `Msg` type (typically an enum) and implements an async
/// `handle` that matches on it.
#[async_trait]
pub trait Actor: Sized + Send + 'static {
    type Msg: Send + 'static;

    /// Process a single message.
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: Self::Msg);

    /// Called once before the first message.
    async fn pre_start(&mut self, _ctx: &mut Context<Self>) {}

    /// Called after the actor has been stopped.
    async fn post_stop(&mut self, _ctx: &mut Context<Self>) {}

    /// Called when the actor is about to be restarted by the supervisor.
    async fn pre_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}

    /// Called after a restart.
    async fn post_restart(&mut self, _ctx: &mut Context<Self>, _err: &str) {}

    /// Called when a watched actor terminates. The `path` argument is
    /// the path of the actor that just stopped. Default is a no-op.
    /// Implementations may translate this into a user-visible message
    /// (the Python binding does this for `Terminated` events).
    async fn on_terminated(&mut self, _ctx: &mut Context<Self>, _path: &super::path::ActorPath) {}

    /// Called when the supervisor pushes a *graded* operating-mode change
    /// ([`Directive::Throttle`], [`Directive::Suspend`], or
    /// [`Directive::ResumeFrom`]) into this still-running actor — no restart,
    /// state preserved (FR-6). The actor is expected to gate its own outbound
    /// effects (order rate/size, flat-only, etc.) accordingly. Default no-op.
    ///
    /// The crash-recovery directives (`Resume`/`Restart`/`Stop`/`Escalate`) do
    /// **not** invoke this hook — they keep their existing lifecycle semantics.
    async fn on_directive(&mut self, _ctx: &mut Context<Self>, _directive: &Directive) {}

    /// The supervisor strategy this actor applies to its own children.
    fn supervisor_strategy(&self) -> SupervisorStrategy {
        SupervisorStrategy::default()
    }
}
