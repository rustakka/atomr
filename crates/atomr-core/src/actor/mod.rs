//! The `actor` subsystem.
//!
//! Everything user-visible about actors lives here: [`Actor`], [`ActorRef`],
//! [`ActorSystem`], [`Props`], [`Context`], plus paths/addresses and the
//! internal `ActorCell` machinery.

mod actor_cell;
mod actor_ref;
mod actor_system;
mod address;
mod context;
mod coordinated_shutdown;
mod deploy;
mod extensions;
mod fsm;
mod inbox;
mod metadata;
mod observer;
mod path;
mod props;
mod provider;
mod remote;
pub mod scheduler;
mod sender;
mod stash;
mod traits;

pub use actor_cell::{ActorCell, SystemMsg};
pub use actor_ref::{ActorRef, AskError, UntypedActorRef};
pub use actor_system::{ActorSystem, ActorSystemError};
pub use address::Address;
pub use context::{
    Context, LifecyclePhase, PhaseMarker, Running, Starting, Stopping, SystemHandle, TypedContext,
};
pub use coordinated_shutdown::{CoordinatedShutdown, Phase, PhaseConfig};
pub use deploy::{Deploy, Scope};
pub use extensions::{Extension, ExtensionId, Extensions};
pub use fsm::{FiniteStateMachine, Fsm, FsmBuilder, FsmStopReason, FsmTransition};
pub use inbox::Inbox;
pub use metadata::{MessageInterceptor, Metadata, SpanGuard};
pub use observer::{DeadLetterObserver, SpawnObserver};
pub use path::{ActorPath, PathElement};
pub use props::{BoxedProps, Props};
pub use provider::{ActorRefProvider, LocalActorRefProvider};
pub use remote::{RemoteProvider, RemoteRef, RemoteSystemMsg, SerializedMessage};
pub use sender::Sender;
pub use stash::{BoundedStash, Stash, StashOverflow, StashResult};
pub use traits::{Actor, MessageEnvelope};
