//! # atomr-core
//!
//! Idiomatic Rust actor runtime — typed actors, hierarchical supervision,
//! dispatchers, mailboxes, scheduler, dead letters, and event streams.
//!
//! ## Quick start
//!
//! ```no_run
//! use atomr_core::prelude::*;
//!
//! #[derive(Default)]
//! struct Echo;
//!
//! #[async_trait::async_trait]
//! impl Actor for Echo {
//!     type Msg = String;
//!     async fn handle(&mut self, _ctx: &mut Context<Self>, msg: String) {
//!         println!("echo: {msg}");
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let sys = ActorSystem::create("S", atomr_config::Config::reference()).await?;
//! let echo = sys.actor_of(Props::create(Echo::default), "echo")?;
//! echo.tell("hi".to_string());
//! sys.terminate().await;
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod actor;
pub mod dispatch;
#[macro_use]
mod fsm_macro;
pub mod event;
pub mod io;
pub mod pattern;
pub mod routing;
pub mod serialization;
pub mod supervision;
pub mod time;
pub mod util;

pub mod prelude {
    pub use crate::actor::{
        Actor, ActorPath, ActorRef, ActorSystem, Address, Context, Props, UntypedActorRef,
    };
    pub use crate::pattern::{ask, pipe_to};
    pub use crate::supervision::{
        Directive, OneForOneStrategy, PanicPayload, SupervisionError, SupervisorOf, SupervisorStrategy,
    };
    pub use crate::time::{Clock, LogicalTime, ManualClock, SystemClock};
    pub use async_trait::async_trait;
    pub use atomr_config::Config;
}
