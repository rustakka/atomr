//! # atomr-fix
//!
//! A FIX (Financial Information eXchange) session layer built on
//! `atomr-streams` `Tcp` + `Framing` and an actor session FSM. Handles the
//! session protocol — logon, heartbeat / test-request, sequence-number
//! management, resend-request and gap-fill, orderly logout — so execution code
//! can exchange application messages without re-implementing the wire protocol.
//!
//! Sequence numbers are persisted through a pluggable `FixSeqStore` so a
//! reconnect can issue / honor `ResendRequest` and recover cleanly.

#![forbid(unsafe_code)]

pub mod message;
pub mod seq_store;
pub mod session;

pub use message::{FixField, FixMessage, FixParseError, MsgType, SOH};
pub use seq_store::{FixSeqStore, InMemorySeqStore};
pub use session::{FixSession, FixSessionConfig, FixVersion, InboundOutcome, SessionState};
