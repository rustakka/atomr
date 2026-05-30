//! FIX session-state machine.
//!
//! [`FixSession`] is a self-contained, runtime-agnostic driver for the FIX
//! session layer. It is deliberately *not* an `atomr` `Actor`: the FSM is a
//! plain async struct whose transitions are exercised by feeding it inbound
//! [`FixMessage`]s and observing the outbound messages it produces. This makes
//! the protocol logic unit-testable without a socket. A thin [`run`](FixSession::run)
//! helper wires the same FSM onto an `atomr-streams` TCP connection +
//! SOH framing for real deployments.
//!
//! # What the FSM covers
//!
//! * **Logon handshake** — [`build_logon`](FixSession::build_logon) emits a
//!   Logon with `HeartBtInt(108)` and an optional `ResetSeqNumFlag(141)`; an
//!   inbound Logon transitions the session to [`SessionState::Active`].
//! * **Heartbeats / test requests** — [`heartbeat`](FixSession::heartbeat)
//!   emits a Heartbeat on the configured interval; an inbound `TestRequest(1)`
//!   is answered with a Heartbeat echoing `TestReqID(112)`.
//! * **Sequence management** — every outbound message is stamped with the next
//!   `MsgSeqNum` from the [`FixSeqStore`]; every accepted inbound message
//!   advances the expected-inbound counter.
//! * **Gap detection + resend** — an inbound message whose `MsgSeqNum` is
//!   *higher* than expected triggers a `ResendRequest(2)` for the gap range.
//! * **Resend / gap-fill** — an inbound `ResendRequest(2)` is answered with a
//!   `SequenceReset(4)` carrying `GapFillFlag(123)=Y` (administrative messages
//!   are never resent; the gap is filled instead).
//! * **SequenceReset** — an inbound `SequenceReset` advances the expected
//!   inbound counter to `NewSeqNo(36)`.
//! * **Orderly logout** — [`build_logout`](FixSession::build_logout) emits a
//!   Logout; an inbound Logout is answered (if we did not initiate) and the
//!   session returns to [`SessionState::Disconnected`].
//!
//! # Template-level simplifications
//!
//! This is a real FSM, not a toy, but a few production behaviours are left as
//! documented extension points rather than implemented:
//!
//! * Resend of *application* messages replays via `SequenceReset`-GapFill only;
//!   a real engine would keep a per-session outbound message log and resend the
//!   original application messages with `PossDupFlag(43)=Y`. The hook for that
//!   is [`FixSession::set_outbound_log`]-style storage, intentionally omitted to
//!   keep the store contract narrow.
//! * `PossResend(97)` / `OrigSendingTime(122)` handling on inbound duplicates,
//!   message-level validation (required-field / value checks beyond the header),
//!   and logon authentication are out of scope.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::message::{tags, FixMessage, MsgType};
use crate::seq_store::FixSeqStore;

/// FIX protocol version, selected at runtime (not via cargo features) so the
/// default build exercises every begin-string branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixVersion {
    Fix42,
    Fix44,
    Fix50Sp2,
}

impl FixVersion {
    /// The `BeginString(8)` value for this version. FIX 5.0 SP2 uses the FIXT
    /// transport begin-string `FIXT.1.1`.
    pub fn begin_string(&self) -> &'static str {
        match self {
            FixVersion::Fix42 => "FIX.4.2",
            FixVersion::Fix44 => "FIX.4.4",
            FixVersion::Fix50Sp2 => "FIXT.1.1",
        }
    }

    /// Whether this version uses the FIXT transport (FIX 5.0+), which requires
    /// `DefaultApplVerID(1137)` on the Logon.
    pub fn is_fixt(&self) -> bool {
        matches!(self, FixVersion::Fix50Sp2)
    }
}

/// Static configuration for a session.
#[derive(Debug, Clone)]
pub struct FixSessionConfig {
    pub version: FixVersion,
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub heartbeat: Duration,
    /// Send `ResetSeqNumFlag(141)=Y` on logon (and reset our store).
    pub reset_on_logon: bool,
}

impl FixSessionConfig {
    pub fn new(
        version: FixVersion,
        sender_comp_id: impl Into<String>,
        target_comp_id: impl Into<String>,
    ) -> Self {
        FixSessionConfig {
            version,
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
            heartbeat: Duration::from_secs(30),
            reset_on_logon: false,
        }
    }
}

/// Lifecycle state of the session FSM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Disconnected,
    LogonSent,
    Active,
    LogoutSent,
}

/// The driver. Holds the FSM state, config, and the sequence store.
pub struct FixSession {
    config: FixSessionConfig,
    store: Arc<dyn FixSeqStore>,
    state: SessionState,
}

impl FixSession {
    /// Construct a session from config and a sequence store.
    pub fn new(config: FixSessionConfig, store: Arc<dyn FixSeqStore>) -> Self {
        FixSession { config, store, state: SessionState::Disconnected }
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn config(&self) -> &FixSessionConfig {
        &self.config
    }

    pub fn store(&self) -> &Arc<dyn FixSeqStore> {
        &self.store
    }

    // --- outbound message construction --------------------------------------

    /// Stamp the standard header (BeginString, SenderCompID, TargetCompID,
    /// MsgSeqNum) onto a message and return it ready to encode. Advances the
    /// outbound sequence counter.
    async fn finalize_outbound(&self, mut msg: FixMessage) -> FixMessage {
        let seq = self.store.next_out().await;
        msg.set(tags::BEGIN_STRING, self.config.version.begin_string());
        msg.set(tags::SENDER_COMP_ID, self.config.sender_comp_id.clone());
        msg.set(tags::TARGET_COMP_ID, self.config.target_comp_id.clone());
        msg.set(tags::MSG_SEQ_NUM, seq.to_string());
        msg.set(tags::SENDING_TIME, sending_time());
        msg
    }

    /// Build and "send" a Logon. Transitions to [`SessionState::LogonSent`].
    /// When [`reset_on_logon`](FixSessionConfig::reset_on_logon) is set, the
    /// store is reset first and `ResetSeqNumFlag(141)=Y` is included.
    pub async fn build_logon(&mut self) -> FixMessage {
        if self.config.reset_on_logon {
            self.store.reset().await;
        }
        let mut msg = FixMessage::of_type(MsgType::Logon);
        msg.set(tags::ENCRYPT_METHOD, "0");
        msg.set(tags::HEART_BT_INT, self.config.heartbeat.as_secs().to_string());
        if self.config.reset_on_logon {
            msg.set(tags::RESET_SEQ_NUM_FLAG, "Y");
        }
        if self.config.version.is_fixt() {
            // FIX 5.0 SP2 application version (9 == FIX50SP2).
            msg.set(tags::DEFAULT_APPL_VER_ID, "9");
        }
        let out = self.finalize_outbound(msg).await;
        self.state = SessionState::LogonSent;
        out
    }

    /// Build and "send" a Logout with optional reason text. Transitions to
    /// [`SessionState::LogoutSent`].
    pub async fn build_logout(&mut self, reason: Option<&str>) -> FixMessage {
        let mut msg = FixMessage::of_type(MsgType::Logout);
        if let Some(r) = reason {
            msg.set(tags::TEXT, r);
        }
        let out = self.finalize_outbound(msg).await;
        self.state = SessionState::LogoutSent;
        out
    }

    /// Build a Heartbeat, optionally echoing a `TestReqID`.
    pub async fn build_heartbeat(&mut self, test_req_id: Option<&str>) -> FixMessage {
        let mut msg = FixMessage::of_type(MsgType::Heartbeat);
        if let Some(id) = test_req_id {
            msg.set(tags::TEST_REQ_ID, id);
        }
        self.finalize_outbound(msg).await
    }

    /// Periodic heartbeat (alias for `build_heartbeat(None)`), to be driven by
    /// a timer on the [`heartbeat`](FixSessionConfig::heartbeat) interval.
    pub async fn heartbeat(&mut self) -> FixMessage {
        self.build_heartbeat(None).await
    }

    /// Build a `ResendRequest(2)` for `[begin, end]`. `end == 0` means
    /// "infinity" per the FIX spec (all messages from `begin` onward).
    pub async fn build_resend_request(&mut self, begin: u64, end: u64) -> FixMessage {
        let mut msg = FixMessage::of_type(MsgType::ResendRequest);
        msg.set(tags::BEGIN_SEQ_NO, begin.to_string());
        msg.set(tags::END_SEQ_NO, end.to_string());
        self.finalize_outbound(msg).await
    }

    /// Build a `SequenceReset(4)` with `GapFillFlag(123)=Y` advancing the
    /// counterparty's expected inbound sequence to `new_seq_no`. The message is
    /// stamped with `MsgSeqNum = begin_seq` rather than consuming a fresh
    /// outbound number (gap-fill must occupy the slot of the first skipped
    /// message).
    pub async fn build_gap_fill(&self, begin_seq: u64, new_seq_no: u64) -> FixMessage {
        let mut msg = FixMessage::of_type(MsgType::SequenceReset);
        msg.set(tags::GAP_FILL_FLAG, "Y");
        msg.set(tags::POSS_DUP_FLAG, "Y");
        msg.set(tags::NEW_SEQ_NO, new_seq_no.to_string());
        msg.set(tags::BEGIN_STRING, self.config.version.begin_string());
        msg.set(tags::SENDER_COMP_ID, self.config.sender_comp_id.clone());
        msg.set(tags::TARGET_COMP_ID, self.config.target_comp_id.clone());
        msg.set(tags::MSG_SEQ_NUM, begin_seq.to_string());
        msg.set(tags::SENDING_TIME, sending_time());
        msg
    }

    /// Build an outbound application message of `msg_type`, stamping the header
    /// and consuming an outbound sequence number. The caller sets the
    /// application fields before/after as needed; this is the entry point the
    /// application uses to send (e.g.) a `NewOrderSingle`.
    pub async fn build_app_message(&self, mut msg: FixMessage) -> FixMessage {
        // ensure tag 35 already set by caller
        let seq = self.store.next_out().await;
        msg.set(tags::BEGIN_STRING, self.config.version.begin_string());
        msg.set(tags::SENDER_COMP_ID, self.config.sender_comp_id.clone());
        msg.set(tags::TARGET_COMP_ID, self.config.target_comp_id.clone());
        msg.set(tags::MSG_SEQ_NUM, seq.to_string());
        msg.set(tags::SENDING_TIME, sending_time());
        msg
    }

    // --- inbound handling ---------------------------------------------------

    /// Process one inbound message and return the FSM's reaction:
    /// the administrative replies to send and, if it is an application
    /// message that should be surfaced, that message.
    ///
    /// This is the single, fully-testable entry point for the inbound side of
    /// the protocol.
    pub async fn handle_inbound(&mut self, msg: FixMessage) -> InboundOutcome {
        let mut outcome = InboundOutcome::default();
        let msg_type = msg.msg_type();
        let seq = msg.seq_num();

        // --- gap detection (applies to every sequenced inbound message) -----
        // A SequenceReset with PossDupFlag/GapFill is allowed to carry a lower
        // number, and we treat ResetSeqNumFlag logons specially below, so guard
        // those cases.
        let is_seq_reset = matches!(msg_type, Some(MsgType::SequenceReset));
        let is_reset_logon = matches!(msg_type, Some(MsgType::Logon))
            && msg.get(tags::RESET_SEQ_NUM_FLAG).map(|v| v == "Y" || v == "y").unwrap_or(false);

        if let Some(seq) = seq {
            if !is_seq_reset && !is_reset_logon {
                let expected = self.store.current_in().await;
                if seq > expected {
                    // Gap: request resend of [expected, 0]=to-infinity. Do NOT
                    // advance the inbound counter; we are waiting to fill the
                    // gap first.
                    let rr = self.build_resend_request(expected, 0).await;
                    outcome.outbound.push(rr);
                    outcome.gap_detected = Some((expected, seq));
                    return outcome;
                } else if seq < expected {
                    // Stale/duplicate without PossDup — ignore (a real engine
                    // would check PossDupFlag). Do not advance.
                    outcome.ignored_duplicate = true;
                    return outcome;
                }
            }
        }

        match msg_type {
            Some(MsgType::Logon) => {
                if is_reset_logon {
                    self.store.reset().await;
                }
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
                // If we initiated logon we are now Active; if the peer
                // initiated, reply with our own Logon then go Active.
                if self.state == SessionState::LogonSent {
                    self.state = SessionState::Active;
                } else {
                    let reply = self.build_logon().await;
                    outcome.outbound.push(reply);
                    self.state = SessionState::Active;
                }
            }
            Some(MsgType::TestRequest) => {
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
                let id = msg.get(tags::TEST_REQ_ID).map(|s| s.to_string());
                let hb = self.build_heartbeat(id.as_deref()).await;
                outcome.outbound.push(hb);
            }
            Some(MsgType::Heartbeat) => {
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
            }
            Some(MsgType::ResendRequest) => {
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
                let begin = msg.get_u64(tags::BEGIN_SEQ_NO).unwrap_or(1);
                let end = msg.get_u64(tags::END_SEQ_NO).unwrap_or(0);
                // Resend policy: gap-fill the whole requested range. The new
                // sequence number is one past our current outbound counter so
                // the peer's expected-inbound resyncs to where we actually are.
                let next_out = self.store.peek_out().await;
                let new_seq_no = if end == 0 { next_out } else { (end + 1).max(next_out) };
                let gap_fill = self.build_gap_fill(begin, new_seq_no).await;
                outcome.outbound.push(gap_fill);
            }
            Some(MsgType::SequenceReset) => {
                // Gap-fill or reset: trust NewSeqNo(36) and advance the expected
                // inbound counter to it. (We do not enforce that GapFill resets
                // only move forward here; that validation is an extension point.)
                if let Some(new_seq) = msg.get_u64(tags::NEW_SEQ_NO) {
                    // current_in becomes new_seq, so observe new_seq - 1.
                    if new_seq >= 1 {
                        self.store.observed_in(new_seq - 1).await;
                    }
                }
            }
            Some(MsgType::Logout) => {
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
                if self.state == SessionState::LogoutSent {
                    // Our logout was acked: fully disconnected.
                    self.state = SessionState::Disconnected;
                } else {
                    // Peer-initiated: ack with our own Logout, then disconnect.
                    let reply = self.build_logout(None).await;
                    outcome.outbound.push(reply);
                    self.state = SessionState::Disconnected;
                }
            }
            Some(MsgType::NewOrderSingle) | Some(MsgType::ExecutionReport) | Some(MsgType::Other(_)) => {
                if let Some(seq) = seq {
                    self.store.observed_in(seq).await;
                }
                outcome.application = Some(msg);
            }
            None => {
                // No MsgType — malformed at the session layer; surface as ignored.
                outcome.ignored_duplicate = true;
            }
        }

        outcome
    }

    /// Drive this FSM over a live `atomr-streams` TCP connection.
    ///
    /// Reads SOH-delimited frames from `inbound` (already framed `Bytes`),
    /// parses them, runs [`handle_inbound`](Self::handle_inbound), and writes
    /// every produced outbound message to `writer`. Application messages are
    /// forwarded to `app_tx`. Outbound application messages submitted on
    /// `app_rx` are stamped and written. The loop ends when the inbound stream
    /// closes or the session reaches [`SessionState::Disconnected`] after a
    /// logout.
    ///
    /// This helper exists for real deployments; the protocol logic itself is
    /// fully testable through [`handle_inbound`](Self::handle_inbound) without
    /// any socket.
    pub async fn run(
        mut self,
        inbound: atomr_streams::Source<Result<Bytes, atomr_streams::FramingError>>,
        writer: tokio::sync::mpsc::UnboundedSender<Bytes>,
        app_tx: tokio::sync::mpsc::UnboundedSender<FixMessage>,
        mut app_rx: tokio::sync::mpsc::UnboundedReceiver<FixMessage>,
    ) {
        // Bridge the framed inbound Source into a channel using the public
        // streams Sink, so we can `select!` it against the timer and app side.
        let (frame_tx, mut frame_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<Bytes, atomr_streams::FramingError>>();
        tokio::spawn(async move {
            atomr_streams::Sink::to_sender(inbound, frame_tx).await;
        });

        // Kick off with a logon.
        let logon = self.build_logon().await;
        let _ = writer.send(logon.to_wire());

        let mut hb = tokio::time::interval(self.config.heartbeat);
        hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else { break; };
                    let Ok(bytes) = frame else { continue; };
                    let Ok(msg) = FixMessage::parse(&bytes) else { continue; };
                    let outcome = self.handle_inbound(msg).await;
                    for out in &outcome.outbound {
                        let _ = writer.send(out.to_wire());
                    }
                    if let Some(app) = outcome.application {
                        let _ = app_tx.send(app);
                    }
                    if self.state == SessionState::Disconnected {
                        break;
                    }
                }
                app = app_rx.recv() => {
                    let Some(app) = app else { continue; };
                    if self.state == SessionState::Active {
                        let out = self.build_app_message(app).await;
                        let _ = writer.send(out.to_wire());
                    }
                }
                _ = hb.tick() => {
                    if self.state == SessionState::Active {
                        let hb_msg = self.heartbeat().await;
                        let _ = writer.send(hb_msg.to_wire());
                    }
                }
            }
        }
    }
}

/// The result of [`FixSession::handle_inbound`].
#[derive(Debug, Default)]
pub struct InboundOutcome {
    /// Administrative replies the session wants to send, in order.
    pub outbound: Vec<FixMessage>,
    /// An application message to surface to the application, if any.
    pub application: Option<FixMessage>,
    /// `Some((expected, received))` when a sequence gap was detected.
    pub gap_detected: Option<(u64, u64)>,
    /// True when a stale duplicate / malformed message was ignored.
    pub ignored_duplicate: bool,
}

/// A minimal UTC `SendingTime(52)` stamp (`YYYYMMDD-HH:MM:SS`). We avoid a
/// chrono dependency: the value's exact resolution is not load-bearing for the
/// session FSM, only its presence and format.
fn sending_time() -> String {
    let now =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    // Decompose seconds-since-epoch into a civil UTC date/time without chrono.
    let days = now / 86_400;
    let secs_of_day = now % 86_400;
    let (h, m, s) = (secs_of_day / 3600, (secs_of_day % 3600) / 60, secs_of_day % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}{mo:02}{d:02}-{h:02}:{m:02}:{s:02}")
}

/// Convert days-since-Unix-epoch to a civil (year, month, day) using Howard
/// Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}
