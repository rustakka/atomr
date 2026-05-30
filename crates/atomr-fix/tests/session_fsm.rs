//! Hermetic FSM tests for the FIX session layer — no real sockets.
//!
//! Each test constructs a [`FixSession`] over an [`InMemorySeqStore`], feeds it
//! inbound [`FixMessage`]s, and asserts on the outbound messages + state.

use std::sync::Arc;
use std::time::Duration;

use atomr_fix::message::tags;
use atomr_fix::{
    FixMessage, FixSeqStore, FixSession, FixSessionConfig, FixVersion, InMemorySeqStore, MsgType,
    SessionState,
};

fn config() -> FixSessionConfig {
    let mut c = FixSessionConfig::new(FixVersion::Fix44, "CLIENT", "SERVER");
    c.heartbeat = Duration::from_secs(30);
    c
}

/// Build an inbound message *as seen from the counterparty*: SenderCompID is
/// the peer (SERVER), TargetCompID is us (CLIENT), stamped with `seq`.
fn inbound(msg_type: MsgType, seq: u64) -> FixMessage {
    let mut m = FixMessage::of_type(msg_type);
    m.set(tags::BEGIN_STRING, "FIX.4.4");
    m.set(tags::SENDER_COMP_ID, "SERVER");
    m.set(tags::TARGET_COMP_ID, "CLIENT");
    m.set(tags::MSG_SEQ_NUM, seq.to_string());
    m
}

#[tokio::test]
async fn logon_handshake_peer_initiated() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store.clone());
    assert_eq!(session.state(), SessionState::Disconnected);

    // Peer sends Logon first (seq 1).
    let mut logon = inbound(MsgType::Logon, 1);
    logon.set(tags::HEART_BT_INT, "30");
    let outcome = session.handle_inbound(logon).await;

    // We reply with our own Logon and go Active.
    assert_eq!(session.state(), SessionState::Active);
    assert_eq!(outcome.outbound.len(), 1);
    assert_eq!(outcome.outbound[0].msg_type(), Some(MsgType::Logon));
    // Inbound seq 1 observed -> next expected inbound is 2.
    assert_eq!(store.current_in().await, 2);
    // Our reply consumed outbound seq 1 -> next outbound is 2.
    assert_eq!(store.peek_out().await, 2);
}

#[tokio::test]
async fn logon_handshake_self_initiated() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store.clone());

    // We initiate.
    let logon = session.build_logon().await;
    assert_eq!(logon.msg_type(), Some(MsgType::Logon));
    assert_eq!(logon.get(tags::HEART_BT_INT), Some("30"));
    assert_eq!(session.state(), SessionState::LogonSent);

    // Peer acks with their Logon — no reply, just Active.
    let outcome = session.handle_inbound(inbound(MsgType::Logon, 1)).await;
    assert!(outcome.outbound.is_empty());
    assert_eq!(session.state(), SessionState::Active);
}

#[tokio::test]
async fn test_request_produces_heartbeat_with_id() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store);
    // Active first.
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;

    let mut tr = inbound(MsgType::TestRequest, 2);
    tr.set(tags::TEST_REQ_ID, "PING-42");
    let outcome = session.handle_inbound(tr).await;

    assert_eq!(outcome.outbound.len(), 1);
    let hb = &outcome.outbound[0];
    assert_eq!(hb.msg_type(), Some(MsgType::Heartbeat));
    assert_eq!(hb.get(tags::TEST_REQ_ID), Some("PING-42"));
}

#[tokio::test]
async fn gap_detection_triggers_resend_request() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store.clone());
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;
    // Now expecting inbound seq 2. Peer skips ahead to seq 5.
    let outcome = session.handle_inbound(inbound(MsgType::Heartbeat, 5)).await;

    assert_eq!(outcome.gap_detected, Some((2, 5)));
    assert_eq!(outcome.outbound.len(), 1);
    let rr = &outcome.outbound[0];
    assert_eq!(rr.msg_type(), Some(MsgType::ResendRequest));
    assert_eq!(rr.get(tags::BEGIN_SEQ_NO), Some("2"));
    assert_eq!(rr.get(tags::END_SEQ_NO), Some("0")); // 0 == to-infinity
                                                     // Inbound counter NOT advanced — still waiting to fill the gap.
    assert_eq!(store.current_in().await, 2);
}

#[tokio::test]
async fn resend_request_produces_gap_fill() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store.clone());
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;
    // Send a few app messages so our outbound counter has advanced.
    for _ in 0..3 {
        let mut nos = FixMessage::of_type(MsgType::NewOrderSingle);
        nos.set(tags::CL_ORD_ID, "x");
        let _ = session.build_app_message(nos).await;
    }
    let next_out = store.peek_out().await; // 5 (logon=1, three app=2..4)
    assert_eq!(next_out, 5);

    // Peer asks us to resend 2..4.
    let mut rr = inbound(MsgType::ResendRequest, 2);
    rr.set(tags::BEGIN_SEQ_NO, "2");
    rr.set(tags::END_SEQ_NO, "4");
    let outcome = session.handle_inbound(rr).await;

    assert_eq!(outcome.outbound.len(), 1);
    let gf = &outcome.outbound[0];
    assert_eq!(gf.msg_type(), Some(MsgType::SequenceReset));
    assert_eq!(gf.get(tags::GAP_FILL_FLAG), Some("Y"));
    // Gap-fill occupies the slot of the first requested message.
    assert_eq!(gf.get(tags::MSG_SEQ_NUM), Some("2"));
    // NewSeqNo advances peer past the requested range / to our real position.
    assert_eq!(gf.get_u64(tags::NEW_SEQ_NO), Some(5));
}

#[tokio::test]
async fn sequence_reset_advances_inbound_counter() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store.clone());
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;
    assert_eq!(store.current_in().await, 2);

    // Peer gap-fills: NewSeqNo=10 -> we now expect inbound 10 next.
    let mut sr = inbound(MsgType::SequenceReset, 2);
    sr.set(tags::GAP_FILL_FLAG, "Y");
    sr.set(tags::NEW_SEQ_NO, "10");
    let outcome = session.handle_inbound(sr).await;
    assert!(outcome.outbound.is_empty());
    assert_eq!(store.current_in().await, 10);
}

#[tokio::test]
async fn orderly_logout_peer_initiated() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store);
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;

    let outcome = session.handle_inbound(inbound(MsgType::Logout, 2)).await;
    assert_eq!(outcome.outbound.len(), 1);
    assert_eq!(outcome.outbound[0].msg_type(), Some(MsgType::Logout));
    assert_eq!(session.state(), SessionState::Disconnected);
}

#[tokio::test]
async fn orderly_logout_self_initiated() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store);
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;

    let lo = session.build_logout(Some("done")).await;
    assert_eq!(lo.msg_type(), Some(MsgType::Logout));
    assert_eq!(lo.get(tags::TEXT), Some("done"));
    assert_eq!(session.state(), SessionState::LogoutSent);

    // Peer acks: fully disconnected, no reply.
    let outcome = session.handle_inbound(inbound(MsgType::Logout, 2)).await;
    assert!(outcome.outbound.is_empty());
    assert_eq!(session.state(), SessionState::Disconnected);
}

#[tokio::test]
async fn new_order_single_to_execution_report_round_trip() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut session = FixSession::new(config(), store);
    session
        .handle_inbound({
            let mut l = inbound(MsgType::Logon, 1);
            l.set(tags::HEART_BT_INT, "30");
            l
        })
        .await;

    // Build a NewOrderSingle on the wire, parse it back (message-layer round trip).
    let mut nos = FixMessage::of_type(MsgType::NewOrderSingle);
    nos.set(tags::CL_ORD_ID, "ORD-1");
    nos.set(tags::SYMBOL, "AAPL");
    nos.set(tags::SIDE, "1"); // buy
    nos.set(tags::ORDER_QTY, "100");
    let out = session.build_app_message(nos).await;
    let wire = out.to_wire();
    let parsed = FixMessage::parse(&wire).unwrap();
    assert_eq!(parsed.msg_type(), Some(MsgType::NewOrderSingle));
    assert_eq!(parsed.get(tags::CL_ORD_ID), Some("ORD-1"));
    assert_eq!(parsed.get(tags::SYMBOL), Some("AAPL"));

    // Inbound ExecutionReport is surfaced as an application message.
    let mut er = inbound(MsgType::ExecutionReport, 2);
    er.set(tags::CL_ORD_ID, "ORD-1");
    er.set(tags::EXEC_TYPE, "0"); // new
    er.set(tags::ORD_STATUS, "0");
    let outcome = session.handle_inbound(er).await;
    assert!(outcome.outbound.is_empty());
    let app = outcome.application.expect("execution report surfaced");
    assert_eq!(app.msg_type(), Some(MsgType::ExecutionReport));
    assert_eq!(app.get(tags::CL_ORD_ID), Some("ORD-1"));
}

#[tokio::test]
async fn sequence_recovery_after_reconnect_uses_persisted_numbers() {
    // First session: logon + a couple of app messages, observe some inbound.
    let store = Arc::new(InMemorySeqStore::new());
    {
        let dyn_store: Arc<dyn FixSeqStore> = store.clone();
        let mut session = FixSession::new(config(), dyn_store);
        session
            .handle_inbound({
                let mut l = inbound(MsgType::Logon, 1);
                l.set(tags::HEART_BT_INT, "30");
                l
            })
            .await;
        // We sent a logon-reply (seq1), send two app messages (seq 2,3).
        for _ in 0..2 {
            let mut nos = FixMessage::of_type(MsgType::NewOrderSingle);
            nos.set(tags::CL_ORD_ID, "x");
            let _ = session.build_app_message(nos).await;
        }
        // Observe a couple more inbound heartbeats (seq 2,3).
        session.handle_inbound(inbound(MsgType::Heartbeat, 2)).await;
        session.handle_inbound(inbound(MsgType::Heartbeat, 3)).await;
    }
    // After "disconnect" the persisted store remembers where we left off.
    assert_eq!(store.peek_out().await, 4); // next outbound 4
    assert_eq!(store.current_in().await, 4); // next expected inbound 4

    // Reconnect with the SAME store (reset_on_logon = false). The new session
    // must continue the sequence, not restart at 1.
    let dyn_store: Arc<dyn FixSeqStore> = store.clone();
    let mut session = FixSession::new(config(), dyn_store);
    let logon = session.build_logon().await;
    assert_eq!(logon.get(tags::MSG_SEQ_NUM), Some("4"));
    // No ResetSeqNumFlag since reset_on_logon is false.
    assert_eq!(logon.get(tags::RESET_SEQ_NUM_FLAG), None);

    // A subsequent inbound at the expected number (4) is accepted, not a gap.
    let outcome = session.handle_inbound(inbound(MsgType::Logon, 4)).await;
    assert!(outcome.gap_detected.is_none());
    assert_eq!(store.current_in().await, 5);
}

#[tokio::test]
async fn reset_on_logon_restarts_sequence_and_sets_flag() {
    let store = Arc::new(InMemorySeqStore::with_counters(99, 99));
    let dyn_store: Arc<dyn FixSeqStore> = store.clone();
    let mut c = config();
    c.reset_on_logon = true;
    let mut session = FixSession::new(c, dyn_store);

    let logon = session.build_logon().await;
    assert_eq!(logon.get(tags::RESET_SEQ_NUM_FLAG), Some("Y"));
    // Reset before stamping -> our logon is seq 1.
    assert_eq!(logon.get(tags::MSG_SEQ_NUM), Some("1"));
    assert_eq!(store.peek_out().await, 2);
    assert_eq!(store.current_in().await, 1);
}

#[tokio::test]
async fn fixt_logon_carries_default_appl_ver_id() {
    let store: Arc<dyn FixSeqStore> = Arc::new(InMemorySeqStore::new());
    let mut c = FixSessionConfig::new(FixVersion::Fix50Sp2, "C", "S");
    c.heartbeat = Duration::from_secs(10);
    let mut session = FixSession::new(c, store);
    let logon = session.build_logon().await;
    // finalize_outbound stamps BeginString in-memory; it's also re-emitted on the wire.
    assert_eq!(logon.get(tags::BEGIN_STRING), Some("FIXT.1.1"));
    let wire = logon.to_wire();
    let parsed = FixMessage::parse(&wire).unwrap();
    assert_eq!(parsed.get(tags::BEGIN_STRING), Some("FIXT.1.1"));
    assert_eq!(parsed.get(tags::DEFAULT_APPL_VER_ID), Some("9"));
}
