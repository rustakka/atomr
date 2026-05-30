//! TcpManager / UdpManager spec parity.
//! `IO.Tcp.Manager` / `IO.Udp.Manager` with their Bind / Connect /
//! Connected / Closed event taxonomy.

use std::time::Duration;

use atomr_core::io::manager::{IoEvent, TcpManager, UdpManager};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn next_event(rx: &mut tokio::sync::mpsc::UnboundedReceiver<IoEvent>) -> IoEvent {
    next_event_within(rx, Duration::from_millis(500)).await
}

async fn next_event_within(rx: &mut tokio::sync::mpsc::UnboundedReceiver<IoEvent>, dur: Duration) -> IoEvent {
    tokio::time::timeout(dur, rx.recv()).await.expect("event timeout").expect("rx closed")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_bind_emits_bound_event() {
    let (mgr, mut events) = TcpManager::spawn();
    mgr.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let evt = next_event(&mut events).await;
    let bound = match evt {
        IoEvent::Bound { addr } => addr,
        other => panic!("expected Bound, got {other:?}"),
    };
    assert!(bound.ip().is_loopback());
    assert_ne!(bound.port(), 0);
    mgr.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_outbound_connect_emits_connected_event() {
    // Server side: bind a passive listener.
    let (server, mut server_events) = TcpManager::spawn();
    server.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let bound = match next_event(&mut server_events).await {
        IoEvent::Bound { addr } => addr,
        other => panic!("server expected Bound, got {other:?}"),
    };

    // Client side: outbound connect via Connect command.
    let (client, mut client_events) = TcpManager::spawn();
    client.connect(bound).unwrap();
    let _id = match next_event(&mut client_events).await {
        IoEvent::Connected { id, peer } => {
            assert_eq!(peer, bound);
            id
        }
        other => panic!("client expected Connected, got {other:?}"),
    };
    // Server side observes Connected too.
    match next_event(&mut server_events).await {
        IoEvent::Connected { .. } => {}
        other => panic!("server expected Connected, got {other:?}"),
    }
    server.shutdown();
    client.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_outbound_to_unbound_addr_yields_error_event() {
    let (mgr, mut events) = TcpManager::spawn();
    // Connect to an address that is not bound. Use a port the OS is
    // unlikely to have reserved. Windows surfaces ECONNREFUSED on a
    // dead loopback port only after its initial SYN-retransmit window
    // (~2s), so this case needs more headroom than the default 500ms.
    mgr.connect("127.0.0.1:1".parse().unwrap()).unwrap();
    match next_event_within(&mut events, Duration::from_secs(5)).await {
        IoEvent::Error { .. } => {}
        other => panic!("expected Error, got {other:?}"),
    }
    mgr.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_client_close_drives_closed_event() {
    let (mgr, mut events) = TcpManager::spawn();
    mgr.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let bound = match next_event(&mut events).await {
        IoEvent::Bound { addr } => addr,
        other => panic!("expected Bound, got {other:?}"),
    };
    let mut client = TcpStream::connect(bound).await.unwrap();
    let _id = match next_event(&mut events).await {
        IoEvent::Connected { id, .. } => id,
        other => panic!("expected Connected, got {other:?}"),
    };
    // Drop the client side; the server should observe a Closed event.
    client.shutdown().await.unwrap();
    drop(client);
    let mut closed_seen = false;
    for _ in 0..3 {
        match next_event(&mut events).await {
            IoEvent::Closed { .. } => {
                closed_seen = true;
                break;
            }
            IoEvent::Received { .. } => continue,
            other => panic!("unexpected event after client close: {other:?}"),
        }
    }
    assert!(closed_seen);
    mgr.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_send_back_and_forth_round_trip() {
    let (mgr, mut events) = TcpManager::spawn();
    mgr.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let bound = match next_event(&mut events).await {
        IoEvent::Bound { addr } => addr,
        other => panic!("{other:?}"),
    };
    let mut client = TcpStream::connect(bound).await.unwrap();
    let id = match next_event(&mut events).await {
        IoEvent::Connected { id, .. } => id,
        other => panic!("{other:?}"),
    };
    client.write_all(b"hello").await.unwrap();
    match next_event(&mut events).await {
        IoEvent::Received { bytes, .. } => assert_eq!(&bytes, b"hello"),
        other => panic!("{other:?}"),
    }
    mgr.send_bytes(id, b"world".to_vec()).unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"world");
    mgr.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_two_managers_can_exchange_datagrams() {
    let (a, mut a_rx) = UdpManager::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let (b, mut b_rx) = UdpManager::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    a.send_to(b.local_addr(), b"ping".to_vec()).unwrap();
    b.send_to(a.local_addr(), b"pong".to_vec()).unwrap();
    match next_event(&mut a_rx).await {
        IoEvent::Datagram { bytes, .. } => assert_eq!(&bytes, b"pong"),
        other => panic!("{other:?}"),
    }
    match next_event(&mut b_rx).await {
        IoEvent::Datagram { bytes, .. } => assert_eq!(&bytes, b"ping"),
        other => panic!("{other:?}"),
    }
    a.shutdown();
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_local_addr_is_loopback_with_assigned_port() {
    let (a, _rx) = UdpManager::bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let local = a.local_addr();
    assert!(local.ip().is_loopback());
    assert_ne!(local.port(), 0);
    a.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_manager_close_command_drops_send_capability() {
    let (mgr, mut events) = TcpManager::spawn();
    mgr.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let bound = match next_event(&mut events).await {
        IoEvent::Bound { addr } => addr,
        other => panic!("{other:?}"),
    };
    let _client = TcpStream::connect(bound).await.unwrap();
    let id = match next_event(&mut events).await {
        IoEvent::Connected { id, .. } => id,
        other => panic!("{other:?}"),
    };
    mgr.close(id).unwrap();
    // After close, sending bytes should not panic; manager just drops them.
    mgr.send_bytes(id, b"after-close".to_vec()).unwrap();
    mgr.shutdown();
}
