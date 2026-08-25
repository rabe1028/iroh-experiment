//! End-to-end probe tests against a mock STUN server on loopback.
//!
//! Exercises the full UDP round trip (request -> response -> authenticated
//! parse) without external network access, including retry and timeout
//! behavior.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use cloudflare_stun::probe::{self, ProbeConfig};
use cloudflare_stun::{
    encode_binding_error, encode_binding_success, parse_binding_request, TransactionId,
};
use tokio::net::UdpSocket;

const FAST: ProbeConfig = ProbeConfig {
    attempt_timeout: Duration::from_millis(150),
    attempts: 2,
};

/// Mock server responding to each Binding Request with the peer's address
/// XOR-encoded back. Returns its bind address.
async fn spawn_mock() -> SocketAddr {
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 1500];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                return;
            };
            if !cloudflare_stun::is_stun_packet(&buf[..n]) {
                continue;
            }
            let txn = match parse_binding_request(&buf[..n]) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let response = encode_binding_success(peer, &txn);
            let _ = sock.send_to(&response, peer).await;
        }
    });
    addr
}

#[tokio::test]
async fn probe_reports_peer_observed_address_and_rtt() {
    let server = spawn_mock().await;
    // Our "external" view from the mock is the loopback 5-tuple itself.
    let local = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let expected_peer = local.local_addr().unwrap();

    let obs = probe::probe(&local, server, &FAST).await.expect("probe ok");
    assert_eq!(obs.observed_addr, expected_peer);
    assert_eq!(obs.method, "cloudflare-stun");
    assert_eq!(obs.server, server);
    assert!(obs.rtt < Duration::from_secs(5));
}

/// The client must ignore datagrams with a foreign transaction id and keep
/// waiting for the real response within the same attempt window.
#[tokio::test]
async fn stale_responses_are_ignored_until_real_one_arrives() {
    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 1500];
        let (n, peer) = listener.recv_from(&mut buf).await.unwrap();
        let real_txn = parse_binding_request(&buf[..n]).expect("valid request");
        // First: a response for some *other* transaction (stale/spoofed).
        let stale = TransactionId::random();
        let _ = listener
            .send_to(&encode_binding_success(peer, &stale), peer)
            .await;
        // Then: the correct one. Client must end up using this.
        let _ = listener
            .send_to(&encode_binding_success(peer, &real_txn), peer)
            .await;
    });

    let local = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let obs = probe::probe(&local, server_addr, &FAST)
        .await
        .expect("eventually matches real transaction");
    assert_eq!(
        obs.observed_addr,
        local.local_addr().unwrap(),
        "must decode from the authentic response"
    );
}

#[tokio::test]
async fn error_response_fails_probe_with_code() {
    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 1500];
        let (n, peer) = listener.recv_from(&mut buf).await.unwrap();
        let txn = parse_binding_request(&buf[..n]).unwrap();
        let _ = listener
            .send_to(&encode_binding_error(420, "Unknown Attribute", &txn), peer)
            .await;
    });

    let local = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let err = probe::probe(&local, server_addr, &FAST)
        .await
        .expect_err("error response must fail the probe");
    assert!(err.to_string().contains("420"));
}

#[tokio::test]
async fn silent_server_times_out_with_retries() {
    // Bind but never respond.
    let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let local = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let started = Instant::now();
    let err = probe::probe(&local, server_addr, &FAST)
        .await
        .expect_err("must time out");
    assert!(err.to_string().contains("no response"));

    // attempts(2) x timeout(150ms) + one 200ms linear backoff between them.
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(450),
        "retries must actually happen; elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "probe must not hang; elapsed={elapsed:?}"
    );
}
