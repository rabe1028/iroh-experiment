//! Exercise the real iroh IP transport while STUN shares its receive loop.
use std::{net::SocketAddr, time::Duration};

use cloudflare_stun::{
    encode_binding_error, encode_binding_success, parse_binding_request,
    probe::{self, ProbeConfig},
    TransactionId,
};
use iroh::{
    endpoint::{presets, IncomingAddr},
    Endpoint, EndpointAddr,
};
use tokio::net::UdpSocket;

const ALPN: &[u8] = b"same-socket-test/0";
const FAST: ProbeConfig = ProbeConfig {
    attempt_timeout: Duration::from_millis(300),
    attempts: 1,
};

async fn endpoint(bind: &str) -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr(bind)
        .unwrap()
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

async fn mock(bind: &str) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let socket = UdpSocket::bind(bind).await.unwrap();
    let address = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        loop {
            let (n, peer) = socket.recv_from(&mut packet).await.unwrap();
            let txn = parse_binding_request(&packet[..n]).unwrap();
            socket
                .send_to(&encode_binding_success(peer, &txn), peer)
                .await
                .unwrap();
        }
    });
    (address, task)
}

async fn round_trip(bind: &str) {
    let endpoint = endpoint(bind).await;
    let local = endpoint.bound_sockets()[0];
    let (server, task) = mock(bind).await;
    let observation = probe::discover(&endpoint, server, &FAST).await.unwrap();
    assert_eq!(observation.local_addr, local);
    assert_eq!(observation.observed_addr, local);
    assert!(observation.same_socket_as_iroh);
    endpoint.close().await;
    task.abort();
}

#[tokio::test]
async fn shared_socket_ipv4() {
    round_trip("127.0.0.1:0").await;
}

#[tokio::test]
async fn shared_socket_ipv6() {
    round_trip("[::1]:0").await;
}

#[tokio::test]
async fn stun_and_live_iroh_echo_use_the_same_port() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let client = endpoint("127.0.0.1:0").await;
        let server = endpoint("127.0.0.1:0").await;
        let local = client.bound_sockets()[0];
        let server_task = tokio::spawn({
            let server = server.clone();
            async move {
                let incoming = server.accept().await.unwrap();
                assert!(matches!(incoming.remote_addr(), IncomingAddr::Ip(addr) if addr == local));
                let conn = incoming.accept().unwrap().await.unwrap();
                let (mut send, mut recv) = conn.accept_bi().await.unwrap();
                let payload = recv.read_to_end(1024 * 1024).await.unwrap();
                send.write_all(&payload).await.unwrap();
                send.finish().unwrap();
                send.stopped().await.unwrap();
            }
        });
        let conn = client
            .connect(
                EndpointAddr::new(server.id()).with_ip_addr(server.bound_sockets()[0]),
                ALPN,
            )
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        let (stun_server, stun_task) = mock("127.0.0.1:0").await;
        let payload = vec![0x6d; 512 * 1024];
        let transfer = async {
            send.write_all(&payload).await.unwrap();
            send.finish().unwrap();
            assert_eq!(recv.read_to_end(1024 * 1024).await.unwrap(), payload);
        };
        let probes = async {
            for _ in 0..16 {
                let observation = probe::discover(&client, stun_server, &FAST).await.unwrap();
                assert_eq!(observation.observed_addr, local);
            }
        };
        tokio::join!(transfer, probes);
        server_task.await.unwrap();
        client.close().await;
        server.close().await;
        stun_task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn source_and_transaction_are_checked_before_registering_candidate() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server = socket.local_addr().unwrap();
    let forged: SocketAddr = "203.0.113.1:45000".parse().unwrap();
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (n, peer) = socket.recv_from(&mut packet).await.unwrap();
        let txn = parse_binding_request(&packet[..n]).unwrap();
        let rogue = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        rogue
            .send_to(&encode_binding_success(forged, &txn), peer)
            .await
            .unwrap();
        socket
            .send_to(
                &encode_binding_success(forged, &TransactionId::random()),
                peer,
            )
            .await
            .unwrap();
        socket
            .send_to(&encode_binding_success(peer, &txn), peer)
            .await
            .unwrap();
    });
    let observation = probe::discover(&endpoint, server, &FAST).await.unwrap();
    assert_eq!(observation.observed_addr, endpoint.bound_sockets()[0]);
    assert!(!endpoint.addr().ip_addrs().any(|addr| *addr == forged));
    task.await.unwrap();
    endpoint.close().await;
}

#[tokio::test]
async fn concurrent_probes_do_not_steal_responses() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..12 {
            let mut packet = [0; 1500];
            let (n, peer) = socket.recv_from(&mut packet).await.unwrap();
            requests.push((parse_binding_request(&packet[..n]).unwrap(), peer));
        }
        // All callers have subscribed; reply in the opposite order.
        for (txn, peer) in requests.into_iter().rev() {
            socket
                .send_to(&encode_binding_success(peer, &txn), peer)
                .await
                .unwrap();
        }
    });
    let mut probes = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let endpoint = endpoint.clone();
        probes.spawn(async move { probe::discover(&endpoint, server, &FAST).await });
    }
    while let Some(result) = probes.join_next().await {
        assert_eq!(
            result.unwrap().unwrap().observed_addr,
            endpoint.bound_sockets()[0]
        );
    }
    task.await.unwrap();
    endpoint.close().await;
}

#[tokio::test]
async fn endpoint_closure_cancels_probe_and_disables_handle() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let handle = endpoint.stun_sockets().remove(0);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server = socket.local_addr().unwrap();
    let pending = tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            probe::discover(
                &endpoint,
                server,
                &ProbeConfig {
                    attempt_timeout: Duration::from_secs(30),
                    attempts: 1,
                },
            )
            .await
        }
    });
    let mut packet = [0; 1500];
    tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    endpoint.close().await;
    let error = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(format!("{error:#}").contains("closed"));
    assert!(handle.local_addr().is_err());
    assert!(handle
        .send_to(
            &cloudflare_stun::encode_binding_request(&TransactionId::random()),
            server
        )
        .await
        .is_err());
}

#[tokio::test]
async fn server_error_and_timeout_do_not_register_candidates() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (n, peer) = socket.recv_from(&mut packet).await.unwrap();
        let txn = parse_binding_request(&packet[..n]).unwrap();
        socket
            .send_to(&encode_binding_error(420, "Unknown Attribute", &txn), peer)
            .await
            .unwrap();
    });
    let error = probe::discover(&endpoint, server, &FAST).await.unwrap_err();
    assert!(format!("{error:#}").contains("420"));
    task.await.unwrap();
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert!(
        probe::discover(&endpoint, silent.local_addr().unwrap(), &FAST)
            .await
            .is_err()
    );
    endpoint.close().await;
}

#[tokio::test]
async fn valid_observation_is_registered_as_an_external_address() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server = socket.local_addr().unwrap();
    let observed: SocketAddr = "203.0.113.20:53124".parse().unwrap();
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (n, peer) = socket.recv_from(&mut packet).await.unwrap();
        let txn = parse_binding_request(&packet[..n]).unwrap();
        socket
            .send_to(&encode_binding_success(observed, &txn), peer)
            .await
            .unwrap();
    });
    probe::discover(&endpoint, server, &FAST).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !endpoint.addr().ip_addrs().any(|addr| *addr == observed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    task.await.unwrap();
    endpoint.close().await;
}
