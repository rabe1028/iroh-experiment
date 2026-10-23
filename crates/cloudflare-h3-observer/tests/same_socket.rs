//! Real H3, STUN and iroh handshakes through one existing iroh UDP binding.
use std::{net::SocketAddr, sync::Arc, time::Duration};

use cloudflare_h3_observer::{observe_on_endpoint_with_config, Comparison};
use iroh::{
    endpoint::{presets, IncomingAddr},
    Endpoint, EndpointAddr,
};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rcgen::generate_simple_self_signed;

const HOST: &str = "observe.example.invalid";
const ALPN: &[u8] = b"same-socket-h3-echo/0";

struct Mock {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    client: quinn::ClientConfig,
    seen: tokio::sync::mpsc::Receiver<SocketAddr>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"test finished");
        self.task.abort();
    }
}

async fn mock(bind: &str, overrides: Vec<(&'static str, &'static str)>) -> Mock {
    let certificate = generate_simple_self_signed(vec![HOST.into()]).unwrap();
    let cert = certificate.cert.der().clone();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der())
                .into(),
        )
        .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap())),
        bind.parse().unwrap(),
    )
    .unwrap();
    let addr = endpoint.local_addr().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let client = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
    let (tx, seen) = tokio::sync::mpsc::channel(8);
    let task = tokio::spawn({
        let endpoint = endpoint.clone();
        async move {
            while let Some(incoming) = endpoint.accept().await {
                let peer = incoming.remote_address();
                let peer = SocketAddr::new(peer.ip().to_canonical(), peer.port());
                let Ok(conn) = incoming.await else { continue };
                tx.send(peer).await.unwrap();
                let Ok(mut h3) =
                    h3::server::Connection::<_, bytes::Bytes>::new(h3_quinn::Connection::new(conn))
                        .await
                else {
                    continue;
                };
                while let Ok(Some(resolver)) = h3.accept().await {
                    let Ok((request, mut stream)) = resolver.resolve_request().await else {
                        continue;
                    };
                    assert_eq!(request.uri().path(), "/observe");
                    let mut response = http::Response::builder()
                        .status(200)
                        .header("x-observed-ip", peer.ip().to_string())
                        .header("x-observed-port", peer.port().to_string())
                        .header("x-observed-rtt-ms", "42")
                        .header("x-observed-is-tcp", "false")
                        .header("x-observed-http-version", "HTTP/3")
                        .body(())
                        .unwrap();
                    for (key, value) in &overrides {
                        response.headers_mut().insert(
                            http::HeaderName::from_static(key),
                            http::HeaderValue::from_static(value),
                        );
                    }
                    stream.send_response(response).await.unwrap();
                    let _ = stream.finish().await;
                }
            }
        }
    });
    Mock {
        endpoint,
        addr,
        client,
        seen,
        task,
    }
}

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

#[tokio::test]
async fn explicit_edge_address_still_rejects_an_untrusted_certificate() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let mock = mock("127.0.0.1:0", vec![]).await;
    let error = cloudflare_h3_observer::observe_on_endpoint_at(
        &endpoint,
        mock.addr,
        HOST,
        "/observe",
        Duration::from_secs(3),
    )
    .await
    .unwrap_err();
    assert!(format!("{error:#}").contains("certificate"), "{error:#}");
    endpoint.close().await;
}

#[tokio::test]
async fn stun_h3_and_iroh_share_the_actual_udp_port() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let endpoint = endpoint("127.0.0.1:0").await;
        let local = endpoint.bound_sockets()[0];
        let mut mock = mock("127.0.0.1:0", vec![]).await;
        let stun = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stun_addr = stun.local_addr().unwrap();
        let stun_task = tokio::spawn(async move {
            let mut packet = [0; 1500];
            let (n, peer) = stun.recv_from(&mut packet).await.unwrap();
            let txn = cloudflare_stun::parse_binding_request(&packet[..n]).unwrap();
            stun.send_to(&cloudflare_stun::encode_binding_success(peer, &txn), peer)
                .await
                .unwrap();
        });
        let config = cloudflare_stun::probe::ProbeConfig {
            attempt_timeout: Duration::from_secs(2),
            attempts: 1,
        };
        let (stun, h3) = tokio::join!(
            cloudflare_stun::probe::discover(&endpoint, stun_addr, &config),
            observe_on_endpoint_with_config(
                &endpoint,
                mock.addr,
                HOST,
                "/observe",
                Duration::from_secs(3),
                mock.client.clone()
            ),
        );
        let stun = stun.unwrap();
        let h3 = h3.unwrap();
        assert!(stun.same_socket_as_iroh && h3.same_socket_as_iroh);
        assert_eq!(stun.local_addr, local);
        assert_eq!(h3.local_addr, Some(local));
        assert_eq!(stun.observed_addr, local);
        assert_eq!(mock.seen.recv().await.unwrap(), local);
        assert_eq!(h3.observed_ip, Some(local.ip()));
        assert_eq!(h3.observed_port, Some(local.port()));
        assert!(h3.candidate_registered);
        assert_eq!(h3.rtt_ms, Some(42.0));
        assert_eq!(
            cloudflare_h3_observer::compare(stun.observed_addr, &h3).unwrap(),
            Comparison::SameIpSamePort
        );
        stun_task.await.unwrap();

        // Finishing an H3 observation must not close the shared endpoint.
        let peer = self::endpoint("127.0.0.1:0").await;
        let echo = tokio::spawn({
            let peer = peer.clone();
            async move {
                let incoming = peer.accept().await.unwrap();
                assert!(matches!(incoming.remote_addr(), IncomingAddr::Ip(addr) if addr == local));
                let conn = incoming.accept().unwrap().await.unwrap();
                let (mut send, mut recv) = conn.accept_bi().await.unwrap();
                let bytes = recv.read_to_end(1024).await.unwrap();
                send.write_all(&bytes).await.unwrap();
                send.finish().unwrap();
                send.stopped().await.unwrap();
            }
        });
        let conn = endpoint
            .connect(
                EndpointAddr::new(peer.id()).with_ip_addr(peer.bound_sockets()[0]),
                ALPN,
            )
            .await
            .unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        send.write_all(b"after-h3").await.unwrap();
        send.finish().unwrap();
        assert_eq!(recv.read_to_end(1024).await.unwrap(), b"after-h3");
        echo.await.unwrap();
        endpoint.close().await;
        peer.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shared_h3_supports_ipv6() {
    let endpoint = endpoint("[::1]:0").await;
    let mut mock = mock("[::1]:0", vec![]).await;
    let observation = observe_on_endpoint_with_config(
        &endpoint,
        mock.addr,
        HOST,
        "/observe",
        Duration::from_secs(3),
        mock.client.clone(),
    )
    .await
    .unwrap();
    assert_eq!(mock.seen.recv().await.unwrap(), endpoint.bound_sockets()[0]);
    assert!(observation.same_socket_as_iroh);
    endpoint.close().await;
}

#[tokio::test]
async fn invalid_edge_measurements_fail_without_closing_endpoint() {
    let endpoint = endpoint("127.0.0.1:0").await;
    for headers in [
        vec![("x-observed-port", "0")],
        vec![("x-observed-ip", "garbage")],
        vec![("x-observed-is-tcp", "true")],
        vec![("x-observed-http-version", "HTTP/2")],
    ] {
        let mock = mock("127.0.0.1:0", headers).await;
        assert!(observe_on_endpoint_with_config(
            &endpoint,
            mock.addr,
            HOST,
            "/observe",
            Duration::from_secs(3),
            mock.client.clone()
        )
        .await
        .is_err());
        assert!(!endpoint.is_closed());
    }
    let mock = mock("127.0.0.1:0", vec![("x-observed-port", "")]).await;
    let observation = observe_on_endpoint_with_config(
        &endpoint,
        mock.addr,
        HOST,
        "/observe",
        Duration::from_secs(3),
        mock.client.clone(),
    )
    .await
    .unwrap();
    assert_eq!(observation.observed_port, None);
    assert!(!observation.candidate_registered);
    endpoint.close().await;
}

#[tokio::test]
async fn shared_h3_still_checks_tls_hostname() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let mock = mock("127.0.0.1:0", vec![]).await;
    let result = observe_on_endpoint_with_config(
        &endpoint,
        mock.addr,
        "wrong.example.invalid",
        "/observe",
        Duration::from_secs(2),
        mock.client.clone(),
    )
    .await;
    assert!(result.is_err());
    endpoint.close().await;
}

#[tokio::test]
async fn truncated_h3_response_never_registers_a_candidate() {
    let endpoint = endpoint("127.0.0.1:0").await;
    let observed: SocketAddr = "203.0.113.66:45000".parse().unwrap();
    let mock = mock(
        "127.0.0.1:0",
        vec![
            ("x-observed-ip", "203.0.113.66"),
            ("x-observed-port", "45000"),
            ("content-length", "1"),
        ],
    )
    .await;
    // The mock sends headers and FIN without the promised response body.
    let result = observe_on_endpoint_with_config(
        &endpoint,
        mock.addr,
        HOST,
        "/observe",
        Duration::from_secs(3),
        mock.client.clone(),
    )
    .await;
    assert!(
        result.is_err(),
        "a truncated response must not be a successful observation"
    );
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!endpoint.addr().ip_addrs().any(|addr| *addr == observed));
    endpoint.close().await;
}
