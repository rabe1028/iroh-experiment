//! Offline end-to-end test: a local HTTP/3 server mimicking the configured
//! Cloudflare zone (see infra/cloudflare/) and the real H3 client stack
//! against it over loopback QUIC with a self-signed certificate.
//!
//! The public [`cloudflare_h3_observer::observe`] uses webpki roots and DNS,
//! which need the real zone; this test drives the identical QUIC+H3 request
//! path directly so only trust/DNS injection differs.

use std::{net::SocketAddr, sync::Arc};

use quinn::crypto::rustls::QuicServerConfig;
use rcgen::generate_simple_self_signed;

fn cert_and_key(host: &str) -> (Vec<u8>, Vec<u8>) {
    let cert = generate_simple_self_signed(vec![host.to_string()]).unwrap();
    (cert.cert.der().to_vec(), cert.key_pair.serialize_der())
}

#[tokio::test]
async fn observes_headers_over_real_h3() {
    let host = "observe.example.invalid";
    let (cert_der, key_der) = cert_and_key(host);

    // --- server: mimics the configured Cloudflare zone ---
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        key_der,
    ));
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert_der.clone().into()], key)
    .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls).unwrap()));
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr: SocketAddr = endpoint.local_addr().unwrap();

    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let Ok(conn) = incoming.await else { continue };
            let Ok(mut h3_conn) =
                h3::server::Connection::<_, bytes::Bytes>::new(h3_quinn::Connection::new(conn))
                    .await
            else {
                continue;
            };
            while let Ok(Some(resolver)) = h3_conn.accept().await {
                let Ok((req, mut stream)) = resolver.resolve_request().await else {
                    continue;
                };
                if req.uri().path() == "/observe" {
                    // Same headers infra/cloudflare/ configures the zone to
                    // emit. Note there is deliberately no port header: the
                    // platform cannot produce one.
                    let response = http::Response::builder()
                        .status(200)
                        .header("x-observed-ip", "203.0.113.20")
                        .header("x-observed-rtt-ms", "42")
                        .header("x-observed-colo", "NRT")
                        .body(())
                        .unwrap();
                    stream.send_response(response).await.ok();
                } else {
                    let response = http::Response::builder().status(404).body(()).unwrap();
                    stream.send_response(response).await.ok();
                }
                let _ = stream.finish().await;
            }
        }
    });

    // --- client: same stack as observe(), trusting our self-signed cert ---
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();
    let mut client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client = quinn::crypto::rustls::QuicClientConfig::try_from(client_tls).unwrap();
    let mut client_cfg = quinn::ClientConfig::new(Arc::new(quic_client));
    client_cfg.transport_config(Arc::new(quinn::TransportConfig::default()));
    let mut client_endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client_endpoint.set_default_client_config(client_cfg);

    let conn = client_endpoint.connect(addr, host).unwrap().await.unwrap();
    let (mut driver, mut send_request) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .unwrap();
    let driver_task = tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });

    let req = http::Request::builder()
        .uri(format!("https://{host}/observe"))
        .body(())
        .unwrap();
    let mut req_stream = send_request.send_request(req).await.unwrap();
    let response = req_stream.recv_response().await.unwrap();
    assert_eq!(response.status(), 200);
    let get = |n: &str| {
        response
            .headers()
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    assert_eq!(get("x-observed-ip").as_deref(), Some("203.0.113.20"));
    assert_eq!(get("x-observed-rtt-ms").as_deref(), Some("42"));
    assert_eq!(get("x-observed-colo").as_deref(), Some("NRT"));
    assert!(get("x-observed-port").is_none(), "port must stay missing");
    let _ = req_stream.finish().await;

    driver_task.abort();
}

#[test]
fn comparison_categories_match_plan_e3() {
    use std::net::{IpAddr, Ipv4Addr};

    use cloudflare_h3_observer::{compare, Comparison, H3Observation};
    let base = H3Observation {
        server_host: "observe.example.invalid".into(),
        observed_ip: Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 20))),
        observed_port: None,
        rtt_ms: None,
        colo: None,
        duration: std::time::Duration::ZERO,
        same_socket_as_iroh: false,
    };
    let stun_same = SocketAddr::new(base.observed_ip.unwrap(), 40000);
    // Port missing from H3 (platform limit): best case is port-missing.
    assert_eq!(
        compare(stun_same, &base).unwrap(),
        Comparison::SameIpPortMissing
    );

    // Different IP.
    let other = H3Observation {
        observed_ip: Some(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9))),
        ..base.clone()
    };
    assert_eq!(compare(stun_same, &other).unwrap(), Comparison::DifferentIp);

    // If a future platform exposes the port, both agree/differ correctly.
    let with_port = H3Observation {
        observed_port: Some(40001),
        ..base.clone()
    };
    assert_eq!(
        compare(stun_same, &with_port).unwrap(),
        Comparison::SameIpDifferentPort
    );
    let matching = H3Observation {
        observed_port: Some(40000),
        ..base
    };
    assert_eq!(
        compare(stun_same, &matching).unwrap(),
        Comparison::SameIpSamePort
    );
}
