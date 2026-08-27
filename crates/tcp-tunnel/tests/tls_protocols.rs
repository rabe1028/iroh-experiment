//! HTTPS / HTTP/2 / gRPC protocol tests through the tunnel (plan E6 / 13.5).
//!
//! The raw byte tests in `http_compat.rs` cover the stream semantics; these
//! run actual protocol stacks through the tunnelled stream so the 13.5
//! acceptance list is exercised by real implementations: a real TLS
//! handshake (SNI, certificate validation, ALPN negotiation), real HTTP/2
//! framing (multiple streams multiplexed on one connection), and gRPC-style
//! unary calls with trailers (`application/grpc`, `te: trailers`,
//! `grpc-status`).

mod common;

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use common::{connect_tunnel, service_map};
use http::{HeaderMap, Method, Request, Response, StatusCode};use tokio::net::TcpListener;
use tokio_stream::StreamExt;

/// Host name shared by the certificate SAN, the TLS client's SNI, and the
/// request URIs; all three must agree or certificate validation fails with a
/// generic error.
const HOST: &str = "localhost";

/// A distinct LAN origin hostname. The E6 flow keeps the original origin in
/// URLs and SNI while connecting through the tunnel's local listener, so the
/// suite also exercises that original-hostname path rather than only the
/// tunnel-local `HOST`.
const LAN_ORIGIN: &str = "camera-ui.lan";

type BoxFuture = Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
type H2Handler = Arc<
    dyn Fn(Request<h2::RecvStream>, h2::server::SendResponse<Bytes>) -> BoxFuture + Send + Sync,
>;

/// Bind a TLS LAN service on loopback that terminates TLS with ALPN `h2`
/// and serves one h2 connection per accepted TCP connection, handing each
/// request stream to `handler`. `snis` lists the certificate hostnames. The
/// `sni` parameter is the name the client connects with. Returns the service
/// address and the certificate to trust as a client-side root.
async fn spawn_h2_tls_service_with_sni(
    handler: H2Handler,
    snis: Vec<String>,
    sni: &str,
) -> Result<(String, rustls::pki_types::CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(snis)
        .context("generate self-signed cert")?;
    let cert_der = cert.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()),
    );
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)
        .context("build TLS server config")?;
    server_config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = Arc::new(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)));

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            let handler = handler.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(sock).await else {
                    return;
                };
                let Ok(mut conn) = h2::server::handshake(tls).await else {
                    return;
                };
                // Serve each request stream on its own task so concurrent
                // streams multiplexed on the connection all progress.
                while let Some(Ok((request, respond))) = conn.accept().await {
                    tokio::spawn(handler(request, respond));
                }
            });
        }
    });
    Ok((addr, cert_der))
}

/// Bring up one TLS(h2) service behind the tunnel and return the h2 client
/// send handle towards it. The connection driver runs in a background task
/// whose lifetime ends with the test runtime.
async fn tls_h2_session(
    handler: H2Handler,
    service_id: &str,
) -> Result<h2::client::SendRequest<Bytes>> {
    let sender = tls_h2_session_with_sni(handler, service_id, HOST).await?;
    Ok(sender)
}

/// Default LAN service: certificate and SNI both use [`HOST`].
async fn spawn_h2_tls_service(
    handler: H2Handler,
) -> Result<(String, rustls::pki_types::CertificateDer<'static>)> {
    spawn_h2_tls_service_with_sni(handler, vec![HOST.to_string()], HOST).await
}

/// Bring up one TLS(h2) service behind the tunnel and connect with SNI
/// `sni` (certificate SAN must cover it); the request URL keeps the original
/// origin hostname, as real clients do through the tunnel's local listener.
async fn tls_h2_session_with_sni(
    handler: H2Handler,
    service_id: &str,
    sni: &str,
) -> Result<h2::client::SendRequest<Bytes>> {
    let snis = vec![HOST.to_string(), sni.to_string()];
    let (service_addr, cert) = spawn_h2_tls_service_with_sni(handler, snis, sni).await?;
    let services = service_map(&[format!("{service_id}={service_addr}")])?;
    let app_stream = connect_tunnel(&services, service_id).await?;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("valid root certificate");
    let mut client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let server_name = rustls::pki_types::ServerName::try_from(sni.to_string())
        .context("server name")?;
    let tls = connector
        .connect(server_name, app_stream)
        .await
        .context("TLS handshake through tunnel")?;
    let (request_sender, connection) =
        h2::client::handshake(tls).await.context("h2 client handshake")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(request_sender)
}

/// Read the whole h2 body, then the trailers (one stream, in order).
async fn recv_body_and_trailers(mut body: h2::RecvStream) -> Result<(Bytes, Option<HeaderMap>)> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.context("h2 body data")?;
        let _ = body.flow_control().release_capacity(chunk.len());
        out.extend_from_slice(&chunk);
    }
    let trailers = poll_fn(|cx| body.poll_trailers(cx))
        .await
        .context("h2 trailers")?;
    Ok((Bytes::from(out), trailers))
}

/// Read a whole h2 body to bytes (no trailers expected).
async fn recv_body(body: h2::RecvStream) -> Result<Bytes> {
    Ok(recv_body_and_trailers(body).await?.0)
}

/// Serve a plain GET: 200 with `body`.
async fn respond_with(mut respond: h2::server::SendResponse<Bytes>, body: &[u8]) {
    let mut send = respond
        .send_response(Response::new(()), false)
        .expect("send h2 response headers");
    send.send_data(Bytes::copy_from_slice(body), true)
        .expect("send h2 response body");
}

/// A GET request to `path` on [`HOST`].
fn get(path: &str) -> Request<()> {
    Request::builder()
        .method(Method::GET)
        .uri(format!("https://{HOST}{path}"))
        .body(())
        .unwrap()
}

#[tokio::test]
async fn https_tls_handshake_alpn_over_tunnel() -> Result<()> {
    // HTTPS over the tunnel: real rustls handshake with SNI and certificate
    // validation, ALPN negotiating h2, then one h2 GET answered with a body.
    let handler: H2Handler = Arc::new(|_req, respond| {
        Box::pin(async move {
            respond_with(respond, b"hello https").await;
        })
    });
    let mut request_sender = tls_h2_session(handler, "https").await?;
    let (response, _) = request_sender
        .send_request(get("/"), true)
        .context("send h2 request")?;
    let response = response.await.context("h2 response")?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = recv_body(response.into_body()).await?;
    assert_eq!(&body[..], b"hello https");
    Ok(())
}

#[tokio::test]
async fn https_original_lan_hostname_through_tunnel() -> Result<()> {
    // E6 keeps the original origin hostname in the URL and SNI while the
    // connection goes through the tunnel's local listener; a hostname
    // rewrite would break exactly here (certificate/authority mismatch).
    let handler: H2Handler = Arc::new(|_req, respond| {
        Box::pin(async move {
            respond_with(respond, b"lan-origin-ok").await;
        })
    });
    let mut request_sender = tls_h2_session_with_sni(handler, "lan", LAN_ORIGIN).await?;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("https://{LAN_ORIGIN}/"))
        .body(())
        .unwrap();
    let (response, _) = request_sender.send_request(request, true)?;
    let response = response.await.context("h2 response")?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = recv_body(response.into_body()).await?;
    assert_eq!(&body[..], b"lan-origin-ok");
    Ok(())
}

#[tokio::test]
async fn http2_concurrent_streams_over_tunnel() -> Result<()> {
    // HTTP/2 multiplexing: several concurrent GETs on one tunnelled
    // connection, each answered with its own body.
    let handler: H2Handler = Arc::new(|req, respond| {
        Box::pin(async move {
            let body = format!("body-for-{}", req.uri().path());
            respond_with(respond, body.as_bytes()).await;
        })
    });
    let mut request_sender = tls_h2_session(handler, "h2").await?;

    // Send all requests first; they are in flight simultaneously on the
    // single connection, which is the multiplexing under test.
    let mut responses = Vec::new();
    for i in 0..3 {
        let (response, _) = request_sender.send_request(get(&format!("/resource/{i}")), true)?;
        responses.push((i, response));
    }
    for (i, response) in responses {
        let response = response.await.context("h2 response")?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = recv_body(response.into_body()).await?;
        assert_eq!(&body[..], format!("body-for-/resource/{i}").as_bytes());
    }
    Ok(())
}

#[tokio::test]
async fn grpc_unary_with_trailers_over_tunnel() -> Result<()> {
    // gRPC unary call on the tunnelled stream: `application/grpc` content
    // type, `te: trailers`, a length-prefixed message both ways, and the
    // `grpc-status: 0` trailer — the wire shape gRPC clients require.
    let handler: H2Handler = Arc::new(|req, respond| {
        Box::pin(async move {
            let message = recv_body(req.into_body()).await.unwrap_or_default();
            let mut respond = respond;
            let mut send = respond
                .send_response(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap(),
                    false,
                )
                .expect("send gRPC response headers");
            send.send_data(message, false).expect("send gRPC message");
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", "0".parse().unwrap());
            send.send_trailers(trailers).expect("send gRPC trailers");
        })
    });
    let mut request_sender = tls_h2_session(handler, "grpc").await?;

    // gRPC length-prefixed message: compressed flag + 4-byte BE length.
    let payload = Bytes::from_static(b"grpc-payload");
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(0);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let frame = Bytes::from(frame);

    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("https://{HOST}/test.Service/Echo"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(())
        .unwrap();
    let (response, mut send_stream) = request_sender.send_request(request, false)?;
    send_stream
        .send_data(frame.clone(), true)
        .context("send gRPC message")?;
    let response = response.await.context("gRPC response")?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/grpc"
    );

    let (echoed, trailers) = recv_body_and_trailers(response.into_body()).await?;
    assert_eq!(echoed, frame, "gRPC message must round-trip");
    let trailers = trailers.context("gRPC trailers missing")?;
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    Ok(())
}
