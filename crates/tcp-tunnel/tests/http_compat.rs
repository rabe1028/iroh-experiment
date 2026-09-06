//! HTTP / raw-TCP compatibility tests for the tunnel (plan E6).
//!
//! The gateway and client protocol layers run against in-memory transports
//! (`tokio::io::duplex` stands in for one iroh bidirectional stream), while
//! the "LAN services" are real `TcpListener`s on loopback. This exercises the
//! exact byte path an application sees through the tunnel.
//!
//! HTTPS, HTTP/2 and gRPC are TLS/ALPN passthrough for this tunnel: bytes are
//! forwarded verbatim and SNI is preserved because the stream is never
//! terminated. Their wire requirements (long-lived full-duplex byte streams)
//! are covered structurally by the echo, interleaving, SSE and half-close
//! tests here.

mod common;

use anyhow::Result;
use common::connect_tunnel;
use rand::RngCore;
use tcp_tunnel::{
    drive_client, read_status, serve_stream, write_request, ServiceMap, TunnelStatus,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::TcpListener;

use common::service_map;

/// Raw duplex pair wired through [`tcp_tunnel::serve_stream`], without
/// asserting handshake success; for rejection tests.
fn raw_tunnel(services: &ServiceMap) -> DuplexStream {
    let (client_end, mut gateway_end) = tokio::io::duplex(4096);
    let services = services.clone();
    tokio::spawn(async move {
        serve_stream(&mut gateway_end, &services).await.ok();
    });
    client_end
}

// ---------------------------------------------------------------------------
// Simulated LAN services
// ---------------------------------------------------------------------------

/// TCP echo service; returns its `host:port` address.
async fn spawn_echo() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut rd, mut wr) = tokio::io::split(sock);
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
            });
        }
    });
    addr
}

/// Minimal HTTP/1.1 server delegating each request to `handler`
/// (request head + body -> response bytes). Handles sequential keep-alive
/// requests until EOF. Returns its address.
async fn spawn_http<F, Fut>(handler: F) -> String
where
    F: Fn(Vec<u8>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Vec<u8>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let handler = handler.clone();
            tokio::spawn(async move {
                loop {
                    // Read until end of request head, then Content-Length body.
                    let mut buf = Vec::with_capacity(1024);
                    let mut chunk = [0u8; 512];
                    let head_end = loop {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        if let Some(pos) = find_head_end(&buf) {
                            break pos;
                        }
                    };
                    let content_length = parse_content_length(&buf[..head_end]);
                    let body_end = head_end + content_length;
                    while buf.len() < body_end {
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let response = handler(buf[..body_end].to_vec()).await;
                    if sock.write_all(&response).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

fn parse_content_length(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Raw TCP behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_echo_round_trip() -> Result<()> {
    let services = service_map(&[format!("echo={}", spawn_echo().await)])?;
    let mut tunnel = connect_tunnel(&services, "echo").await?;

    let payload = b"hello over iroh";
    tunnel.write_all(payload).await?;
    tunnel.flush().await?;
    let mut buf = vec![0u8; payload.len()];
    tunnel.read_exact(&mut buf).await?;
    assert_eq!(&buf, payload);
    Ok(())
}

#[tokio::test]
async fn large_bidirectional_transfer_is_byte_exact() -> Result<()> {
    const SIZE: usize = 8 * 1024 * 1024;
    let services = service_map(&[format!("echo={}", spawn_echo().await)])?;
    let tunnel = connect_tunnel(&services, "echo").await?;

    let mut tx = vec![0u8; SIZE];
    rand::thread_rng().fill_bytes(&mut tx);
    let expected = tx.clone();

    let (mut reader, mut writer) = tokio::io::split(tunnel);
    let writer_task = tokio::spawn(async move {
        for chunk in tx.chunks(64 * 1024) {
            writer.write_all(chunk).await.expect("write");
        }
        writer.flush().await.expect("flush");
        writer.shutdown().await.expect("shutdown");
    });

    let mut rx = Vec::with_capacity(SIZE);
    reader.read_to_end(&mut rx).await?;
    writer_task.await.unwrap();
    assert_eq!(rx.len(), SIZE);
    assert_eq!(rx, expected);
    Ok(())
}

/// Half-close must propagate: client sends EOF, service replies *after*
/// seeing it, client still receives the reply before its read side closes.
#[tokio::test]
async fn half_close_semantics() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap(); // wait for client FIN
        let _ = sock.write_all(b"after-your-eof").await;
    });

    let services = service_map(&[format!("halfclose={addr}")])?;
    let mut tunnel = connect_tunnel(&services, "halfclose").await?;
    tunnel.write_all(b"request").await?;
    tunnel.flush().await?;
    tunnel.shutdown().await?; // send our FIN through both hops

    let mut reply = Vec::new();
    tunnel.read_to_end(&mut reply).await?;
    assert_eq!(reply, b"after-your-eof");
    Ok(())
}

// ---------------------------------------------------------------------------
// HTTP compatibility (plan E6 table)
// ---------------------------------------------------------------------------

const GET_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";

#[tokio::test]
async fn http11_get_and_post_keep_alive() -> Result<()> {
    let http = spawn_http(|req| async move {
        let text = String::from_utf8_lossy(&req);
        if text.starts_with("GET") {
            GET_RESPONSE.to_vec()
        } else if let Some(body) = text.split_once("\r\n\r\n").map(|(_, b)| b) {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body.to_uppercase()
            )
            .into_bytes()
        } else {
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_vec()
        }
    })
    .await;

    let services = service_map(&[format!("web={http}")])?;
    let mut tunnel = connect_tunnel(&services, "web").await?;

    // GET with Host header like curl would send.
    tunnel
        .write_all(b"GET /camera HTTP/1.1\r\nHost: cam.lan\r\nConnection: keep-alive\r\n\r\n")
        .await?;
    let mut buf = vec![0u8; GET_RESPONSE.len()];
    tunnel.read_exact(&mut buf).await?;
    assert_eq!(&buf, GET_RESPONSE);

    // POST on the same connection (keep-alive works through the tunnel).
    const POST_BODY: &str = "{\"key\":\"val\"}";
    let post = format!(
        "POST /api HTTP/1.1\r\nHost: api.lan\r\nContent-Length: {}\r\n\r\n{}",
        POST_BODY.len(),
        POST_BODY
    );
    tunnel.write_all(post.as_bytes()).await?;
    let expected_head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n";
    let resp = read_until_contains(&mut tunnel, &POST_BODY.to_uppercase().into_bytes()).await?;
    assert!(resp.starts_with(expected_head.as_bytes()));
    Ok(())
}

/// Read from `stream` until `needle` was seen, returning everything read.
async fn read_until_contains<S: AsyncRead + Unpin>(
    stream: &mut S,
    needle: &[u8],
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        if find_subslice(&buf, needle).is_some() {
            return Ok(buf);
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!(
                "stream ended before {:?} was seen",
                String::from_utf8_lossy(needle)
            );
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len().max(1))
        .position(|w| w == needle)
}

#[tokio::test]
async fn http_chunked_response() -> Result<()> {
    let http = spawn_http(|_| async move {
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
          5\r\nfirst\r\nA\r\nchunk-two!\r\n0\r\n\r\n"
            .to_vec()
    })
    .await;
    let services = service_map(&[format!("web={http}")])?;
    let mut tunnel = connect_tunnel(&services, "web").await?;

    tunnel
        .write_all(b"GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
        .await?;
    let resp = read_until_contains(&mut tunnel, b"0\r\n\r\n").await?;
    let text = String::from_utf8(resp)?;
    assert!(text.contains("Transfer-Encoding: chunked"));
    // Chunked framing passes through unmodified for the client's HTTP parser.
    assert!(text.contains("5\r\nfirst\r\nA\r\nchunk-two!\r\n"));
    assert!(text.ends_with("0\r\n\r\n"));
    Ok(())
}

/// SSE: events must arrive incrementally, proving the tunnel does not buffer
/// whole responses before forwarding.
#[tokio::test]
async fn sse_streams_incrementally() -> Result<()> {
    const FIRST_EVENT: &[u8] =
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nevent: a\ndata: 1\n\n";
    let http = spawn_http(|_| async move { FIRST_EVENT.to_vec() }).await;
    let services = service_map(&[format!("sse={http}")])?;
    let mut tunnel = connect_tunnel(&services, "sse").await?;

    tunnel
        .write_all(b"GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
        .await?;
    let mut first = vec![0u8; FIRST_EVENT.len()];
    tunnel.read_exact(&mut first).await?;
    assert_eq!(&first, FIRST_EVENT);
    Ok(())
}

/// WebSocket upgrade handshake passes through verbatim and frames flow both
/// ways afterwards.
#[tokio::test]
async fn websocket_upgrade_and_frames() -> Result<()> {
    const WS_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
    const WS_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
    let upgrade_response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {WS_ACCEPT}\r\n\r\n"
    )
    .into_bytes();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr().unwrap().to_string();
    let server_upgrade = upgrade_response.clone();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 512];
        // Read the upgrade request head.
        let _ = sock.read(&mut buf).await.unwrap();
        sock.write_all(&server_upgrade).await.unwrap();
        // Echo every frame byte-for-byte after the switch.
        let (mut rd, mut wr) = tokio::io::split(sock);
        let _ = tokio::io::copy(&mut rd, &mut wr).await;
    });

    let services = service_map(&[format!("ws={addr}")])?;
    let mut tunnel = connect_tunnel(&services, "ws").await?;

    tunnel
        .write_all(
            format!(
                "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {WS_KEY}\r\nSec-WebSocket-Version: 13\r\n\r\n"
            )
            .as_bytes(),
        )
        .await?;
    let mut resp = vec![0u8; upgrade_response.len()];
    tunnel.read_exact(&mut resp).await?;
    assert_eq!(&resp, &upgrade_response[..]);

    let frame = [0x81u8, 0x05, b'h', b'e', b'l', b'l', b'o'];
    tunnel.write_all(&frame).await?;
    tunnel.flush().await?;
    let mut echo = vec![0u8; frame.len()];
    tunnel.read_exact(&mut echo).await?;
    assert_eq!(&echo, &frame);
    Ok(())
}

// ---------------------------------------------------------------------------
// Routing / authorization failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_service_is_rejected() -> Result<()> {
    let services = service_map(&[format!("echo={}", spawn_echo().await)])?;
    let mut client_end = raw_tunnel(&services);
    write_request(&mut client_end, "nope").await?;
    let status = read_status(&mut client_end).await?;
    assert_eq!(status, TunnelStatus::UnknownService);
    Ok(())
}

#[tokio::test]
async fn unreachable_upstream_reports_status() -> Result<()> {
    // Reserve a port with no listener bound to it.
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        let addr = l.local_addr()?;
        drop(l); // port now likely closed
        addr.to_string()
    };
    let services = service_map(&[format!("dead={dead}")])?;
    let mut client_end = raw_tunnel(&services);
    write_request(&mut client_end, "dead").await?;
    let status = read_status(&mut client_end).await?;
    assert_eq!(status, TunnelStatus::UpstreamUnreachable);
    Ok(())
}

/// The client byte counters must follow the advertised direction: N bytes
/// upstream and a different M bytes downstream must be labeled correctly
/// (regression for the swapped-tuple bug).
#[tokio::test]
async fn drive_client_byte_counters_follow_direction() -> Result<()> {
    // Service that reads exactly 100 bytes, then writes exactly 7000 bytes.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut sink = [0u8; 100];
        sock.read_exact(&mut sink).await.unwrap();
        let payload = vec![0x5a; 7000];
        sock.write_all(&payload).await.unwrap();
    });
    let services = service_map(&[format!("asym={addr}")])?;
    // raw end: drive_client performs the tunnel handshake itself, so the
    // stream must not be pre-connected (a double handshake would corrupt
    // the byte stream).
    let mut tunnel = raw_tunnel(&services);
    let (mut app, mut local) = tokio::io::duplex(64 * 1024);
    let driver = tokio::spawn(async move { drive_client(&mut tunnel, &mut local, "asym").await });
    app.write_all(&[0u8; 100]).await?;
    let mut got = vec![0u8; 7000];
    app.read_exact(&mut got).await?;
    app.shutdown().await?;
    let counts = tokio::time::timeout(std::time::Duration::from_secs(10), driver)
        .await
        .expect("drive_client must return after both sides close")
        .unwrap()?;
    assert_eq!(counts.to_gateway, 100, "UP must be client->gateway");
    assert_eq!(counts.from_gateway, 7000, "DOWN must be gateway->client");
    Ok(())
}

/// Client-side rejection path: `drive_client` errors on non-OK status.
#[tokio::test]
async fn drive_client_surfaces_rejection() {
    let services = service_map(&[]).expect("valid specs");
    let mut client_end = raw_tunnel(&services);

    let (mut local_a, _local_b) = tokio::io::duplex(4096);
    let err = drive_client(&mut client_end, &mut local_a, "anything").await;
    assert!(err.is_err());
}
