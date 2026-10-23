//! HTTP/3 pseudo-QAD observer for Cloudflare (plan E3 / PR 5).
//!
//! Dials a Cloudflare-proxied host over HTTP/3 (`ALPN=h3`, QUIC) and reads
//! the observation headers the zone is configured to emit (see
//! `infra/cloudflare/`): the edge-visible client IP and optional port/RTT.
//!
//! ## Observation configuration (rechecked 2026-10-23)
//!
//! Cloudflare's response-header Transform Rules list `cf.edge.client_port`
//! and `cf.timings.client_quic_rtt_msec`. A rule can be configured to emit
//! the port header this client already parses; availability and mapping
//! semantics still require measurement on the target zone. The Worker-only
//! example omits the port, yielding [`Comparison::SameIpPortMissing`].
//! See `infra/cloudflare/README.md` for official sources and setup.
//!
//! ## Same-socket integration
//!
//! `observe_on_endpoint` opens H3 on an existing iroh noq endpoint. It uses
//! the same UDP sockets and CID demultiplexer; complete observations are
//! registered as external addresses. The CLI uses this path by default.
//! The legacy `observe` helper still uses its own socket and reports false.
//! Mapping usefulness still depends on the NAT and must be measured.

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use bytes::Buf;
use http::{HeaderName, HeaderValue};
use quinn::crypto::rustls::QuicClientConfig;
use serde::{Deserialize, Serialize};

/// Response headers the zone must emit for this observer to work.
/// Configured via infra/cloudflare/ (Transform Rule or Worker).
pub const HDR_OBSERVED_IP: &str = "x-observed-ip";
pub const HDR_OBSERVED_PORT: &str = "x-observed-port";
pub const HDR_OBSERVED_RTT_MS: &str = "x-observed-rtt-ms";
pub const HDR_COLLO: &str = "x-observed-colo";

/// One HTTP/3 observation from the Cloudflare edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct H3Observation {
    /// Hostname probed (never an IP in public results).
    pub server_host: String,
    /// Client IP as seen by the edge.
    pub observed_ip: Option<IpAddr>,
    /// Optional source port emitted by the configured Transform Rule.
    pub observed_port: Option<u16>,
    /// Smoothed QUIC RTT from a Transform Rule or `request.cf.clientQuicRtt`, ms.
    pub rtt_ms: Option<f64>,
    /// IATA code of the responding data center.
    pub colo: Option<String>,
    /// Total probe wall time.
    #[serde(with = "millis")]
    pub duration: Duration,
    /// Whether this observation used an existing iroh transport socket.
    pub same_socket_as_iroh: bool,
    /// Actual iroh UDP binding, present for a shared-endpoint observation.
    #[serde(default)]
    pub local_addr: Option<SocketAddr>,
    /// True when a complete same-socket observation was registered with iroh.
    #[serde(default)]
    pub candidate_registered: bool,
}

mod millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_millis().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

/// Plan E3 comparison categories between two discovery methods.
///
/// Port comparison requires an `x-observed-port` header. Use live STUN and
/// H3 observations from the same endpoint to compare mappings. Even then,
/// candidate usefulness requires a real peer connectivity measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Comparison {
    /// Same IP, and both methods agree on the port.
    SameIpSamePort,
    /// Same IP but ports differ (socket or NAT mapping differences).
    SameIpDifferentPort,
    /// Same IP; one method could not report a port.
    SameIpPortMissing,
    /// Different public IPs were observed.
    DifferentIp,
}

/// Compare a STUN observation against an H3 observation (plan E3).
pub fn compare(stun_addr: SocketAddr, h3: &H3Observation) -> Result<Comparison> {
    let Some(h3_ip) = h3.observed_ip else {
        return Err(anyhow!("H3 observation has no observed ip"));
    };
    if stun_addr.ip() != h3_ip {
        return Ok(Comparison::DifferentIp);
    }
    match h3.observed_port {
        None => Ok(Comparison::SameIpPortMissing),
        Some(p) if p == stun_addr.port() => Ok(Comparison::SameIpSamePort),
        Some(_) => Ok(Comparison::SameIpDifferentPort),
    }
}

/// Run one H3 probe: connect over QUIC+H3, GET `https://{host}{path}`,
/// parse the observation headers.
///
/// Any failure here means "did not complete over HTTP/3" — per plan E3 step 6
/// such results are invalid rather than degraded, so they surface as errors.
pub async fn observe(
    host: &str,
    path: &str,
    port: u16,
    timeout: Duration,
) -> Result<H3Observation> {
    let started = std::time::Instant::now();
    tokio::time::timeout(timeout, observe_inner(host, path, port))
        .await
        .map_err(|_| anyhow!("probe timed out after {timeout:?}"))?
        .map(|mut o| {
            o.duration = started.elapsed();
            o
        })
}

/// Observe HTTP/3 using the same noq endpoint, UDP sockets, and CID demux
/// as an existing iroh endpoint. WebPKI certificate validation stays enabled.
/// Only families with exactly one IP socket are accepted, making socket
/// identity unambiguous for comparisons against STUN on that endpoint.
pub async fn observe_on_endpoint(
    endpoint: &iroh::Endpoint,
    host: &str,
    path: &str,
    port: u16,
    timeout: Duration,
) -> Result<H3Observation> {
    let started = std::time::Instant::now();
    tokio::time::timeout(timeout, async {
        let addresses = tokio::net::lookup_host((host, port))
            .await
            .context("DNS resolution")?;
        let mut last_error = anyhow!("no usable DNS addresses for the endpoint's IP sockets");
        for addr in addresses {
            match observe_on_endpoint_at(endpoint, addr, host, path, timeout).await {
                Ok(mut observation) => {
                    observation.duration = started.elapsed();
                    return Ok(observation);
                }
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    })
    .await
    .map_err(|_| anyhow!("probe timed out after {timeout:?}"))?
}

/// Probe a known edge address while validating the original hostname with
/// WebPKI. Useful for diagnosing a stale local DNS resolver without changing
/// system DNS, SNI, HTTP authority, or certificate verification.
pub async fn observe_on_endpoint_at(
    endpoint: &iroh::Endpoint,
    addr: SocketAddr,
    host: &str,
    path: &str,
    timeout: Duration,
) -> Result<H3Observation> {
    observe_on_endpoint_with_config(endpoint, addr, host, path, timeout, client_config(host)?).await
}

/// Same-endpoint probe with an explicit address and TLS configuration.
///
/// Useful for deterministic tests with a private CA. Production callers
/// should normally use `observe_on_endpoint`, which uses WebPKI roots.
pub async fn observe_on_endpoint_with_config(
    endpoint: &iroh::Endpoint,
    addr: SocketAddr,
    host: &str,
    path: &str,
    timeout: Duration,
    config: quinn::ClientConfig,
) -> Result<H3Observation> {
    let started = std::time::Instant::now();
    tokio::time::timeout(timeout, async {
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
        let bindings: Vec<_> = endpoint
            .stun_sockets()
            .iter()
            .filter_map(|socket| socket.local_addr().ok())
            .filter(|local| local.is_ipv4() == addr.is_ipv4())
            .collect();
        anyhow::ensure!(
            bindings.len() == 1,
            "need exactly one matching iroh IP socket"
        );
        let conn = endpoint
            .connect_quic(config, addr, host)
            .context("start shared-endpoint H3 connection")?
            .await
            .context("H3 QUIC handshake")?;
        let mut observation = observe_connection(conn, host, path, addr.port()).await?;
        let current: Vec<_> = endpoint
            .stun_sockets()
            .iter()
            .filter_map(|socket| socket.local_addr().ok())
            .filter(|local| local.is_ipv4() == addr.is_ipv4())
            .collect();
        anyhow::ensure!(
            current == bindings && !endpoint.is_closed(),
            "iroh socket changed during H3 probe"
        );
        let observed_ip = observation.observed_ip.context("missing observed IP")?;
        anyhow::ensure!(
            observed_ip.is_ipv4() == bindings[0].is_ipv4()
                && !observed_ip.is_unspecified()
                && !observed_ip.is_multicast(),
            "observed IP is not usable on the probed socket family"
        );
        if let Some(port) = observation.observed_port {
            endpoint
                .add_external_addr(SocketAddr::new(observed_ip, port))
                .await;
            observation.candidate_registered = true;
        }
        observation.same_socket_as_iroh = true;
        observation.local_addr = Some(bindings[0]);
        observation.duration = started.elapsed();
        Ok(observation)
    })
    .await
    .map_err(|_| anyhow!("probe timed out after {timeout:?}"))?
}

/// Wrapper so the spawned H3 driver task is aborted on every exit path,
/// including `?`-propagated errors and the outer probe timeout.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn observe_inner(host: &str, path: &str, port: u16) -> Result<H3Observation> {
    // Resolve once; QUIC dials a concrete socket address.
    let addr = tokio::net::lookup_host((host, port))
        .await
        .context("DNS resolution")?
        .next()
        .context("no addresses resolved")?;

    let endpoint =
        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).context("bind client endpoint")?;
    endpoint.set_default_client_config(client_config(host)?);

    let conn = endpoint
        .connect(addr, host)
        .context("start QUIC connection")?
        .await
        .context("QUIC handshake (HTTP/3 fallback or UDP block shows up here)")?;

    observe_connection(conn, host, path, port).await
}

struct CloseConnection(quinn::Connection);
impl Drop for CloseConnection {
    fn drop(&mut self) {
        self.0.close(0u32.into(), b"observation complete");
    }
}

async fn observe_connection(
    conn: quinn::Connection,
    host: &str,
    path: &str,
    port: u16,
) -> Result<H3Observation> {
    let _connection = CloseConnection(conn.clone());
    let (mut driver, mut send_request) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .context("h3 init failed")?;
    // Abort-on-drop: the configured keep-alive would otherwise keep the
    // driver (and the QUIC connection) alive after any early return, such as
    // a probe timeout or a non-2xx response. Bound to this function's scope;
    // dropping it (normally or via `?`) aborts the task.
    let _driver_task = AbortOnDrop(tokio::spawn(async move {
        // wait_idle resolves with the connection error when it ends.
        let err = driver.wait_idle().await;
        tracing::debug!(error = %err, "h3 driver ended");
    }));

    // Absolute-form authority: include the port when it is not the default,
    // so servers that validate or route on :authority see the real origin
    // the connection actually targets.
    let authority = if port == 443 {
        host.to_string()
    } else {
        format!("{host}:{port}")
    };
    let req = http::Request::builder()
        .uri(format!("https://{authority}{path}"))
        .header(
            "user-agent",
            concat!("iroh-experiment/", env!("CARGO_PKG_VERSION")),
        )
        .body(())
        .context("build request")?;

    let mut stream = send_request
        .send_request(req)
        .await
        .context("send H3 request")?;
    // HTTP/3 HEADERS do not end the request stream, and a GET may legally
    // carry a body; a server that waits for the stream FIN before dispatching
    // would otherwise deadlock this probe until its timeout. The request is
    // empty, so finish it right away.
    stream.finish().await.context("finish H3 request")?;
    let response = stream.recv_response().await.context("await H3 response")?;
    let status = response.status();
    anyhow::ensure!(
        status.is_success(),
        "edge returned {status}; check zone config in infra/cloudflare/"
    );

    let header = |name: &str| -> Option<String> {
        HeaderName::from_str(name)
            .ok()
            .and_then(|n| response.headers().get(n).to_owned())
            .and_then(|v: &HeaderValue| v.to_str().ok().map(str::to_owned))
    };

    // A 2xx without a parsable x-observed-ip means the zone's Worker route
    // or Transform Rule is missing, misconfigured, or emitting garbage; the
    // required observation did not happen, so this must fail the probe
    // instead of silently reporting None (which STUN comparison treats as
    // optional and would record as a successful run).
    if let Some(is_tcp) = header("x-observed-is-tcp") {
        anyhow::ensure!(is_tcp == "false", "edge reports a TCP observation");
    }
    if let Some(version) = header("x-observed-http-version") {
        anyhow::ensure!(
            matches!(version.as_str(), "HTTP/3" | "HTTP/3.0"),
            "edge reports a non-H3 observation"
        );
    }
    let raw_ip = header(HDR_OBSERVED_IP);
    let observed_ip: Option<IpAddr> = Some(
        raw_ip
            .context("response lacks x-observed-ip; check zone config in infra/cloudflare/")?
            .parse()
            .context("x-observed-ip is not a valid IP address")?,
    );
    // An intentionally empty header means "port not reported" (absent);
    // a non-empty value must parse, so malformed measurement data fails the
    // probe instead of silently degrading to SameIpPortMissing.
    let observed_port = match header(HDR_OBSERVED_PORT) {
        Some(v) if v.is_empty() => None,
        Some(v) => {
            let port: u16 = v
                .parse()
                .context("x-observed-port is not a valid u16 port")?;
            anyhow::ensure!(port != 0, "x-observed-port must be nonzero");
            Some(port)
        }
        None => None,
    };
    // Same contract as x-observed-port: empty means "not reported"; a
    // non-empty value that does not parse is malformed data and fails the
    // probe instead of silently degrading to an absent measurement.
    let rtt_ms = match header(HDR_OBSERVED_RTT_MS) {
        Some(v) if v.is_empty() => None,
        Some(v) => {
            let parsed: f64 = v
                .parse()
                .context("x-observed-rtt-ms is not a valid number")?;
            anyhow::ensure!(
                parsed.is_finite() && parsed >= 0.0,
                "x-observed-rtt-ms is not a physically valid RTT"
            );
            Some(parsed)
        }
        None => None,
    };
    let colo = header(HDR_COLLO);

    // h3 exposes the body/trailers separately and does not enforce the
    // advertised Content-Length here. A truncated measurement must never
    // register its candidate, even if valid observation headers arrived.
    let expected_len = response
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .map(|value| -> Result<u64> { Ok(value.to_str()?.parse()?) })
        .transpose()
        .context("invalid H3 Content-Length")?;
    let mut received_len = 0u64;
    while let Some(chunk) = stream.recv_data().await.context("read H3 response body")? {
        received_len = received_len
            .checked_add(chunk.remaining() as u64)
            .context("H3 response body length overflow")?;
    }
    stream
        .recv_trailers()
        .await
        .context("read H3 response trailers")?;
    if let Some(expected_len) = expected_len {
        anyhow::ensure!(
            received_len == expected_len,
            "H3 response body length mismatch"
        );
    }

    Ok(H3Observation {
        server_host: host.to_string(),
        observed_ip,
        // Optional: depends on the zone's observation header configuration.
        observed_port,
        rtt_ms,
        colo,
        duration: Duration::ZERO,
        same_socket_as_iroh: false,
        local_addr: None,
        candidate_registered: false,
    })
}

fn client_config(host: &str) -> Result<quinn::ClientConfig> {
    use rustls::pki_types::ServerName;

    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth();
    // HTTP/3 only: if the edge answers over TCP instead, this handshake
    // fails and the probe errors out (plan E3 step 6: invalid, not degraded).
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let _ = ServerName::try_from(host.to_string()); // validated at connect

    let quic = QuicClientConfig::try_from(tls).context("convert TLS config to QUIC")?;
    let mut config = quinn::ClientConfig::new(std::sync::Arc::new(quic));
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    config.transport_config(std::sync::Arc::new(transport));
    Ok(config)
}
