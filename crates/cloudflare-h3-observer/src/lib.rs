//! HTTP/3 pseudo-QAD observer for Cloudflare (plan E3 / PR 5).
//!
//! Dials a Cloudflare-proxied host over HTTP/3 (`ALPN=h3`, QUIC) and reads
//! the observation headers the zone is configured to emit (see
//! `infra/cloudflare/`): the edge-visible client IP and, if available, RTT.
//!
//! ## Known platform limit (verified 2026-08 against current CF docs)
//!
//! **Cloudflare does not expose the client's source port anywhere**: neither
//! Transform Rules (no rules-language field for it; `cf.edge.server_port` is
//! the *edge's* port) nor Workers `request.cf` (which has `clientQuicRtt`,
//! `httpProtocol`, `colo`, ... but no client port). So H3 pseudo-QAD can
//! confirm **IP equality** only; the port component of plan E3's comparison
//! categories lands in [`Comparison::SameIpPortMissing`] at best. The
//! experiment still distinguishes "same IP" from "different IP", which is
//! the part that matters for candidate usefulness.
//!
//! ## Same-socket caveat
//!
//! As established in PR 4, iroh 1.0.3's public API cannot share its UDP
//! socket, so this observer runs on its own socket. All outputs carry
//! `same_socket_as_iroh: false`; NATs with per-destination mappings may show
//! a different mapping than iroh's (plan §20.1).

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
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
    /// Always `None` with current Cloudflare (platform does not expose it).
    pub observed_port: Option<u16>,
    /// Smoothed QUIC RTT reported by `request.cf.client_quic_rtt`, ms.
    pub rtt_ms: Option<f64>,
    /// IATA code of the responding data center.
    pub colo: Option<String>,
    /// Total probe wall time.
    #[serde(with = "millis")]
    pub duration: Duration,
    /// Always false until same-socket becomes possible (PR 4 finding).
    pub same_socket_as_iroh: bool,
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
/// Note there is no `SameIpSamePort` variant reachable through H3 today:
/// Cloudflare never reports the client port (see crate docs), so the best
/// possible outcome is [`Comparison::SameIpPortMissing`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Comparison {
    /// Same IP, and both methods agree on the port.
    SameIpSamePort,
    /// Same IP but ports differ (per-destination NAT mapping).
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

async fn observe_inner(host: &str, path: &str, port: u16) -> Result<H3Observation> {
    // Resolve once; QUIC dials a concrete socket address.
    let addr = tokio::net::lookup_host((host, port))
        .await
        .context("DNS resolution")?
        .next()
        .context("no addresses resolved")?;

    let mut endpoint =
        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).context("bind client endpoint")?;
    endpoint.set_default_client_config(client_config(host)?);

    let conn = endpoint
        .connect(addr, host)
        .context("start QUIC connection")?
        .await
        .context("QUIC handshake (HTTP/3 fallback or UDP block shows up here)")?;

    let (mut driver, mut send_request) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .context("h3 init failed")?;
    let driver_task = tokio::spawn(async move {
        // wait_idle resolves with the connection error when it ends.
        let err = driver.wait_idle().await;
        tracing::debug!(error = %err, "h3 driver ended");
    });

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
        Some(v) => Some(v.parse().context("x-observed-port is not a valid u16 port")?),
        None => None,
    };
    let rtt_ms = header(HDR_OBSERVED_RTT_MS).and_then(|v| v.parse().ok());
    let colo = header(HDR_COLLO);

    // Consume the body so the connection can close cleanly.
    loop {
        match stream.recv_data().await {
            Ok(Some(chunk)) => drop(chunk),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    driver_task.abort();

    Ok(H3Observation {
        server_host: host.to_string(),
        observed_ip,
        // Platform limit: Cloudflare never exposes the client source port.
        observed_port,
        rtt_ms,
        colo,
        duration: Duration::ZERO,
        same_socket_as_iroh: false,
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
