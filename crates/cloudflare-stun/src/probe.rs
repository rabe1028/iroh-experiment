//! External-address probe runner over a caller-provided UDP socket.
//!
//! The socket is a parameter, not created internally, so the same runner
//! works today against a standalone socket and later against a socket shared
//! with iroh once a same-socket demux path exists (see crate docs).
//!
//! Mirrors the observation shape of plan section E2:
//! method / addr / rtt / observed_at.

use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use tokio::net::UdpSocket;

use crate::{encode_binding_request, parse_binding_response, TransactionId};

/// Probe tuning knobs.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    /// Per-attempt wait for *any* matching response datagram.
    pub attempt_timeout: Duration,
    /// Attempts per server (RFC 5389 suggests generous retransmission; NAT
    /// binding probes are cheap, so a small fixed count is fine here).
    pub attempts: u32,
}

impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_secs(2),
            attempts: 3,
        }
    }
}

/// One successful external-address observation.
#[derive(Debug, Clone)]
pub struct Observation {
    /// Discovery method label, e.g. `cloudflare-stun`.
    pub method: &'static str,
    /// Server the probe was sent to.
    pub server: SocketAddr,
    /// Server-reflexive address (our external ip:port as seen by `server`).
    pub observed_addr: SocketAddr,
    /// Round-trip time of the successful exchange.
    pub rtt: Duration,
    /// When the response arrived.
    pub observed_at: SystemTime,
}

/// Send one Binding Request from `socket` and await the authenticated
/// response from `server`, retrying per [`ProbeConfig`].
///
/// Datagrams that do not parse or carry a different transaction id are
/// ignored (not fatal): on a shared socket this is how unrelated traffic and
/// late responses get skipped.
pub async fn probe(
    socket: &UdpSocket,
    server: SocketAddr,
    config: &ProbeConfig,
) -> Result<Observation> {
    let txn = TransactionId::random();
    let request = encode_binding_request(&txn);

    let mut last_err = None;
    // RTT origin is the first send, not the matching attempt's send: every
    // attempt reuses one transaction id, so a delayed response to an earlier
    // attempt is indistinguishable from the current one and measuring from
    // the latest retransmission could report near-zero RTTs exactly on the
    // lossy networks where retries happen.
    let first_sent = Instant::now();
    for attempt in 0..config.attempts {
        if attempt > 0 {
            // Linear backoff keeps worst-case latency bounded for an
            // experiment tool while still riding out single-packet loss.
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }

        socket
            .send_to(&request, server)
            .await
            .context("sending binding request")?;

        let deadline = tokio::time::Instant::now() + config.attempt_timeout;
        // Diagnostic from non-matching datagrams, if any arrived; preferred
        // over a bare "timeout" so silent servers and wrong-traffic servers
        // are distinguishable.
        let mut parse_diag = None;
        let timed_out = 'attempt: {
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break 'attempt true;
                }
                let mut buf = vec![0u8; 1500];
                let recv = tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await;
                match recv {
                    Err(_elapsed) => break 'attempt true,
                    Ok(Err(e)) => {
                        last_err = Some(anyhow::Error::new(e).context("recv failed"));
                        break 'attempt false;
                    }
                    Ok(Ok((n, from))) => {
                        if from != server {
                            continue;
                        }
                        match parse_binding_response(&buf[..n], &txn) {
                            Ok(resp) => {
                                return Ok(Observation {
                                    method: "cloudflare-stun",
                                    server,
                                    observed_addr: resp.xor_mapped_address,
                                    rtt: first_sent.elapsed(),
                                    observed_at: SystemTime::now(),
                                });
                            }
                            Err(parse_err) => {
                                // An authenticated ERROR response is an
                                // explicit server rejection: fail now instead
                                // of retrying a request the server refused.
                                if let Ok(stun_err) =
                                    crate::parse_authenticated_error(&buf[..n], &txn)
                                {
                                    return Err(anyhow::anyhow!(
                                        "STUN server rejected probe: {} {}",
                                        stun_err.code,
                                        stun_err.reason
                                    ));
                                }
                                // Foreign/stale/garbage traffic: keep
                                // listening until the window closes.
                                parse_diag = Some(parse_err);
                            }
                        }
                    }
                }
            }
        };
        if timed_out {
            last_err = Some(
                parse_diag
                    .map(|e| e.context("datagrams arrived but none matched"))
                    .unwrap_or_else(|| anyhow::anyhow!("no response within attempt window")),
            );
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("probe failed")))
}

/// Resolve `host:port` once and return every resolved address, so callers can
/// implement fallback orderings like plan E2 (`3478/udp` then `53/udp`).
pub fn resolve(server: &str) -> Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;
    server
        .to_socket_addrs()
        .context(format!("resolving {server}"))
        .map(|iter| iter.collect())
}
