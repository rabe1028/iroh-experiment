//! External-address probe runner over a caller-provided UDP socket.
//!
//! Supports standalone sockets and the pinned iroh same-socket STUN tap.
//! Only iroh reads its UDP socket; probes receive bounded copies of STUN
//! datagrams and never compete with QUIC for incoming packets.
//!
//! Mirrors the observation shape of plan section E2:
//! method / addr / rtt / observed_at.

use std::{
    future::Future,
    io,
    net::SocketAddr,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result};
use iroh::{endpoint::StunSocket, Endpoint};
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
    /// Actual UDP binding used for this observation.
    pub local_addr: SocketAddr,
    /// True only when the packet used an existing iroh IP transport socket.
    pub same_socket_as_iroh: bool,
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
    run_probe(&mut Standalone(socket), server, config).await
}

/// Probe from an existing iroh UDP socket, with no competing reader.
pub async fn probe_same_socket(
    socket: &StunSocket,
    server: SocketAddr,
    config: &ProbeConfig,
) -> Result<Observation> {
    let mut transport = Shared {
        socket,
        packets: socket.subscribe(), // subscribe before the first send
    };
    run_probe(&mut transport, server, config).await
}

/// Observe and register an address from the endpoint's own UDP socket.
///
/// The caller must refresh/remove this address when its network or mapping
/// changes. Multiple sockets of the same family are rejected: selecting one
/// by list order could advertise a mapping for a different route than QUIC.
pub async fn discover(
    endpoint: &Endpoint,
    server: SocketAddr,
    config: &ProbeConfig,
) -> Result<Observation> {
    let server = SocketAddr::new(server.ip().to_canonical(), server.port());
    let sockets: Vec<_> = endpoint
        .stun_sockets()
        .into_iter()
        .filter(|socket| {
            socket
                .local_addr()
                .is_ok_and(|addr| addr.is_ipv4() == server.is_ipv4())
        })
        .collect();
    anyhow::ensure!(
        sockets.len() == 1,
        "need exactly one iroh socket for the server address family, found {}",
        sockets.len()
    );
    let observation = probe_same_socket(&sockets[0], server, config).await?;
    anyhow::ensure!(
        !endpoint.is_closed(),
        "endpoint closed before candidate registration"
    );
    endpoint.add_external_addr(observation.observed_addr).await;
    Ok(observation)
}

/// Resolve a STUN hostname and discover an address on the endpoint itself.
/// Tries all resolved addresses with a matching transport family.
pub async fn discover_server(
    endpoint: &Endpoint,
    server: &str,
    config: &ProbeConfig,
) -> Result<Observation> {
    let mut error = anyhow::anyhow!("no usable STUN address for {server}");
    for addr in tokio::net::lookup_host(server)
        .await
        .context("resolve STUN server")?
    {
        match discover(endpoint, addr, config).await {
            Ok(observation) => return Ok(observation),
            Err(e) => error = e,
        }
    }
    Err(error)
}

trait ProbeTransport {
    fn local_addr(&self) -> io::Result<SocketAddr>;
    fn same_socket(&self) -> bool;
    fn send(
        &self,
        packet: &[u8],
        server: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send;
    fn recv(
        &mut self,
        buffer: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;
}

struct Standalone<'a>(&'a UdpSocket);
impl ProbeTransport for Standalone<'_> {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
    fn same_socket(&self) -> bool {
        false
    }
    async fn send(&self, packet: &[u8], server: SocketAddr) -> io::Result<usize> {
        self.0.send_to(packet, server).await
    }
    async fn recv(&mut self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.recv_from(buffer).await
    }
}

struct Shared<'a> {
    socket: &'a StunSocket,
    packets: tokio::sync::broadcast::Receiver<iroh::endpoint::StunDatagram>,
}
impl ProbeTransport for Shared<'_> {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    fn same_socket(&self) -> bool {
        true
    }
    async fn send(&self, packet: &[u8], server: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(packet, server).await
    }
    async fn recv(&mut self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        tokio::select! {
            biased;
            _ = self.socket.closed() => Err(io::Error::new(io::ErrorKind::NotConnected, "iroh endpoint closed")),
            received = self.packets.recv() => {
                let packet = received.map_err(io::Error::other)?;
                if packet.bytes.len() > buffer.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized STUN datagram"));
                }
                buffer[..packet.bytes.len()].copy_from_slice(&packet.bytes);
                Ok((packet.bytes.len(), packet.from))
            }
        }
    }
}

async fn run_probe(
    socket: &mut impl ProbeTransport,
    server: SocketAddr,
    config: &ProbeConfig,
) -> Result<Observation> {
    anyhow::ensure!(config.attempts > 0, "probe attempts must be positive");
    anyhow::ensure!(
        !config.attempt_timeout.is_zero(),
        "probe timeout must be positive"
    );
    let local_addr = socket.local_addr().context("probe socket binding")?;
    let server = SocketAddr::new(server.ip().to_canonical(), server.port());
    anyhow::ensure!(
        local_addr.is_ipv4() == server.is_ipv4(),
        "probe address family mismatch"
    );
    let txn = TransactionId::random();
    let request = encode_binding_request(&txn);

    let mut last_err = None;
    // RTT origin is the first send, not the matching attempt's send: every
    // attempt reuses one transaction id, so a delayed response to an earlier
    // attempt is indistinguishable from the current one and measuring from
    // the latest retransmission could report near-zero RTTs exactly on the
    // lossy networks where retries happen.
    let first_sent = Instant::now();
    // A shared socket may receive unrelated, fragmented STUN traffic above
    // the usual MTU. Keep the whole datagram so it can be rejected by source
    // and transaction rather than failing every outstanding probe.
    let mut buf = vec![0u8; u16::MAX as usize];
    for attempt in 0..config.attempts {
        if attempt > 0 {
            // Linear backoff keeps worst-case latency bounded for an
            // experiment tool while still riding out single-packet loss.
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }

        tokio::time::timeout(config.attempt_timeout, socket.send(&request, server))
            .await
            .context("STUN send timed out")?
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
                let recv = tokio::time::timeout(remaining, socket.recv(&mut buf)).await;
                match recv {
                    Err(_elapsed) => break 'attempt true,
                    Ok(Err(e)) => {
                        return Err(anyhow::Error::new(e).context("recv failed"));
                    }
                    Ok(Ok((n, from))) => {
                        if from != server {
                            continue;
                        }
                        match parse_binding_response(&buf[..n], &txn) {
                            Ok(resp) => {
                                anyhow::ensure!(
                                    socket.local_addr()? == local_addr,
                                    "socket rebound during probe"
                                );
                                let addr = resp.xor_mapped_address;
                                anyhow::ensure!(
                                    addr.is_ipv4() == local_addr.is_ipv4()
                                        && !addr.ip().is_unspecified()
                                        && !addr.ip().is_multicast()
                                        && addr.port() != 0,
                                    "unusable STUN observed address"
                                );
                                return Ok(Observation {
                                    method: "cloudflare-stun",
                                    server,
                                    observed_addr: resp.xor_mapped_address,
                                    rtt: first_sent.elapsed(),
                                    observed_at: SystemTime::now(),
                                    local_addr,
                                    same_socket_as_iroh: socket.same_socket(),
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
