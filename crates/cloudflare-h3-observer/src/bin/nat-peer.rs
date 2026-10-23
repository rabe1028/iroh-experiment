//! Direct-only peer experiment using only a same-socket observed address.
//! Candidate files are exchanged out of band (e.g. SSH), never via media relay.
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use iroh::{
    endpoint::{presets, Connection},
    Endpoint, EndpointAddr, EndpointId, RelayMode,
};
use serde::{Deserialize, Serialize};

const ALPN: &[u8] = b"iroh-experiment/nat-echo/1";
const PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Method {
    Stun,
    H3,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Role {
    Listen,
    Dial,
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    method: Method,
    #[arg(long)]
    role: Role,
    /// Fresh identifier shared by the two peers; use unique file paths per run.
    #[arg(long)]
    run_id: String,
    #[arg(long)]
    candidate_out: PathBuf,
    #[arg(long)]
    peer_file: PathBuf,
    #[arg(long)]
    results: PathBuf,
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
    #[arg(long, default_value = "stun.cloudflare.com:3478")]
    stun_server: String,
    #[arg(long)]
    h3_host: Option<String>,
    /// Diagnostic edge IP; the H3 hostname still uses WebPKI validation.
    #[arg(long)]
    resolve: Option<IpAddr>,
    #[arg(long, default_value_t = 120)]
    setup_timeout_secs: u64,
    #[arg(long, default_value_t = 20)]
    connect_timeout_secs: u64,
    /// Restart setup instead of testing an old, possibly expired mapping.
    #[arg(long, default_value_t = 30)]
    candidate_max_age_secs: u64,
    /// Explicit local test only. Never count such a run as cross-NAT evidence.
    #[arg(long)]
    allow_private_peer: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Candidate {
    schema: u32,
    run_id: String,
    method: Method,
    role: Role,
    endpoint_id: EndpointId,
    addr: SocketAddr,
}

#[derive(Default, Serialize)]
struct Report {
    schema: &'static str,
    run_id: String,
    method: Option<Method>,
    role: Option<Role>,
    diagnostic_private_peers: bool,
    local_observation: Option<Candidate>,
    peer_observation: Option<Candidate>,
    candidate_registered: bool,
    same_socket_as_iroh: bool,
    different_observed_ips: Option<bool>,
    ipv4_pair: Option<bool>,
    direct_connection_success: Option<bool>,
    echo_success: Option<bool>,
    time_to_connect_ms: Option<u64>,
    observation_age_at_connect_ms: Option<u64>,
    payload_bytes: usize,
    selected_path: Option<&'static str>,
    failure_reason: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(
        !args.run_id.is_empty() && args.run_id.len() <= 128,
        "invalid run id"
    );
    anyhow::ensure!(
        args.candidate_out != args.peer_file,
        "candidate and peer files must differ"
    );
    for path in [&args.candidate_out, &args.peer_file, &args.results] {
        anyhow::ensure!(
            !path.exists(),
            "experiment file already exists; use a fresh run directory"
        );
    }
    let mut report = Report {
        schema: "nat-peer-v1",
        run_id: args.run_id.clone(),
        method: Some(args.method),
        role: Some(args.role),
        diagnostic_private_peers: args.allow_private_peer,
        ..Default::default()
    };
    let result = run(&args, &mut report).await;
    if let Err(error) = &result {
        report.failure_reason = Some(format!("{error:#}"));
    }
    write_json(&args.results, &report).await?;
    if result.is_ok() {
        println!(
            "Direct echo verified ({} bytes); inspect results for topology.",
            report.payload_bytes
        );
    }
    result
}

async fn write_json(path: &std::path::Path, data: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(data)?;
    tokio::fs::write(&temporary, bytes).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
    }
    tokio::fs::rename(temporary, path).await?;
    Ok(())
}

async fn run(args: &Args, report: &mut Report) -> Result<()> {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr(args.bind)?
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    let result = run_endpoint(args, &endpoint, report).await;
    endpoint.close().await;
    result
}

async fn run_endpoint(args: &Args, endpoint: &Endpoint, report: &mut Report) -> Result<()> {
    anyhow::ensure!(
        endpoint.addr().addrs.iter().all(|a| a.is_ip()),
        "relay address on experiment endpoint"
    );
    let observed = discover(args, endpoint).await?;
    let observed_at = Instant::now();
    report.candidate_registered = true;
    report.same_socket_as_iroh = true;
    let local = Candidate {
        schema: 1,
        run_id: args.run_id.clone(),
        method: args.method,
        role: args.role,
        endpoint_id: endpoint.id(),
        addr: observed,
    };
    write_json(&args.candidate_out, &local).await?;
    report.local_observation = Some(local);
    println!("READY (candidate file written; endpoint stays open)");

    let peer = wait_peer(args).await?;
    report.observation_age_at_connect_ms = Some(observed_at.elapsed().as_millis() as u64);
    anyhow::ensure!(
        observed_at.elapsed() <= Duration::from_secs(args.candidate_max_age_secs),
        "observation too old; restart both peers with a new run id and exchange fresh candidates"
    );
    validate_peer(args, endpoint.id(), observed, &peer)?;
    report.different_observed_ips = Some(peer.addr.ip() != observed.ip());
    report.ipv4_pair = Some(observed.is_ipv4());
    let target = EndpointAddr::new(peer.endpoint_id).with_ip_addr(peer.addr);
    report.peer_observation = Some(peer);
    let started = Instant::now();
    report.direct_connection_success = Some(false);
    let conn = connect_peer(args, endpoint, target).await?;
    anyhow::ensure!(
        conn.remote_id() == report.peer_observation.as_ref().unwrap().endpoint_id,
        "unexpected authenticated peer"
    );
    check_direct(&conn)?;
    report.direct_connection_success = Some(true);
    report.selected_path = Some("direct-ip");
    report.time_to_connect_ms = Some(started.elapsed().as_millis() as u64);
    report.echo_success = Some(false);
    echo(&conn, args.role).await?;
    report.echo_success = Some(true);
    report.payload_bytes = PAYLOAD_BYTES;
    Ok(())
}

async fn discover(args: &Args, endpoint: &Endpoint) -> Result<SocketAddr> {
    Ok(match args.method {
        Method::Stun => {
            cloudflare_stun::probe::discover_server(
                endpoint,
                &args.stun_server,
                &Default::default(),
            )
            .await
            .context("STUN discovery")?
            .observed_addr
        }
        Method::H3 => {
            let host = args
                .h3_host
                .as_deref()
                .context("--h3-host is required for h3")?;
            let timeout = Duration::from_secs(10);
            let obs = match args.resolve {
                Some(ip) => {
                    cloudflare_h3_observer::observe_on_endpoint_at(
                        endpoint,
                        SocketAddr::new(ip, 443),
                        host,
                        "/observe",
                        timeout,
                    )
                    .await
                }
                None => {
                    cloudflare_h3_observer::observe_on_endpoint(
                        endpoint, host, "/observe", 443, timeout,
                    )
                    .await
                }
            }
            .context("H3 discovery")?;
            anyhow::ensure!(
                obs.candidate_registered && obs.same_socket_as_iroh,
                "H3 did not register a complete same-socket observation"
            );
            SocketAddr::new(
                obs.observed_ip.context("missing H3 IP")?,
                obs.observed_port.context("missing H3 port")?,
            )
        }
    })
}

async fn wait_peer(args: &Args) -> Result<Candidate> {
    tokio::time::timeout(Duration::from_secs(args.setup_timeout_secs), async {
        loop {
            match tokio::fs::read(&args.peer_file).await {
                Ok(bytes) => {
                    anyhow::ensure!(bytes.len() <= 8192, "peer candidate file too large");
                    return serde_json::from_slice::<Candidate>(&bytes)
                        .context("decode peer candidate; copy the file atomically");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("timed out waiting for peer file")?
}

fn validate_peer(
    args: &Args,
    local_id: EndpointId,
    observed: SocketAddr,
    peer: &Candidate,
) -> Result<()> {
    anyhow::ensure!(
        peer.schema == 1 && peer.run_id == args.run_id && peer.method == args.method,
        "peer file belongs to a different experiment"
    );
    anyhow::ensure!(
        peer.role != args.role && peer.endpoint_id != local_id,
        "invalid peer role/identity"
    );
    anyhow::ensure!(
        peer.addr.is_ipv4() == args.bind.is_ipv4(),
        "peer uses a different IP family"
    );
    anyhow::ensure!(
        peer.addr.port() != 0 && !peer.addr.ip().is_unspecified() && !peer.addr.ip().is_multicast(),
        "invalid peer address"
    );
    anyhow::ensure!(
        args.allow_private_peer || (public_ip(peer.addr.ip()) && public_ip(observed.ip())),
        "private/overlay address rejected; --allow-private-peer is only for local diagnostics"
    );
    Ok(())
}

async fn connect_peer(
    args: &Args,
    endpoint: &Endpoint,
    target: EndpointAddr,
) -> Result<Connection> {
    tokio::time::timeout(Duration::from_secs(args.connect_timeout_secs), async {
        match args.role {
            Role::Dial => endpoint.connect(target, ALPN).await.context("dial peer"),
            Role::Listen => {
                // Open the reverse NAT filter with authenticated QUIC traffic
                // toward the sole peer candidate while accepting its dial.
                // The dialer does not accept this auxiliary connection.
                let poke = tokio::spawn({
                    let endpoint = endpoint.clone();
                    async move {
                        let _connection = endpoint.connect(target, ALPN).await;
                        std::future::pending::<()>().await;
                    }
                });
                let _poke = AbortTask(poke);
                endpoint
                    .accept()
                    .await
                    .context("endpoint closed")?
                    .accept()
                    .context("accept incoming")?
                    .await
                    .context("peer handshake")
            }
        }
    })
    .await
    .context("direct connection timeout")?
}

async fn echo(conn: &Connection, role: Role) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let expected: Vec<u8> = (0..PAYLOAD_BYTES).map(|i| (i % 251) as u8).collect();
        match role {
            Role::Dial => {
                let (mut send, mut recv) = conn.open_bi().await?;
                send.write_all(&expected).await?;
                send.finish()?;
                let echoed = recv.read_to_end(PAYLOAD_BYTES).await?;
                anyhow::ensure!(echoed == expected, "echo payload mismatch");
            }
            Role::Listen => {
                let (mut send, mut recv) = conn.accept_bi().await?;
                let received = recv.read_to_end(PAYLOAD_BYTES).await?;
                anyhow::ensure!(received == expected, "request payload mismatch");
                send.write_all(&received).await?;
                send.finish()?;
                let stopped = send.stopped().await.context("await echo acknowledgement")?;
                anyhow::ensure!(stopped.is_none(), "peer rejected the echo stream");
            }
        }
        anyhow::Ok(())
    })
    .await
    .context("echo timeout")?
}

fn check_direct(conn: &Connection) -> Result<()> {
    let paths = conn.paths();
    anyhow::ensure!(paths.iter().all(|p| !p.is_relay()), "relay path observed");
    anyhow::ensure!(
        paths.iter().any(|p| p.is_selected() && !p.is_relay()),
        "no selected direct path"
    );
    Ok(())
}

fn public_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let reserved = matches!(
                ip.octets(),
                [0, _, _, _]
                    | [224..=255, _, _, _]
                    | [198, 18..=19, _, _]
                    | [100, 64..=127, _, _]
                    | [192, 0, 0, _]
            );
            let local_or_documentation = [
                ip.is_private(),
                ip.is_loopback(),
                ip.is_link_local(),
                ip.is_documentation(),
            ]
            .contains(&true);
            !reserved && !local_or_documentation
        }
        IpAddr::V6(ip) => {
            // Only global unicast 2000::/3, excluding the documentation prefix.
            // The canonicalization above also handles IPv4-mapped overlay IPs.
            let segments = ip.segments();
            let global_unicast = (segments[0] & 0xe000) == 0x2000;
            let documentation = segments[0] == 0x2001 && segments[1] == 0x0db8;
            global_unicast && !documentation
        }
    }
}

struct AbortTask(tokio::task::JoinHandle<()>);
impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::public_ip;

    #[test]
    fn overlay_and_local_addresses_cannot_count_as_public_candidates() {
        for ip in [
            "100.64.0.1",
            "100.127.255.254",
            "::ffff:100.64.1.1",
            "fd7a:115c:a1e0::1",
            "127.0.0.1",
            "10.1.2.3",
            "192.168.1.2",
            "169.254.1.1",
            "198.18.0.1",
            "240.1.2.3",
            "::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "accepted {ip}");
        }
        assert!(public_ip("162.159.207.0".parse().unwrap()));
        assert!(public_ip("2606:4700:49::".parse().unwrap()));
    }
}
