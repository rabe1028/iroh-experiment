//! STUN probe CLI: external-address discovery via Cloudflare STUN.
//!
//! Tries each `--server` in order (default is the plan E2 list:
//! `stun.cloudflare.com:3478`, then `:53`) and prints one JSON observation
//! line per successful probe.
//!
//! ## Caveat (plan sections 3.2 / 20.1)
//!
//! This runs on a socket separate from iroh's. On destination-dependent NATs
//! the observed mapping may differ from what an iroh connection would get;
//! treat single-method results accordingly until same-socket integration
//! lands (section 20.3). Output lines carry `"same_socket_as_iroh": false`
//! so aggregation can filter them out of published artifacts, which must not
//! contain public IPs anyway (section 18).

use std::net::UdpSocket;

use anyhow::{Context, Result};
use clap::Parser;
use cloudflare_stun::probe::{self, ProbeConfig};

#[derive(Parser)]
struct Args {
    /// STUN servers to try, in order. Defaults to the plan E2 list:
    /// UDP 3478 first, then UDP 53 as the fallback for N7.
    #[arg(
        long = "server",
        default_values_t = [
            "stun.cloudflare.com:3478".to_string(),
            "stun.cloudflare.com:53".to_string()
        ]
    )]
    servers: Vec<String>,
    /// Milliseconds to wait per attempt.
    #[arg(long, default_value_t = 2000)]
    attempt_timeout_ms: u64,
    /// Attempts per server.
    #[arg(long, default_value_t = 3)]
    attempts: u32,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let runtime = tokio::runtime::Runtime::new()?;

    let mut any_ok = false;
    let mut failures = Vec::new();

    for server in &args.servers {
        match probe_server(&runtime, &args, server) {
            Ok(json) => {
                println!("STUN_OK server={server} {json}");
                any_ok = true;
            }
            Err(e) => {
                tracing::warn!(server = %server, error = %e, "probe failed");
                failures.push(format!("{server}: {e:#}"));
            }
        }
    }

    if !any_ok {
        anyhow::bail!("all probes failed; {}", failures.join("; "));
    }
    Ok(())
}

/// Bind a UDP socket restricted to one family; a v4 socket cannot send to
/// v6 targets and vice versa.
fn bind_family(is_v4: bool) -> Result<tokio::net::UdpSocket> {
    let std_sock = if is_v4 {
        UdpSocket::bind("0.0.0.0:0")
    } else {
        UdpSocket::bind("[::]:0")
    }
    .context("binding probe socket")?;
    tokio::net::UdpSocket::from_std(std_sock).context("async socket")
}

/// Probe one server from a dedicated ephemeral socket.
fn probe_server(runtime: &tokio::runtime::Runtime, args: &Args, server: &str) -> Result<String> {
    runtime.block_on(async {
        let addrs = probe::resolve(server).context("resolving server")?;
        anyhow::ensure!(!addrs.is_empty(), "no addresses for {server}");

        let config = ProbeConfig {
            attempt_timeout: std::time::Duration::from_millis(args.attempt_timeout_ms),
            attempts: args.attempts,
        };

        // Keep one socket per family, created when the candidate family
        // changes. Binding [::]:0 succeeds even on hosts without an IPv6
        // route, so filtering candidates by the first socket's family would
        // skip every IPv4 address there and then fail all IPv6 probes for
        // lack of a route; instead each family gets its own socket and the
        // v4 candidates are still tried after IPv6 proves unusable.
        let mut cur: Option<(bool, tokio::net::UdpSocket)> = None;
        let mut last_err = None;
        for addr in addrs {
            if cur
                .as_ref()
                .is_none_or(|(is_v4, _)| *is_v4 != addr.is_ipv4())
            {
                match bind_family(addr.is_ipv4()) {
                    Ok(s) => cur = Some((addr.is_ipv4(), s)),
                    Err(e) => {
                        tracing::warn!(
                            family = if addr.is_ipv4() { "v4" } else { "v6" },
                            error = %e,
                            "binding probe socket failed"
                        );
                        last_err = Some(e);
                        continue;
                    }
                }
            }
            let socket = &cur.as_ref().expect("socket was just bound").1;
            match probe::probe(socket, addr, &config).await {
                Ok(obs) => {
                    return Ok(serde_json::json!({
                        "method": obs.method,
                        "server": obs.server.to_string(),
                        "observed_addr": obs.observed_addr.to_string(),
                        "rtt_ms": obs.rtt.as_millis() as u64,
                        "observed_at_unix_ms": cloudflare_stun::unix_ms_now(),
                        "same_socket_as_iroh": false,
                    })
                    .to_string());
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no usable address family")))
    })
}
