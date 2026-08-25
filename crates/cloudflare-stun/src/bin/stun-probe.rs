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
    /// STUN servers to try, in order. Defaults to the plan E2 list.
    #[arg(long = "server", default_value = "stun.cloudflare.com:3478")]
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

/// Probe one server from a dedicated ephemeral socket.
fn probe_server(runtime: &tokio::runtime::Runtime, args: &Args, server: &str) -> Result<String> {
    runtime.block_on(async {
        let std_sock = UdpSocket::bind("[::]:0")
            .or_else(|_| UdpSocket::bind("0.0.0.0:0"))
            .context("binding probe socket")?;
        let socket = tokio::net::UdpSocket::from_std(std_sock).context("async socket")?;

        let addrs = probe::resolve(server).context("resolving server")?;
        anyhow::ensure!(!addrs.is_empty(), "no addresses for {server}");

        let config = ProbeConfig {
            attempt_timeout: std::time::Duration::from_millis(args.attempt_timeout_ms),
            attempts: args.attempts,
        };
        // A v4-bound socket cannot send to v6 targets and vice versa.
        let local_is_v4 = socket.local_addr().map(|l| l.is_ipv4()).unwrap_or(true);

        let mut last_err = None;
        for addr in addrs {
            if local_is_v4 != addr.is_ipv4() {
                continue;
            }
            match probe::probe(&socket, addr, &config).await {
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
