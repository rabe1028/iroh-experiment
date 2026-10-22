//! STUN observations on the existing iroh UDP socket (standalone opt-out).

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use cloudflare_stun::probe::{self, Observation, ProbeConfig};
use iroh::{endpoint::presets, Endpoint};

#[derive(Parser)]
struct Args {
    /// STUN servers to observe, in order; all use the same bound socket.
    #[arg(long = "server", default_values_t = [
        "stun.cloudflare.com:3478".to_string(), "stun.cloudflare.com:53".to_string()
    ])]
    servers: Vec<String>,
    /// Bind exactly one iroh IP socket, making socket identity unambiguous.
    #[arg(long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
    /// Use a standalone UDP socket instead of the iroh transport.
    #[arg(long)]
    standalone: bool,
    #[arg(long, default_value_t = 2000)]
    attempt_timeout_ms: u64,
    #[arg(long, default_value_t = 3)]
    attempts: u32,
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
    let config = ProbeConfig {
        attempt_timeout: Duration::from_millis(args.attempt_timeout_ms),
        attempts: args.attempts,
    };
    let endpoint = if args.standalone {
        None
    } else {
        Some(
            Endpoint::builder(presets::Minimal)
                .clear_ip_transports()
                .bind_addr(args.bind)?
                .bind()
                .await
                .context("bind iroh endpoint")?,
        )
    };
    let standalone = if args.standalone {
        Some(
            tokio::net::UdpSocket::bind(args.bind)
                .await
                .context("bind standalone socket")?,
        )
    } else {
        None
    };

    let mut successes = 0;
    let mut failures = Vec::new();
    for server in &args.servers {
        let outcome = match &endpoint {
            Some(endpoint) => probe::discover_server(endpoint, server, &config).await,
            None => standalone_probe(standalone.as_ref().unwrap(), server, &config).await,
        };
        match outcome {
            Ok(obs) => {
                println!(
                    "{}",
                    serde_json::json!({
                        "status": "ok",
                        "requested_server": server,
                        "method": obs.method,
                        "server": obs.server,
                        "observed_addr": obs.observed_addr,
                        "local_addr": obs.local_addr,
                        "rtt_ms": obs.rtt.as_millis() as u64,
                        "observed_at_unix_ms": obs.observed_at.duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64,
                        "same_socket_as_iroh": obs.same_socket_as_iroh,
                        "candidate_registered": obs.same_socket_as_iroh,
                    })
                );
                successes += 1;
            }
            Err(error) => {
                tracing::warn!(%server, %error, "probe failed");
                failures.push(format!("{server}: {error:#}"));
            }
        }
    }
    if let Some(endpoint) = endpoint {
        endpoint.close().await;
    }
    anyhow::ensure!(successes > 0, "all probes failed; {}", failures.join("; "));
    Ok(())
}

async fn standalone_probe(
    socket: &tokio::net::UdpSocket,
    server: &str,
    config: &ProbeConfig,
) -> Result<Observation> {
    let mut error = anyhow::anyhow!("no matching address family for {server}");
    let ipv4 = socket.local_addr()?.is_ipv4();
    for addr in tokio::net::lookup_host(server)
        .await
        .context("resolve STUN server")?
    {
        if addr.is_ipv4() != ipv4 {
            continue;
        }
        match probe::probe(socket, addr, config).await {
            Ok(observation) => return Ok(observation),
            Err(e) => error = e,
        }
    }
    Err(error)
}
