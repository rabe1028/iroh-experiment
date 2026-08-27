//! Raw TCP tunnel gateway: Endpoint A (LAN gateway role).
//!
//! Accepts iroh connections on the tunnel ALPN and routes each bidirectional
//! stream to the LAN service named in its handshake. Only services passed via
//! `--service name=host:port` are routable; everything else is rejected, so
//! the gateway is not an open proxy (plan E6 / PR 2).

use anyhow::{Context, Result};
use clap::Parser;
use tcp_tunnel::{ServiceMap, StreamPair, TUNNEL_ALPN};

#[derive(Parser)]
struct Args {
    /// Routable service, as `name=host:port`. Repeat for multiple services.
    #[arg(long = "service")]
    services: Vec<String>,
    /// EndpointId allowed to open tunnels (hex). Repeat for several peers.
    /// Omit to allow any endpoint (fine for local experiments only).
    #[arg(long = "allow-endpoint")]
    allow_endpoints: Vec<String>,
    /// File holding the gateway's 32-byte secret key, created with a fresh
    /// key on first use (0600). Without it every restart mints a new
    /// EndpointId while clients keep dialing the old one, so the advertised
    /// automatic redial can never recover from a gateway restart.
    #[arg(long)]
    key_file: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let services = ServiceMap::from_specs(&args.services)?;
    anyhow::ensure!(!services.is_empty(), "at least one --service is required");
    for spec in &args.services {
        println!("SERVICE={spec}");
    }

    let allow: Vec<iroh::EndpointId> = args
        .allow_endpoints
        .iter()
        .map(|s| s.parse().context("invalid EndpointId in --allow-endpoint"))
        .collect::<Result<_>>()?;
    if allow.is_empty() {
        println!("AUTHORIZATION=any-endpoint (no --allow-endpoint configured)");
    }

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(&services, &allow, args.key_file.as_deref()))
}

/// Load the gateway's secret key from `path`, creating it (with parent
/// directories, mode 0600) with a fresh key on first use. A persistent key
/// keeps the EndpointId stable across gateway restarts so the clients'
/// configured target stays valid.
fn load_or_create_key(path: &std::path::Path) -> Result<iroh::SecretKey> {
    match std::fs::read(path) {
        Ok(bytes) => {
            iroh::SecretKey::try_from(bytes.as_slice()).context("parse gateway key file")
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = iroh::SecretKey::generate();
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).context("create key file parent")?;
                }
            }
            std::fs::write(path, key.to_bytes()).context("write gateway key file")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                    .context("restrict gateway key file permissions")?;
            }
            Ok(key)
        }
        Err(e) => Err(e).context("read gateway key file"),
    }
}

async fn run(
    services: &ServiceMap,
    allow: &[iroh::EndpointId],
    key_file: Option<&std::path::Path>,
) -> Result<()> {
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0);
    if let Some(path) = key_file {
        builder = builder.secret_key(load_or_create_key(path)?);
    }
    let endpoint = builder
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    endpoint.set_alpns(vec![TUNNEL_ALPN.to_vec()]);
    println!("ENDPOINT_ID={}", endpoint.id());
    for addr in endpoint.addr().addrs {
        println!("ADDR={addr}");
    }

    while let Some(incoming) = endpoint.accept().await {
        match incoming.accept() {
            Ok(connecting) => match connecting.await {
                Ok(conn) => {
                    if !authorized(&conn, allow) {
                        continue;
                    }
                    tokio::spawn(serve_connection(conn, services.clone()));
                }
                Err(e) => tracing::warn!(error = %e, "handshake failed"),
            },
            Err(e) => tracing::warn!(error = %e, "incoming rejected"),
        }
    }
    Ok(())
}

/// Reject connections from endpoints outside the allowlist.
fn authorized(conn: &iroh::endpoint::Connection, allow: &[iroh::EndpointId]) -> bool {
    let remote = conn.remote_id();
    if allow.is_empty() || allow.contains(&remote) {
        true
    } else {
        tracing::warn!(endpoint = %remote, "unauthorized endpoint rejected");
        false
    }
}

/// Serve every stream of one connection until it closes.
///
/// One tunnelled TCP connection == one bidirectional stream, so a single
/// connection multiplexes any number of tunnels.
async fn serve_connection(conn: iroh::endpoint::Connection, services: ServiceMap) {
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let services = services.clone();
                tokio::spawn(async move {
                    let mut pair = StreamPair::new(send, recv);
                    match tcp_tunnel::serve_stream(&mut pair, &services).await {
                        Ok(tcp_tunnel::TunnelStatus::Ok) => {}
                        Ok(status) => {
                            tracing::warn!(status = status.message(), "tunnel rejected")
                        }
                        Err(e) => {
                            let detail = format!("{e:#}");
                            tracing::warn!(error = detail, "tunnel failed")
                        }
                    }
                });
            }
            Err(e) => {
                // ConnectionClosed / Reset are normal shutdown paths.
                tracing::debug!(error = %e, "connection closed");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_file_reuse_keeps_the_same_key() {
        let dir = std::env::temp_dir().join(format!("gwkey-test-{}", std::process::id()));
        let path = dir.join("key");
        let _ = std::fs::remove_dir_all(&dir);
        let first = load_or_create_key(&path).expect("create key");
        let second = load_or_create_key(&path).expect("reload key");
        assert_eq!(
            first.to_bytes(),
            second.to_bytes(),
            "restart must reuse the persisted key so the EndpointId stays stable"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "key file must be owner-only");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
