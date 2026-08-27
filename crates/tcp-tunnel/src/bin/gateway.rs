//! Raw TCP tunnel gateway: Endpoint A (LAN gateway role).
//!
//! Accepts iroh connections on the tunnel ALPN and routes each bidirectional
//! stream to the LAN service named in its handshake. Only services passed via
//! `--service name=host:port` are routable; everything else is rejected, so
//! the gateway is not an open proxy (plan E6 / PR 2).

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tcp_tunnel::{ServiceMap, StreamPair, TUNNEL_ALPN};

#[derive(Parser)]
struct Args {
    /// Routable service, as `name=host:port`. Repeat for multiple services.
    #[arg(long = "service")]
    services: Vec<String>,
    /// Access rule for a remote endpoint, as `ID` (all configured services)
    /// or `ID=SERVICE` (that service only). Repeat for several rules; the
    /// plan section 19 authorization boundary is EndpointId x ServiceId.
    /// Omit to allow any endpoint (fine for local experiments only).
    #[arg(long = "allow-endpoint", value_name = "ID[=SERVICE]")]
    allow_rules: Vec<String>,
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

    let allow: Vec<AllowRule> = args
        .allow_rules
        .iter()
        .map(|s| parse_allow_rule(s))
        .collect::<Result<_>>()?;
    if allow.is_empty() {
        println!("AUTHORIZATION=any-endpoint (no --allow-endpoint configured)");
    } else {
        for rule in &allow {
            match &rule.service {
                Some(service) => println!("AUTHORIZATION={}={service}", rule.endpoint),
                None => println!("AUTHORIZATION={}", rule.endpoint),
            }
        }
    }

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(&services, &allow, args.key_file.as_deref()))
}

/// One access rule parsed from `ID` or `ID=SERVICE`.
#[derive(Clone)]
struct AllowRule {
    endpoint: iroh::EndpointId,
    /// None = every configured service.
    service: Option<String>,
}

fn parse_allow_rule(raw: &str) -> Result<AllowRule> {
    if let Some((id, service)) = raw.split_once('=') {
        Ok(AllowRule {
            endpoint: id.parse().with_context(|| format!("invalid EndpointId in {raw:?}"))?,
            service: Some(service.to_string()),
        })
    } else {
        Ok(AllowRule {
            endpoint: raw.parse().with_context(|| format!("invalid EndpointId in {raw:?}"))?,
            service: None,
        })
    }
}

/// Load the gateway's secret key from `path`, creating it (with parent
/// directories, mode 0600) with a fresh key on first use. A persistent key
/// keeps the EndpointId stable across gateway restarts so the clients'
/// configured target stays valid.
fn load_or_create_key(path: &std::path::Path) -> Result<iroh::SecretKey> {
    match std::fs::read(path) {
        Ok(bytes) => {
            restrict_key_permissions(path)?;
            iroh::SecretKey::try_from(bytes.as_slice()).context("parse gateway key file")
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = iroh::SecretKey::generate();
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).context("create key file parent")?;
                }
            }
            // Create owner-only up front: writing the raw key first and
            // restricting later would leave a window where a 0644 file
            // holds the private key.
            #[cfg(unix)]
            {
                use std::io::Write;
                use std::os::unix::fs::OpenOptionsExt;
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .context("create gateway key file")?;
                f.write_all(&key.to_bytes()).context("write gateway key file")?;
            }
            #[cfg(not(unix))]
            std::fs::write(path, key.to_bytes()).context("write gateway key file")?;
            Ok(key)
        }
        Err(e) => Err(e).context("read gateway key file"),
    }
}

/// Correct an existing key file that is readable beyond the owner.
#[cfg(unix)]
fn restrict_key_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::metadata(path).context("stat gateway key file")?.permissions();
    if perms.mode() & 0o077 != 0 {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .context("restrict gateway key file permissions")?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_key_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

async fn run(
    services: &ServiceMap,
    allow: &[AllowRule],
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

    // Each connection completes its handshake concurrently and bounded, so
    // one peer that stops mid-handshake cannot starve later clients.
    const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    let services = Arc::new(services.clone());
    let allow = Arc::new(allow.to_vec());
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connecting) = incoming.accept() else {
            tracing::warn!("incoming rejected");
            continue;
        };
        let services = Arc::clone(&services);
        let allow = Arc::clone(&allow);
        tokio::spawn(async move {
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
                Ok(Ok(conn)) => handle_connection(conn, &services, &allow),
                Ok(Err(e)) => tracing::warn!(error = %e, "handshake failed"),
                Err(_) => tracing::warn!("handshake timed out"),
            }
        });
    }
    Ok(())
}

/// Authorize one connection and serve it with its scoped service map.
fn handle_connection(conn: iroh::endpoint::Connection, services: &ServiceMap, allow: &[AllowRule]) {
    let scoped = connection_scope(&conn, allow, services);
    let Some(scoped) = scoped else {
        // Read the request just far enough to answer with the protocol's
        // Unauthorized status instead of a bare close, then drop the
        // connection. Bounded: a peer that completes the handshake but never
        // opens a stream (or sends a partial request) must not park this
        // task and connection forever.
        const REJECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        tokio::spawn(async move {
            let attempt = async {
                let (send, recv) = conn.accept_bi().await?;
                let mut pair = StreamPair::new(send, recv);
                tcp_tunnel::read_request(&mut pair).await?;
                Ok::<_, anyhow::Error>(pair)
            };
            match tokio::time::timeout(REJECT_TIMEOUT, attempt).await {
                Ok(Ok(mut pair)) => {
                    let _ = tcp_tunnel::send_terminal_status(
                        &mut pair,
                        tcp_tunnel::TunnelStatus::Unauthorized,
                    )
                    .await;
                }
                _ => {
                    // Timeout, refusal, or malformed request: close instead
                    // of lingering.
                    conn.close(1u32.into(), b"unauthorized");
                }
            }
        });
        return;
    };
    tokio::spawn(serve_connection(conn, scoped));
}

/// Services the connecting endpoint may request (plan section 19:
/// EndpointId x ServiceId). `None` rejects the connection; an empty map
/// accepts the connection but no service.
fn connection_scope(
    conn: &iroh::endpoint::Connection,
    allow: &[AllowRule],
    services: &ServiceMap,
) -> Option<ServiceMap> {
    let remote = conn.remote_id();
    if allow.is_empty() {
        return Some(services.clone());
    }
    let mut scoped = ServiceMap::default();
    let mut matched = false;
    for rule in allow {
        if rule.endpoint != remote {
            continue;
        }
        matched = true;
        match &rule.service {
            None => return Some(services.clone()),
            Some(name) => {
                if let Some(addr) = services.get(name) {
                    scoped.insert_spec(&format!("{name}={addr}")).ok()?;
                }
            }
        }
    }
    if matched {
        Some(scoped)
    } else {
        tracing::warn!(endpoint = %remote, "unauthorized endpoint rejected");
        None
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
