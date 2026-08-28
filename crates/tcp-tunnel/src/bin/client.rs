//! Raw TCP tunnel client: Endpoint B (remote client role).
//!
//! Listens on a local TCP port. Every accepted local connection is forwarded
//! through one iroh bidirectional stream to the gateway, which routes it to
//! the LAN service selected by `--service` (plan E6 / PR 2).
//!
//! Existing TCP clients just connect to the local port:
//!
//! ```text
//! curl http://127.0.0.1:18080/
//!     │ local TCP listener (this binary)
//!     │ iroh bidirectional stream
//!     ▼
//! gateway -> LAN service
//! ```

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use tcp_tunnel::{drive_client, StreamPair, TUNNEL_ALPN};

#[derive(Parser)]
struct Args {
    /// Gateway EndpointId (hex).
    id: String,
    /// Local address to listen on.
    #[arg(long, default_value = "127.0.0.1:18080")]
    listen: String,
    /// Service id requested for every tunnelled connection.
    #[arg(long)]
    service: String,
    /// File holding the client's 32-byte secret key, created with a fresh
    /// key on first use (0600). Without it every launch mints a new
    /// EndpointId, so a gateway using --allow-endpoint drops the restarted
    /// client until its allowlist is updated.
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

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(&args))
}

/// Shared handle holding the live gateway connection; redials when lost so a
/// gateway restart does not require restarting the client.
type SharedConn = Arc<Mutex<Option<iroh::endpoint::Connection>>>;

async fn run(args: &Args) -> Result<()> {
    // Fail before binding: an ID the wire protocol or the gateway's service
    // map can never accept would otherwise advertise a listener that routes
    // nothing.
    anyhow::ensure!(!args.service.is_empty(), "--service must not be empty");
    anyhow::ensure!(
        args.service.len() <= tcp_tunnel::MAX_SERVICE_ID_LEN as usize,
        "--service exceeds {} bytes",
        tcp_tunnel::MAX_SERVICE_ID_LEN
    );
    let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0);
    if let Some(path) = &args.key_file {
        builder = builder.secret_key(load_or_create_key(path)?);
    }
    let endpoint = builder
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    let target: iroh::EndpointId = args.id.parse().context("invalid EndpointId")?;
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .context(format!("bind {}", args.listen))?;

    let conn: SharedConn = Arc::new(Mutex::new(None));
    println!("LISTENING={}", args.listen);
    println!("GATEWAY={target}");
    println!("SERVICE={}", args.service);
    // Print the client's own ID: with a fresh --key-file the operator needs
    // it to add this client to the gateway's --allow-endpoint list.
    println!("ENDPOINT_ID={}", endpoint.id());

    loop {
        let (local, peer) = listener.accept().await?;
        tracing::debug!(%peer, "local connection accepted");
        let conn = Arc::clone(&conn);
        let endpoint = endpoint.clone();
        let service = args.service.clone();
        tokio::spawn(async move {
            if let Err(e) = tunnel_one(&endpoint, target, &conn, local, &service).await {
                let detail = format!("{e:#}");
                tracing::warn!(error = detail, %peer, "tunnel failed");
            }
        });
    }
}

/// Forward one local TCP connection over a (possibly new) gateway stream.
async fn tunnel_one(
    endpoint: &iroh::Endpoint,
    target: iroh::EndpointId,
    conn: &SharedConn,
    mut local: tokio::net::TcpStream,
    service_id: &str,
) -> Result<()> {
    let gateway = get_or_dial(endpoint, target, conn).await?;
    let (send, recv) = gateway.open_bi().await.context("open_bi failed")?;
    let mut pair = StreamPair::new(send, recv);

    match drive_client(&mut pair, &mut local, service_id).await {
        Ok(counts) => {
            println!(
                "TUNNEL_CLOSED service={service_id} UP_BYTES={} DOWN_BYTES={}",
                counts.to_gateway, counts.from_gateway
            )
        }
        Err(e) => {
            // A dead cached connection is recycled by the next caller —
            // but only if the cache still holds that dead connection:
            // another task may have already redialled and cached a
            // healthy replacement, which must not be discarded.
            if is_connection_lost(&e) {
                let mut guard = conn.lock().unwrap();
                if guard.as_ref().is_some_and(|c| c.close_reason().is_some()) {
                    *guard = None;
                }
            }
            return Err(e);
        }
    }
    Ok(())
}

fn is_connection_lost(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|c| c.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::NotConnected
            )
        })
        || e.to_string().contains("connection lost")
}

/// Return the cached live connection or dial a new one.
///
/// Dials happen without holding the lock (the mutex guard must never be held
/// across an await), so concurrent local connections may race to redial;
/// whichever lands first wins the cache slot and both are valid.
async fn get_or_dial(
    endpoint: &iroh::Endpoint,
    target: iroh::EndpointId,
    conn: &SharedConn,
) -> Result<iroh::endpoint::Connection> {
    if let Some(c) = conn.lock().unwrap().as_ref() {
        if c.close_reason().is_none() {
            return Ok(c.clone());
        }
    }

    let c = endpoint
        .connect(target, TUNNEL_ALPN)
        .await
        .context("connect failed")?;

    let mut guard = conn.lock().unwrap();
    match guard.as_ref() {
        Some(existing) if existing.close_reason().is_none() => Ok(existing.clone()),
        _ => {
            *guard = Some(c.clone());
            Ok(c)
        }
    }
}


/// Load the client's secret key from `path`, creating it owner-only on
/// first use, so the EndpointId (and the gateway's --allow-endpoint entry)
/// survives client restarts.
fn load_or_create_key(path: &std::path::Path) -> Result<iroh::SecretKey> {
    match std::fs::read(path) {
        Ok(bytes) => {
            restrict_key_permissions(path)?;
            iroh::SecretKey::try_from(bytes.as_slice()).context("parse client key file")
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let key = iroh::SecretKey::generate();
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).context("create key file parent")?;
                }
            }
            #[cfg(unix)]
            {
                use std::io::Write;
                use std::os::unix::fs::OpenOptionsExt;
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .context("create client key file")?;
                f.write_all(&key.to_bytes()).context("write client key file")?;
            }
            #[cfg(not(unix))]
            std::fs::write(path, key.to_bytes()).context("write client key file")?;
            Ok(key)
        }
        Err(e) => Err(e).context("read client key file"),
    }
}

/// Correct an existing key file that is readable beyond the owner.
#[cfg(unix)]
fn restrict_key_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::metadata(path).context("stat client key file")?.permissions();
    if perms.mode() & 0o077 != 0 {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .context("restrict client key file permissions")?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_key_permissions(_path: &std::path::Path) -> Result<()> {
    Ok(())
}
