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
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
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
        Ok((up, down)) => {
            println!("TUNNEL_CLOSED service={service_id} UP_BYTES={up} DOWN_BYTES={down}")
        }
        Err(e) => {
            // A dead cached connection is recycled by the next caller.
            if is_connection_lost(&e) {
                conn.lock().unwrap().take();
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
