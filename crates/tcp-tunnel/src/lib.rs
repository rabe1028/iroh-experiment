//! Raw TCP-over-iroh tunnel (plan E6 / PR 2).
//!
//! Maps one local TCP connection onto one iroh bidirectional stream, so
//! existing TCP clients (curl, reqwest, browsers, ...) reach LAN services
//! through an iroh connection without any modification.
//!
//! Wire protocol on each stream:
//!
//! ```text
//! client -> gateway: u8 version(=0), u16 BE len(service_id), service_id
//! gateway -> client: u8 status
//! afterwards:        raw bidirectional bytes until both sides close
//! ```
//!
//! The gateway only forwards to services listed in its [`ServiceMap`], so it
//! is never an open proxy (plan section 19).

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use iroh::endpoint::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// ALPN used by the raw TCP tunnel protocol (PR 2).
pub const TUNNEL_ALPN: &[u8] = b"iroh-experiment/tcp-tunnel/0";

/// Current handshake protocol version.
pub const PROTOCOL_VERSION: u8 = 0;

/// Maximum accepted length of a service id.
const MAX_SERVICE_ID_LEN: u16 = 256;

/// Gateway response to a tunnel request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TunnelStatus {
    /// Tunnel established; raw bytes follow.
    Ok = 0,
    /// Requested service id is not in the gateway's allowlist.
    UnknownService = 1,
    /// Remote endpoint is not authorized to use this gateway.
    Unauthorized = 2,
    /// Service is allowlisted but its upstream TCP address is unreachable.
    UpstreamUnreachable = 3,
    /// Malformed handshake request.
    BadRequest = 4,
}

impl TunnelStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::UnknownService),
            2 => Some(Self::Unauthorized),
            3 => Some(Self::UpstreamUnreachable),
            4 => Some(Self::BadRequest),
            _ => None,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::UnknownService => "unknown service",
            Self::Unauthorized => "unauthorized endpoint",
            Self::UpstreamUnreachable => "upstream unreachable",
            Self::BadRequest => "bad request",
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake framing
// ---------------------------------------------------------------------------

/// Send a tunnel request for `service_id`.
pub async fn write_request<W: AsyncWrite + Unpin>(w: &mut W, service_id: &str) -> Result<()> {
    let bytes = service_id.as_bytes();
    let len = u16::try_from(bytes.len()).context("service id longer than u16::MAX")?;
    anyhow::ensure!(
        bytes.len() <= MAX_SERVICE_ID_LEN as usize,
        "service id exceeds {MAX_SERVICE_ID_LEN} bytes"
    );
    w.write_u8(PROTOCOL_VERSION).await?;
    w.write_u16(len).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}

/// Read a tunnel request, returning the requested service id.
pub async fn read_request<R: AsyncRead + Unpin>(r: &mut R) -> Result<String> {
    let version = r.read_u8().await.context("reading protocol version")?;
    if version != PROTOCOL_VERSION {
        anyhow::bail!("unsupported tunnel protocol version {version}");
    }
    let len = r.read_u16().await.context("reading service id length")?;
    anyhow::ensure!(
        len <= MAX_SERVICE_ID_LEN,
        "service id length {len} too large"
    );
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await.context("reading service id")?;
    String::from_utf8(buf).context("service id is not valid UTF-8")
}

/// Send a [`TunnelStatus`] reply.
pub async fn write_status<W: AsyncWrite + Unpin>(w: &mut W, status: TunnelStatus) -> Result<()> {
    w.write_u8(status as u8).await?;
    w.flush().await?;
    Ok(())
}

/// Read a [`TunnelStatus`] reply.
pub async fn read_status<R: AsyncRead + Unpin>(r: &mut R) -> Result<TunnelStatus> {
    let v = r.read_u8().await.context("reading tunnel status")?;
    TunnelStatus::from_u8(v).with_context(|| format!("unknown tunnel status code {v}"))
}

// ---------------------------------------------------------------------------
// Service routing table
// ---------------------------------------------------------------------------

/// Allowlist of routable services: `service id -> host:port` (plan section 19).
///
/// Anything not listed here is rejected by the gateway, so exposing the
/// gateway can never turn it into an open proxy.
#[derive(Debug, Clone, Default)]
pub struct ServiceMap(BTreeMap<String, Target>);

/// A validated upstream target address.
#[derive(Debug, Clone)]
struct Target(String);

impl ServiceMap {
    /// Build a map from `name=host:port` specs, rejecting malformed entries.
    pub fn from_specs<I, S>(specs: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut map = Self::default();
        for spec in specs {
            map.insert_spec(spec.as_ref())?;
        }
        Ok(map)
    }

    /// Insert one `name=host:port` spec.
    pub fn insert_spec(&mut self, spec: &str) -> Result<()> {
        let (name, addr) = spec.split_once('=').context(format!(
            "invalid --service {spec:?}: expected name=host:port"
        ))?;
        anyhow::ensure!(!name.is_empty(), "empty service name in {spec:?}");
        self.validate_upstream(addr)
            .context(format!("invalid upstream for service {name:?}"))?;
        self.0.insert(name.to_string(), Target(addr.to_string()));
        Ok(())
    }

    /// Reject obviously invalid upstreams early so typos fail at startup.
    fn validate_upstream(&self, addr: &str) -> Result<()> {
        let (_, port) = addr.rsplit_once(':').context("missing :port")?;
        port.parse::<u16>()
            .context(format!("port {port:?} is not a valid u16"))?;
        Ok(())
    }

    pub fn get(&self, service_id: &str) -> Option<&str> {
        self.0.get(service_id).map(|t| t.0.as_str())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Stream plumbing
// ---------------------------------------------------------------------------

/// Joins an iroh send/recv pair into one object implementing
/// `AsyncRead + AsyncWrite`, usable with helpers like
/// [`tokio::io::copy_bidirectional`].
///
/// Shutting down writes finishes the send side (QUIC FIN), so half-close
/// semantics propagate correctly in both directions.
pub struct StreamPair {
    send: SendStream,
    recv: RecvStream,
}

impl StreamPair {
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

impl AsyncRead for StreamPair {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for StreamPair {
    // Fully-qualified calls: noq's inherent `poll_write` shadows the trait
    // method but returns its own `WriteError` instead of `io::Error`.
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        <SendStream as AsyncWrite>::poll_write(std::pin::Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        <SendStream as AsyncWrite>::poll_flush(std::pin::Pin::new(&mut self.send), cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        <SendStream as AsyncWrite>::poll_shutdown(std::pin::Pin::new(&mut self.send), cx)
    }
}

/// Gateway-side handler for one tunnelled stream.
///
/// Reads the handshake, routes to the allowlisted upstream over TCP, then
/// pipes raw bytes both ways until both sides close. Returns the status sent
/// to the client so callers can log outcomes.
///
/// Generic over the stream type so tests can run it against in-memory
/// transports instead of live iroh connections.
pub async fn serve_stream<S>(stream: &mut S, services: &ServiceMap) -> Result<TunnelStatus>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let service_id = match read_request(stream).await {
        Ok(id) => id,
        Err(e) => {
            let _ = write_status(stream, TunnelStatus::BadRequest).await;
            return Err(e.context("malformed tunnel request"));
        }
    };

    let Some(target) = services.get(&service_id).map(str::to_owned) else {
        tracing::warn!(service = %service_id, "rejected unknown service");
        write_status(stream, TunnelStatus::UnknownService).await?;
        return Ok(TunnelStatus::UnknownService);
    };

    let mut up = match TcpStream::connect(&target).await {
        Ok(up) => up,
        Err(e) => {
            tracing::warn!(service = %service_id, target = %target, "upstream dial failed");
            write_status(stream, TunnelStatus::UpstreamUnreachable).await?;
            return Err(anyhow::Error::new(e)).with_context(|| format!("dial upstream {target}"));
        }
    };

    write_status(stream, TunnelStatus::Ok).await?;
    tracing::info!(service = %service_id, target = %target, "tunnel open");

    match tokio::io::copy_bidirectional(stream, &mut up).await {
        Ok((up_bytes, down_bytes)) => {
            println!(
                "TUNNEL_CLOSED service={service_id} UP_BYTES={up_bytes} DOWN_BYTES={down_bytes}"
            );
            Ok(TunnelStatus::Ok)
        }
        Err(e) => Err(anyhow::Error::new(e).context("tunnel pipe failed")),
    }
}

/// Bytes moved through one tunnelled stream, by direction.
///
/// Named fields (not a bare tuple) so an upload/download swap is a type
/// error rather than a silent mislabel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteCounts {
    /// Bytes sent from the client towards the gateway (upload).
    pub to_gateway: u64,
    /// Bytes received from the gateway by the client (download).
    pub from_gateway: u64,
}

/// Client-side handler for one tunnelled stream.
///
/// Runs the handshake for `service_id` on an already-open stream and, on
/// success, pipes raw bytes between `stream` and `local` until both sides
/// close.
pub async fn drive_client<S, L>(
    stream: &mut S,
    local: &mut L,
    service_id: &str,
) -> Result<ByteCounts>
where
    S: AsyncRead + AsyncWrite + Unpin,
    L: AsyncRead + AsyncWrite + Unpin,
{
    write_request(stream, service_id).await?;
    let status = read_status(stream).await?;
    if status != TunnelStatus::Ok {
        anyhow::bail!(
            "gateway rejected service {service_id:?}: {}",
            status.message()
        );
    }
    let (from_gateway, to_gateway) = tokio::io::copy_bidirectional(stream, local)
        .await
        .context("tunnel pipe failed")?;
    Ok(ByteCounts {
        to_gateway,
        from_gateway,
    })
}
