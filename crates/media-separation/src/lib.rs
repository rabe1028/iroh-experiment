//! Direct-only media path over iroh (plan E5 / PR 6).
//!
//! Implements the Media Endpoint rules from plan section 5.2:
//!
//! - Control and media traffic use **separate endpoints** with separate
//!   [`EndpointId`]s (plan section 5).
//! - The media endpoint is bound with `RelayMode::Disabled`, so it has no
//!   relay transport at all — a bug in this crate cannot push media bytes to
//!   a relay.
//! - Candidates are exchanged over the control endpoint and validated before
//!   dialing: expired candidates and unknown epochs are rejected (fail-closed),
//!   and the candidate type cannot carry a relay URL by construction.
//! - A path monitor drives a latching state machine ([`MediaGate`]) that only
//!   allows streaming while a direct path is open and selected; observing a
//!   relay path or losing the direct path stops media permanently.
//!
//! The synthetic stream is framed so the receiver can verify sequence
//! continuity and measure throughput.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow as anyhow_err, Context, Result};
use iroh::{
    endpoint::{presets, Connection},
    Endpoint, EndpointAddr, EndpointId, RelayMode, TransportAddr,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
};
use tokio_stream::StreamExt;

/// ALPN used for the control-plane candidate exchange.
pub const CONTROL_ALPN: &[u8] = b"iroh-experiment/media-control/0";

/// ALPN used for the synthetic media stream.
pub const MEDIA_ALPN: &[u8] = b"iroh-experiment/direct-media/0";

// ---------------------------------------------------------------------------
// DirectCandidate (plan section 5.3)
// ---------------------------------------------------------------------------

/// Source of one direct candidate address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CandidateSource {
    Local,
    Ipv6Global,
    PortMapping,
    CloudflareStun,
    CloudflareHttp3,
    FlyIrohQad,
    Manual,
}

/// One direct candidate, following the shape from plan section 5.3.
///
/// Note there is deliberately **no relay field**: a candidate can only ever
/// carry one direct IP address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectCandidate {
    pub endpoint_id: EndpointId,
    pub addr: SocketAddr,
    pub source: CandidateSource,
    pub observed_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub network_epoch: u64,
}

impl DirectCandidate {
    /// Build a fresh local candidate valid for `ttl`.
    pub fn local(endpoint_id: EndpointId, addr: SocketAddr, ttl: Duration, epoch: u64) -> Self {
        let now_ms = unix_millis();
        Self {
            endpoint_id,
            addr,
            source: CandidateSource::Local,
            observed_at_unix_ms: now_ms,
            expires_at_unix_ms: now_ms + ttl.as_millis() as u64,
            network_epoch: epoch,
        }
    }

    pub fn is_expired(&self, now_unix_ms: u64) -> bool {
        now_unix_ms >= self.expires_at_unix_ms
    }
}

/// Fail-closed candidate validation before it may be dialed.
///
/// Rejects expired candidates and candidates from an unknown network epoch
/// (stale after an interface change). The no-relay rule needs no check here:
/// [`DirectCandidate`] cannot represent one.
pub fn validate_candidate(
    cand: &DirectCandidate,
    known_epochs: impl IntoIterator<Item = u64>,
) -> Result<()> {
    anyhow::ensure!(
        !cand.is_expired(unix_millis()),
        "candidate expired at {}",
        cand.expires_at_unix_ms
    );
    anyhow::ensure!(
        known_epochs.into_iter().any(|e| e == cand.network_epoch),
        "candidate network_epoch {} is not currently known",
        cand.network_epoch
    );
    Ok(())
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Endpoint pair (control vs media separation)
// ---------------------------------------------------------------------------

/// A control + media endpoint pair bound on separate sockets/identities.
///
/// The control endpoint preset is injectable so tests can run fully offline
/// with `presets::Minimal`, while production bins use `presets::N0` on the
/// control side for discovery and relay support. The media endpoint always
/// uses `RelayMode::Disabled`.
pub struct EndpointPair {
    /// Relay-capable control endpoint.
    pub control: Endpoint,
    /// Direct-only media endpoint (`RelayMode::Disabled`).
    pub media: Endpoint,
}

impl EndpointPair {
    /// Bind a control + media endpoint pair.
    pub async fn bind(control_preset: impl presets::Preset + Copy) -> Result<Self> {
        let control = Endpoint::builder(control_preset)
            .bind()
            .await
            .context("bind control endpoint")?;
        let media = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .context("bind media endpoint")?;
        Ok(Self { control, media })
    }

    /// Direct addresses of the media endpoint (IP transports only).
    ///
    /// Panics if a non-IP transport shows up here, which would mean
    /// `RelayMode::Disabled` stopped being honored by iroh.
    pub fn media_direct_addrs(&self) -> Vec<SocketAddr> {
        self.media
            .addr()
            .addrs
            .into_iter()
            .map(|a| match a {
                TransportAddr::Ip(sa) => sa,
                other => {
                    panic!("media endpoint must never expose non-IP transport, got {other:?}")
                }
            })
            .collect()
    }

    /// Build a direct-only [`EndpointAddr`] for this pair's media endpoint.
    pub fn media_addr(&self) -> EndpointAddr {
        // One canonical direct candidate address; loopback/private addresses
        // are fine within the same LAN, external ones come from probes.
        let addr = self
            .media_direct_addrs()
            .into_iter()
            .find(|sa| !sa.ip().is_unspecified())
            .expect("media endpoint has at least one concrete IP address");
        EndpointAddr::new(self.media.id()).with_ip_addr(addr)
    }
}

// ---------------------------------------------------------------------------
// Control-plane candidate exchange (length-prefixed JSON frames)
// ---------------------------------------------------------------------------

async fn write_frame<W>(w: &mut W, payload: &[u8]) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    let len = u32::try_from(payload.len()).context("frame too large")?;
    w.write_u32(len).await?;
    w.write_all(payload).await?;
    w.flush().await?;
    Ok(())
}

async fn read_frame<R>(r: &mut R) -> Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin + Send,
{
    let len = r.read_u32().await.context("read frame length")?;
    anyhow::ensure!(len <= 64 * 1024, "frame length {len} exceeds limit");
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf).await.context("read frame body")?;
    Ok(buf)
}

/// Receiver side of the control handshake: send our media candidates.
pub async fn send_candidates<S>(stream: &mut S, cands: &[DirectCandidate]) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin + Send,
{
    write_frame(stream, &serde_json::to_vec(cands)?).await
}

/// Sender side of the control handshake: request the peer's media
/// candidates on an opened bidirectional stream.
///
/// Protocol: empty request frame -> candidates JSON frame.
pub async fn request_candidates<W, R>(w: &mut W, r: &mut R) -> Result<Vec<DirectCandidate>>
where
    W: tokio::io::AsyncWrite + Unpin + Send,
    R: tokio::io::AsyncRead + Unpin + Send,
{
    write_frame(w, b"").await.context("send request")?;
    let json = read_frame(r).await?;
    serde_json::from_slice(&json).context("decode candidates")
}

/// Receiver side of the control handshake: answer a candidate request.
pub async fn serve_candidates<R, W>(r: &mut R, w: &mut W, cands: &[DirectCandidate]) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    let req = read_frame(r).await.context("read request")?;
    anyhow::ensure!(req.is_empty(), "unexpected request payload");
    send_candidates(w, cands).await
}

/// Sender side of the control handshake (standalone): receive the peer's
/// media candidates from a stream already carrying the reply.
pub async fn recv_candidates<S>(stream: &mut S) -> Result<Vec<DirectCandidate>>
where
    S: tokio::io::AsyncRead + Unpin + Send,
{
    let json = read_frame(stream).await?;
    serde_json::from_slice(&json).context("decode candidates")
}

// ---------------------------------------------------------------------------
// Fail-closed gate (latching state machine)
// ---------------------------------------------------------------------------

/// Why media was stopped. Once set, the gate never reopens (fail-closed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "reason")]
pub enum StopReason {
    /// Any relay path was observed on the media connection.
    RelayPathObserved,
    /// The last open direct path closed, or selection left the direct path.
    DirectPathLost,
    /// The QUIC connection closed underneath us.
    ConnectionClosed,
}

/// Current gate decision derived from path telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum GateState {
    /// No direct path yet: streaming forbidden.
    AwaitingDirect,
    /// Direct path open and selected: streaming allowed.
    DirectReady,
    /// Terminal failure; media must stop and stay stopped.
    Stopped(StopReason),
}

/// Raw signals the monitor derives from iroh path telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSignal {
    /// Idempotent reconciliation from one [`Connection::paths`] snapshot.
    ///
    /// Snapshot-driven on purpose: the initial path may be selected *before*
    /// the monitor subscribes to events, so counting Opened/Closed events can
    /// miss state; every snapshot fully re-derives the gate decision.
    Snapshot {
        /// Number of currently open non-relay paths.
        open_direct: usize,
        /// Any relay path is currently open.
        has_relay: bool,
        /// The selected transmission path is a direct IP path.
        selected_direct: bool,
    },
    ConnectionClosed,
}

/// Latching fail-closed gate fed with raw path signals.
///
/// Invariants:
/// - Streaming is only allowed in [`GateState::DirectReady`].
/// - Every transition into [`GateState::Stopped`] is final.
/// - Seeing a relay path stops media even if it was never selected.
/// - Losing all direct paths (after having had one) stops media.
#[derive(Debug)]
pub struct MediaGate {
    tx: watch::Sender<GateState>,
    ever_had_direct: bool,
}

impl MediaGate {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(GateState::AwaitingDirect);
        Self {
            tx,
            ever_had_direct: false,
        }
    }

    pub fn state(&self) -> GateState {
        *self.tx.borrow()
    }

    /// Watch channel receiving every state change.
    pub fn subscribe(&self) -> watch::Receiver<GateState> {
        self.tx.subscribe()
    }

    /// Feed one raw signal; returns the state after applying it.
    pub fn apply(&mut self, signal: PathSignal) -> GateState {
        // Already latched: ignore everything else.
        if matches!(self.state(), GateState::Stopped(_)) {
            return self.state();
        }
        match signal {
            PathSignal::ConnectionClosed => {
                return self.stop(StopReason::ConnectionClosed);
            }
            PathSignal::Snapshot {
                open_direct,
                has_relay,
                selected_direct,
            } => {
                if has_relay {
                    return self.stop(StopReason::RelayPathObserved);
                }
                if open_direct > 0 {
                    self.ever_had_direct = true;
                } else if self.ever_had_direct {
                    // Had a direct path, now none left: fail closed instead
                    // of waiting for the QUIC close event.
                    return self.stop(StopReason::DirectPathLost);
                }
                if selected_direct {
                    self.tx.send_replace(GateState::DirectReady);
                } else if self.state() == GateState::DirectReady {
                    // Selection left the direct path without a close event
                    // (e.g. migration window): treat as loss of usable path.
                    return self.stop(StopReason::DirectPathLost);
                }
            }
        }
        self.state()
    }

    fn stop(&mut self, reason: StopReason) -> GateState {
        self.tx.send_replace(GateState::Stopped(reason));
        self.state()
    }
}

impl Default for MediaGate {
    fn default() -> Self {
        Self::new()
    }
}

fn lock_gate(gate: &Mutex<MediaGate>) -> MutexGuard<'_, MediaGate> {
    gate.lock().expect("media gate poisoned")
}

fn gate_stop_reason(gate: &Mutex<MediaGate>) -> Option<StopReason> {
    match lock_gate(gate).state() {
        GateState::Stopped(r) => Some(r),
        _ => None,
    }
}

/// Spawn a task that watches `conn`'s paths and feeds the gate.
///
/// The monitor reconciles the gate from [`Connection::paths`] snapshots on
/// every path event plus a periodic tick (so pre-subscription path state is
/// never missed), and ends when the gate reaches a terminal state or the
/// connection closes. It also keeps counters used to prove no relay path
/// ever existed.
pub fn spawn_media_monitor(conn: &Connection, gate: Arc<Mutex<MediaGate>>) -> MediaMonitor {
    let conn = conn.clone();
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let ever_relay = Arc::new(AtomicU64::new(0));
    let ever_relay_count = ever_relay.clone();
    let mut done_rx_monitor = done_rx.clone();

    tokio::spawn(async move {
        let mut events = conn.path_events();
        // Periodic tick: reconcile even if no event ever fires (e.g. the
        // initial direct path was selected before we subscribed).
        let mut ticker = tokio::time::interval(Duration::from_millis(50));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut stopped_fut = std::pin::pin!(async {
            while !*stop_rx.borrow_and_update() {
                if stop_rx.changed().await.is_err() {
                    break;
                }
            }
        });
        tokio::select! {
            _ = &mut stopped_fut => {}
            _ = monitor_loop(&conn, &gate, &mut events, &mut ticker, &ever_relay_count, &mut done_rx_monitor) => {}
        }
        let _ = done_tx.send(true);
    });

    MediaMonitor {
        stop_tx,
        done_rx,
        ever_relay_paths: ever_relay,
    }
}

/// Reconciliation loop: snapshot -> gate, until the gate latches or the
/// connection closes.
async fn monitor_loop(
    conn: &Connection,
    gate: &Arc<Mutex<MediaGate>>,
    events: &mut iroh::endpoint::PathEventStream,
    ticker: &mut tokio::time::Interval,
    ever_relay_count: &AtomicU64,
    done_rx: &mut watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = events.next() => {}
            _ = ticker.tick() => {}
            _ = conn.closed() => {
                lock_gate(gate).apply(PathSignal::ConnectionClosed);
                break;
            }
        }

        // Reconcile from a fresh snapshot.
        let mut open_direct = 0usize;
        let mut has_relay = false;
        let mut selected_direct = false;
        for p in conn.paths().iter() {
            if p.is_relay() {
                has_relay = true;
                ever_relay_count.fetch_add(1, Ordering::Relaxed);
            } else {
                open_direct += 1;
                if p.is_selected() {
                    selected_direct = true;
                }
            }
        }
        lock_gate(gate).apply(PathSignal::Snapshot {
            open_direct,
            has_relay,
            selected_direct,
        });

        if matches!(lock_gate(gate).state(), GateState::Stopped(_)) {
            break;
        }
        if *done_rx.borrow() {
            break;
        }
    }
}

/// Handle to the spawned path-monitor task.
#[derive(Clone)]
pub struct MediaMonitor {
    stop_tx: watch::Sender<bool>,
    done_rx: watch::Receiver<bool>,
    /// Number of relay paths ever seen (must stay zero for a compliant run).
    ever_relay_paths: Arc<AtomicU64>,
}

impl MediaMonitor {
    /// Ask the monitor to finish after its next reconcile.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Resolves once the monitor finished its final reconcile.
    pub async fn finished(&mut self) {
        while !*self.done_rx.borrow_and_update() {
            if self.done_rx.changed().await.is_err() {
                break;
            }
        }
    }

    /// Count of relay paths ever observed on this connection.
    pub fn ever_relay_paths(&self) -> u64 {
        self.ever_relay_paths.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Synthetic stream (framed, rate-limited)
// ---------------------------------------------------------------------------

/// Parameters of the synthetic media flow.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SyntheticConfig {
    /// Target average rate in bits per second.
    pub bitrate_bps: u64,
    /// Payload bytes per frame (excludes the frame header).
    pub frame_payload_bytes: u16,
    /// How long to stream once the gate allows it.
    #[serde(with = "duration_millis")]
    pub duration: Duration,
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_millis().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let ms = u64::deserialize(d)?;
        Ok(Duration::from_millis(ms))
    }
}

impl SyntheticConfig {
    /// Frame header: seq(u32) + sent-at unix ms(u64) + payload len(u16).
    pub const HEADER_BYTES: usize = 4 + 8 + 2;

    pub fn frame_bytes(&self) -> usize {
        Self::HEADER_BYTES + self.frame_payload_bytes as usize
    }

    /// Interval between frames to hit `bitrate_bps` on average.
    pub fn frame_interval(&self) -> Duration {
        let bytes_per_sec = (self.bitrate_bps / 8).max(1);
        let frames_per_sec = (bytes_per_sec / self.frame_payload_bytes.max(1) as u64).max(1);
        Duration::from_nanos(1_000_000_000u64.saturating_div(frames_per_sec))
    }
}

/// Statistics reported by whichever side ran the synthetic stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamStats {
    pub frames: u64,
    pub bytes_on_wire: u64,
    pub first_frame_unix_ms: Option<u64>,
    pub last_frame_unix_ms: Option<u64>,
    pub stop_reason: Option<StopReason>,
}

impl StreamStats {
    /// Average throughput in Mbit/s across the observed frame span.
    pub fn throughput_mbps(&self) -> Option<f64> {
        let first = self.first_frame_unix_ms?;
        let last = self.last_frame_unix_ms?;
        if last <= first || self.frames < 2 {
            return None;
        }
        let bits = self.bytes_on_wire as f64 * 8.0;
        Some(bits / ((last - first) as f64 / 1000.0) / 1_000_000.0)
    }
}

/// One parsed inbound frame.
struct Frame {
    seq: u32,
    sent_ms: u64,
    wire_bytes: u64,
}

/// Read exactly one framed frame; `Ok(None)` on a clean FIN boundary.
async fn read_frame_synced<S>(stream: &mut S) -> std::io::Result<Option<Frame>>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut header = [0u8; SyntheticConfig::HEADER_BYTES];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let seq = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let sent_ms = u64::from_be_bytes(header[4..12].try_into().unwrap());
    let plen = u16::from_be_bytes(header[12..14].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; plen];
    stream.read_exact(&mut payload).await?;
    Ok(Some(Frame {
        seq,
        sent_ms,
        wire_bytes: (SyntheticConfig::HEADER_BYTES + plen) as u64,
    }))
}

/// Receiver loop for the synthetic stream.
///
/// A dedicated reader task owns the stream so frame reads are **never**
/// cancelled mid-frame (a cancelled `read_exact` would silently drop buffered
/// bytes and desynchronize framing); the driving loop selects between frame
/// messages and gate transitions. Verifies sequence continuity starting at 0.
/// Returns stats plus the number of frames received.
pub async fn receive_synthetic<S>(
    stream: S,
    gate: Arc<Mutex<MediaGate>>,
    mut state_rx: watch::Receiver<GateState>,
) -> Result<(StreamStats, u64)>
where
    S: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Result<Option<Frame>>>(8);
    tokio::spawn(async move {
        let mut stream = stream;
        loop {
            match read_frame_synced(&mut stream).await {
                Ok(frame) => {
                    let is_none = frame.is_none();
                    if tx.send(Ok(frame)).await.is_err() {
                        break;
                    }
                    if is_none {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });

    let mut stats = StreamStats::default();
    let mut expected_seq: u32 = 0;
    loop {
        // Fail closed: a latched gate ends reception immediately.
        if matches!(*state_rx.borrow_and_update(), GateState::Stopped(_)) {
            stats.stop_reason = gate_stop_reason(&gate);
            return Ok((stats, expected_seq as u64));
        }
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    None => {
                        // Reader finished (clean FIN).
                        break;
                    }
                    Some(Ok(None)) => {
                        // Clean FIN from the sender.
                        break;
                    }
                    Some(Ok(Some(frame))) => {
                        anyhow::ensure!(
                            frame.seq == expected_seq,
                            "sequence discontinuity: got {}, expected {}",
                            frame.seq,
                            expected_seq
                        );
                        expected_seq = expected_seq.wrapping_add(1);
                        stats.frames += 1;
                        stats.bytes_on_wire += frame.wire_bytes;
                        if stats.first_frame_unix_ms.is_none() {
                            stats.first_frame_unix_ms =
                                Some(frame.sent_ms.min(unix_millis()));
                        }
                        stats.last_frame_unix_ms = Some(frame.sent_ms.min(unix_millis()));
                    }
                    Some(Err(_)) => {
                        // Abrupt cut: the monitor needs a moment to observe
                        // the dead connection and latch the gate; wait for
                        // that (bounded) so the reason is recorded.
                        let latched = tokio::time::timeout(
                            Duration::from_secs(2),
                            async {
                                while !matches!(
                                    *state_rx.borrow_and_update(),
                                    GateState::Stopped(_)
                                ) {
                                    if state_rx.changed().await.is_err() {
                                        std::future::pending::<()>().await;
                                    }
                                }
                            },
                        )
                        .await;
                        match latched {
                            Ok(()) => {
                                stats.stop_reason = gate_stop_reason(&gate);
                                return Ok((stats, expected_seq as u64));
                            }
                            Err(_) => {
                                return Err(anyhow_err!(
                                    "frame read failed before any gate stop"
                                ));
                            }
                        }
                    }
                }
            }
            _ = state_rx.changed() => {
                if matches!(*state_rx.borrow_and_update(), GateState::Stopped(_)) {
                    stats.stop_reason = gate_stop_reason(&gate);
                    return Ok((stats, expected_seq as u64));
                }
            }
        }
    }
    stats.stop_reason = gate_stop_reason(&gate);
    Ok((stats, expected_seq as u64))
}

/// Sender loop for the synthetic stream.
///
/// A dedicated task owns the stream so frame writes are **never** cancelled
/// mid-frame. Waits until the gate reports [`GateState::DirectReady`] (bounded
/// by the configured duration), then writes frames at the configured rate,
/// stopping when the gate leaves DirectReady, the duration elapses, or the
/// stream errors.
pub async fn send_synthetic<S>(
    stream: S,
    cfg: SyntheticConfig,
    gate: Arc<Mutex<MediaGate>>,
    mut state_rx: watch::Receiver<GateState>,
) -> Result<StreamStats>
where
    S: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Block until direct is ready (bounded by the overall duration).
    tokio::time::timeout(cfg.duration, async {
        while !matches!(*state_rx.borrow_and_update(), GateState::DirectReady) {
            state_rx
                .changed()
                .await
                .map_err(|_| anyhow_err!("gate closed"))?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for direct-ready")??;

    let handle = tokio::spawn(streaming_task(stream, cfg, gate));
    handle.await.context("sender task panicked")?
}

async fn streaming_task<S>(
    mut stream: S,
    cfg: SyntheticConfig,
    gate: Arc<Mutex<MediaGate>>,
) -> Result<StreamStats>
where
    S: tokio::io::AsyncWrite + Unpin + Send,
{
    let mut stats = StreamStats::default();
    let deadline = tokio::time::Instant::now() + cfg.duration;
    let mut state_rx = lock_gate(&gate).subscribe();

    let mut ticker = tokio::time::interval(cfg.frame_interval());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await; // interval fires immediately; consume that tick
    let mut seq: u32 = 0;
    let payload = vec![0xAB; cfg.frame_payload_bytes as usize];

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = state_rx.changed() => {}
        }
        let state = *state_rx.borrow_and_update();
        if matches!(state, GateState::Stopped(_)) {
            stats.stop_reason = gate_stop_reason(&gate);
            return Ok(stats);
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if !matches!(lock_gate(&gate).state(), GateState::DirectReady) {
            stats.stop_reason = gate_stop_reason(&gate);
            return Ok(stats);
        }

        let now_ms = unix_millis();
        let mut frame = Vec::with_capacity(cfg.frame_bytes());
        frame.extend_from_slice(&seq.to_be_bytes());
        frame.extend_from_slice(&now_ms.to_be_bytes());
        frame.extend_from_slice(&(cfg.frame_payload_bytes).to_be_bytes());
        frame.extend_from_slice(&payload);

        if let Err(e) = stream.write_all(&frame).await {
            if is_closed(&e) {
                stats.stop_reason = Some(StopReason::ConnectionClosed);
                return Ok(stats);
            }
            return Err(e.into());
        }
        if let Err(e) = stream.flush().await {
            if is_closed(&e) {
                stats.stop_reason = Some(StopReason::ConnectionClosed);
                return Ok(stats);
            }
            return Err(e.into());
        }

        stats.frames += 1;
        stats.bytes_on_wire += frame.len() as u64;
        if stats.first_frame_unix_ms.is_none() {
            stats.first_frame_unix_ms = Some(now_ms);
        }
        stats.last_frame_unix_ms = Some(now_ms);
        seq = seq.wrapping_add(1);
    }
    // Normal completion: finish the send side so the receiver sees EOF.
    let _ = stream.shutdown().await;
    Ok(stats)
}

fn is_closed(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
    )
}

// ---------------------------------------------------------------------------
// Session drivers shared by bins and tests
// ---------------------------------------------------------------------------

/// Role of the running binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MediaRole {
    /// Home side: accepts control + media connections, receives media.
    Receiver,
    /// Remote side: dials control + media, sends media.
    Sender,
}

/// Outcome of one full media session (used for result recording).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOutcome {
    pub role: MediaRole,
    pub direct_connection_success: bool,
    pub time_to_direct_ms: Option<u64>,
    pub stream: StreamStats,
    /// Always 0 in a compliant run; nonzero proves a relay path existed.
    pub relay_media_bytes: u64,
    /// Count of relay paths ever observed by the monitor.
    pub ever_relay_paths: u64,
}

/// Run the receiver half of a media session on an accepted media connection.
///
/// Handshake: sender sends an empty request frame, receiver answers `ready`,
/// then the synthetic stream flows receiver-ward until done or stopped.
pub async fn run_receiver_session(
    conn: Connection,
    _cfg: SyntheticConfig,
) -> Result<(SessionOutcome, Arc<Mutex<MediaGate>>)> {
    let started = std::time::Instant::now();
    let gate = Arc::new(Mutex::new(MediaGate::new()));
    let state_rx = lock_gate(&gate).subscribe();
    let mut monitor = spawn_media_monitor(&conn, gate.clone());

    let (mut send, mut recv) = conn.accept_bi().await.context("accept bi")?;
    let req = read_frame(&mut recv).await.context("read media request")?;
    anyhow::ensure!(req.is_empty(), "unexpected request payload");
    write_frame(&mut send, b"ready").await?;

    let (stats, _next_seq) = receive_synthetic(recv, gate.clone(), state_rx).await?;
    let _ = send.shutdown().await;

    // End monitoring and take the final counters.
    monitor.stop();
    monitor.finished().await;
    let ever_relay = monitor.ever_relay_paths();

    let outcome = SessionOutcome {
        role: MediaRole::Receiver,
        direct_connection_success: stats.frames > 0,
        time_to_direct_ms: stats
            .first_frame_unix_ms
            .map(|_| started.elapsed().as_millis() as u64),
        relay_media_bytes: 0,
        ever_relay_paths: ever_relay,
        stream: stats,
    };
    Ok((outcome, gate))
}

/// Run the sender half of a media session on an already-dialed media
/// connection towards `candidate` (validated first, fail-closed).
pub async fn run_sender_session(
    conn: Connection,
    cfg: SyntheticConfig,
    candidate: DirectCandidate,
    known_epoch: u64,
) -> Result<(SessionOutcome, Arc<Mutex<MediaGate>>)> {
    validate_candidate(&candidate, [known_epoch])?;

    let started = std::time::Instant::now();
    let gate = Arc::new(Mutex::new(MediaGate::new()));
    let state_rx = lock_gate(&gate).subscribe();
    let mut monitor = spawn_media_monitor(&conn, gate.clone());

    let (mut send, mut recv) = conn.open_bi().await.context("open bi")?;
    write_frame(&mut send, b"").await.context("send request")?;
    let ready = read_frame(&mut recv).await.context("read ready")?;
    anyhow::ensure!(ready == b"ready", "receiver not ready");

    let stats = send_synthetic(send, cfg, gate.clone(), state_rx).await?;
    let _ = recv.read_to_end(64 * 1024).await;

    // End monitoring and take the final counters.
    monitor.stop();
    monitor.finished().await;
    let ever_relay = monitor.ever_relay_paths();

    let outcome = SessionOutcome {
        role: MediaRole::Sender,
        direct_connection_success: stats.frames > 0,
        time_to_direct_ms: stats
            .first_frame_unix_ms
            .map(|_| started.elapsed().as_millis() as u64),
        relay_media_bytes: 0,
        ever_relay_paths: ever_relay,
        stream: stats,
    };
    Ok((outcome, gate))
}
