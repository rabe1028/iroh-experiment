//! Media receiver: home-side binary for the direct-only media experiment
//! (plan E5 / PR 6).
//!
//! Binds a control endpoint (relay-capable) and a media endpoint
//! (RelayMode::Disabled, separate EndpointId). Publishes its media candidates
//! to the dialer over the control connection, then receives the synthetic
//! media stream on the media endpoint under the fail-closed gate.

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use common::new_result;
use media_separation::{DirectCandidate, EndpointPair, CONTROL_ALPN, MEDIA_ALPN};

#[derive(Parser)]
struct Args {
    /// File to append a JSON result line to (JSONL).
    #[arg(long)]
    results: String,
    /// Network profile label recorded in the result.
    #[arg(long, default_value = "unspecified")]
    network_profile: String,
    /// Target synthetic bitrate in Mbit/s.
    #[arg(long, default_value_t = 5.0)]
    bitrate_mbps: f64,
    /// How long the sender should stream once direct is confirmed.
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    /// Candidate time-to-live advertised to the sender.
    #[arg(long, default_value_t = 30_000)]
    candidate_ttl_ms: u64,
    /// Externally reachable address to advertise in addition to the local
    /// interface addresses (repeatable). The local candidates alone are
    /// private/interface addresses, so a sender outside the receiver's LAN
    /// cannot dial them; supply e.g. the media endpoint's port behind a NAT
    /// static mapping, or the address a STUN probe observed for this host
    /// (use the media endpoint's port if the mapping preserves it).
    #[arg(long = "advertise-addr")]
    advertise_addrs: Vec<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let runtime = tokio::runtime::Runtime::new()?;
    let outcome = runtime.block_on(run(&args));

    let mut result = new_result(
        format!("media-recv-{}", run_suffix()),
        "direct-media",
        &args.network_profile,
    );
    match outcome {
        Ok((outcome, gate_state)) => {
            result.direct_connection_success = Some(outcome.direct_connection_success);
            result.time_to_direct_ms = outcome.time_to_direct_ms;
            // A successful run streamed only under a direct-selected gate.
            if outcome.direct_connection_success {
                result.selected_path = Some(common::SelectedPath::DirectIp);
            }
            result.payload_bytes = outcome.stream.payload_bytes;
            result.media_throughput_mbps = outcome.stream.throughput_mbps();
            result.relay_media_rx_bytes = outcome.relay_media_bytes;
            if outcome.ever_relay_paths > 0
                || matches!(gate_state, media_separation::GateState::Stopped(_))
            {
                result.failure_reason = Some(format!(
                    "fail-closed gate tripped: ever_relay_paths={} gate={gate_state:?}",
                    outcome.ever_relay_paths
                ));
            }
            println!("OUTCOME={}", serde_json::to_string(&outcome)?);
            println!(
                "THROUGHPUT_MBPS={}",
                outcome.stream.throughput_mbps().unwrap_or(0.0)
            );
        }
        Err(e) => {
            // The media workflow always waits for a direct connection, so
            // an error here is a known failed attempt, not an unattempted
            // check.
            result.direct_connection_success = Some(false);
            result.failure_reason = Some(format!("{e:#}"));
        }
    }
    common::append_result_line(&args.results, &result)?;
    if let Some(reason) = result.failure_reason.clone() {
        anyhow::bail!("run failed: {reason}");
    }
    Ok(())
}

async fn run(
    args: &Args,
) -> Result<(
    media_separation::SessionOutcome,
    media_separation::GateState,
)> {
    let pair = EndpointPair::bind(iroh::endpoint::presets::N0).await?;
    pair.control.set_alpns(vec![CONTROL_ALPN.to_vec()]);
    pair.media.set_alpns(vec![MEDIA_ALPN.to_vec()]);

    println!("CONTROL_EP_ID={}", pair.control.id());
    println!("MEDIA_EP_ID={}", pair.media.id());

    // Accept the control connection and publish candidates on it.
    let control_conn = pair
        .control
        .accept()
        .await
        .context("control endpoint closed")?
        .accept()
        .context("control incoming rejected")?
        .await
        .context("control connect failed")?;

    // Snapshot the media addresses only now, right before advertising: an
    // interface change while waiting for the dialer must not publish stale
    // pre-wait addresses stamped as freshly observed for the whole TTL.
    let addrs = pair.media_direct_addrs();
    for addr in &addrs {
        println!("MEDIA_ADDR={addr}");
    }
    anyhow::ensure!(!addrs.is_empty(), "media endpoint has no direct addresses");

    const EPOCH: u64 = 0;
    let mut cands: Vec<DirectCandidate> = addrs
        .iter()
        .map(|addr| {
            DirectCandidate::local(
                pair.media.id(),
                *addr,
                Duration::from_millis(args.candidate_ttl_ms),
                EPOCH,
            )
        })
        .collect();
    for raw in &args.advertise_addrs {
        let addr: std::net::SocketAddr = raw
            .parse()
            .with_context(|| format!("invalid --advertise-addr {raw}"))?;
        tracing::info!(addr = %addr, "advertising manual candidate");
        cands.push(DirectCandidate::manual(
            pair.media.id(),
            addr,
            Duration::from_millis(args.candidate_ttl_ms),
            EPOCH,
        ));
    }

    // One-shot session token: only whoever completes this control handshake
    // can pass the media handshake (plan section 19 capability).
    let token = media_separation::session_token();
    let (mut ctl_send, mut ctl_recv) = control_conn.accept_bi().await?;
    media_separation::serve_candidates(&mut ctl_recv, &mut ctl_send, &cands, &token)
        .await
        .context("serve candidates")?;

    // Wait for the media connection on the direct-only endpoint. The origin
    // for time_to_direct_ms is taken before waiting, so the measurement
    // covers accept start to direct-ready rather than the stream duration.
    tracing::info!("waiting for media connection");
    let started_unix_ms = media_separation::unix_millis();
    let conn = tokio::time::timeout(Duration::from_secs(60), async {
        pair.media
            .accept()
            .await
            .context("media endpoint closed")?
            .accept()
            .context("media incoming rejected")?
            .await
            .context("media connect failed")
    })
    .await
    .context("timed out waiting for media connection")??;

    let (outcome, gate) =
        media_separation::run_receiver_session(conn, started_unix_ms, &token).await?;
    let state = gate.lock().unwrap().state();
    Ok((outcome, state))
}

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}
