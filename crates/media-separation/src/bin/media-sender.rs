//! Media sender: remote-side binary for the direct-only media experiment
//! (plan E5 / PR 6).
//!
//! Binds its own control + media endpoint pair (media endpoint has
//! `RelayMode::Disabled`), fetches the receiver's media candidates over the
//! control connection, validates them (fail-closed), dials the media endpoint
//! directly, and streams synthetic frames under the gate.

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use common::new_result;
use iroh::{EndpointAddr, EndpointId};
use media_separation::{
    request_candidates, run_sender_session, EndpointPair, GateState, MediaGate, SyntheticConfig,
    CONTROL_ALPN, MEDIA_ALPN,
};
use std::sync::{Arc, Mutex};

#[derive(Parser)]
struct Args {
    /// Receiver's control EndpointId (hex).
    control_id: String,
    /// File to append a JSON result line to (JSONL).
    #[arg(long)]
    results: String,
    /// Network profile label recorded in the result.
    #[arg(long, default_value = "unspecified")]
    network_profile: String,
    /// Target synthetic bitrate in Mbit/s.
    #[arg(long, default_value_t = 5.0)]
    bitrate_mbps: f64,
    /// How long to stream once direct is confirmed.
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
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
        format!("media-send-{}", run_suffix()),
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
            result.relay_media_tx_bytes = outcome.relay_media_bytes;
            if outcome.ever_relay_paths > 0 || matches!(gate_state, GateState::Stopped(_)) {
                result.failure_reason = Some(format!(
                    "fail-closed gate tripped: ever_relay_paths={} gate={gate_state:?}",
                    outcome.ever_relay_paths
                ));
            } else if !outcome.receiver_confirmed {
                result.failure_reason =
                    Some("receiver never confirmed stream completion".into());
            }
            println!("OUTCOME={}", serde_json::to_string(&outcome)?);
        }
        Err(e) => {
            // The media workflow always attempts a direct connection, so an
            // error here is a known failed attempt, not an unattempted
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

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}

async fn run(args: &Args) -> Result<(media_separation::SessionOutcome, GateState)> {
    let pair = EndpointPair::bind(iroh::endpoint::presets::N0).await?;

    // --- control plane: fetch the receiver's media candidates ---
    let target: EndpointId = args.control_id.parse().context("invalid EndpointId")?;
    let control_conn = tokio::time::timeout(
        Duration::from_secs(30),
        pair.control.connect(target, CONTROL_ALPN),
    )
    .await
    .context("control connect timed out")?
    .context("control connect failed")?;

    let (mut ctl_send, mut ctl_recv) = control_conn.open_bi().await?;
    let (cands, token) = request_candidates(&mut ctl_send, &mut ctl_recv)
        .await
        .context("request candidates")?;
    anyhow::ensure!(!cands.is_empty(), "receiver published no candidates");

    const KNOWN_EPOCH: u64 = 0;
    // Validate every candidate from the known epoch and dial all of them:
    // on a multi-homed receiver any single address may belong to an
    // unreachable VPN, container, or interface, while a later advertised
    // candidate is reachable. Putting all candidates into the EndpointAddr
    // lets iroh race them instead of biasing direct results by candidate
    // order.
    let usable: Vec<&media_separation::DirectCandidate> = cands
        .iter()
        .filter(|c| c.network_epoch == KNOWN_EPOCH)
        .collect();
    anyhow::ensure!(!usable.is_empty(), "no candidate from a known epoch");
    for c in &usable {
        media_separation::validate_candidate(c, [KNOWN_EPOCH])
            .context("candidate rejected (fail-closed)")?;
    }
    let endpoint_id = usable[0].endpoint_id;
    anyhow::ensure!(
        usable.iter().all(|c| c.endpoint_id == endpoint_id),
        "advertised candidates disagree on endpoint id"
    );
    for c in &usable {
        tracing::info!(addr = %c.addr, source = ?c.source, "dialing media candidate");
    }

    // --- media plane: direct-only dial ---
    let started_unix_ms = media_separation::unix_millis();
    let media_addr = usable.iter().fold(EndpointAddr::new(endpoint_id), |a, c| {
        a.with_ip_addr(c.addr)
    });
    let conn = tokio::time::timeout(
        Duration::from_secs(30),
        pair.media.connect(media_addr, MEDIA_ALPN),
    )
    .await
    .context("media connect timed out")?
    .context("media connect failed")?;

    let cfg = SyntheticConfig {
        bitrate_bps: (args.bitrate_mbps * 1_000_000.0) as u64,
        frame_payload_bytes: 1200,
        duration: Duration::from_secs(args.duration_secs),
    };
    let (outcome, gate): (_, Arc<Mutex<MediaGate>>) =
        run_sender_session(conn, cfg, usable[0].clone(), KNOWN_EPOCH, started_unix_ms, &token)
        .await?;
    let state = gate.lock().unwrap().state();
    Ok((outcome, state))
}
