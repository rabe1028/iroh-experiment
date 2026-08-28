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
use common::{new_result, RunFailure};
use media_separation::{DirectCandidate, EndpointPair, SyntheticConfig, CONTROL_ALPN, MEDIA_ALPN};

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
            result.payload_bytes = outcome.stream.bytes_on_wire;
            result.media_throughput_mbps = outcome.stream.throughput_mbps();
            result.relay_media_rx_bytes = Some(outcome.relay_media_bytes);
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
            // Only an error once we started waiting for the direct media
            // connection is a measured failure; a control-plane error before
            // it stays unattempted (null).
            result.direct_connection_success = if e.attempted_direct {
                Some(false)
            } else {
                None
            };
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
) -> Result<
    (
        media_separation::SessionOutcome,
        media_separation::GateState,
    ),
    RunFailure,
> {
    // Control plane: failures here happen before any direct media attempt,
    // so a failure record stays unattempted (direct_connection_success =
    // null).
    let pair = setup_control_plane(args).await?;

    accept_media_and_measure(&pair, args)
        .await
        .map_err(|err| RunFailure {
            attempted_direct: true,
            err,
        })
}

/// Bind the endpoint pair, publish media candidates over the control
/// connection, and serve them to the sender.
async fn setup_control_plane(args: &Args) -> anyhow::Result<EndpointPair> {
    let pair = EndpointPair::bind(iroh::endpoint::presets::N0).await?;
    pair.control.set_alpns(vec![CONTROL_ALPN.to_vec()]);
    pair.media.set_alpns(vec![MEDIA_ALPN.to_vec()]);

    println!("CONTROL_EP_ID={}", pair.control.id());
    println!("MEDIA_EP_ID={}", pair.media.id());
    let addrs = pair.media_direct_addrs();
    for addr in &addrs {
        println!("MEDIA_ADDR={addr}");
    }
    anyhow::ensure!(!addrs.is_empty(), "media endpoint has no direct addresses");

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

    const EPOCH: u64 = 0;
    let cands: Vec<DirectCandidate> = addrs
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

    let (mut ctl_send, mut ctl_recv) = control_conn.accept_bi().await?;
    media_separation::serve_candidates(&mut ctl_recv, &mut ctl_send, &cands)
        .await
        .context("serve candidates")?;
    Ok(pair)
}

/// Wait for the direct media connection and receive the stream. Everything
/// in here happens after the direct attempt began, so any error is a
/// measured failure (direct_connection_success = Some(false)).
async fn accept_media_and_measure(
    pair: &EndpointPair,
    args: &Args,
) -> anyhow::Result<(
    media_separation::SessionOutcome,
    media_separation::GateState,
)> {
    // Wait for the media connection on the direct-only endpoint.
    tracing::info!("waiting for media connection");
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

    let cfg = SyntheticConfig {
        bitrate_bps: (args.bitrate_mbps * 1_000_000.0) as u64,
        frame_payload_bytes: 1200,
        duration: Duration::from_secs(args.duration_secs),
    };
    let (outcome, gate) = media_separation::run_receiver_session(conn, cfg).await?;
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
