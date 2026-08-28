//! Baseline acceptor: Endpoint A (LAN gateway role).
//!
//! Binds an iroh endpoint with the default relay configuration, prints its
//! EndpointId + addresses, accepts one baseline echo connection, echoes all
//! bytes back, and records path telemetry (plan E0 / PR 1).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use common::{ExperimentResult, RunFailure, SelectedPath, TEST_PAYLOAD_BYTES};
use iroh::endpoint::PathEvent;
use tokio_stream::StreamExt;

/// Experiment-wide upper bound for waiting on a relay -> direct migration
/// after the payload transfer completes.
const DIRECT_MIGRATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared telemetry snapshot written by the path-event watcher task.
#[derive(Default)]
struct PathTelemetry {
    first_direct: Option<Duration>,
    /// Whether the most recent `Selected` event chose a relay path.
    last_selected_is_relay: bool,
}

#[derive(Parser)]
struct Args {
    /// File to append a JSON result line to (JSONL).
    #[arg(long)]
    results: String,
    /// Network profile label recorded in the result.
    #[arg(long, default_value = "unspecified")]
    network_profile: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let runtime = tokio::runtime::Runtime::new()?;
    let outcome = runtime.block_on(run(&args.network_profile));

    // Persist a result line even on failure (with failure_reason set).
    let result = match outcome {
        Ok(r) => r,
        Err(e) => {
            let mut r = common::new_result(
                format!("baseline-accept-{}", run_suffix()),
                "baseline",
                &args.network_profile,
            );
            // Null until an attempt is made; a failed attempt stays false;
            // echo-phase errors carry their own outcome (see below).
            r.direct_connection_success = e.direct_connection_success;
            r.failure_reason = Some(format!("{e:#}"));
            r
        }
    };
    common::append_result_line(&args.results, &result)?;

    if let Some(reason) = result.failure_reason.clone() {
        anyhow::bail!("run failed: {reason}");
    }
    Ok(())
}

async fn run(network_profile: &str) -> Result<ExperimentResult, RunFailure> {
    // Setup: failures here happen before any direct attempt, so a failure
    // record stays unattempted (direct_connection_success = null).
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    endpoint.set_alpns(vec![common::BASELINE_ALPN.to_vec()]);
    println!("ENDPOINT_ID={}", endpoint.id());
    for addr in endpoint.addr().addrs {
        println!("ADDR={addr}");
    }

    accept_and_measure(&endpoint, network_profile)
        .await
        .map_err(RunFailure::failed_direct)
}

/// Accept one connection and measure the echo transfer. Accept errors are
/// measured failed attempts (`Some(false)`); echo errors after the connect
/// keep the observed path telemetry in a partial result.
async fn accept_and_measure(
    endpoint: &iroh::Endpoint,
    network_profile: &str,
) -> anyhow::Result<ExperimentResult> {
    // Accept exactly one connection for the baseline run.
    let conn = endpoint
        .accept()
        .await
        .context("endpoint closed before incoming connection")?
        .accept()?
        .await
        .context("accept failed")?;

    let t_start = Instant::now();
    println!("CONNECTED_AT_MS={}", t_start.elapsed().as_millis());

    // Watch path events from before the echo transfer starts, so a
    // relay -> direct migration during the transfer is observed.
    // PathEventStream is 'static; the snapshot mutex keeps observed values
    // even though the stream stays pending on a live connection.
    let mut events = conn.path_events();
    let telemetry = Arc::new(Mutex::new(PathTelemetry {
        first_direct: None,
        last_selected_is_relay: true,
    }));
    let watcher = tokio::spawn({
        let telemetry = Arc::clone(&telemetry);
        async move {
            while let Some(event) = events.next().await {
                match event {
                    PathEvent::Selected { remote_addr, .. } => {
                        let mut t = telemetry.lock().unwrap();
                        t.last_selected_is_relay = remote_addr.is_relay();
                        if !remote_addr.is_relay() && t.first_direct.is_none() {
                            t.first_direct = Some(t_start.elapsed());
                        }
                        tracing::info!(addr = %remote_addr, "path selected");
                    }
                    PathEvent::Opened { remote_addr, .. } => {
                        tracing::info!(addr = %remote_addr, "path opened");
                    }
                    _ => {}
                }
            }
        }
    });

    let (send, recv) = conn.accept_bi().await.context("accept_bi failed")?;
    // A transfer error after a direct path was established must not turn the
    // run into a direct-connection failure, so the echo outcome carries the
    // observed telemetry either way.
    let result =
        match echo_and_sample(&conn, &telemetry, send, recv, t_start, network_profile).await {
            Ok(result) => result,
            Err(err) => {
                let mut r = common::new_result(
                    format!("baseline-accept-{}", run_suffix()),
                    "baseline",
                    network_profile,
                );
                let fallback_relay = telemetry.lock().unwrap().last_selected_is_relay;
                let (first_direct, _rtt, selected_is_relay) =
                    snapshot_path_state(&conn, &telemetry, fallback_relay);
                r.direct_connection_success = Some(first_direct.is_some() || !selected_is_relay);
                r.time_to_direct_ms = first_direct.map(|d| d.as_millis() as u64);
                r.selected_path = Some(if selected_is_relay {
                    SelectedPath::Relay
                } else {
                    SelectedPath::DirectIp
                });
                r.failure_reason = Some(format!("{err:#}"));
                r
            }
        };
    watcher.abort();

    endpoint.close().await;
    Ok(result)
}

/// Sample the live path snapshot plus the event telemetry: first-direct
/// timing, the selected path's RTT, and whether it is a relay.
/// `fallback_relay` is used when no path was ever selected (dialer assumes
/// relay, acceptor tracks its last event).
fn snapshot_path_state(
    conn: &iroh::endpoint::Connection,
    telemetry: &Mutex<PathTelemetry>,
    fallback_relay: bool,
) -> (Option<Duration>, Option<Duration>, bool) {
    let first_direct = telemetry.lock().unwrap().first_direct;
    let (selected_rtt, selected_is_relay) = match conn.paths().iter().find(|p| p.is_selected()) {
        Some(p) => (Some(p.rtt()), p.is_relay()),
        None => (None, fallback_relay),
    };
    (first_direct, selected_rtt, selected_is_relay)
}

/// Echo the payload and wait out the migration window, building the
/// success-path result.
async fn echo_and_sample(
    conn: &iroh::endpoint::Connection,
    telemetry: &Mutex<PathTelemetry>,
    mut send: iroh::endpoint::SendStream,
    mut recv: iroh::endpoint::RecvStream,
    t_start: Instant,
    network_profile: &str,
) -> anyhow::Result<ExperimentResult> {
    // Echo loop: read everything the dialer sends, write it back.
    let mut echoed: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    while echoed < TEST_PAYLOAD_BYTES as u64 {
        let n = recv.read(&mut buf).await?.context("stream ended early")?;
        if n == 0 {
            break;
        }
        send.write_all(&buf[..n]).await?;
        echoed += n as u64;
    }
    send.finish()?;
    println!("ECHOED_BYTES={echoed}");
    let elapsed = t_start.elapsed();

    // Wait for the direct migration instead of a fixed sleep: a fixed window
    // would truncate observation on high-latency / lossy profiles and bias
    // direct-success rates. Stop as soon as a direct path was selected, the
    // connection closed, or the experiment-wide timeout expired.
    loop {
        let done = {
            let t = telemetry.lock().unwrap();
            !t.last_selected_is_relay
                || t.first_direct.is_some()
                || conn.close_reason().is_some()
                || t_start.elapsed() > DIRECT_MIGRATION_TIMEOUT
        };
        if done {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let stats = conn.stats();
    // Final state comes from the live path snapshot; the event snapshot is
    // only used for transition timing. If the direct path was selected before
    // the watcher subscribed, or its event is still pending, the live snapshot
    // still reports it correctly.
    let fallback_relay = telemetry.lock().unwrap().last_selected_is_relay;
    let (first_direct, selected_rtt, selected_is_relay) =
        snapshot_path_state(conn, telemetry, fallback_relay);
    if let Some(ms) = first_direct {
        println!("TIME_TO_DIRECT_MS={}", ms.as_millis());
    }

    let mut result = common::new_result(
        format!("baseline-accept-{}", run_suffix()),
        "baseline",
        network_profile,
    );
    result.direct_connection_success = Some(first_direct.is_some() || !selected_is_relay);
    result.time_to_direct_ms = first_direct.map(|d| d.as_millis() as u64);
    result.selected_path = Some(if selected_is_relay {
        SelectedPath::Relay
    } else {
        SelectedPath::DirectIp
    });
    result.payload_bytes = echoed;
    let secs = elapsed.as_secs_f64().max(0.001);
    result.media_throughput_mbps = Some(echoed as f64 * 8.0 / secs / 1_000_000.0);

    println!(
        "RTT_MS={}",
        selected_rtt.map(|d| d.as_millis()).unwrap_or(0)
    );
    println!("ELAPSED_SECS={:.3}", elapsed.as_secs_f64());
    println!(
        "UDP_TX_DATAGRAMS={} UDP_RX_DATAGRAMS={}",
        stats.udp_tx.datagrams, stats.udp_rx.datagrams
    );

    Ok(result)
}

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}
