//! Baseline dialer: Endpoint B (remote client role).
//!
//! Dials the acceptor by EndpointId, opens a bidirectional stream, sends a
//! run-id header followed by 10 MiB of random data, verifies the echo, and
//! records path telemetry (relay -> direct transition timing) as a JSON
//! result line (plan E0 / PR 1).
//!
//! The run id is sent as the first bytes of the stream so the acceptor
//! records the same id; this correlates the two result rows of one physical
//! run. The send and receive halves run concurrently: waiting for the full
//! echo of each 64 KiB chunk would cap throughput at roughly 2 Mbit/s on the
//! plan's 250 ms RTT profile, measuring the stop-and-wait protocol instead
//! of the path.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use common::{
    new_result, ExperimentResult, BASELINE_ALPN, SelectedPath, TEST_PAYLOAD_BYTES,
};
use iroh::endpoint::{PathEvent, RecvStream, SendStream};
use iroh::EndpointId;
use rand::RngCore;
use tokio_stream::StreamExt;

/// Chunk size used by both halves of the transfer.
const CHUNK_BYTES: usize = 64 * 1024;

/// Shared telemetry snapshot written by the path-event watcher task.
#[derive(Default)]
struct PathTelemetry {
    /// Time of the first `Selected` event on a non-relay path,
    /// measured from dial start.
    first_direct: Option<Duration>,
}

/// Experiment-wide upper bound for waiting on a relay -> direct migration
/// after the payload transfer completes.
const DIRECT_MIGRATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
struct Args {
    /// Acceptor EndpointId (hex).
    id: String,
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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let runtime = tokio::runtime::Runtime::new()?;
    // run() reports failures after connect as failure_reason on a populated
    // result row, so only setup errors reach the fallback here.
    let outcome = runtime.block_on(run(&args));

    let result = match outcome {
        Ok(r) => r,
        Err(e) => {
            let mut r = new_result(
                format!("baseline-{}", run_suffix()),
                "baseline",
                "dialer",
                &args.network_profile,
            );
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

async fn run(args: &Args) -> Result<ExperimentResult> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    let target: EndpointId = args.id.parse().context("invalid EndpointId")?;
    let run_id = format!("baseline-{}", run_suffix());
    let t_dial = Instant::now();

    let conn = match endpoint.connect(target, BASELINE_ALPN).await {
        Ok(conn) => conn,
        Err(e) => {
            // A connect failure happens before any direct path can exist, so
            // a fresh result (direct_connection_success = false) is correct.
            endpoint.close().await;
            let mut r = new_result(run_id, "baseline", "dialer", &args.network_profile);
            r.failure_reason = Some(format!("connect failed: {e:#}"));
            return Ok(r);
        }
    };
    println!("CONNECTED_AT_MS={}", t_dial.elapsed().as_millis());

    // Watch path events from before any data is transferred; PathEventStream
    // is 'static so the watcher runs in a spawned task. The snapshot mutex
    // keeps observed values even though the stream stays pending on a live
    // connection. Timing is measured from the dial start (t_dial) so that
    // relay connection and QUIC handshake time are included in
    // time_to_direct_ms.
    let mut events = conn.path_events();
    let telemetry = Arc::new(Mutex::new(PathTelemetry::default()));
    let watcher = tokio::spawn({
        let telemetry = Arc::clone(&telemetry);
        async move {
            while let Some(event) = events.next().await {
                if let PathEvent::Selected { remote_addr, .. } = event {
                    if !remote_addr.is_relay() {
                        let mut t = telemetry.lock().unwrap();
                        if t.first_direct.is_none() {
                            t.first_direct = Some(t_dial.elapsed());
                        }
                    }
                    tracing::info!(addr = %remote_addr, "path selected");
                }
            }
        }
    });

    // A fast LAN can select the direct path during connect(), before the
    // event stream is subscribed. Seed the first observation from the live
    // snapshot so time_to_direct_ms is never lost; the seeded value is an
    // upper bound no later than the connect completion time.
    {
        let paths = conn.paths();
        if let Some(p) = paths.iter().find(|p| p.is_selected()) {
            if !p.is_relay() {
                telemetry.lock().unwrap().first_direct = Some(t_dial.elapsed());
            }
        }
    }

    // Wire layout: [u32 LE run-id length][run id][random payload]. The run id
    // lets the acceptor record the same identifier and pair the two rows of
    // this run; payload_bytes never counts the header.
    let mut wire = Vec::with_capacity(4 + run_id.len() + TEST_PAYLOAD_BYTES);
    wire.extend_from_slice(&(run_id.len() as u32).to_le_bytes());
    wire.extend_from_slice(run_id.as_bytes());
    let mut payload = vec![0u8; TEST_PAYLOAD_BYTES];
    rand::thread_rng().fill_bytes(&mut payload);
    wire.extend_from_slice(&payload);
    let header_len = wire.len() - TEST_PAYLOAD_BYTES;
    let wire = Arc::new(wire);

    // From here on, failures happen after the connection exists, so the
    // accumulated path telemetry must survive into the result row; a failed
    // transfer is not a direct-connection failure.
    let t_start = Instant::now();
    let mut echoed = 0usize;
    let failure = match conn.open_bi().await {
        Ok((send, recv)) => transfer(send, recv, Arc::clone(&wire), &mut echoed).await,
        Err(e) => Some(format!("open_bi failed: {e:#}")),
    };
    let elapsed = t_start.elapsed();

    // Wait for the direct migration instead of a fixed sleep: a fixed window
    // would truncate observation on high-latency / lossy profiles and bias
    // direct-success rates. Stop as soon as a direct path was observed, the
    // connection closed, or the experiment-wide timeout expired.
    let first_direct = loop {
        let current = telemetry.lock().unwrap().first_direct;
        if current.is_some()
            || conn.close_reason().is_some()
            || t_dial.elapsed() > DIRECT_MIGRATION_TIMEOUT
        {
            break current;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    watcher.abort();

    let stats = conn.stats();
    // Extract owned values from the borrowed path snapshot. A transport
    // failure can leave no selected path at the sampling instant; that is an
    // unknown final path (null), not a relay fallback.
    let (selected_rtt, selected_is_relay) = {
        let paths = conn.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(p) => (Some(p.rtt()), Some(p.is_relay())),
            None => (None, None),
        }
    };

    let payload_bytes = (echoed.saturating_sub(header_len)) as u64;
    println!("SENT_BYTES={}", wire.len());
    println!("ECHOED_BYTES={echoed}");
    println!("ELAPSED_SECS={:.3}", elapsed.as_secs_f64());
    println!(
        "RTT_MS={}",
        selected_rtt.map(|d| d.as_millis()).unwrap_or(0)
    );
    if let Some(ms) = first_direct {
        println!("TIME_TO_DIRECT_MS={}", ms.as_millis());
    }

    let mut result = new_result(run_id, "baseline", "dialer", &args.network_profile);
    // A direct path may be established and later lost before sampling;
    // success means a direct path was observed at any point in the run.
    result.direct_connection_success = first_direct.is_some() || selected_is_relay == Some(false);
    result.time_to_direct_ms = first_direct.map(|d| d.as_millis() as u64);
    result.selected_path = selected_is_relay.map(|is_relay| if is_relay {
        SelectedPath::Relay
    } else {
        SelectedPath::DirectIp
    });
    result.direct_path_rtt_ms = selected_rtt.map(|d| d.as_millis() as u64);
    result.payload_bytes = payload_bytes;
    // A failed transfer reports partial payload_bytes (how far it got) but no
    // throughput: the elapsed time of an aborted run does not measure the
    // path.
    if failure.is_none() {
        let secs = elapsed.as_secs_f64().max(0.001);
        result.media_throughput_mbps = Some(payload_bytes as f64 * 8.0 / secs / 1_000_000.0);
    }
    result.failure_reason = failure;

    println!(
        "UDP_TX_DATAGRAMS={} UDP_RX_DATAGRAMS={}",
        stats.udp_tx.datagrams, stats.udp_rx.datagrams
    );

    endpoint.close().await;
    Ok(result)
}

/// Send the whole wire payload while concurrently reading and verifying the
/// echo, updating `echoed` with the number of verified bytes. Keeping chunks
/// in flight is what makes `media_throughput_mbps` measure the path rather
/// than a stop-and-wait cycle.
async fn transfer(
    send: SendStream,
    mut recv: RecvStream,
    wire: Arc<Vec<u8>>,
    echoed: &mut usize,
) -> Option<String> {
    let send_task = tokio::spawn({
        let wire = Arc::clone(&wire);
        async move {
            let mut send = send;
            for chunk in wire.chunks(CHUNK_BYTES) {
                send.write_all(chunk).await?;
            }
            send.finish()?;
            Ok::<(), std::io::Error>(())
        }
    });

    let mut failure = None;
    let mut rx = vec![0u8; CHUNK_BYTES];
    while *echoed < wire.len() {
        let n = CHUNK_BYTES.min(wire.len() - *echoed);
        if let Err(e) = recv.read_exact(&mut rx[..n]).await {
            failure = Some(format!("echo read failed: {e:#}"));
            break;
        }
        if rx[..n] != wire[*echoed..*echoed + n] {
            failure = Some(format!("echo mismatch at offset {echoed}"));
            break;
        }
        *echoed += n;
    }
    if failure.is_none() {
        match send_task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failure = Some(format!("send failed: {e:#}")),
            Err(e) => failure = Some(format!("send task failed: {e}")),
        }
    } else {
        send_task.abort();
    }
    failure
}

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}
