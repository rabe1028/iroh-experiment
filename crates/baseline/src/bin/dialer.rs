//! Baseline dialer: Endpoint B (remote client role).
//!
//! Dials the acceptor by EndpointId, opens a bidirectional stream, sends 10 MiB
//! of random data, verifies the echo, and records path telemetry (relay ->
//! direct transition timing) as a JSON result line (plan E0 / PR 1).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use common::{
    new_result, ExperimentResult, BASELINE_ALPN, SelectedPath, TEST_PAYLOAD_BYTES,
};
use iroh::endpoint::PathEvent;
use iroh::EndpointId;
use rand::RngCore;
use tokio_stream::StreamExt;

/// Shared telemetry snapshot written by the path-event watcher task.
#[derive(Default)]
struct PathTelemetry {
    /// Time of the first `Selected` event on a non-relay path.
    first_direct: Option<Duration>,
}

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
    let outcome = runtime.block_on(run(&args));

    let result = match outcome {
        Ok(r) => r,
        Err(e) => {
            let mut r = new_result(
                format!("baseline-dial-{}", run_suffix()),
                "baseline",
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
    let t_dial = Instant::now();

    let conn = endpoint
        .connect(target, BASELINE_ALPN)
        .await
        .context("connect failed")?;
    println!("CONNECTED_AT_MS={}", t_dial.elapsed().as_millis());

    // Watch path events from before any data is transferred; PathEventStream
    // is 'static so the watcher runs in a spawned task. The snapshot mutex
    // keeps observed values even though the stream stays pending on a live
    // connection.
    let mut events = conn.path_events();
    let t_events = Instant::now();
    let telemetry = Arc::new(Mutex::new(PathTelemetry::default()));
    let watcher = tokio::spawn({
        let telemetry = Arc::clone(&telemetry);
        async move {
            while let Some(event) = events.next().await {
                if let PathEvent::Selected { remote_addr, .. } = event {
                    if !remote_addr.is_relay() {
                        let mut t = telemetry.lock().unwrap();
                        if t.first_direct.is_none() {
                            t.first_direct = Some(t_events.elapsed());
                        }
                    }
                    tracing::info!(addr = %remote_addr, "path selected");
                }
            }
        }
    });

    let (mut send, mut recv) = conn.open_bi().await.context("open_bi failed")?;

    // Send TEST_PAYLOAD_BYTES of random data in chunks; verify echo.
    let t_start = Instant::now();
    let mut sent: u64 = 0;
    let mut echoed: u64 = 0;
    let mut tx_buf = vec![0u8; 64 * 1024];
    let mut rx_buf = vec![0u8; 64 * 1024];
    while sent < TEST_PAYLOAD_BYTES as u64 {
        let n = std::cmp::min(tx_buf.len(), TEST_PAYLOAD_BYTES - sent as usize);
        rand::thread_rng().fill_bytes(&mut tx_buf[..n]);
        send.write_all(&tx_buf[..n]).await?;
        sent += n as u64;

        recv.read_exact(&mut rx_buf[..n])
            .await
            .context("echo read failed")?;
        if rx_buf[..n] != tx_buf[..n] {
            anyhow::bail!("echo mismatch at offset {sent}");
        }
        echoed += n as u64;
    }
    send.finish()?;
    drop(recv);
    let elapsed = t_start.elapsed();

    // Give late direct migration a brief window, then read the snapshot.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first_direct = telemetry.lock().unwrap().first_direct;
    watcher.abort();

    let stats = conn.stats();
    // Extract owned values from the borrowed path snapshot.
    let (selected_rtt, selected_is_relay) = {
        let paths = conn.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(p) => (Some(p.rtt()), p.is_relay()),
            None => (None, true),
        }
    };

    println!("SENT_BYTES={sent}");
    println!("ECHOED_BYTES={echoed}");
    println!("ELAPSED_SECS={:.3}", elapsed.as_secs_f64());
    println!(
        "RTT_MS={}",
        selected_rtt.map(|d| d.as_millis()).unwrap_or(0)
    );
    if let Some(ms) = first_direct {
        println!("TIME_TO_DIRECT_MS={}", ms.as_millis());
    }

    let mut result = new_result(
        format!("baseline-dial-{}", run_suffix()),
        "baseline",
        &args.network_profile,
    );
    // A direct path may be established and later lost before sampling;
    // success means a direct path was observed at any point in the run.
    result.direct_connection_success = first_direct.is_some() || !selected_is_relay;
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
        "UDP_TX_DATAGRAMS={} UDP_RX_DATAGRAMS={}",
        stats.udp_tx.datagrams, stats.udp_rx.datagrams
    );

    endpoint.close().await;
    Ok(result)
}

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}
