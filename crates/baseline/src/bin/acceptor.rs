//! Baseline acceptor: Endpoint A (LAN gateway role).
//!
//! Binds an iroh endpoint with the default relay configuration, prints its
//! EndpointId + addresses, accepts one baseline echo connection, echoes all
//! bytes back, and records path telemetry (plan E0 / PR 1).

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use common::{ExperimentResult, SelectedPath, TEST_PAYLOAD_BYTES};
use iroh::endpoint::PathEvent;
use tokio_stream::StreamExt;

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
    let outcome = runtime.block_on(run());

    // Persist a result line even on failure (with failure_reason set).
    let result = match outcome {
        Ok(r) => r,
        Err(e) => {
            let mut r = common::new_result(
                format!("baseline-accept-{}", run_suffix()),
                "baseline",
                "unspecified",
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

async fn run() -> Result<ExperimentResult> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    endpoint.set_alpns(vec![common::BASELINE_ALPN.to_vec()]);
    println!("ENDPOINT_ID={}", endpoint.id());
    for addr in endpoint.addr().addrs {
        println!("ADDR={addr}");
    }

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

    // Echo loop: read everything the dialer sends, write it back.
    let (mut send, mut recv) = conn.accept_bi().await.context("accept_bi failed")?;
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

    // Give late direct migration a brief window, then read the snapshot.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (first_direct, last_selected_is_relay) = {
        let t = telemetry.lock().unwrap();
        (t.first_direct, t.last_selected_is_relay)
    };
    watcher.abort();

    let stats = conn.stats();
    // Extract owned values from the borrowed path snapshot.
    let (selected_rtt, _selected_is_relay) = {
        let paths = conn.paths();
        match paths.iter().find(|p| p.is_selected()) {
            Some(p) => (Some(p.rtt()), p.is_relay()),
            None => (None, true),
        }
    };
    if let Some(ms) = first_direct {
        println!("TIME_TO_DIRECT_MS={}", ms.as_millis());
    }

    let mut result =
        common::new_result(format!("baseline-accept-{}", run_suffix()), "baseline", "unspecified");
    result.direct_connection_success = !last_selected_is_relay;
    result.time_to_direct_ms = first_direct.map(|d| d.as_millis() as u64);
    result.selected_path = Some(if last_selected_is_relay {
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
