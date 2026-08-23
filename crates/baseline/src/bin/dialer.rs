//! Baseline dialer: Endpoint B (remote client role).
//!
//! Dials the acceptor by EndpointId, opens a bidirectional stream, sends 10 MiB
//! of random data, verifies the echo, and records path telemetry (relay ->
//! direct transition timing) as a JSON result line (plan E0 / PR 1).

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use common::{new_result, ExperimentResult, BASELINE_ALPN, SelectedPath, TEST_PAYLOAD_BYTES};
use iroh::endpoint::PathEvent;
use tokio_stream::StreamExt;
use iroh::EndpointId;
use rand::RngCore;

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
    let outcome = runtime.block_on(run(&args.id));

    let result = match outcome {
        Ok(r) => r,
        Err(e) => {
            let mut r =
                new_result(format!("baseline-dial-{}", run_suffix()), "baseline", &args.network_profile);
            r.failure_reason = Some(format!("{e:#}"));
            r
        }
    };
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.results)?;
    writeln!(f, "{}", serde_json::to_string(&result)?)?;

    if let Some(reason) = result.failure_reason.clone() {
        anyhow::bail!("run failed: {reason}");
    }
    Ok(())
}

async fn run(id: &str) -> Result<ExperimentResult> {
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
        .bind()
        .await
        .context("failed to bind iroh endpoint")?;

    let target: EndpointId = id.parse().context("invalid EndpointId")?;
    let t_dial = Instant::now();

    let conn = endpoint.connect(target, BASELINE_ALPN).await.context("connect failed")?;
    println!("CONNECTED_AT_MS={}", t_dial.elapsed().as_millis());

    // Collect path events while transferring; PathEventStream is 'static.
    let mut events = conn.path_events();
    let t_events = Instant::now();
    let event_task = tokio::spawn(async move {
        let mut first_direct: Option<Duration> = None;
        while let Some(event) = events.next().await {
            if let PathEvent::Selected { remote_addr, .. } = event {
                if !remote_addr.is_relay() && first_direct.is_none() {
                    first_direct = Some(t_events.elapsed());
                }
            }
        }
        first_direct
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

        recv.read_exact(&mut rx_buf[..n]).await.context("echo read failed")?;
        if rx_buf[..n] != tx_buf[..n] {
            anyhow::bail!("echo mismatch at offset {sent}");
        }
        echoed += n as u64;
    }
    send.finish()?;
    drop(recv);
    let elapsed = t_start.elapsed();

    // Wait briefly for remaining path events (e.g. late direct migration),
    // then take the final path snapshot.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first_direct: Option<Duration> = if event_task.is_finished() {
        event_task.await.unwrap_or(None)
    } else {
        event_task.abort();
        None
    };

    let stats = conn.stats();
    let selected_rtt = conn
        .paths()
        .iter()
        .filter(|p| p.is_selected())
        .map(|p| p.rtt())
        .next();

    println!("SENT_BYTES={sent}");
    println!("ECHOED_BYTES={echoed}");
    println!("ELAPSED_SECS={:.3}", elapsed.as_secs_f64());
    println!("RTT_MS={}", selected_rtt.map(|d| d.as_millis()).unwrap_or(0));

    let mut result =
        new_result(format!("baseline-dial-{}", run_suffix()), "baseline", "unspecified");
    // The dialer's own path view: use live paths snapshot for final state.
    let selected_is_relay = conn
        .paths()
        .iter()
        .filter(|p| p.is_selected())
        .map(|p| p.is_relay())
        .next()
        .unwrap_or(true);
    result.direct_connection_success = !selected_is_relay;
    result.time_to_direct_ms = first_direct.map(|d| d.as_millis() as u64);
    result.selected_path = Some(if selected_is_relay {
        SelectedPath::Relay
    } else {
        SelectedPath::DirectIp
    });
    result.payload_bytes = echoed;
    result.throughput_mbps = Some(echoed as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0);

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
