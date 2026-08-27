//! H3 probe CLI: one observation against a Cloudflare-proxied host
//! (plan E3 / PR 5).
//!
//! Prints the JSON observation; optionally compares against a STUN probe
//! result file and prints the plan E3 comparison verdict.

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use cloudflare_h3_observer::{Comparison, H3Observation};
use common::new_result;

#[derive(Parser)]
struct Args {
    /// Proxied hostname to probe (e.g. observe.example.invalid).
    host: String,
    /// Path carrying the observation headers.
    #[arg(long, default_value = "/observe")]
    path: String,
    /// Port for the HTTPS/QUIC connection.
    #[arg(long, default_value_t = 443)]
    port: u16,
    /// Probe timeout.
    #[arg(long, default_value_t = 10)]
    timeout_secs: u64,
    /// File to append a JSON result line to (JSONL).
    #[arg(long)]
    results: String,
    /// Network profile label recorded in the result.
    #[arg(long, default_value = "unspecified")]
    network_profile: String,
    /// Optional STUN observation JSON (from stun-probe) to compare against.
    #[arg(long)]
    compare_stun_json: Option<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();
    let args = Args::parse();

    let runtime = tokio::runtime::Runtime::new()?;
    let outcome = runtime.block_on(run(&args));

    let mut result = new_result(
        format!("h3-obs-{}", run_suffix()),
        "cloudflare-h3",
        &args.network_profile,
    );
    match outcome {
        Ok((obs, comparison)) => {
            // The comparison reference is the STUN probe; record it only
            // when a comparison actually happened, so an un-referenced run
            // does not claim one (plan E3).
            result.reference_method = comparison.map(|_| "cloudflare-stun".to_string());
            result.observed_ip_equal = comparison.map(|c| {
                matches!(
                    c,
                    Comparison::SameIpSamePort
                        | Comparison::SameIpDifferentPort
                        | Comparison::SameIpPortMissing
                )
            });
            result.probe_latency_ms = Some(obs.duration.as_millis() as u64);
            println!("OBSERVATION={}", serde_json::to_string(&obs)?);
            if let Some(c) = comparison {
                println!("COMPARISON={}", serde_json::to_string(&c)?);
            }
        }
        Err(e) => {
            result.failure_reason = Some(format!("{e:#}"));
        }
    }
    common::append_result_line(&args.results, &result)?;
    if let Some(reason) = result.failure_reason.clone() {
        anyhow::bail!("run failed: {reason}");
    }
    Ok(())
}

async fn run(args: &Args) -> Result<(H3Observation, Option<Comparison>)> {
    let obs = cloudflare_h3_observer::observe(
        &args.host,
        &args.path,
        args.port,
        Duration::from_secs(args.timeout_secs),
    )
    .await
    .context("H3 observation failed")?;

    let mut comparison = None;
    if let Some(stun_path) = &args.compare_stun_json {
        let stun: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(stun_path).context("read STUN json")?)
                .context("parse STUN json")?;
        let addr: std::net::SocketAddr = stun
            .get("observed_addr")
            .and_then(|v| v.as_str())
            .context("STUN json lacks observed_addr")?
            .parse()
            .context("parse observed_addr")?;
        comparison = Some(cloudflare_h3_observer::compare(addr, &obs)?);
    }
    Ok((obs, comparison))
}

fn run_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
        .to_string()
}
