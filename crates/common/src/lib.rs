//! Shared types for the iroh NAT traversal experiment.
//!
//! Contains the result schema (plan section 11) and telemetry helpers.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// ALPN used by the baseline echo protocol (PR 1).
pub const BASELINE_ALPN: &[u8] = b"iroh-experiment/baseline-echo/0";

/// Amount of test data transferred in the baseline experiment (plan E0).
pub const TEST_PAYLOAD_BYTES: usize = 10 * 1024 * 1024; // 10 MiB

/// Selected path kind as reported by iroh connection type changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelectedPath {
    /// Direct IP path only.
    DirectIp,
    /// Relay path only.
    Relay,
    /// Both direct and relay paths are active (migration period).
    Mixed,
    /// No usable path.
    None,
}

/// One run of one experiment cell, following the result schema from plan
/// section 11. Public IPs are never stored; only comparison results.
///
/// Fields that this PR does not measure yet are `None` / `null`, so an absent
/// measurement is never mistaken for a measured zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentResult {
    pub schema_version: u32,
    pub run_id: String,
    #[serde(with = "rfc3339")]
    pub timestamp: SystemTime,
    pub git_revision: Option<String>,
    pub iroh_version: String,
    pub method: String,
    pub network_profile: String,
    // --- discovery (filled by later PRs) ---
    pub reference_method: Option<String>,
    pub observed_ip_equal: Option<bool>,
    pub observed_port_equal: Option<bool>,
    pub probe_latency_ms: Option<u64>,
    /// Whether both ends of this observation used the same kind of socket
    /// (false for standalone probes: NATs with per-destination mappings may
    /// show a different mapping than an iroh connection would get). Null
    /// when the run does not measure it.
    pub same_socket_as_iroh: Option<bool>,
    // --- direct connection ---
    /// Whether an iroh direct connection was established. Null for methods
    /// that do not attempt one (e.g. external-address probes), so an
    /// unattempted check is never mistaken for a measured failure.
    pub direct_connection_success: Option<bool>,
    pub time_to_direct_ms: Option<u64>,
    pub selected_path: Option<SelectedPath>,
    // --- relay traffic (measured from PR 3/6 onwards) ---
    pub relay_control_tx_bytes: Option<u64>,
    pub relay_control_rx_bytes: Option<u64>,
    pub relay_media_tx_bytes: Option<u64>,
    pub relay_media_rx_bytes: Option<u64>,
    // --- media / payload ---
    pub payload_bytes: u64,
    /// Throughput of the payload transfer, in Mbit/s. Matches the
    /// `media_throughput_mbps` field of the plan section 11 contract.
    pub media_throughput_mbps: Option<f64>,
    pub failure_reason: Option<String>,
}

/// Build a fresh result for a run with all counters unknown / unset.
pub fn new_result(
    run_id: impl Into<String>,
    method: &str,
    network_profile: &str,
) -> ExperimentResult {
    ExperimentResult {
        schema_version: 1,
        run_id: run_id.into(),
        timestamp: SystemTime::now(),
        git_revision: None,
        iroh_version: "1.0.3".to_string(),
        method: method.to_string(),
        network_profile: network_profile.to_string(),
        reference_method: None,
        observed_ip_equal: None,
        observed_port_equal: None,
        probe_latency_ms: None,
        same_socket_as_iroh: None,
        direct_connection_success: None,
        time_to_direct_ms: None,
        selected_path: None,
        relay_control_tx_bytes: None,
        relay_control_rx_bytes: None,
        relay_media_tx_bytes: None,
        relay_media_rx_bytes: None,
        payload_bytes: 0,
        media_throughput_mbps: None,
        failure_reason: None,
    }
}

mod rfc3339 {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        if *t < UNIX_EPOCH {
            return Err(serde::ser::Error::custom(
                "timestamp before unix epoch cannot be formatted as RFC 3339",
            ));
        }
        s.serialize_str(&humantime::format_rfc3339_seconds(*t).to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        let s = String::deserialize(d)?;
        humantime::parse_rfc3339(&s)
            .map(|st| UNIX_EPOCH + st.duration_since(UNIX_EPOCH).unwrap_or_default())
            .map_err(serde::de::Error::custom)
    }
}

/// Append one result as a JSONL line, creating the parent directory if needed.
pub fn append_result_line(path: &str, result: &ExperimentResult) -> anyhow::Result<()> {
    use std::io::Write;
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut line = serde_json::to_string(result)?;
    line.push('\n');
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    // One write_all call so concurrent processes appending to the same JSONL
    // file cannot interleave two records.
    f.write_all(line.as_bytes())?;
    Ok(())
}
