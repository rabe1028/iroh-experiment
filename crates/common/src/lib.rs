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
    // --- direct connection ---
    pub direct_connection_success: bool,
    pub time_to_direct_ms: Option<u64>,
    pub selected_path: Option<SelectedPath>,
    // --- relay traffic ---
    pub relay_control_tx_bytes: u64,
    pub relay_control_rx_bytes: u64,
    pub relay_media_tx_bytes: u64,
    pub relay_media_rx_bytes: u64,
    // --- media / payload ---
    pub payload_bytes: u64,
    pub throughput_mbps: Option<f64>,
    pub failure_reason: Option<String>,
}

/// Build a fresh result for a run with all counters at zero / unknown.
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
        direct_connection_success: false,
        time_to_direct_ms: None,
        selected_path: None,
        relay_control_tx_bytes: 0,
        relay_control_rx_bytes: 0,
        relay_media_tx_bytes: 0,
        relay_media_rx_bytes: 0,
        payload_bytes: 0,
        throughput_mbps: None,
        failure_reason: None,
    }
}

mod rfc3339 {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
        let secs = t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        s.serialize_u64(secs)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(u64::deserialize(d)?))
    }
}
