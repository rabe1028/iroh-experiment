//! Shared wiring for the tunnel integration tests: the in-memory "iroh
//! connection" pattern (duplex + spawned `serve_stream`) and service-map
//! construction. Lives in `tests/common/` so it is compiled into each test
//! binary via `mod common;` without becoming a test target itself.

use anyhow::{Context, Result};
use tcp_tunnel::{read_status, serve_stream, write_request, ServiceMap, TunnelStatus};
use tokio::io::DuplexStream;

/// One in-memory "iroh connection": runs the gateway-side handler on a
/// spawned task and hands back the client end after completing the handshake.
pub async fn connect_tunnel(services: &ServiceMap, service_id: &str) -> Result<DuplexStream> {
    let (mut client_end, mut gateway_end) = tokio::io::duplex(64 * 1024);
    let services = services.clone();
    tokio::spawn(async move {
        serve_stream(&mut gateway_end, &services).await.ok();
    });
    write_request(&mut client_end, service_id).await?;
    let status = read_status(&mut client_end).await?;
    anyhow::ensure!(status == TunnelStatus::Ok, "handshake failed");
    Ok(client_end)
}

/// Build a `name=host:port` service map from dynamic specs.
pub fn service_map(specs: &[String]) -> Result<ServiceMap> {
    ServiceMap::from_specs(specs.iter().map(String::as_str)).context("valid service specs")
}
