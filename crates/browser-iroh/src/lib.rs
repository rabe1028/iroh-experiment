//! Browser iroh uses QUIC over a WebSocket relay; it cannot test UDP hole punching.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use anyhow::{anyhow, bail, ensure, Result};
use iroh::{
    endpoint::presets, endpoint::Connection, Endpoint, EndpointAddr, EndpointId, RelayMode,
};
use n0_future::time::{timeout, Duration};
use serde::Serialize;
use wasm_bindgen::prelude::*;

const RELAY: &str = "https://aps1-1.relay.n0.iroh.link/";
const BYTES: usize = 65_536;

#[wasm_bindgen]
pub struct BrowserNode {
    endpoint: Endpoint,
    alpn: Vec<u8>,
}

#[derive(Serialize)]
struct Path {
    selected: bool,
    relay: bool,
    ip: bool,
    address: String,
}

#[derive(Serialize)]
struct Measurement {
    engine: &'static str,
    iroh_version: &'static str,
    role: &'static str,
    endpoint_id: String,
    remote_id: String,
    payload_bytes: usize,
    echo_success: bool,
    paths_before: Vec<Path>,
    paths_after: Vec<Path>,
    relay_only_observed: bool,
    direct_connection_tested: bool,
    nat_traversal_tested: bool,
    nat_traversal_success: Option<bool>,
    same_socket_stun_h3_tested: bool,
}

fn paths(connection: &Connection) -> Vec<Path> {
    connection
        .paths()
        .iter()
        .map(|p| Path {
            selected: p.is_selected(),
            relay: p.is_relay(),
            ip: p.is_ip(),
            address: p.remote_addr().to_string(),
        })
        .collect()
}

fn payload(alpn: &[u8]) -> Vec<u8> {
    (0..BYTES)
        .map(|i| (i as u8).wrapping_add(alpn[i % alpn.len()]))
        .collect()
}

fn js_error(error: impl std::fmt::Display) -> JsError {
    JsError::new(&error.to_string())
}

#[wasm_bindgen]
impl BrowserNode {
    /// A fresh, un-published identity and a capability-specific protocol per room.
    pub async fn start(capability: String) -> Result<BrowserNode, JsError> {
        if capability.len() != 32 || !capability.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(js_error("invalid room capability"));
        }
        let alpn = format!("iroh-experiment/browser-echo/1/{capability}").into_bytes();
        let relay = RELAY.parse::<iroh::RelayUrl>().map_err(js_error)?;
        let endpoint = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Custom(relay.into()))
            .alpns(vec![alpn.clone()])
            .bind()
            .await
            .map_err(js_error)?;
        if timeout(Duration::from_secs(25), endpoint.online())
            .await
            .is_err()
        {
            endpoint.close().await;
            return Err(js_error("iroh relay connection timed out"));
        }
        Ok(Self { endpoint, alpn })
    }

    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    pub async fn close(&self) {
        self.endpoint.close().await;
    }

    pub async fn listen_once(&self) -> Result<String, JsError> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| js_error("endpoint closed"))?;
        timeout(Duration::from_secs(60), async {
            let conn = incoming.await?;
            let before = paths(&conn);
            let result = self.accept_echo(&conn).await;
            self.measurement(&conn, "listen", before, result)
        })
        .await
        .map_err(js_error)?
        .map_err(js_error)
    }

    pub async fn connect_echo(&self, endpoint_id: String) -> Result<String, JsError> {
        let id = endpoint_id.parse::<EndpointId>().map_err(js_error)?;
        if id == self.endpoint.id() {
            return Err(js_error("cannot connect to self"));
        }
        let relay = RELAY.parse().map_err(js_error)?;
        let addr = EndpointAddr::new(id).with_relay_url(relay);
        timeout(Duration::from_secs(60), async {
            let conn = self.endpoint.connect(addr, &self.alpn).await?;
            ensure!(conn.remote_id() == id, "unexpected authenticated peer");
            let before = paths(&conn);
            let result = self.dial_echo(&conn).await;
            self.measurement(&conn, "dial", before, result)
        })
        .await
        .map_err(js_error)?
        .map_err(js_error)
    }
}

impl BrowserNode {
    async fn dial_echo(&self, conn: &Connection) -> Result<()> {
        let expected = payload(&self.alpn);
        let (mut send, mut recv) = conn.open_bi().await?;
        send.write_all(&expected).await?;
        send.finish()?;
        ensure!(
            recv.read_to_end(BYTES).await? == expected,
            "echo content mismatch"
        );
        let mut ack = conn.open_uni().await?;
        ack.write_all(b"verified").await?;
        ack.finish()?;
        ensure!(ack.stopped().await?.is_none(), "peer rejected verification");
        Ok(())
    }

    async fn accept_echo(&self, conn: &Connection) -> Result<()> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let data = recv.read_to_end(BYTES).await?;
        ensure!(data == payload(&self.alpn), "request content mismatch");
        send.write_all(&data).await?;
        send.finish()?;
        ensure!(send.stopped().await?.is_none(), "peer rejected echo");
        let mut ack = conn.accept_uni().await?;
        ensure!(
            ack.read_to_end(8).await? == b"verified",
            "missing content verification"
        );
        Ok(())
    }

    fn measurement(
        &self,
        conn: &Connection,
        role: &'static str,
        before: Vec<Path>,
        result: Result<()>,
    ) -> Result<String> {
        let after = paths(conn);
        let relay_only = [&before, &after].into_iter().all(|list| {
            !list.is_empty()
                && list.iter().any(|p| p.selected && p.relay)
                && list.iter().all(|p| p.relay && !p.ip)
        });
        if let Err(error) = result {
            conn.close(1u32.into(), b"echo failed");
            return Err(error);
        }
        if !relay_only {
            bail!("expected selected relay paths before and after echo");
        }
        serde_json::to_string(&Measurement {
            engine: "iroh-wasm",
            iroh_version: "1.0.3",
            role,
            endpoint_id: self.endpoint.id().to_string(),
            remote_id: conn.remote_id().to_string(),
            payload_bytes: BYTES,
            echo_success: true,
            paths_before: before,
            paths_after: after,
            relay_only_observed: relay_only,
            direct_connection_tested: false,
            nat_traversal_tested: false,
            nat_traversal_success: None,
            same_socket_stun_h3_tested: false,
        })
        .map_err(|error| anyhow!(error))
    }
}
