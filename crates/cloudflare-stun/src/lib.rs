//! Minimal STUN (RFC 8489) subset for external-address discovery probes.
//!
//! Implements exactly what the experiment needs (plan E2 / PR 4): encode a
//! Binding Request, parse a Binding Response carrying XOR-MAPPED-ADDRESS or
//! ERROR-CODE, and validate the transaction id (plan section 19 requires
//! checking it before trusting a response).
//!
//! ## Same-socket status
//!
//! The plan's key requirement is probing from *the same UDP socket iroh uses*
//! (section 3.2), because different sockets can get different NAT mappings.
//! iroh 1.0.3 does not expose a packet-level demux hook on its UDP transport
//! (`EndpointHooks` are connection-level only; `unstable-custom-transports`
//! adds transports on their own sockets), so this crate currently runs on a
//! separate socket and every observation must carry that caveat
//! (section 20.1: destination-dependent NAT).
//!
//! The [`is_stun_packet`] classifier is provided so a future same-socket
//! integration (section 20.3 escalation path: noq-layer hook) can route
//! packets without changing this API: STUN messages start with their two top
//! bits zeroed while QUIC packets always have bit 3 (0x40) set.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use rand::RngCore;

pub mod probe;

/// STUN magic cookie (RFC 8489 section 5).
pub const MAGIC_COOKIE: u32 = 0x2112_a442;

/// Binding Request message type.
pub const BINDING_REQUEST: u16 = 0x0001;
/// Binding Response (success) message type.
pub const BINDING_SUCCESS: u16 = 0x0101;
/// Binding Error Response message type.
pub const BINDING_ERROR: u16 = 0x0111;

/// XOR-MAPPED-ADDRESS attribute type.
pub const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
/// ERROR-CODE attribute type.
pub const ATTR_ERROR_CODE: u16 = 0x0009;

/// Fixed header size: type(2) + len(2) + cookie(4) + txn(12).
const HEADER_LEN: usize = 20;
/// Attribute TLV header size: type(2) + len(2).
const ATTR_HEADER_LEN: usize = 4;

/// Random 96-bit transaction id identifying one request/response pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionId([u8; 12]);

impl TransactionId {
    pub fn random() -> Self {
        let mut id = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut id);
        Self(id)
    }

    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }
}

/// Encode a Binding Request carrying no attributes.
///
/// Layout: `type=0x0001 | msg-len=0 | magic cookie | transaction id`.
pub fn encode_binding_request(txn: &TransactionId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_LEN);
    buf.extend_from_slice(&BINDING_REQUEST.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    buf.extend_from_slice(txn.as_bytes());
    buf
}

/// Cheap packet classification shared by future same-socket demultiplexing:
/// true when the buffer plausibly starts a STUN message.
///
/// STUN keeps its first two bits zero (RFC 8489 section 4); QUIC always sets
/// bit 3 of its first byte (fixed bit, RFC 9000 section 17.2), so this never
/// misclassifies a QUIC datagram as STUN.
pub fn is_stun_packet(buf: &[u8]) -> bool {
    buf.len() >= HEADER_LEN && buf[0] & 0xc0 == 0
}

/// One parsed failure reason carried by an ERROR-CODE attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StunError {
    /// Full code, e.g. `420` for Unknown Attributes.
    pub code: u16,
    /// UTF-8 reason phrase, trimmed; may be empty.
    pub reason: String,
}

/// A successfully parsed Binding Response.
#[derive(Debug, Clone)]
pub struct BindingResponse {
    /// The server-reflexive address, XOR-decoded.
    pub xor_mapped_address: SocketAddr,
}

/// Parse a Binding Request and return its transaction id.
///
/// Used by mock servers in tests; validates type, cookie, and length so the
/// mock only answers well-formed requests.
pub fn parse_binding_request(buf: &[u8]) -> Result<TransactionId> {
    anyhow::ensure!(buf.len() >= HEADER_LEN, "datagram too short");
    anyhow::ensure!(is_stun_packet(buf), "not a STUN packet");
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    anyhow::ensure!(
        msg_type == BINDING_REQUEST,
        "unexpected message type {msg_type:#06x}"
    );
    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    anyhow::ensure!(cookie == MAGIC_COOKIE, "magic cookie mismatch");
    Ok(TransactionId::from_bytes(
        buf[8..20].try_into().expect("header is 20 bytes"),
    ))
}

/// Parse and authenticate a Binding Response.
///
/// Rejects (rather than errors on) foreign traffic by design: any length,
/// type, cookie, or transaction-id mismatch returns an error, so callers can
/// drop the datagram and keep waiting for the real response.
pub fn parse_binding_response(buf: &[u8], expected_txn: &TransactionId) -> Result<BindingResponse> {
    anyhow::ensure!(
        buf.len() >= HEADER_LEN,
        "datagram shorter than STUN header ({})",
        buf.len()
    );
    anyhow::ensure!(is_stun_packet(buf), "not a STUN packet (top bits set)");

    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    anyhow::ensure!(
        msg_type == BINDING_SUCCESS || msg_type == BINDING_ERROR,
        "unexpected message type {msg_type:#06x}"
    );

    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    anyhow::ensure!(
        cookie == MAGIC_COOKIE,
        "magic cookie mismatch ({cookie:#010x})"
    );

    let txn = TransactionId::from_bytes(buf[8..20].try_into().unwrap());
    if msg_type == BINDING_ERROR {
        // Authenticate the error against our transaction before surfacing it,
        // so spoofed errors cannot terminate a live probe.
        anyhow::ensure!(
            txn == *expected_txn,
            "transaction id mismatch on error response"
        );
        let err = parse_error_code(&buf[HEADER_LEN..HEADER_LEN + msg_len])
            .context("no ERROR-CODE attribute in error response")?;
        anyhow::bail!("server returned error {err:?}");
    }
    anyhow::ensure!(
        txn == *expected_txn,
        "transaction id mismatch (spoofed or stale response)"
    );

    let attrs = &buf[HEADER_LEN..HEADER_LEN + msg_len];
    let addr = parse_xor_mapped_address(attrs, &txn)
        .context("no valid XOR-MAPPED-ADDRESS in success response")?;
    Ok(BindingResponse {
        xor_mapped_address: addr,
    })
}

fn parse_xor_mapped_address(mut attrs: &[u8], txn: &TransactionId) -> Option<SocketAddr> {
    loop {
        if attrs.len() < ATTR_HEADER_LEN {
            return None;
        }
        let attr_type = u16::from_be_bytes([attrs[0], attrs[1]]);
        let attr_len = u16::from_be_bytes([attrs[2], attrs[3]]) as usize;
        let value_end = ATTR_HEADER_LEN + attr_len;
        if attrs.len() < value_end {
            return None;
        }
        let value = &attrs[ATTR_HEADER_LEN..value_end];

        if attr_type == ATTR_XOR_MAPPED_ADDRESS {
            return decode_xor_mapped(value, txn);
        }

        // RFC 8489 section 14.15: values are padded to 4-byte alignment but
        // padding bytes are not counted in the attribute length.
        attrs = &attrs[value_end.next_multiple_of(4).min(attrs.len())..];
    }
}

fn decode_xor_mapped(value: &[u8], txn: &TransactionId) -> Option<SocketAddr> {
    if value.len() < 4 {
        return None;
    }
    let family = value[1];
    let xport = u16::from_be_bytes([value[2], value[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    match family {
        0x01 => {
            let ip = value.get(4..8)?;
            let octets: [u8; 4] = ip.try_into().ok()?;
            let decoded = u32::from_be_bytes(octets) ^ MAGIC_COOKIE;
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(decoded)), xport))
        }
        0x02 => {
            let ip = value.get(4..20)?;
            let octets: [u8; 16] = ip.try_into().ok()?;
            let mut decoded = octets;
            // v6 addresses are XORed with cookie || transaction id.
            for (byte, mask) in decoded
                .iter_mut()
                .zip(MAGIC_COOKIE.to_be_bytes().iter().chain(txn.as_bytes()))
            {
                *byte ^= mask;
            }
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(decoded)), xport))
        }
        _ => None,
    }
}

fn parse_error_code(mut attrs: &[u8]) -> Option<StunError> {
    loop {
        if attrs.len() < ATTR_HEADER_LEN {
            return None;
        }
        let attr_type = u16::from_be_bytes([attrs[0], attrs[1]]);
        let attr_len = u16::from_be_bytes([attrs[2], attrs[3]]) as usize;
        let value_end = ATTR_HEADER_LEN + attr_len;
        if attrs.len() < value_end {
            return None;
        }
        let value = &attrs[ATTR_HEADER_LEN..value_end];

        if attr_type == ATTR_ERROR_CODE && value.len() >= 4 {
            let class = (value[2] & 0x07) as u16;
            let number = value[3] as u16;
            let reason = String::from_utf8_lossy(&value[4..]).trim().to_string();
            return Some(StunError {
                code: class * 100 + number,
                reason,
            });
        }

        attrs = &attrs[value_end.next_multiple_of(4).min(attrs.len())..];
    }
}

/// Extract the [`StunError`] from a Binding *Error* Response, authenticating
/// it against `expected_txn` first.
///
/// `Ok(error)` means the server explicitly rejected *our* request (probing
/// should stop); `Err(_)` means this datagram is not an error response
/// addressed to us (callers may ignore it).
pub fn parse_authenticated_error(buf: &[u8], expected_txn: &TransactionId) -> Result<StunError> {
    anyhow::ensure!(buf.len() >= HEADER_LEN, "datagram too short");
    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
    anyhow::ensure!(msg_type == BINDING_ERROR, "not an error response");
    let cookie = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    anyhow::ensure!(cookie == MAGIC_COOKIE, "magic cookie mismatch");
    let txn = TransactionId::from_bytes(buf[8..20].try_into().unwrap());
    anyhow::ensure!(txn == *expected_txn, "transaction id mismatch");
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    parse_error_code(&buf[HEADER_LEN..HEADER_LEN + msg_len]).context("no ERROR-CODE attribute")
}

/// Build a Binding Success Response with XOR-MAPPED-ADDRESS — used by tests
/// and by the mock server to exercise the real client codec both ways.
pub fn encode_binding_success(observed: SocketAddr, txn: &TransactionId) -> Vec<u8> {
    let mut value = Vec::with_capacity(20);
    match observed.ip() {
        IpAddr::V4(ip) => {
            value.extend_from_slice(&[0x00, 0x01]);
            let port = observed.port() ^ (MAGIC_COOKIE >> 16) as u16;
            value.extend_from_slice(&port.to_be_bytes());
            value.extend_from_slice(&(u32::from(ip) ^ MAGIC_COOKIE).to_be_bytes());
        }
        IpAddr::V6(ip) => {
            value.extend_from_slice(&[0x00, 0x02]);
            let port = observed.port() ^ (MAGIC_COOKIE >> 16) as u16;
            value.extend_from_slice(&port.to_be_bytes());
            let octets = u128::from(ip).to_be_bytes();
            for (byte, mask) in octets
                .iter()
                .zip(MAGIC_COOKIE.to_be_bytes().iter().chain(txn.as_bytes()))
            {
                value.push(byte ^ mask);
            }
        }
    }

    let mut buf = Vec::with_capacity(HEADER_LEN + ATTR_HEADER_LEN + value.len());
    // Message length counts complete attributes (TLV headers included).
    buf.extend_from_slice(&BINDING_SUCCESS.to_be_bytes());
    let msg_len = (ATTR_HEADER_LEN + value.len()) as u16;
    buf.extend_from_slice(&msg_len.to_be_bytes());
    buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    buf.extend_from_slice(txn.as_bytes());
    // Attribute TLV header: type then length of the value only.
    buf.extend_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(&value);
    buf
}

/// Build a Binding Error Response with an ERROR-CODE attribute.
pub fn encode_binding_error(code: u16, reason: &str, txn: &TransactionId) -> Vec<u8> {
    let class = (code / 100) as u8;
    let number = (code % 100) as u8;
    let mut value = vec![0x00, 0x00, class & 0x07, number];
    value.extend_from_slice(reason.as_bytes());
    // RFC 5389 §15: attribute values are padded to a four-byte boundary.
    // The padding counts toward the message length but not toward the
    // attribute's own value length; skipping it makes the message length a
    // non-multiple of four, which compliant peers reject.
    let unpadded = value.len();
    value.resize(unpadded.div_ceil(4) * 4, 0);

    let mut buf = Vec::with_capacity(HEADER_LEN + ATTR_HEADER_LEN + value.len());
    buf.extend_from_slice(&BINDING_ERROR.to_be_bytes());
    let msg_len = (ATTR_HEADER_LEN + value.len()) as u16;
    buf.extend_from_slice(&msg_len.to_be_bytes());
    buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    buf.extend_from_slice(txn.as_bytes());
    buf.extend_from_slice(&ATTR_ERROR_CODE.to_be_bytes());
    buf.extend_from_slice(&(unpadded as u16).to_be_bytes());
    buf.extend_from_slice(&value);
    buf
}

/// Current unix time in whole milliseconds, for `observed_at` stamps.
pub fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
