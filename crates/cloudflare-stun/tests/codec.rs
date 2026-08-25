//! Codec unit tests: RFC 8489 wire format, transaction-id authentication,
//! and XOR-MAPPED-ADDRESS decoding for both address families.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use cloudflare_stun::{
    encode_binding_error, encode_binding_request, encode_binding_success, is_stun_packet,
    parse_binding_response, TransactionId, BINDING_REQUEST, MAGIC_COOKIE,
};

/// RFC-style deterministic vector: Binding Request with an all-zero txn.
#[test]
fn binding_request_wire_format() {
    let txn = TransactionId::from_bytes([0u8; 12]);
    let buf = encode_binding_request(&txn);
    assert_eq!(buf.len(), 20);
    assert_eq!(&buf[0..2], &BINDING_REQUEST.to_be_bytes());
    assert_eq!(&buf[2..4], &[0, 0]); // no attributes
    assert_eq!(&buf[4..8], &MAGIC_COOKIE.to_be_bytes());
    assert_eq!(&buf[8..20], &[0u8; 12]);
    // STUN keeps the top two bits of byte 0 zero (demux contract).
    assert!(buf[0] & 0xc0 == 0);
}

#[test]
fn success_response_decodes_v4() {
    let txn = TransactionId::random();
    let observed = SocketAddr::from((Ipv4Addr::new(203, 0, 113, 20), 53124));
    let buf = encode_binding_success(observed, &txn);

    let parsed = parse_binding_response(&buf, &txn).expect("parses");
    assert_eq!(parsed.xor_mapped_address, observed);
}

#[test]
fn success_response_decodes_v6() {
    let txn = TransactionId::random();
    let observed = SocketAddr::from((
        Ipv6Addr::new(0x2001, 0x0db8, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6),
        42424,
    ));
    let buf = encode_binding_success(observed, &txn);

    let parsed = parse_binding_response(&buf, &txn).expect("parses");
    assert_eq!(parsed.xor_mapped_address, observed);
}

#[test]
fn mismatched_transaction_id_is_rejected() {
    let real = TransactionId::random();
    let forged = TransactionId::random();
    let observed = SocketAddr::from((Ipv4Addr::LOCALHOST, 1234));
    let buf = encode_binding_success(observed, &forged);

    let err = parse_binding_response(&buf, &real).expect_err("must reject");
    assert!(err.to_string().contains("transaction id"));
}

#[test]
fn error_response_surfaces_code_and_reason() {
    let txn = TransactionId::random();
    let buf = encode_binding_error(420, "Unknown Attribute", &txn);
    let err = parse_binding_response(&buf, &txn).expect_err("error response");
    assert!(err.to_string().contains("420"));
    assert!(err.to_string().contains("Unknown Attribute"));
}

#[test]
fn error_from_wrong_transaction_is_rejected() {
    let real = TransactionId::random();
    let other = TransactionId::random();
    let buf = encode_binding_error(500, "oops", &other);
    let err = parse_binding_response(&buf, &real).expect_err("must reject");
    // Must be rejected as unauthenticated, not surfaced as a server error.
    assert!(err.to_string().contains("transaction id"));
}

#[test]
fn garbage_and_short_buffers_are_rejected() {
    let txn = TransactionId::random();
    for bad in [
        &[][..],
        &[0u8; 10][..],
        // QUIC long-header-looking first byte must never parse as STUN.
        &[0xc3, 0x00, 0x00, 0x00, 0x01][..],
    ] {
        assert!(
            parse_binding_response(bad, &txn).is_err(),
            "expected rejection of {} bytes",
            bad.len()
        );
    }
}

/// Attribute walking must skip padding after odd-length values and still
/// find a later XOR-MAPPED-ADDRESS.
#[test]
fn attributes_with_padding_are_skipped() {
    let txn = TransactionId::random();
    let observed = SocketAddr::from((Ipv4Addr::new(198, 51, 100, 7), 9999));
    let mut buf = encode_binding_success(observed, &txn);

    // Splice a 3-byte SOFTWARE-like attribute (type 0x8022) in front of the
    // XOR-MAPPED-ADDRESS; its value pads to 4 bytes.
    let header_len = 20;
    let attr: [u8; 8] = [0x80, 0x22, 0x00, 0x03, b'a', b'b', b'c', 0x00];
    let old_tail = buf.split_off(header_len);
    buf.extend_from_slice(&attr);
    buf.extend_from_slice(&old_tail);

    // The spliced attribute extends the message; patch the message-length
    // field accordingly.
    let msg_len = u16::from_be_bytes([buf[2], buf[3]]) + attr.len() as u16;
    buf[2..4].copy_from_slice(&msg_len.to_be_bytes());

    let parsed = parse_binding_response(&buf, &txn).expect("parses past padding");
    assert_eq!(parsed.xor_mapped_address, observed);
}

#[test]
fn demux_classifier_separates_stun_from_quic() {
    let txn = TransactionId::random();
    let stun = encode_binding_request(&txn);
    assert!(is_stun_packet(&stun));
    assert!(is_stun_packet(&[0u8; 20]));

    // QUIC version-negotiation long header starts with 0xc? (fixed bit set).
    assert!(!is_stun_packet(&[0xc3, 0x00, 0x00, 0x00, 0x01]));
    // QUIC short header has fixed bit + one spin/reserved bit pattern.
    assert!(!is_stun_packet(&[0x40 | 0x03, 0x00, 0x00, 0x00, 0x01]));
    assert!(!is_stun_packet(&[]));
    assert!(!is_stun_packet(&[0u8; 19]));
}

/// The encoder/decoder pair must agree on the XOR transform even though the
/// test cannot know the peer's mapping in advance.
#[test]
fn xor_roundtrip_preserves_port_range() {
    for port in [1u16, 53, 3478, 443, 65535] {
        let txn = TransactionId::random();
        let observed = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        let buf = encode_binding_success(observed, &txn);
        let parsed = parse_binding_response(&buf, &txn).unwrap();
        assert_eq!(parsed.xor_mapped_address.port(), port);
    }
}

/// Sanity guard so tests never silently grow multi-second runtimes.
#[test]
fn default_probe_budget_stays_small() {
    let cfg = cloudflare_stun::probe::ProbeConfig::default();
    assert_eq!(cfg.attempts, 3);
    assert_eq!(cfg.attempt_timeout, Duration::from_secs(2));
}
