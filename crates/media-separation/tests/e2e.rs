//! End-to-end test over loopback iroh endpoints (no external network).
//!
//! Verifies the full PR 6 flow: separate endpoint pairs, candidate exchange
//! over the control plane, a direct-only media connection, synthetic stream
//! transfer with sequence verification, and fail-closed stop when the direct
//! path dies.

use std::time::Duration;

use iroh::{endpoint::presets, EndpointAddr, RelayMode};
use media_separation::{
    request_candidates, run_receiver_session, run_sender_session, serve_candidates,
    unix_millis,
    validate_candidate, EndpointPair, GateState, SyntheticConfig, CONTROL_ALPN, MEDIA_ALPN,
};

/// Bind one offline control+media pair. The control endpoint uses the
/// Minimal preset (no discovery/relay) and explicit-address connects.
async fn bind_pair() -> anyhow::Result<EndpointPair> {
    let control = iroh::Endpoint::builder(presets::Minimal).bind().await?;
    let media = iroh::Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;
    Ok(EndpointPair { control, media })
}

fn cfg(frames_secs: u64) -> SyntheticConfig {
    // Small rate so CI machines keep up; still exercises the framing path.
    SyntheticConfig {
        bitrate_bps: 1_000_000,
        frame_payload_bytes: 1200,
        duration: Duration::from_secs(frames_secs),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn happy_path_streams_over_direct_only_media_endpoint() {
    let rx_pair = bind_pair().await.unwrap();
    let tx_pair = bind_pair().await.unwrap();
    rx_pair.control.set_alpns(vec![CONTROL_ALPN.to_vec()]);
    rx_pair.media.set_alpns(vec![MEDIA_ALPN.to_vec()]);
    tx_pair.control.set_alpns(vec![]);
    tx_pair.media.set_alpns(vec![]);

    // Sanity of plan section 5.2: media endpoints must have IP-only addrs.
    assert!(rx_pair.media.addr().addrs.iter().all(|a| a.is_ip()));

    let rx_control_addr = rx_pair
        .control
        .addr()
        .addrs
        .iter()
        .find_map(|a| match a {
            iroh::TransportAddr::Ip(sa) => Some(*sa),
            _ => None,
        })
        .unwrap();
    let rx_control_id = rx_pair.control.id();

    tokio::spawn(async move {
        let incoming = rx_pair.control.accept().await.unwrap().accept().unwrap();
        let conn = incoming.await.unwrap();
        let cands = rx_pair
            .media_direct_addrs()
            .into_iter()
            .map(|addr| {
                media_separation::DirectCandidate::local(
                    rx_pair.media.id(),
                    addr,
                    Duration::from_secs(30),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let token = media_separation::session_token();
        let (mut s, mut r) = conn.accept_bi().await.unwrap();
        serve_candidates(&mut r, &mut s, &cands, &token).await.unwrap();

        let started = unix_millis();
        let media_incoming = rx_pair.media.accept().await.unwrap().accept().unwrap();
        let media_conn = media_incoming.await.unwrap();
        let (outcome, _gate) = run_receiver_session(media_conn, started, &token)
            .await
            .unwrap();
        assert!(outcome.direct_connection_success);
        assert_eq!(outcome.ever_relay_paths, 0);
    });

    // Sender: control handshake for candidates.
    let control_conn = tx_pair
        .control
        .connect(
            EndpointAddr::new(rx_control_id).with_ip_addr(rx_control_addr),
            CONTROL_ALPN,
        )
        .await
        .unwrap();
    let (mut ctl_send, mut ctl_recv) = control_conn.open_bi().await.unwrap();
    let (cands, token) = request_candidates(&mut ctl_send, &mut ctl_recv)
        .await
        .unwrap();
    assert!(!cands.is_empty());

    let candidate = cands.first().unwrap().clone();
    validate_candidate(&candidate, [0]).unwrap();

    let media_addr = EndpointAddr::new(candidate.endpoint_id).with_ip_addr(candidate.addr);
    let media_conn = tx_pair.media.connect(media_addr, MEDIA_ALPN).await.unwrap();

    let (outcome, gate) = run_sender_session(
        media_conn,
        cfg(3),
        candidate,
        0,
        unix_millis(),
        &token,
    )
    .await
    .unwrap();

    assert!(outcome.direct_connection_success);
    assert_eq!(outcome.ever_relay_paths, 0);
    assert!(outcome.stream.frames > 50, "expected real frame count");
    assert!(matches!(
        gate.lock().unwrap().state(),
        GateState::DirectReady | GateState::Stopped(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn killing_direct_path_stops_media_fail_closed() {
    let rx_pair = bind_pair().await.unwrap();
    let tx_pair = bind_pair().await.unwrap();
    rx_pair.control.set_alpns(vec![CONTROL_ALPN.to_vec()]);
    rx_pair.media.set_alpns(vec![MEDIA_ALPN.to_vec()]);

    let rx_control_addr = rx_pair
        .control
        .addr()
        .addrs
        .iter()
        .find_map(|a| match a {
            iroh::TransportAddr::Ip(sa) => Some(*sa),
            _ => None,
        })
        .unwrap();
    let rx_control_id = rx_pair.control.id();

    let rx_handle = tokio::spawn(async move {
        let incoming = rx_pair.control.accept().await.unwrap().accept().unwrap();
        let conn = incoming.await.unwrap();
        let cands = rx_pair
            .media_direct_addrs()
            .into_iter()
            .map(|addr| {
                media_separation::DirectCandidate::local(
                    rx_pair.media.id(),
                    addr,
                    Duration::from_secs(30),
                    0,
                )
            })
            .collect::<Vec<_>>();
        let token = media_separation::session_token();
        let (mut s, mut r) = conn.accept_bi().await.unwrap();
        serve_candidates(&mut r, &mut s, &cands, &token).await.unwrap();

        let media_conn = rx_pair
            .media
            .accept()
            .await
            .unwrap()
            .accept()
            .unwrap()
            .await
            .unwrap();
        // Long nominal stream; the fault injection below must cut it short.
        let started = unix_millis();
        run_receiver_session(media_conn, started, &token).await.unwrap()
    });

    let control_conn = tx_pair
        .control
        .connect(
            EndpointAddr::new(rx_control_id).with_ip_addr(rx_control_addr),
            CONTROL_ALPN,
        )
        .await
        .unwrap();
    let (mut ctl_send, mut ctl_recv) = control_conn.open_bi().await.unwrap();
    let (cands, token) = request_candidates(&mut ctl_send, &mut ctl_recv)
        .await
        .unwrap();
    let candidate = cands.first().unwrap().clone();

    let media_conn = tx_pair
        .media
        .connect(
            EndpointAddr::new(candidate.endpoint_id).with_ip_addr(candidate.addr),
            MEDIA_ALPN,
        )
        .await
        .unwrap();

    let fault_conn = media_conn.clone();
    let injector = tokio::spawn(async move {
        // Let some frames flow, then simulate direct path death by closing.
        tokio::time::sleep(Duration::from_millis(500)).await;
        fault_conn.close(1u32.into(), b"fault-injection: link down");
    });

    let (outcome, gate) = run_sender_session(
        media_conn,
        cfg(60),
        candidate,
        0,
        unix_millis(),
        &token,
    )
    .await
    .unwrap();
    injector.await.unwrap();

    // Fail-closed: streaming stopped well before the nominal duration, and
    // the reason is recorded rather than the sender hanging or erroring out.
    let state = gate.lock().unwrap().state();
    assert!(
        matches!(state, GateState::Stopped(_)),
        "gate must latch stopped after direct loss, got {state:?}"
    );
    assert!(
        outcome.stream.frames < 600,
        "stream should have been cut short"
    );
    assert_eq!(outcome.ever_relay_paths, 0);

    // Receiver side also finished without hanging.
    let (rx_outcome, _) = tokio::time::timeout(Duration::from_secs(20), rx_handle)
        .await
        .expect("receiver session hung")
        .unwrap();
    assert!(rx_outcome.stream.frames > 0);
    assert_eq!(rx_outcome.ever_relay_paths, 0);
}
