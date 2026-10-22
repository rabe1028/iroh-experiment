use iroh::endpoint::presets;
use media_separation::{CandidateSource, EndpointPair};
use std::time::Duration;
use tokio::net::UdpSocket;

#[tokio::test]
async fn discovery_uses_media_socket_and_keeps_stun_candidate_provenance() {
    let pair = EndpointPair::bind(presets::Minimal).await.unwrap();
    let media_port = pair
        .media
        .bound_sockets()
        .into_iter()
        .find(|a| a.is_ipv4())
        .unwrap()
        .port();
    let control_port = pair
        .control
        .bound_sockets()
        .into_iter()
        .find(|a| a.is_ipv4())
        .unwrap()
        .port();
    assert_ne!(media_port, control_port);
    let reject = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let accept = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let servers = vec![
        reject.local_addr().unwrap().to_string(),
        accept.local_addr().unwrap().to_string(),
    ];
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (n, peer) = reject.recv_from(&mut packet).await.unwrap();
        assert_eq!(peer.port(), media_port);
        let txn = cloudflare_stun::parse_binding_request(&packet[..n]).unwrap();
        reject
            .send_to(
                &cloudflare_stun::encode_binding_error(420, "Try next server", &txn),
                peer,
            )
            .await
            .unwrap();
        let (n, peer) = accept.recv_from(&mut packet).await.unwrap();
        assert_eq!(peer.port(), media_port);
        let txn = cloudflare_stun::parse_binding_request(&packet[..n]).unwrap();
        accept
            .send_to(&cloudflare_stun::encode_binding_success(peer, &txn), peer)
            .await
            .unwrap();
    });
    let candidates = pair
        .discover_media(&servers, Duration::from_secs(30), 7)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate.source, CandidateSource::CloudflareStun);
    assert_eq!(candidate.endpoint_id, pair.media.id());
    assert_eq!(candidate.addr.port(), media_port);
    assert_eq!(candidate.network_epoch, 7);
    assert_eq!(
        candidate.expires_at_unix_ms - candidate.observed_at_unix_ms,
        30_000
    );
    assert!(pair.media.addr().addrs.iter().all(|addr| addr.is_ip()));
    task.await.unwrap();
    pair.media.close().await;
    pair.control.close().await;
}
