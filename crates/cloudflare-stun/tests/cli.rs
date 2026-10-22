use cloudflare_stun::{encode_binding_success, parse_binding_request};
use std::time::Duration;
use tokio::net::UdpSocket;

async fn run_cli(standalone: bool) {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut peers = Vec::new();
        for _ in 0..2 {
            let mut packet = [0; 1500];
            let (n, peer) = server.recv_from(&mut packet).await.unwrap();
            peers.push(peer);
            let txn = parse_binding_request(&packet[..n]).unwrap();
            server
                .send_to(&encode_binding_success(peer, &txn), peer)
                .await
                .unwrap();
        }
        assert_eq!(peers[0], peers[1], "both servers must use the same binding");
    });
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_stun-probe"));
    command.args([
        "--bind",
        "127.0.0.1:0",
        "--server",
        &address.to_string(),
        "--server",
        &address.to_string(),
    ]);
    if standalone {
        command.arg("--standalone");
    }
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row["same_socket_as_iroh"], !standalone);
        assert_eq!(row["candidate_registered"], !standalone);
        assert_eq!(row["observed_addr"], row["local_addr"]);
    }
    assert_eq!(rows[0]["local_addr"], rows[1]["local_addr"]);
    task.await.unwrap();
}

#[tokio::test]
async fn cli_defaults_to_same_socket_and_reuses_it() {
    run_cli(false).await;
}

#[tokio::test]
async fn cli_can_explicitly_use_standalone_mode() {
    run_cli(true).await;
}

#[tokio::test]
async fn oversized_unrelated_datagram_does_not_stall_the_shared_socket() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = server.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (n, peer) = server.recv_from(&mut packet).await.unwrap();
        let txn = parse_binding_request(&packet[..n]).unwrap();
        let rogue = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut oversized = encode_binding_success(peer, &cloudflare_stun::TransactionId::random());
        oversized.resize(4000, 0);
        rogue.send_to(&oversized, peer).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        server
            .send_to(&encode_binding_success(peer, &txn), peer)
            .await
            .unwrap();
    });
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_stun-probe"))
            .kill_on_drop(true)
            .args([
                "--bind",
                "127.0.0.1:0",
                "--server",
                &address.to_string(),
                "--attempt-timeout-ms",
                "1000",
                "--attempts",
                "1",
            ])
            .output(),
    )
    .await
    .expect("oversized UDP must not stall the receive loop")
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(row["same_socket_as_iroh"], true);
    assert_eq!(row["observed_addr"], row["local_addr"]);
    task.await.unwrap();
}
