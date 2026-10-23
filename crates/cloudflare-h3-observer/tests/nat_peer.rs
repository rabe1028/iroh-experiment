//! Separate native processes, real QUIC/TLS, and a mock same-socket STUN server.
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{net::UdpSocket, process::Command};

struct Temp(PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn exercise(allow_private: bool, wrong_run: bool) -> Vec<serde_json::Value> {
    let root = std::env::temp_dir().join(format!("iroh-nat-peer-{}", rand::random::<u64>()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let _temp = Temp(root.clone());
    let stun = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = stun.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let mut packet = [0; 1500];
            let (len, peer) = stun.recv_from(&mut packet).await.unwrap();
            let txn = cloudflare_stun::parse_binding_request(&packet[..len]).unwrap();
            stun.send_to(&cloudflare_stun::encode_binding_success(peer, &txn), peer)
                .await
                .unwrap();
        }
    });
    let mut children = Vec::new();
    for role in ["listen", "dial"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_nat-peer"));
        command
            .args([
                "--method",
                "stun",
                "--role",
                role,
                "--run-id",
                "integration",
                "--stun-server",
                &address,
                "--setup-timeout-secs",
                "5",
                "--connect-timeout-secs",
                "5",
            ])
            .arg("--candidate-out")
            .arg(root.join(format!("{role}.candidate.json")))
            .arg("--peer-file")
            .arg(root.join(format!("{role}.peer.json")))
            .arg("--results")
            .arg(root.join(format!("{role}.result.json")))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if allow_private {
            command.arg("--allow-private-peer");
        }
        children.push(command.spawn().unwrap());
    }
    for role in ["listen", "dial"] {
        let path = root.join(format!("{role}.candidate.json"));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("candidate was not published");
    }
    for (from, to) in [("listen", "dial"), ("dial", "listen")] {
        let mut candidate: serde_json::Value =
            read(&root.join(format!("{from}.candidate.json"))).await;
        if wrong_run {
            candidate["run_id"] = "different-run".into();
        }
        let temporary = root.join(format!("{to}.peer.tmp"));
        tokio::fs::write(&temporary, serde_json::to_vec(&candidate).unwrap())
            .await
            .unwrap();
        tokio::fs::rename(temporary, root.join(format!("{to}.peer.json")))
            .await
            .unwrap();
    }
    let mut outputs = Vec::new();
    for child in children {
        let output = tokio::time::timeout(Duration::from_secs(12), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        outputs.push(output);
    }
    server.await.unwrap();
    let mut reports = Vec::new();
    for role in ["listen", "dial"] {
        reports.push(read(&root.join(format!("{role}.result.json"))).await);
    }
    for output in outputs {
        if allow_private && !wrong_run {
            assert!(
                output.status.success(),
                "{}; reports={reports:?}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            assert!(!output.status.success());
        }
    }
    reports
}

async fn read(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&tokio::fs::read(path).await.unwrap()).unwrap()
}

#[tokio::test]
async fn direct_echo_uses_only_observed_candidates_and_marks_local_diagnostic() {
    for report in exercise(true, false).await {
        assert_eq!(report["candidate_registered"], true);
        assert_eq!(report["same_socket_as_iroh"], true);
        assert_eq!(report["automatic_path_discovery_disabled"], true);
        assert_eq!(report["observed_peer_path_only"], true);
        assert_eq!(report["direct_connection_success"], true);
        assert_eq!(report["echo_success"], true);
        assert_eq!(report["payload_bytes"], 65536);
        assert_eq!(report["selected_path"], "direct-ip");
        assert_eq!(report["diagnostic_private_peers"], true);
        assert_eq!(report["different_observed_ips"], false);
    }
}

#[tokio::test]
async fn local_candidates_are_rejected_by_default_without_claiming_a_connection_attempt() {
    for report in exercise(false, false).await {
        assert!(report["direct_connection_success"].is_null());
        assert!(report["failure_reason"]
            .as_str()
            .unwrap()
            .contains("private/overlay"));
    }
}

#[tokio::test]
async fn stale_candidate_from_another_run_is_rejected_before_dial() {
    for report in exercise(true, true).await {
        assert!(report["direct_connection_success"].is_null());
        assert!(report["failure_reason"]
            .as_str()
            .unwrap()
            .contains("different experiment"));
    }
}

async fn exercise_test_client(wrong_advertised_address: bool) -> serde_json::Value {
    let root = std::env::temp_dir().join(format!("iroh-nat-reject-{}", rand::random::<u64>()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let _temp = Temp(root.clone());
    let stun = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = stun.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move {
        let mut packet = [0; 1500];
        let (len, peer) = stun.recv_from(&mut packet).await.unwrap();
        let txn = cloudflare_stun::parse_binding_request(&packet[..len]).unwrap();
        stun.send_to(&cloudflare_stun::encode_binding_success(peer, &txn), peer)
            .await
            .unwrap();
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_nat-peer"))
        .args([
            "--method",
            "stun",
            "--role",
            "listen",
            "--run-id",
            "reject",
            "--stun-server",
            &server_addr,
            "--allow-private-peer",
            "--setup-timeout-secs",
            "5",
            "--connect-timeout-secs",
            "5",
        ])
        .arg("--candidate-out")
        .arg(root.join("candidate.json"))
        .arg("--peer-file")
        .arg(root.join("peer.json"))
        .arg("--results")
        .arg(root.join("result.json"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.join("candidate.json").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let own = read(&root.join("candidate.json")).await;
    let mut peer = own.clone();
    peer["role"] = "dial".into();
    peer["endpoint_id"] = serde_json::to_value(endpoint.id()).unwrap();
    let actual_addr = endpoint.bound_sockets()[0];
    let advertised_addr = if wrong_advertised_address {
        std::net::SocketAddr::new("127.0.0.2".parse().unwrap(), actual_addr.port())
    } else {
        actual_addr
    };
    peer["addr"] = advertised_addr.to_string().into();
    tokio::fs::write(root.join("peer.tmp"), serde_json::to_vec(&peer).unwrap())
        .await
        .unwrap();
    tokio::fs::rename(root.join("peer.tmp"), root.join("peer.json"))
        .await
        .unwrap();
    let target =
        iroh::EndpointAddr::new(serde_json::from_value(own["endpoint_id"].clone()).unwrap())
            .with_ip_addr(own["addr"].as_str().unwrap().parse().unwrap());
    let connection = endpoint
        .connect(target, b"iroh-experiment/nat-echo/1")
        .await;
    if !wrong_advertised_address {
        assert!(connection.is_ok());
    }
    if let Ok(conn) = connection {
        let _ = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            if !wrong_advertised_address {
                recv.stop(0u32.into())?;
            }
            let expected: Vec<u8> = (0..65536).map(|i| (i % 251) as u8).collect();
            send.write_all(&expected).await?;
            send.finish()?;
            if wrong_advertised_address {
                let _ = recv.read_to_end(65536).await;
            }
            anyhow::Ok(())
        }
        .await;
    }
    assert!(!tokio::time::timeout(Duration::from_secs(12), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    let report = read(&root.join("result.json")).await;
    endpoint.close().await;
    server.await.unwrap();
    report
}

#[tokio::test]
async fn rejected_echo_cannot_be_reported_as_success() {
    let report = exercise_test_client(false).await;
    assert_eq!(report["direct_connection_success"], true);
    assert_eq!(report["echo_success"], false);
    assert_eq!(report["payload_bytes"], 0);
    assert!(report["failure_reason"].is_string());
}

#[tokio::test]
async fn authenticated_connection_from_an_unobserved_address_is_rejected() {
    let report = exercise_test_client(true).await;
    assert_eq!(report["direct_connection_success"], false);
    assert_ne!(report["echo_success"], true);
    assert_eq!(report["observed_peer_path_only"], false);
    assert!(report["failure_reason"]
        .as_str()
        .unwrap()
        .contains("unobserved peer address"));
}
