//! Unit tests for the fail-closed gate and candidate validation.

use std::time::Duration;

use media_separation::{
    validate_candidate, DirectCandidate, GateState, MediaGate, PathSignal, StopReason,
};

fn gate() -> MediaGate {
    MediaGate::new()
}

fn snap(open_direct: usize, has_relay: bool, selected_direct: bool) -> PathSignal {
    PathSignal::Snapshot {
        open_direct,
        has_relay,
        selected_direct,
    }
}

#[test]
fn streaming_blocked_until_direct_selected() {
    let mut g = gate();
    assert_eq!(g.state(), GateState::AwaitingDirect);

    // A direct path opens but nothing is selected yet: still not ready.
    assert_eq!(g.apply(snap(1, false, false)), GateState::AwaitingDirect);

    // Selection confirms the direct path.
    assert_eq!(g.apply(snap(1, false, true)), GateState::DirectReady);
}

#[test]
fn last_direct_close_stops_and_latches() {
    let mut g = gate();
    g.apply(snap(1, false, true));
    assert_eq!(g.state(), GateState::DirectReady);

    assert_eq!(
        g.apply(snap(0, false, false)),
        GateState::Stopped(StopReason::DirectPathLost)
    );

    // Latched: later good news must not reopen the gate.
    assert_eq!(
        g.apply(snap(1, false, true)),
        GateState::Stopped(StopReason::DirectPathLost)
    );
}

#[test]
fn relay_seen_stops_even_without_selection() {
    let mut g = gate();
    assert_eq!(
        g.apply(snap(1, true, false)),
        GateState::Stopped(StopReason::RelayPathObserved)
    );
}

#[test]
fn selected_relay_stops_active_stream() {
    let mut g = gate();
    g.apply(snap(1, false, true));
    assert_eq!(
        g.apply(snap(1, true, true)),
        GateState::Stopped(StopReason::RelayPathObserved)
    );
}

#[test]
fn empty_snapshot_before_first_path_is_not_loss() {
    let mut g = gate();
    // Connections start with no paths at all; that must not latch.
    assert_eq!(g.apply(snap(0, false, false)), GateState::AwaitingDirect);
    assert_eq!(g.apply(snap(1, false, true)), GateState::DirectReady);
}

#[test]
fn selection_leaving_direct_is_path_loss() {
    let mut g = gate();
    g.apply(snap(1, false, true));
    // Path still open but selection moved away (migration window).
    assert_eq!(
        g.apply(snap(1, false, false)),
        GateState::Stopped(StopReason::DirectPathLost)
    );
}

#[test]
fn connection_closed_stops() {
    let mut g = gate();
    assert_eq!(
        g.apply(PathSignal::ConnectionClosed),
        GateState::Stopped(StopReason::ConnectionClosed)
    );
}

#[test]
fn multiple_direct_paths_require_all_closed() {
    let mut g = gate();
    g.apply(snap(2, false, true));

    // One of two paths closes; the other stays up, so keep streaming.
    assert_eq!(g.apply(snap(1, false, true)), GateState::DirectReady);

    assert_eq!(
        g.apply(snap(0, false, false)),
        GateState::Stopped(StopReason::DirectPathLost)
    );
}

#[test]
fn stopped_ignores_everything() {
    let mut g = gate();
    g.apply(snap(1, true, false));
    let latched = g.state();
    assert_eq!(g.apply(snap(5, false, true)), latched);
    assert_eq!(g.apply(PathSignal::ConnectionClosed), latched);
}

fn cand(ttl_ms: u64, epoch: u64) -> DirectCandidate {
    let endpoint_id = iroh::SecretKey::generate().public().into();
    DirectCandidate::local(
        endpoint_id,
        "203.0.113.7:4001".parse().unwrap(),
        Duration::from_millis(ttl_ms),
        epoch,
    )
}

#[test]
fn fresh_candidate_with_known_epoch_passes() {
    validate_candidate(&cand(30_000, 0), [0]).unwrap();
}

#[test]
fn expired_candidate_rejected() {
    // TTL zero means already expired when validated a moment later.
    let c = cand(0, 0);
    std::thread::sleep(Duration::from_millis(2));
    assert!(validate_candidate(&c, [0]).is_err());
}

#[test]
fn unknown_epoch_rejected() {
    // Candidate claims epoch 7 but we only know 0 and 1 (interface changed).
    assert!(validate_candidate(&cand(30_000, 7), [0, 1]).is_err());
}
