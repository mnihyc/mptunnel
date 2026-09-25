use super::*;
use crate::runtime::webhook::WebhookTestCaptureHandle;
use serde_json::Value;

fn observed_registry(
    kinds: &[EventKind],
) -> (Arc<ServerReliableStreamRegistry>, WebhookTestCaptureHandle) {
    let registry = Arc::new(ServerReliableStreamRegistry::new(8));
    let (publisher, capture) = EventPublisher::test_capture(kinds, 64);
    registry.attach_webhook_publisher(
        publisher,
        "edge-inbound".into(),
        Arc::new(vec!["wan".into()]),
    );
    (registry, capture)
}

fn identity(path: &ServerCarrierPathRegistration) -> ServerCarrierPathIdentity {
    ServerCarrierPathIdentity {
        session_id: path.session_id(),
        underlay: path.underlay(),
        path_id: path.path_id(),
        path_instance_id: path.path_instance_id(),
    }
}

fn events(capture: &WebhookTestCaptureHandle, kind: EventKind) -> Vec<Value> {
    capture
        .snapshot()
        .into_iter()
        .filter(|event| event["event"]["type"] == kind.as_str())
        .collect()
}

#[test]
fn server_webhooks_require_readiness_and_reject_late_or_duplicate_publication() {
    let (registry, capture) = observed_registry(&EventKind::ALL);
    let path = registry.register_carrier_path(SessionId(1), UnderlayProtocol::Tcp, PathId(1));
    assert!(
        capture.snapshot().is_empty(),
        "registration precedes readiness writes"
    );
    let peer = Some("192.0.2.1:1000".parse().unwrap());
    registry.carrier_ready(identity(&path), None, peer, 0);
    registry.carrier_ready(identity(&path), None, peer, 0);
    let carriers = events(&capture, EventKind::CarrierStateChanged);
    assert_eq!(carriers.len(), 1);
    assert_eq!(carriers[0]["change"]["to"], "ready");
    let _ = path.begin_retirement();
    let before_late = capture.snapshot().len();
    registry.carrier_ready(identity(&path), None, peer, 0);
    registry.carrier_peer_address(identity(&path), "192.0.2.2:1001".parse().unwrap(), 1);
    assert_eq!(capture.snapshot().len(), before_late);
    assert_eq!(events(&capture, EventKind::CarrierStateChanged).len(), 3);
    assert_eq!(capture.dropped(), 0);
}

#[test]
fn server_session_presence_counts_ready_carriers_independently_of_product_references() {
    let (registry, capture) = observed_registry(&[EventKind::SessionStateChanged]);
    let path = registry.register_carrier_path(SessionId(2), UnderlayProtocol::Tcp, PathId(1));
    let flow = registry.register_realtime_flow(SessionId(2)).unwrap();
    registry.carrier_ready(identity(&path), None, None, 0);
    let _ = path.begin_retirement();
    let first = events(&capture, EventKind::SessionStateChanged);
    assert_eq!(
        first
            .iter()
            .map(|e| e["change"]["to"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["attached", "detached"]
    );
    let next = registry.register_carrier_path(SessionId(2), UnderlayProtocol::Tcp, PathId(2));
    registry.carrier_ready(identity(&next), None, None, 0);
    let again = events(&capture, EventKind::SessionStateChanged);
    assert_eq!(again[2]["change"]["to"], "attached");
    assert_eq!(again[2]["change"]["reason"], "reattached");
    assert_eq!(again[2]["initial"], false);
    assert_eq!(
        first[0]["session"]["lifetime"],
        again[2]["session"]["lifetime"]
    );
    registry.retire_session(SessionId(2), crate::protocol::CloseReason::Normal);
    registry.retire_session(SessionId(2), crate::protocol::CloseReason::ProtocolError);
    let final_events = events(&capture, EventKind::SessionStateChanged);
    assert_eq!(
        final_events
            .iter()
            .filter(|e| e["change"]["to"] == "retired")
            .count(),
        1
    );
    assert_eq!(final_events.last().unwrap()["change"]["to"], "retired");
    drop(flow);
}

#[test]
fn server_address_only_subscription_preserves_overlap_and_ignores_port_only_set_changes() {
    let (registry, capture) = observed_registry(&[EventKind::SessionPeerAddressesChanged]);
    let first = registry.register_carrier_path(SessionId(3), UnderlayProtocol::Tcp, PathId(1));
    let second = registry.register_carrier_path(SessionId(3), UnderlayProtocol::Udp, PathId(2));
    registry.carrier_ready(
        identity(&first),
        None,
        Some("192.0.2.1:1".parse().unwrap()),
        0,
    );
    registry.carrier_ready(
        identity(&second),
        None,
        Some("192.0.2.2:2".parse().unwrap()),
        1,
    );
    registry.carrier_peer_address(identity(&second), "192.0.2.2:3".parse().unwrap(), 2);
    let _ = first.begin_retirement();
    registry.carrier_peer_address(identity(&second), "192.0.2.3:4".parse().unwrap(), 3);
    let changes = events(&capture, EventKind::SessionPeerAddressesChanged);
    let sets = changes
        .iter()
        .map(|e| e["change"]["after"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        sets,
        [
            json!(["192.0.2.1"]),
            json!(["192.0.2.1", "192.0.2.2"]),
            json!(["192.0.2.2"]),
            json!(["192.0.2.3"])
        ]
    );
    assert_eq!(changes[0]["initial"], true);
    assert!(changes[1..].iter().all(|e| e["initial"] == false));
}

#[test]
fn server_policy_only_subscription_suppresses_sequence_only_refresh() {
    let (registry, capture) = observed_registry(&[EventKind::CarrierPolicyChanged]);
    let path = registry.register_carrier_path(SessionId(4), UnderlayProtocol::Tcp, PathId(1));
    registry.carrier_ready(identity(&path), None, None, 0);
    registry.record_peer_path_usage(identity(&path), 1, PathUsage::Available);
    registry.record_peer_path_usage(identity(&path), 2, PathUsage::Available);
    registry.record_peer_path_usage(identity(&path), 3, PathUsage::Backup);
    registry.record_peer_path_usage(identity(&path), 2, PathUsage::Available);
    let changes = events(&capture, EventKind::CarrierPolicyChanged);
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[1]["change"]["from"], "available");
    assert_eq!(changes[1]["change"]["to"], "backup");
    assert_eq!(changes[1]["initial"], false);
}

#[test]
fn server_terminal_fence_rejects_ready_after_session_retirement() {
    let (registry, capture) = observed_registry(&EventKind::ALL);
    let path = registry.register_carrier_path(SessionId(5), UnderlayProtocol::Tcp, PathId(1));
    registry.retire_session(SessionId(5), crate::protocol::CloseReason::Normal);
    registry.carrier_ready(identity(&path), None, None, 0);
    assert!(events(&capture, EventKind::CarrierStateChanged).is_empty());
    assert_eq!(events(&capture, EventKind::SessionStateChanged).len(), 1);
}

#[test]
fn server_address_hooks_separate_port_changes_and_coalescing_from_session_ip_changes() {
    let (registry, capture) = observed_registry(&EventKind::ALL);
    let path = registry.register_carrier_path(SessionId(6), UnderlayProtocol::Udp, PathId(1));
    registry.carrier_ready(
        identity(&path),
        None,
        Some("192.0.2.1:1000".parse().unwrap()),
        1,
    );
    registry.carrier_peer_address(identity(&path), "192.0.2.1:1001".parse().unwrap(), 2);
    registry.carrier_peer_address(identity(&path), "192.0.2.1:1001".parse().unwrap(), 4);
    let changes = events(&capture, EventKind::CarrierAddressChanged);
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0]["change"]["components"], json!(["port"]));
    assert_eq!(changes[0]["change"]["before"]["port"], 1000);
    assert_eq!(changes[1]["change"]["skipped_revisions"], 1);
    assert_eq!(changes[1]["change"]["coalesced"], true);
    assert_eq!(
        changes[1]["change"]["before"],
        changes[1]["change"]["after"]
    );
    assert_eq!(changes[1]["event"]["reason"], "native_path_validated");
    assert_eq!(
        events(&capture, EventKind::SessionPeerAddressesChanged).len(),
        1
    );
    registry.retire_session(SessionId(6), crate::protocol::CloseReason::Normal);
    assert_eq!(
        events(&capture, EventKind::SessionPeerAddressesChanged).len(),
        1,
        "physical cleanup after the session terminal is silent"
    );
}
