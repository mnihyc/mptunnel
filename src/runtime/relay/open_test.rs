use super::*;

#[test]
fn reliable_udp_service_and_repair_attachments_wait_for_peer_acceptance() {
    assert!(
        udp_relay_attachment_open_options(StreamOpenRole::Active).wait_for_accept,
        "an Active attachment is not usable until the peer accepts it"
    );
    assert!(
        udp_relay_attachment_open_options(StreamOpenRole::Repair).wait_for_accept,
        "a Repair attachment must exist at the peer before correctness repair uses it"
    );
    assert!(
        !udp_relay_attachment_open_options(StreamOpenRole::Validation).wait_for_accept,
        "Validation remains an optimistic proof attachment"
    );
}

#[tokio::test]
async fn relay_attach_open_timeout_bounds_pending_connection_setup() {
    let result = relay_path_open_with_deadline(
        tokio::time::Instant::now() + Duration::from_millis(1),
        std::future::pending::<Result<(), RuntimeError>>(),
    )
    .await;

    assert!(matches!(result, Err(RuntimeError::PathOpenTimedOut)));
}
