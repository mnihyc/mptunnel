//! Real-peer counterexamples for submitted, pre-first-MAX QUIC OPEN ownership.

use super::*;
use crate::protocol::ResetReason;

const OPEN_OWNERSHIP_GUARD: Duration = Duration::from_secs(5);

#[tokio::test]
async fn submitted_open_cancellation_orders_detach_before_native_close() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let carrier = current_client_carrier(&fixture.session)
        .await
        .expect("established client carrier");
    let stream_id = StreamId(1200);
    let limits = fixture.server_context.codec_limits;
    let opening = spawn_test_open(&fixture.session, stream_id);
    // Keep both peer halves alive and withhold first MAX. A peer response-half
    // drop would instead introduce an H3 refusal into the tested boundary.
    let (_peer_send, mut peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("actual submitted OPEN/MAX reached peer")
    .expect("read actual submitted OPEN/MAX");
    assert!(!opening.is_finished(), "first MAX remains withheld");
    opening.abort();
    assert!(
        opening.await.err().is_some_and(|error| error.is_cancelled()),
        "caller cancellation must be joined"
    );

    let next = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await;
    assert!(
        matches!(next, Ok(Ok(Frame::StreamDetach { stream_id: detached })) if detached == stream_id),
        "fully submitted cancellation must deliver MPP DETACH before native EOF: {next:?}"
    );
    let eof = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        udp_path_read_frame(&mut peer_recv, limits),
    )
    .await
    .expect("ordered native close follows DETACH");
    assert!(
        eof.as_ref().is_err_and(super::super::super::io::udp_path_input_finished),
        "retired request must finish after its DETACH: {eof:?}"
    );
    assert!(!carrier.connection.is_closed());
    assert_eq!(
        current_client_carrier(&fixture.session)
            .await
            .expect("shared carrier remains installed")
            .path_instance_id,
        carrier.path_instance_id,
    );
}

#[tokio::test]
async fn parsed_open_reset_is_not_held_behind_physical_owner_mutex() {
    let fixture = ClientOpenRaceFixture::new().await;
    let accepted = fixture.establish_current().await;
    let stream_id = StreamId(1201);
    let limits = fixture.server_context.codec_limits;
    let mut opening = spawn_test_open(&fixture.session, stream_id);
    let (mut peer_send, _peer_recv) = tokio::time::timeout(
        OPEN_OWNERSHIP_GUARD,
        read_test_stream_open(&accepted.connection, stream_id, limits),
    )
    .await
    .expect("actual OPEN/MAX reached peer")
    .expect("peer reads pending OPEN/MAX");
    let owner = fixture.session.owner.connection.lock().await;
    udp_path_write_frame(
        &mut peer_send,
        &Frame::StreamReset {
            stream_id,
            reason: ResetReason::RemoteClosed,
        },
        limits,
    )
    .await
    .expect("peer publishes real logical terminal");

    let observed = tokio::time::timeout(OPEN_OWNERSHIP_GUARD, &mut opening).await;
    let returned_before_unlock = observed.is_ok();
    drop(owner);
    // Settle the baseline too, so a failing conformance assertion does not
    // intentionally strand the open task behind the test's own mutex guard.
    let result = match observed {
        Ok(joined) => joined.expect("terminal open task join"),
        Err(_) => tokio::time::timeout(OPEN_OWNERSHIP_GUARD, opening)
            .await
            .expect("baseline settles after owner unlock")
            .expect("baseline open task join"),
    };
    assert!(matches!(
        result,
        Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))
    ));
    assert!(
        returned_before_unlock,
        "a parsed logical RESET cannot await independent physical reconciliation"
    );
}
