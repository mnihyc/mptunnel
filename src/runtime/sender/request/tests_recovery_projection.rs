//! Work counting on the actual gap-service producer, not a wall-clock target.
//! Snapshot projection is pure for its frozen native observation and the
//! unchanged Product debt/rate epoch of this one synchronous pass.
use super::*;

fn fragmented_pending_gap() -> PreparedCompletionFixture {
    let mut fixture = PreparedCompletionFixture::new();
    for _ in 0..128 {
        let frame = fixture
            .send_stream
            .send_data(Bytes::from_static(b"fragment"))
            .unwrap();
        fixture
            .sender
            .record_original_frame_for_test(fixture.owner, &frame);
    }
    let end = fixture.send_stream.next_offset();
    fixture.ack(
        Some(0),
        vec![OffsetRange {
            start: end - 8,
            end,
        }],
    );
    assert!(fixture.ack.has_gaps());
    assert_eq!(fixture.send_stream.data_ack_frontier(), 0);
    seed_client_bulk_evidence_for_test(&fixture.context, fixture.owner);
    seed_client_bulk_evidence_for_test(&fixture.context, fixture.target);
    fixture
}

#[tokio::test]
async fn recovery_projection_count_is_bounded_by_captured_paths_not_gap_fragments() {
    let mut fixture = fragmented_pending_gap();
    let now = Instant::now();
    RECOVERY_PROJECTION_CALLS.with(|calls| calls.set(0));
    let service = fixture.sender.data_ack_gap_reinjection_service(
        &fixture.context,
        &fixture.remotes,
        &fixture.send_stream,
        &fixture.queue,
        fixture.ack.gaps(),
        64,
        TrafficClass::Throughput,
        now,
    );
    let projections = RECOVERY_PROJECTION_CALLS.with(|calls| calls.replace(0));
    assert!(service.has_measured_target, "exercise real target projection");
    assert!(!service.ready, "fresh assignments are not yet due");
    assert!(service.next_deadline.is_some_and(|at| at > now));
    assert!(service.range.is_some());
    eprintln!(
        "recovery projection work: paths={} projections={projections}",
        fixture.remotes.paths.len(),
    );
    assert!(
        projections <= fixture.remotes.paths.len(),
        "the same captured path must not be projected again for every exact range: {projections}",
    );
}
