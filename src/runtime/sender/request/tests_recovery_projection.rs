//! Work counting on the actual gap-service producer, not a wall-clock target.
//! Snapshot projection is pure for its frozen native observation and the
//! unchanged Product debt/rate epoch of this one synchronous pass.
use super::*;

fn assert_model_matches(
    fixture: &PreparedCompletionFixture,
    batch: &RequestRecoveryBatchObservation,
    range: OffsetRange,
    lane: TrafficClass,
    early: bool,
) -> RequestDataAckGapObservation {
    let frames = fixture
        .send_stream
        .retransmission_frames_for_ranges(&[range], (range.end - range.start) as usize);
    let frame = frames.first().expect("retained exact test payload");
    let controller = &fixture.sender.multipath;
    let geometry = controller
        .live_owner_uniform_frontier(range, &fixture.remotes.path_instances())
        .expect("actual retained owner");
    let evaluate = |view| {
        controller.recovery_reinjection_model_from_observation(
            &fixture.context,
            &fixture.remotes,
            frame,
            lane,
            (range.end - range.start) as usize,
            Some((
                &fixture.queue,
                fixture.send_stream.reinjection_bytes(),
                fixture.context.mux_limits,
            )),
            &geometry.avoid,
            view,
            early,
            None,
        )
    };
    let original = evaluate((&batch.observation).into());
    let reused = evaluate(batch.view());
    assert_eq!(
        format!("{reused:?}"),
        format!("{original:?}"),
        "every model field, identity, score, availability and captured clock must agree"
    );
    reused
}

fn fragmented_pending_gap() -> PreparedCompletionFixture {
    fragmented_pending_gap_with(128)
}

fn fragmented_pending_gap_with(fragments: usize) -> PreparedCompletionFixture {
    let mut fixture = PreparedCompletionFixture::new();
    for _ in 0..fragments {
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

struct ProjectionModeGuard(bool);
impl ProjectionModeGuard {
    fn new(uncached: bool) -> Self {
        Self(RECOVERY_PROJECTION_UNCACHED.with(|mode| mode.replace(uncached)))
    }
}
impl Drop for ProjectionModeGuard {
    fn drop(&mut self) {
        RECOVERY_PROJECTION_UNCACHED.with(|mode| mode.set(self.0));
    }
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
    assert!(
        service.has_measured_target,
        "exercise real target projection"
    );
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

#[tokio::test]
async fn recovery_projection_reuse_matches_original_models_for_ranges_and_missing_snapshots() {
    let fixture = fragmented_pending_gap();
    let controller = &fixture.sender.multipath;
    let captured = controller.observe_data_ack_gap_reinjection(&fixture.context, &fixture.remotes);
    for mode in ["ordinary", "unavailable", "later_captured_clock"] {
        let mut observation = captured.clone();
        if mode == "unavailable" {
            observation
                .paths
                .iter_mut()
                .find(|p| p.instance == fixture.target)
                .unwrap()
                .shared_snapshot = None;
        } else if mode == "later_captured_clock" {
            observation.observed_at += Duration::from_secs(60);
        }
        let batch = controller.recovery_batch_from_observation(observation);
        for start in (0..128).map(|i| if i == 0 { 0 } else { 4096 + (i - 1) * 8 }) {
            let range = OffsetRange {
                start,
                end: start + 8,
            };
            for lane in [TrafficClass::Throughput, TrafficClass::Latency] {
                for early in [false, true] {
                    assert_model_matches(&fixture, &batch, range, lane, early);
                }
            }
        }
        for path in &batch.observation.paths {
            let original = controller.request_reinjection_target_snapshot_from_observation(
                &batch.observation,
                path.instance,
            );
            assert_eq!(
                format!("{:?}", batch.view().snapshot(controller, path.instance)),
                format!("{original:?}")
            );
        }
        if mode == "unavailable" {
            assert!(batch.view().snapshot(controller, fixture.target).is_none());
        }
    }
}

#[tokio::test]
async fn recovery_projection_reuse_does_not_cache_actual_queue_admission() {
    let mut fixture = fragmented_pending_gap();
    let batch = fixture
        .sender
        .multipath
        .observe_recovery_batch(&fixture.context, &fixture.remotes);
    let range = OffsetRange { start: 0, end: 64 };
    let before = assert_model_matches(&fixture, &batch, range, TrafficClass::Throughput, false);
    assert_eq!(
        before.reinjection_target.map(|(id, _)| id.instance()),
        Some(fixture.target)
    );
    let commands = match &fixture
        .remotes
        .paths
        .iter()
        .find(|p| p.instance() == fixture.target)
        .unwrap()
        .stream
        .output
    {
        ReliablePathStreamOutput::Fixed(output) => output.commands().clone(),
        _ => panic!("fixed test attachment"),
    };
    let filler = data_frame(StreamId(987), 0, 64);
    let mut queued = 0;
    while commands
        .try_enqueue_reinjection_frame(filler.clone(), TrafficClass::Throughput)
        .is_ok()
    {
        queued += 1;
        assert!(
            queued <= 8,
            "actual configured queue, no invented capacity flag"
        );
    }
    assert!(queued > 0);
    let blocked = assert_model_matches(&fixture, &batch, range, TrafficClass::Throughput, false);
    assert!(blocked.reinjection_target.is_none());
    assert!(blocked.target_service_exhausted);
    while let Some(command) = try_recv_reliable_path_command(&mut fixture.target_receivers) {
        fixture
            .target_receivers
            .release_pending_command_bytes(reliable_path_command_pending_bytes(&command));
    }
    let restored = assert_model_matches(&fixture, &batch, range, TrafficClass::Throughput, false);
    assert_eq!(
        restored.reinjection_target.map(|(id, _)| id.instance()),
        Some(fixture.target)
    );
}

#[tokio::test]
async fn recovery_projection_new_pass_observes_product_ack_and_new_native_capture() {
    let mut fixture = fragmented_pending_gap();
    let before = fixture
        .sender
        .multipath
        .observe_recovery_batch(&fixture.context, &fixture.remotes);
    let old_debt = before
        .view()
        .snapshot(&fixture.sender.multipath, fixture.owner)
        .unwrap()
        .data_level_bytes_in_flight;
    drop(before);
    fixture.ack(
        Some(0),
        vec![OffsetRange {
            start: 0,
            end: 1024,
        }],
    );
    let after = fixture
        .sender
        .multipath
        .observe_recovery_batch(&fixture.context, &fixture.remotes);
    let new_debt = after
        .view()
        .snapshot(&fixture.sender.multipath, fixture.owner)
        .unwrap()
        .data_level_bytes_in_flight;
    assert_eq!(new_debt, old_debt - 1024);
    assert_model_matches(
        &fixture,
        &after,
        OffsetRange {
            start: 1024,
            end: 1088,
        },
        TrafficClass::Throughput,
        false,
    );
    let at = after.observation.observed_at;
    drop(after);
    seed_client_bulk_evidence_for_test(&fixture.context, fixture.target);
    let fresh = fixture
        .sender
        .multipath
        .observe_recovery_batch(&fixture.context, &fixture.remotes);
    assert!(fresh.observation.observed_at >= at);
    assert_model_matches(
        &fixture,
        &fresh,
        OffsetRange {
            start: 1024,
            end: 1088,
        },
        TrafficClass::Throughput,
        false,
    );
}

// Full production service, including ownership/index construction, Native
// capture, every range model, clock reconciliation and temporary destruction.
// A fixed explicit policy clock keeps these original assignments not-yet-due;
// fresh shared samples are seeded before, and excluded from, each CPU bracket.
// The test-only legacy view uses the original pure projection every time.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn recovery_projection_complete_pass_cost_probe() {
    fn thread_ns() -> u128 {
        let mut at = std::mem::MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: valid writable timespec; success initializes both fields.
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, at.as_mut_ptr()) },
            0
        );
        // SAFETY: the successful clock_gettime above initialized this value.
        let at = unsafe { at.assume_init() };
        at.tv_sec as u128 * 1_000_000_000 + at.tv_nsec as u128
    }
    let repetitions = std::env::var("MPP_TEST_PROJECTION_COST_REPETITIONS")
        .ok()
        .map(|n| n.parse::<usize>().expect("test repetition count"))
        .unwrap_or(1);
    assert!((1..=128).contains(&repetitions));
    for fragments in [1, 16, 128, 1024] {
        let mut fixture = fragmented_pending_gap_with(fragments);
        let policy_at = Instant::now();
        let mut oracle = None::<String>;
        for (sample, uncached) in [true, false, false, true].into_iter().enumerate() {
            let _mode = ProjectionModeGuard::new(uncached);
            let mut cpu_ns = 0;
            RECOVERY_PROJECTION_CALLS.with(|calls| calls.set(0));
            for _ in 0..repetitions {
                seed_client_bulk_evidence_for_test(&fixture.context, fixture.owner);
                seed_client_bulk_evidence_for_test(&fixture.context, fixture.target);
                let started = thread_ns();
                let result = fixture.sender.data_ack_gap_reinjection_service(
                    &fixture.context,
                    &fixture.remotes,
                    &fixture.send_stream,
                    &fixture.queue,
                    fixture.ack.gaps(),
                    64,
                    TrafficClass::Throughput,
                    policy_at,
                );
                cpu_ns += thread_ns() - started;
                assert!(result.has_measured_target && !result.ready);
                let state = format!("{result:?}");
                assert_eq!(
                    oracle.get_or_insert_with(|| state.clone()),
                    &state,
                    "same whole-service result across both algorithms and repeated clock reconciliation"
                );
            }
            let projections = RECOVERY_PROJECTION_CALLS.with(|calls| calls.replace(0));
            eprintln!(
                "projection_cost {{\"fragments\":{fragments},\"sample\":{sample},\"uncached\":{uncached},\"repetitions\":{repetitions},\"thread_cpu_ns\":{cpu_ns},\"projections\":{projections}}}"
            );
            if !uncached {
                assert!(projections <= repetitions * fixture.remotes.paths.len());
            }
        }
    }
}
