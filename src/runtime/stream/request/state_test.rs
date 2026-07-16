use super::RequestPathSamplingState;
use crate::model::capacity::{MIN_RATE_SAMPLE_BYTES, reliable_path_startup_sample_limit_bytes};
use crate::model::path::{CarrierPathInstanceId, RelayPathInstance, RelayPathKey};
use crate::mux::MuxLimits;
use crate::protocol::UnderlayProtocol;
use std::collections::HashSet;
use std::time::{Duration, Instant};

fn path(underlay: UnderlayProtocol, index: usize, id: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey { underlay, index },
        path_instance_id: CarrierPathInstanceId::from_raw(id.max(1)),
        attachment_id: id,
    }
}

#[test]
fn path_sample_planning_is_bounded_single_path_and_seals_before_overflow() {
    let limits = MuxLimits::default();
    let candidate = path(UnderlayProtocol::Tcp, 0, 1);
    let other = path(UnderlayProtocol::Udp, 1, 2);
    let sample_limit = usize::try_from(reliable_path_startup_sample_limit_bytes(limits))
        .expect("default sample limit fits usize");
    let initial_bytes = MIN_RATE_SAMPLE_BYTES as usize;
    assert!(sample_limit > initial_bytes);
    let mut state = RequestPathSamplingState::default();

    let commit = state
        .plan_sample(limits, candidate, initial_bytes)
        .expect("the first exact path can start a bounded sample");
    assert!(state.sample().is_none(), "planning is side-effect free");
    assert!(!state.sampled_paths.contains(&candidate));
    state.commit_sample(commit);

    let sample = state.sample().expect("commit installs the sample");
    assert_eq!(sample.path(), candidate);
    assert_eq!(sample.sent_bytes, initial_bytes as u64);
    assert_eq!(sample.limit_bytes, sample_limit as u64);
    assert!(!sample.is_sealed());
    assert!(state.plan_sample(limits, other, 1).is_none());

    assert_eq!(
        state.seal_if_next_frame_exceeds_limit(sample_limit),
        Some(candidate)
    );
    assert_eq!(
        state.sample().and_then(|sample| sample.sealed_bytes()),
        Some(initial_bytes as u64)
    );
    assert!(state.plan_sample(limits, candidate, 1).is_none());
}

#[test]
fn completed_and_cancelled_samples_keep_one_attempt_per_exact_instance() {
    let limits = MuxLimits::default();
    let first = path(UnderlayProtocol::Tcp, 0, 1);
    let replacement = path(UnderlayProtocol::Tcp, 0, 2);
    let mut state = RequestPathSamplingState::default();

    let first_commit = state
        .plan_sample(limits, first, 1024)
        .expect("first attachment can be sampled");
    state.commit_sample(first_commit);
    assert!(!state.complete_sample(replacement));
    assert!(state.complete_sample(first));
    assert!(state.sample().is_none());
    assert!(state.plan_sample(limits, first, 1024).is_none());

    let replacement_commit = state
        .plan_sample(limits, replacement, 1024)
        .expect("a replacement is a distinct attachment instance");
    state.commit_sample(replacement_commit);
    state.record_acked(replacement, 512, Instant::now());
    state.set_receipt_proof(replacement, (1, 512));
    state.cancel_sample(replacement);

    assert!(state.sample().is_none());
    assert!(state.acked_sample(replacement).is_none());
    assert!(state.receipt_proof(replacement).is_none());
    assert!(state.sampled_paths.contains(&replacement));
    assert!(state.plan_sample(limits, replacement, 1024).is_none());
}

#[test]
fn path_sampling_retains_evidence_by_exact_live_instance() {
    let limits = MuxLimits::default();
    let stale = path(UnderlayProtocol::Udp, 0, 41);
    let replacement = path(UnderlayProtocol::Udp, 0, 42);
    let now = Instant::now();
    let mut state = RequestPathSamplingState::default();

    let stale_commit = state
        .plan_sample(limits, stale, 1024)
        .expect("stale attachment starts the active sample");
    state.commit_sample(stale_commit);
    state.record_acked(stale, 1024, now);
    state.set_receipt_proof(stale, (10, 1024));
    state.record_acked(replacement, 2048, now + Duration::from_millis(1));
    state.set_receipt_proof(replacement, (11, 2048));
    state.sampled_paths.insert(replacement);

    state.retain_live(&HashSet::from([replacement]));

    assert!(state.sample().is_none());
    assert!(!state.sampled_paths.contains(&stale));
    assert!(state.sampled_paths.contains(&replacement));
    assert!(state.acked_sample(stale).is_none());
    assert_eq!(
        state.acked_sample(replacement),
        Some((2048, now + Duration::from_millis(1)))
    );
    assert_eq!(state.receipt_proof(replacement), Some((11, 2048)));
}
