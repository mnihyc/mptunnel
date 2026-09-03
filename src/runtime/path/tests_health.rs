use super::*;
use crate::protocol::{PathId, PathMetricDirection};
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::model::{path_metrics_from_snapshot_at, path_snapshot};
use crate::runtime::path::tcp::metrics::{TcpNativeObservation, TcpSenderMetricTracker};
use crate::scheduler::PathState as SchedulerPathState;
use crate::transport::PathSpec;
use crate::transport::tcp_telemetry::{
    TcpNativeFlight, TcpNativeLossCounters, TcpNativeRtt, TcpNativeSnapshot,
};

fn request_tcp_native_observation(path_index: usize) -> TcpNativeObservation {
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 180_000,
            rttvar_us: Some(10_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(64 * 1_024),
            inflight_limit_bytes: 512 * 1_024,
            inflight_hi_bytes: Some(512 * 1_024),
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        pacing_rate_bytes_per_second: Some(250_000_000),
        delivery_rate_bytes_per_second: Some(125_000_000),
        app_limited: Some(false),
    };
    TcpSenderMetricTracker::new(snapshot).observe(
        PathId(path_index as u16),
        PathMetricDirection::ClientToServer,
        snapshot,
    )
}

fn request_quic_path_metrics(now: Instant, deadline: Instant) -> UdpPathMetrics {
    UdpPathMetrics {
        controller_path_epoch: 1,
        direction: PathMetricDirection::ClientToServer,
        srtt: Duration::from_millis(180),
        rttvar: Duration::from_millis(20),
        rtt_observed: true,
        delivery_rate_bps: 200_000_000.0,
        pacing_rate_bps: 250_000_000.0,
        controller_bandwidth_bps: None,
        inflight_hi: 4 * 1024 * 1024,
        bytes_in_flight: 0,
        pending_bytes: 0,
        loss_ppm: Some(0),
        ecn_ppm: Some(0),
        app_limited: true,
        ack_derived_data_seen: true,
        delivery_sample_count: 10,
        delivery_sample_bytes: 512 * 1024,
        last_delivery_sample_at: Some(now),
        bulk_proof_expires_at: Some(deadline),
        latest_delivery_sample_bytes: 0,
        latest_delivery_sample_count: 0,
        latest_carrier_ack_elapsed: None,
        latest_rate_sample_elapsed: None,
        #[cfg(feature = "lab-diagnostics")]
        ack_poll: crate::runtime::path::quic::metrics::QuicAckPollDiagnostics::default(),
    }
}

#[test]
fn quic_native_authority_bypasses_ack_rate_and_freshness_gates() {
    let instance = crate::model::path::next_carrier_path_instance_id();
    let scope = crate::model::carrier_rate_authority::CarrierRateAuthorityScope::new(
        instance,
        PathMetricDirection::ClientToServer,
    );
    let authority =
        NativeCarrierRateAuthorityHandle::from_observation_for_test(scope, 25_000_000, 1, 7, None)
            .expect("central native authority");
    let shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            None,
            Duration::from_millis(80),
            Duration::from_millis(12),
            2 * 1024 * 1024,
            256 * 1024,
            1400,
            Some(30_000_000),
            false,
        )
        .expect("matching activation-local shape");
    let now = Instant::now();
    let mut raw_ack_metrics = request_quic_path_metrics(now, now + Duration::from_millis(1));
    raw_ack_metrics.delivery_rate_bps = 900_000_000.0;
    raw_ack_metrics.pacing_rate_bps = 950_000_000.0;
    raw_ack_metrics.inflight_hi = 32 * 1024 * 1024;

    let mut record = ClientPathHealthRecord::default();
    record.install_udp_peer_usage(instance, 7, 0, PathUsage::Available);
    record.mark_quic_native_authority_metrics(instance, raw_ack_metrics, shape);

    let observation = record.observation_at(now + Duration::from_secs(3_600));
    assert_eq!(observation.carrier_delivery_rate_bps, Some(25_000_000.0));
    assert_eq!(observation.carrier_pacing_rate_bps, Some(30_000_000.0));
    assert_eq!(observation.carrier_inflight_limit_bytes, 2 * 1024 * 1024);
    assert_eq!(observation.carrier_bytes_in_flight, 256 * 1024);
    assert_eq!(observation.carrier_delivery_samples, 0);
    assert!(!observation.carrier_ack_derived_data_seen);
    assert!(!observation.explicit_carrier_capacity_proof);
    assert_eq!(
        observation.native_carrier_authority_basis,
        Some(crate::model::carrier_rate_authority::CarrierRateAuthorityBasis::StartupPrior)
    );
    assert!(!observation.carrier_queue_bytes_observed);
    let path = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("QUIC path");
    let snapshot = path_snapshot(&path, 0, observation);
    assert_eq!(snapshot.delivery_rate_bps, 25_000_000.0);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(25_000_000.0));
    assert_eq!(snapshot.product_progress_rate_bps, None);
    let wire = path_metrics_from_snapshot_at(
        snapshot,
        observation,
        PathMetricDirection::ClientToServer,
        now + Duration::from_secs(3_600),
    );
    assert!(!wire.rate_observed);
    assert!(!wire.has_ack_derived_data_sample);
    assert_eq!(wire.data_sample_count, 0);
    assert_eq!(wire.data_sample_bytes, 0);
}

#[test]
fn native_window_authority_expires_independently_of_retained_diagnostics() {
    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(instance, 0, PathUsage::Available);
    let observed_at = Instant::now();
    assert!(record.mark_tcp_transport_state_at(
        instance,
        request_tcp_native_observation(0),
        observed_at,
    ));
    let fresh = record.observation_at(observed_at);
    assert_eq!(fresh.carrier_inflight_limit_bytes, 512 * 1024);
    let sample = record
        .carrier_native_window_sample
        .expect("TCP native C publishes its own frozen epoch");
    assert_eq!(sample.inflight_limit_bytes, 512 * 1024);
    assert_eq!(sample.observed_at, observed_at);
    assert!(sample.fresh_at(observed_at));

    let expires_at = sample.expires_at;
    assert!(!sample.fresh_at(expires_at));
    let expired = record.observation_at(expires_at);
    assert_eq!(expired.carrier_inflight_limit_bytes, 0);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert_eq!(record.carrier_native_window_sample, Some(sample));
}

#[test]
fn client_native_capacity_epoch_tracks_only_the_exact_native_lifetime() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(1);
    let udp_instance = crate::model::path::next_carrier_path_instance_id();
    let mut udp = ClientPathHealthRecord::default();
    udp.mutate_eligibility(|record| {
        record.install_udp_peer_usage(udp_instance, 7, 0, PathUsage::Available);
    });
    assert_eq!(udp.native_capacity_epoch(), 7);
    let eligibility_epoch = udp.eligibility_epoch();

    let metrics = UdpPathMetrics {
        controller_path_epoch: 7,
        ..request_quic_path_metrics(now, deadline)
    };
    udp.mutate_eligibility(|record| record.mark_quic_path_metrics(udp_instance, metrics));
    assert_eq!(udp.native_capacity_epoch(), 7);
    assert_eq!(udp.eligibility_epoch(), eligibility_epoch);

    udp.mutate_eligibility(|record| {
        record.mark_quic_path_metrics(
            udp_instance,
            UdpPathMetrics {
                delivery_rate_bps: 1.0,
                pacing_rate_bps: 2.0,
                ..metrics
            },
        );
    });
    assert_eq!(udp.native_capacity_epoch(), 7);
    assert_eq!(udp.eligibility_epoch(), eligibility_epoch);

    udp.mutate_eligibility(|record| {
        record.mark_quic_path_metrics(
            udp_instance,
            UdpPathMetrics {
                controller_path_epoch: 8,
                ..metrics
            },
        );
    });
    assert_eq!(udp.native_capacity_epoch(), 8);
    assert_eq!(udp.eligibility_epoch(), eligibility_epoch);

    udp.mutate_eligibility(|record| {
        record.mark_quic_path_metrics(
            udp_instance,
            UdpPathMetrics {
                controller_path_epoch: 7,
                delivery_rate_bps: 3.0,
                ..metrics
            },
        );
    });
    assert_eq!(
        udp.native_capacity_epoch(),
        8,
        "a late native snapshot cannot regress controller lifetime",
    );
    assert_eq!(udp.eligibility_epoch(), eligibility_epoch);

    let tcp_instance = crate::model::path::next_carrier_path_instance_id();
    let mut tcp = ClientPathHealthRecord::default();
    tcp.mutate_eligibility(|record| {
        record.install_tcp_peer_usage(PathId(3), tcp_instance, 0, PathUsage::Available);
    });
    let tcp_eligibility_epoch = tcp.eligibility_epoch();
    assert_eq!(tcp.native_capacity_epoch(), 0);
    tcp.mutate_eligibility(|record| {
        assert!(record.mark_tcp_transport_state(tcp_instance, request_tcp_native_observation(3),));
    });
    assert_eq!(tcp.native_capacity_epoch(), 0);
    assert_eq!(tcp.eligibility_epoch(), tcp_eligibility_epoch);
}

#[test]
fn stale_quic_path_proof_ack_cannot_relabel_a_replacement_instance() {
    let predecessor = crate::model::path::next_carrier_path_instance_id();
    let replacement = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_udp_peer_usage(predecessor, 3, 0, PathUsage::Available);
    record.install_udp_peer_usage(replacement, 4, 0, PathUsage::Available);

    // Deliberately timestamp this after replacement. The exact-instance fence,
    // rather than the older coarse proof-time fence, must reject it.
    let observation = PathProofObservation {
        proof_id: 17,
        elapsed: Duration::from_millis(8),
        sent_at: Instant::now(),
    };
    assert!(!record.mark_path_proof_success(predecessor, observation));
    assert_eq!(record.path_instance_id(), Some(replacement));
    assert!(!record.path_proof_success);

    assert!(record.mark_path_proof_success(replacement, observation));
    assert!(record.path_proof_success);
}

#[test]
fn stale_tcp_path_proof_ack_cannot_relabel_a_replacement_instance() {
    let predecessor = crate::model::path::next_carrier_path_instance_id();
    let replacement = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_tcp_peer_usage(PathId(2), predecessor, 0, PathUsage::Available);
    record.install_tcp_peer_usage(PathId(2), replacement, 0, PathUsage::Available);

    let observation = PathProofObservation {
        proof_id: 23,
        elapsed: Duration::from_millis(11),
        sent_at: Instant::now(),
    };
    assert!(!record.mark_path_proof_success(predecessor, observation));
    assert_eq!(record.path_instance_id(), Some(replacement));
    assert!(!record.path_proof_success);

    assert!(record.mark_path_proof_success(replacement, observation));
    assert!(record.path_proof_success);
}

#[test]
fn new_quic_native_epoch_clears_old_diagnostics_before_partial_snapshot() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(2);
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_udp_peer_usage(path_instance_id, 7, 0, PathUsage::Available);

    let mut old_epoch = request_quic_path_metrics(now, deadline);
    old_epoch.controller_path_epoch = 7;
    old_epoch.controller_bandwidth_bps = Some(480_000_000);
    old_epoch.inflight_hi = 4 * 1_024 * 1_024;
    old_epoch.bytes_in_flight = 512 * 1_024;
    old_epoch.pending_bytes = 768 * 1_024;
    old_epoch.loss_ppm = Some(25_000);
    old_epoch.ecn_ppm = Some(8_000);
    old_epoch.app_limited = false;
    record.mark_quic_path_metrics(path_instance_id, old_epoch);

    let mut partial_new_epoch = old_epoch;
    partial_new_epoch.controller_path_epoch = 8;
    partial_new_epoch.rtt_observed = false;
    partial_new_epoch.controller_bandwidth_bps = None;
    partial_new_epoch.loss_ppm = None;
    partial_new_epoch.ecn_ppm = None;
    partial_new_epoch.delivery_rate_bps = 1.0;
    partial_new_epoch.pacing_rate_bps = 1.0;
    partial_new_epoch.delivery_sample_count = 0;
    partial_new_epoch.delivery_sample_bytes = 0;
    partial_new_epoch.last_delivery_sample_at = None;
    partial_new_epoch.bulk_proof_expires_at = None;
    partial_new_epoch.ack_derived_data_seen = false;
    partial_new_epoch.app_limited = true;
    partial_new_epoch.inflight_hi = 96 * 1_024;
    partial_new_epoch.bytes_in_flight = 12 * 1_024;
    partial_new_epoch.pending_bytes = 20 * 1_024;
    record.mark_quic_path_metrics(path_instance_id, partial_new_epoch);

    assert_eq!(record.native_capacity_epoch(), 8);
    assert_eq!(record.carrier_srtt_ms, None);
    assert_eq!(record.carrier_rttvar_ms, None);
    assert_eq!(record.carrier_loss_rate, None);
    assert_eq!(record.carrier_ecn_rate, None);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_pacing_rate_bps, None);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert_eq!(record.carrier_delivery_sample_bytes, 0);
    assert_eq!(record.carrier_last_delivery_at, None);
    assert_eq!(record.carrier_bulk_proof_expires_at, None);
    assert!(!record.carrier_ack_derived_data_seen);
    assert_eq!(record.carrier_bytes_in_flight, 12 * 1_024);
    assert!(record.carrier_bytes_in_flight_observed);
    assert_eq!(record.carrier_queue_bytes, 8 * 1_024);
    assert!(record.carrier_queue_bytes_observed);
    assert_eq!(record.carrier_inflight_limit_bytes, 96 * 1_024);
    assert!(record.carrier_app_limited);
    assert_eq!(record.carrier_current_app_limited, Some(true));
}

#[test]
fn client_eligibility_epoch_is_the_exact_structural_fingerprint_lifetime() {
    let now = Instant::now();
    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    assert_eq!(record.eligibility_epoch(), Some(0));

    record.mutate_eligibility(|record| {
        record.install_udp_peer_usage(instance, 3, 0, PathUsage::Available);
    });
    assert_eq!(record.eligibility_epoch(), Some(1));

    record.mutate_eligibility(|record| {
        assert!(record.update_peer_usage(instance, 1, PathUsage::Available));
        assert!(record.reserve_load(TrafficClass::Throughput, now));
        record.release_load(TrafficClass::Throughput);
        record.invalidate_path_proofs();
    });
    assert_eq!(
        record.eligibility_epoch(),
        Some(1),
        "sequence, proof, and transient load changes are not structural",
    );

    record.mutate_eligibility(|record| {
        assert!(record.update_peer_usage(instance, 2, PathUsage::Backup));
    });
    assert_eq!(record.eligibility_epoch(), Some(2));

    record.mutate_eligibility(|record| record.mark_failure(now, false));
    assert_eq!(record.eligibility_epoch(), Some(3));
    record.mutate_eligibility(|record| record.mark_failure(now, false));
    assert_eq!(
        record.eligibility_epoch(),
        Some(3),
        "failure counters and deadlines do not invent structural transitions",
    );

    record.mutate_eligibility(|record| {
        assert!(record.mark_success_for_instance(instance, Duration::from_millis(10)));
    });
    assert_eq!(record.eligibility_epoch(), Some(4));
    record.mutate_eligibility(ClientPathHealthRecord::begin_planned_retirement);
    assert_eq!(record.eligibility_epoch(), Some(5));
    record.mutate_eligibility(ClientPathHealthRecord::begin_planned_retirement);
    assert_eq!(record.eligibility_epoch(), Some(5));
    record.mutate_eligibility(|record| {
        assert!(record.retire_planned_instance(instance));
    });
    assert_eq!(record.eligibility_epoch(), Some(6));

    let deadline = now + Duration::from_millis(1);
    let mut cooldown = ClientPathHealthRecord {
        state: SchedulerPathState::Failed,
        failed_until: Some(deadline),
        ..ClientPathHealthRecord::default()
    };
    cooldown.mutate_eligibility(|record| record.maintain(deadline));
    assert_eq!(cooldown.state, SchedulerPathState::Suspect);
    assert_eq!(cooldown.eligibility_epoch(), Some(1));

    cooldown.eligibility_epoch = Some(u64::MAX);
    cooldown.mutate_eligibility(|record| record.state = SchedulerPathState::Failed);
    assert_eq!(
        cooldown.eligibility_epoch(),
        None,
        "epoch exhaustion is permanent and non-fatal",
    );
    cooldown.mutate_eligibility(|record| record.state = SchedulerPathState::Active);
    assert_eq!(cooldown.eligibility_epoch(), None);
}

#[test]
fn draining_client_path_cannot_be_revived_by_same_instance_evidence() {
    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.mutate_eligibility(|record| {
        record.install_tcp_peer_usage(PathId(3), instance, 0, PathUsage::Available);
    });
    record.mutate_eligibility(ClientPathHealthRecord::begin_planned_retirement);
    let retirement_epoch = record.eligibility_epoch();
    assert_eq!(record.state, SchedulerPathState::Draining);

    record.mutate_eligibility(|record| {
        assert!(record.mark_tcp_transport_state(instance, request_tcp_native_observation(3)));
    });
    assert_eq!(record.state, SchedulerPathState::Draining);
    assert_eq!(record.eligibility_epoch(), retirement_epoch);
    assert!(
        record.carrier_srtt_ms.is_some(),
        "native diagnostics remain visible"
    );

    let sample = PathRateSample::new(64 * 1024, Duration::from_millis(10)).expect("rate sample");
    record.mutate_eligibility(|record| {
        record.mark_product_delivery_for_instance(instance, sample);
    });
    assert_eq!(record.state, SchedulerPathState::Draining);
    assert_eq!(record.eligibility_epoch(), retirement_epoch);
    assert_eq!(record.product_delivery_sample_bytes, sample.bytes());

    record.mutate_eligibility(|record| {
        record.mark_product_delivery_replacing_rate_for_instance(instance, sample);
    });
    assert_eq!(record.state, SchedulerPathState::Draining);
    assert_eq!(record.eligibility_epoch(), retirement_epoch);
    assert_eq!(
        record.measured_rate_bps, None,
        "replacement Product points remain raw Product diagnostics, not generic capacity",
    );

    let successor = crate::model::path::next_carrier_path_instance_id();
    record.mutate_eligibility(|record| {
        record.install_tcp_peer_usage(PathId(4), successor, 0, PathUsage::Available);
    });
    assert_eq!(record.state, SchedulerPathState::Active);
    assert_ne!(record.eligibility_epoch(), retirement_epoch);
}

#[test]
fn quic_bulk_proof_deadline_survives_a_later_app_limited_poll() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(2);
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let initial = request_quic_path_metrics(now, deadline);
    record.mark_quic_path_metrics(path_instance_id, initial);

    record.mark_quic_path_metrics(
        path_instance_id,
        UdpPathMetrics {
            delivery_rate_bps: 1.0,
            pacing_rate_bps: 2.0,
            ..initial
        },
    );

    let fresh = record.observation_at(now + Duration::from_secs(1));
    assert!(
        !fresh.carrier_app_limited,
        "the retained qualified delivery epoch remains non-application-limited"
    );
    assert_eq!(fresh.carrier_current_app_limited, Some(true));
    assert_eq!(fresh.carrier_bulk_proof_expires_at, Some(deadline));
    assert_eq!(fresh.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(fresh.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(
        record
            .observation_at(deadline)
            .carrier_bulk_proof_expires_at,
        None
    );
}

#[test]
fn expired_quic_proof_remains_stale_diagnostic_until_transport_epoch_reset() {
    let observed_at = Instant::now();
    let deadline = observed_at + Duration::from_secs(2);
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let live = request_quic_path_metrics(observed_at, deadline);
    record.mark_quic_path_metrics(path_instance_id, live);

    let mut expired = live;
    expired.delivery_rate_bps = 1.0;
    expired.pacing_rate_bps = 1.0;
    expired.delivery_sample_count = 0;
    expired.delivery_sample_bytes = 0;
    expired.bulk_proof_expires_at = None;
    record.mark_quic_path_metrics(path_instance_id, expired);

    let diagnostics = record.rate_diagnostics();
    assert_eq!(diagnostics.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(diagnostics.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(diagnostics.carrier_delivery_samples, 10);
    assert_eq!(diagnostics.carrier_delivery_sample_bytes, 512 * 1024);
    assert_eq!(diagnostics.carrier_last_delivery_at, Some(observed_at));
    assert_eq!(diagnostics.carrier_bulk_proof_expires_at, Some(deadline));
    assert_eq!(
        record.observation_at(deadline).carrier_delivery_rate_bps,
        None,
        "retained diagnostics must have no scheduling authority at expiry",
    );

    let mut reset = expired;
    reset.controller_path_epoch = reset.controller_path_epoch.saturating_add(1);
    reset.last_delivery_sample_at = None;
    reset.ack_derived_data_seen = false;
    record.mark_quic_path_metrics(path_instance_id, reset);
    let reset_diagnostics = record.rate_diagnostics();
    assert_eq!(reset_diagnostics.carrier_delivery_rate_bps, None);
    assert_eq!(reset_diagnostics.carrier_pacing_rate_bps, None);
    assert_eq!(reset_diagnostics.carrier_delivery_samples, 0);
    assert_eq!(reset_diagnostics.carrier_delivery_sample_bytes, 0);
    assert_eq!(reset_diagnostics.carrier_last_delivery_at, None);
    assert_eq!(reset_diagnostics.carrier_bulk_proof_expires_at, None);
    assert!(!reset_diagnostics.carrier_ack_derived_data_seen);
}

#[test]
fn tcp_partial_metric_availability_distinguishes_unknown_from_observed_zero() {
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let partial = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 20_000,
            rttvar_us: Some(5_000),
        }),
        flight: Some(TcpNativeFlight {
            // macOS exposes congestion-window shape but not exact network flight.
            bytes_in_flight: None,
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: Some(512 * 1024),
        }),
        notsent_bytes: None,
        ..TcpNativeSnapshot::default()
    };
    let mut tracker = TcpSenderMetricTracker::new(partial);
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        tracker.observe(PathId(0), PathMetricDirection::ClientToServer, partial),
    ));
    let unknown = record.observation_at(Instant::now());
    assert!(!unknown.carrier_bytes_in_flight_observed);
    assert!(!unknown.carrier_queue_bytes_observed);

    let partial_observed_zero = TcpNativeSnapshot {
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(0),
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: Some(512 * 1024),
        }),
        ..partial
    };
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        tracker.observe(
            PathId(0),
            PathMetricDirection::ClientToServer,
            partial_observed_zero,
        ),
    ));
    let partial_observed = record.observation_at(Instant::now());
    assert!(partial_observed.carrier_bytes_in_flight_observed);
    assert_eq!(partial_observed.carrier_bytes_in_flight, 0);
    assert!(!partial_observed.carrier_queue_bytes_observed);

    let fully_observed_zero = TcpNativeSnapshot {
        notsent_bytes: Some(0),
        ..partial_observed_zero
    };
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        tracker.observe(
            PathId(0),
            PathMetricDirection::ClientToServer,
            fully_observed_zero,
        ),
    ));
    let fully_observed = record.observation_at(Instant::now());
    assert!(fully_observed.carrier_bytes_in_flight_observed);
    assert_eq!(fully_observed.carrier_bytes_in_flight, 0);
    assert!(fully_observed.carrier_queue_bytes_observed);
    assert_eq!(fully_observed.carrier_queue_bytes, 0);

    let observed_nonzero = TcpNativeSnapshot {
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(64 * 1024),
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: Some(512 * 1024),
        }),
        notsent_bytes: Some(4 * 1024),
        ..partial
    };
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        tracker.observe(
            PathId(0),
            PathMetricDirection::ClientToServer,
            observed_nonzero,
        ),
    ));
    assert!(record.carrier_bytes_in_flight_observed);
    assert!(record.carrier_queue_bytes_observed);
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        tracker.observe(PathId(0), PathMetricDirection::ClientToServer, partial),
    ));
    let unavailable_after_nonzero = record.observation_at(Instant::now());
    assert!(!unavailable_after_nonzero.carrier_bytes_in_flight_observed);
    assert!(!unavailable_after_nonzero.carrier_queue_bytes_observed);
    let path = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    let unavailable_snapshot = path_snapshot(&path, 0, unavailable_after_nonzero);
    assert_eq!(
        unavailable_snapshot.bytes_in_flight, 0,
        "retained raw flight is diagnostic-only once the current capability is absent"
    );
    assert_eq!(
        unavailable_snapshot.queue_bytes, 0,
        "retained raw queue is diagnostic-only once the current capability is absent"
    );
    assert_eq!(
        unavailable_snapshot.carrier_inflight_limit_bytes,
        512 * 1024,
        "the independently observed carrier window remains available without exact flight"
    );

    let loss_baseline = TcpNativeSnapshot {
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        ..partial
    };
    let loss_current = TcpNativeSnapshot {
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 20,
        }),
        ..partial
    };
    let mut loss_tracker = TcpSenderMetricTracker::new(loss_baseline);
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        loss_tracker.observe(PathId(0), PathMetricDirection::ClientToServer, loss_current),
    ));
    assert_eq!(record.carrier_loss_rate, Some(0.0));
    assert!(record.mark_tcp_transport_state(
        path_instance_id,
        loss_tracker.observe(PathId(0), PathMetricDirection::ClientToServer, partial),
    ));
    assert_eq!(record.carrier_loss_rate, None);
}

#[test]
fn stale_rate_evidence_loses_placement_rights_but_remains_diagnostic() {
    let now = Instant::now();
    let sample_at = now - Duration::from_secs(60);
    let proof_deadline = sample_at + Duration::from_secs(1);
    let record = ClientPathHealthRecord {
        measured_srtt_ms: Some(20.0),
        measured_jitter_ms: Some(5.0),
        measured_rate_bps: Some(80_000_000.0),
        delivery_samples: 7,
        delivery_sample_bytes: 600_000,
        product_delivery_rate_bps: Some(70_000_000.0),
        product_delivery_samples: 5,
        product_delivery_sample_bytes: 700_000,
        datagram_feedback_samples: 2,
        last_delivery_at: Some(sample_at),
        delivery_rate_expires_at: Some(proof_deadline),
        product_last_delivery_at: Some(sample_at),
        product_delivery_rate_expires_at: Some(proof_deadline),
        carrier_srtt_ms: Some(20.0),
        carrier_rttvar_ms: Some(5.0),
        carrier_loss_rate: Some(0.125),
        carrier_delivery_rate_bps: Some(90_000_000.0),
        carrier_pacing_rate_bps: Some(100_000_000.0),
        carrier_delivery_samples: 9,
        carrier_delivery_sample_bytes: 900_000,
        carrier_delivery_window_covered: true,
        carrier_last_delivery_at: Some(sample_at),
        carrier_bulk_proof_expires_at: Some(proof_deadline),
        carrier_app_limited: false,
        carrier_current_app_limited: Some(false),
        carrier_ack_derived_data_seen: true,
        ..ClientPathHealthRecord::default()
    };

    let observation = record.observation_at(now);
    assert_eq!(observation.measured_rate_bps, None);
    assert_eq!(observation.product_delivery_rate_bps, None);
    assert_eq!(observation.delivery_samples, 0);
    assert_eq!(observation.delivery_sample_bytes, 0);
    assert_eq!(observation.product_delivery_samples, 0);
    assert_eq!(observation.product_delivery_sample_bytes, 0);
    assert_eq!(observation.datagram_feedback_samples, 0);
    assert_eq!(observation.last_delivery_at, None);
    assert_eq!(observation.product_last_delivery_at, None);
    assert_eq!(observation.carrier_delivery_rate_bps, None);
    assert_eq!(observation.carrier_pacing_rate_bps, None);
    assert_eq!(observation.carrier_delivery_samples, 0);
    assert_eq!(observation.carrier_delivery_sample_bytes, 0);
    assert!(!observation.carrier_delivery_window_covered);
    assert_eq!(observation.carrier_last_delivery_at, None);
    assert_eq!(observation.carrier_bulk_proof_expires_at, None);
    assert!(observation.carrier_app_limited);
    assert_eq!(
        observation.carrier_current_app_limited,
        Some(false),
        "delivery-evidence expiry must not manufacture current native underfill"
    );
    assert!(!observation.carrier_ack_derived_data_seen);
    assert_eq!(observation.carrier_loss_rate, Some(0.125));

    assert_eq!(
        record.rate_diagnostics(),
        ClientPathRateDiagnostics {
            measured_rate_bps: Some(80_000_000.0),
            product_delivery_rate_bps: Some(70_000_000.0),
            delivery_samples: 7,
            delivery_sample_bytes: 600_000,
            product_delivery_samples: 5,
            product_delivery_sample_bytes: 700_000,
            datagram_feedback_samples: 2,
            last_delivery_at: Some(sample_at),
            delivery_rate_expires_at: Some(proof_deadline),
            product_last_delivery_at: Some(sample_at),
            product_delivery_rate_expires_at: Some(proof_deadline),
            carrier_delivery_rate_bps: Some(90_000_000.0),
            carrier_pacing_rate_bps: Some(100_000_000.0),
            carrier_delivery_samples: 9,
            carrier_delivery_sample_bytes: 900_000,
            carrier_delivery_window_covered: true,
            carrier_last_delivery_at: Some(sample_at),
            carrier_bulk_proof_expires_at: Some(proof_deadline),
            carrier_app_limited: false,
            carrier_ack_derived_data_seen: true,
        }
    );
}

#[test]
fn fresh_rate_evidence_projects_without_value_or_provenance_changes() {
    let now = Instant::now();
    let sample_at = now - Duration::from_millis(1);
    let proof_deadline = now + Duration::from_secs(1);
    let record = ClientPathHealthRecord {
        measured_srtt_ms: Some(20.0),
        measured_jitter_ms: Some(5.0),
        measured_rate_bps: Some(80_000_000.0),
        delivery_samples: 7,
        delivery_sample_bytes: 600_000,
        product_delivery_rate_bps: Some(70_000_000.0),
        product_delivery_samples: 5,
        product_delivery_sample_bytes: 700_000,
        datagram_feedback_samples: 2,
        last_delivery_at: Some(sample_at),
        delivery_rate_expires_at: Some(proof_deadline),
        product_last_delivery_at: Some(sample_at),
        product_delivery_rate_expires_at: Some(proof_deadline),
        carrier_srtt_ms: Some(20.0),
        carrier_rttvar_ms: Some(5.0),
        carrier_delivery_rate_bps: Some(90_000_000.0),
        carrier_pacing_rate_bps: Some(100_000_000.0),
        carrier_delivery_samples: 9,
        carrier_delivery_sample_bytes: 900_000,
        carrier_delivery_window_covered: true,
        carrier_last_delivery_at: Some(sample_at),
        carrier_bulk_proof_expires_at: Some(proof_deadline),
        carrier_app_limited: false,
        carrier_ack_derived_data_seen: true,
        ..ClientPathHealthRecord::default()
    };

    let observation = record.observation_at(now);
    assert_eq!(observation.measured_rate_bps, Some(80_000_000.0));
    assert_eq!(observation.product_delivery_rate_bps, Some(70_000_000.0));
    assert_eq!(observation.delivery_samples, 7);
    assert_eq!(observation.delivery_sample_bytes, 600_000);
    assert_eq!(observation.product_delivery_samples, 5);
    assert_eq!(observation.product_delivery_sample_bytes, 700_000);
    assert_eq!(observation.datagram_feedback_samples, 2);
    assert_eq!(observation.last_delivery_at, Some(sample_at));
    assert_eq!(observation.product_last_delivery_at, Some(sample_at));
    assert_eq!(observation.carrier_delivery_rate_bps, Some(90_000_000.0));
    assert_eq!(observation.carrier_pacing_rate_bps, Some(100_000_000.0));
    assert_eq!(observation.carrier_delivery_samples, 9);
    assert_eq!(observation.carrier_delivery_sample_bytes, 900_000);
    assert!(observation.carrier_delivery_window_covered);
    assert_eq!(observation.carrier_last_delivery_at, Some(sample_at));
    assert_eq!(
        observation.carrier_bulk_proof_expires_at,
        Some(proof_deadline)
    );
    assert!(!observation.carrier_app_limited);
    assert!(observation.carrier_ack_derived_data_seen);
}

#[test]
fn rate_freshness_uses_the_established_three_pto_boundary() {
    let sample_at = Instant::now();
    let srtt = Duration::from_millis(80);
    let established_horizon = transport_rate_sample_freshness_horizon(srtt, srtt / 8);
    let mut record = ClientPathHealthRecord {
        measured_srtt_ms: Some(srtt.as_secs_f64() * 1_000.0),
        measured_rate_bps: Some(10_000_000.0),
        delivery_samples: 1,
        last_delivery_at: Some(sample_at),
        delivery_rate_expires_at: Some(sample_at + established_horizon),
        ..ClientPathHealthRecord::default()
    };

    assert_eq!(record.rate_sample_freshness_horizon(), established_horizon);
    record.measured_srtt_ms = Some(5_000.0);
    assert_eq!(
        record
            .observation_at(sample_at + established_horizon)
            .measured_rate_bps,
        None,
        "later RTT growth cannot extend the immutable sample deadline"
    );
    record.measured_srtt_ms = Some(1.0);
    assert_eq!(
        record
            .observation_at(sample_at + established_horizon - Duration::from_nanos(1))
            .measured_rate_bps,
        Some(10_000_000.0),
        "later RTT shrink cannot shorten the immutable sample deadline"
    );
    assert_eq!(
        record
            .observation_at(sample_at + established_horizon)
            .measured_rate_bps,
        None
    );
}

#[test]
fn generic_udp_feedback_does_not_refresh_expired_product_authority() {
    let generic_at = Instant::now();
    let product_at = generic_at - Duration::from_secs(2);
    let product_expires_at = generic_at - Duration::from_secs(1);
    let generic_expires_at = generic_at + Duration::from_secs(2);
    let mut record = ClientPathHealthRecord {
        measured_srtt_ms: Some(100.0),
        product_delivery_rate_bps: Some(800_000_000.0),
        product_delivery_samples: 99,
        product_delivery_sample_bytes: 8 * 1024 * 1024,
        product_last_delivery_at: Some(product_at),
        product_delivery_rate_expires_at: Some(product_expires_at),
        ..ClientPathHealthRecord::default()
    };
    let generic_sample =
        PathRateSample::new(64 * 1024, Duration::from_millis(10)).expect("rate sample");
    record.mark_udp_datagram_feedback_at(
        UdpDatagramPathObservation {
            rtt: Duration::from_millis(100),
            jitter: Duration::from_millis(5),
            loss_rate: None,
            rate_sample: Some(generic_sample),
            rate_sample_expires_at: Some(generic_expires_at),
        },
        generic_at,
    );

    let observation = record.observation_at(generic_at);
    assert_eq!(
        observation.measured_rate_bps,
        Some(generic_sample.rate_bps())
    );
    assert_eq!(record.delivery_samples, 1);
    assert_eq!(record.delivery_sample_bytes, generic_sample.bytes());
    assert_eq!(observation.product_delivery_rate_bps, None);
    assert_eq!(observation.product_delivery_samples, 0);
    assert_eq!(observation.product_last_delivery_at, None);
    let diagnostics = record.rate_diagnostics();
    assert_eq!(diagnostics.product_delivery_rate_bps, Some(800_000_000.0));
    assert_eq!(diagnostics.product_delivery_samples, 99);
    assert_eq!(diagnostics.product_last_delivery_at, Some(product_at));
    assert_eq!(
        diagnostics.product_delivery_rate_expires_at,
        Some(product_expires_at)
    );

    let path = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("QUIC path");
    let snapshot = path_snapshot(&path, 0, observation);
    let metrics = path_metrics_from_snapshot_at(
        snapshot,
        observation,
        PathMetricDirection::ClientToServer,
        generic_at,
    );
    assert_eq!(snapshot.delivery_rate_bps, generic_sample.rate_bps());
    assert_eq!(metrics.rate_valid_for_us, 2_000_000);
    assert_eq!(metrics.data_sample_count, 1);
    assert_eq!(metrics.data_sample_bytes, generic_sample.bytes());
}

#[test]
fn product_delivery_does_not_refresh_expired_generic_or_its_path_metrics_epoch() {
    let product_at = Instant::now();
    let generic_at = product_at - Duration::from_secs(2);
    let generic_expires_at = product_at - Duration::from_secs(1);
    let mut record = ClientPathHealthRecord {
        measured_rate_bps: Some(800_000_000.0),
        delivery_samples: 100,
        delivery_sample_bytes: 8 * 1024 * 1024,
        datagram_feedback_samples: 40,
        last_delivery_at: Some(generic_at),
        delivery_rate_expires_at: Some(generic_expires_at),
        ..ClientPathHealthRecord::default()
    };
    let sample = PathRateSample::new(64 * 1024, Duration::from_millis(10)).expect("sample");

    record.mark_product_delivery_at(sample, product_at, false);

    let observation = record.observation_at(product_at);
    assert_eq!(observation.measured_rate_bps, None);
    assert_eq!(observation.delivery_samples, 0);
    assert_eq!(observation.last_delivery_at, None);
    assert_eq!(record.product_delivery_rate_bps, Some(sample.rate_bps()));
    assert_eq!(record.product_delivery_samples, 1);
    assert_eq!(record.product_delivery_sample_bytes, sample.bytes());
    assert_eq!(record.product_last_delivery_at, Some(product_at));
    let diagnostics = record.rate_diagnostics();
    assert_eq!(diagnostics.measured_rate_bps, Some(800_000_000.0));
    assert_eq!(diagnostics.delivery_samples, 100);
    assert_eq!(diagnostics.last_delivery_at, Some(generic_at));
    assert_eq!(
        diagnostics.delivery_rate_expires_at,
        Some(generic_expires_at)
    );

    let path = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("QUIC path");
    let snapshot = path_snapshot(&path, 0, observation);
    let metrics = path_metrics_from_snapshot_at(
        snapshot,
        observation,
        PathMetricDirection::ClientToServer,
        product_at,
    );
    assert_eq!(snapshot.delivery_rate_bps, sample.rate_bps());
    assert_eq!(metrics.data_sample_count, 1);
    assert_eq!(metrics.data_sample_bytes, sample.bytes());
    assert!(metrics.rate_valid_for_us > 0);
}

#[test]
fn quic_native_congestion_remains_diagnostic_without_becoming_product_feedback() {
    use crate::runtime::path::model::{path_metrics_from_snapshot, path_snapshot};
    use crate::transport::PathSpec;

    let now = Instant::now();
    let deadline = now + Duration::from_secs(2);
    let path = "quic://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("QUIC path");
    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(instance, 0, PathUsage::Available);
    let mut metrics = request_quic_path_metrics(now, deadline);
    metrics.loss_ppm = Some(125_000);
    metrics.ecn_ppm = Some(25_000);
    record.mark_quic_path_metrics(instance, metrics);

    let observation = record.observation_at(Instant::now());
    assert_eq!(observation.carrier_loss_rate, Some(0.125));
    assert_eq!(observation.carrier_ecn_rate, Some(0.025));
    assert_eq!(observation.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(observation.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(observation.carrier_delivery_samples, 10);
    assert_eq!(observation.carrier_delivery_sample_bytes, 512 * 1024);
    assert!(observation.carrier_ack_derived_data_seen);
    assert_eq!(observation.product_delivery_rate_bps, None);
    assert_eq!(observation.product_delivery_sample_bytes, 0);
    assert_eq!(observation.delivery_samples, 0);
    let snapshot = path_snapshot(&path, 0, observation);
    assert_eq!(snapshot.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(snapshot.product_progress_rate_bps, None);
    assert_eq!(snapshot.delivery_rate_bps, 200_000_000.0);
    assert_eq!(snapshot.carrier_inflight_limit_bytes, 4 * 1024 * 1024);
    assert_eq!(snapshot.srtt_ms, 180.0);
    assert_eq!(snapshot.loss_rate, 0.0);
    let published =
        path_metrics_from_snapshot(snapshot, observation, PathMetricDirection::ClientToServer);
    assert!(published.loss_observed);
    assert_eq!(published.loss_ppm, 125_000);
    assert!(published.ecn_observed);
    assert_eq!(published.ecn_ppm, 25_000);
    assert!(published.rate_observed);
    assert!(published.has_ack_derived_data_sample);
    assert_eq!(published.delivery_rate_bps, 200_000_000);
    assert_eq!(published.data_sample_bytes, 512 * 1024);

    metrics.loss_ppm = None;
    metrics.ecn_ppm = None;
    record.mark_quic_path_metrics(instance, metrics);
    let retained_observation = record.observation_at(Instant::now());
    assert_eq!(retained_observation.carrier_loss_rate, Some(0.125));
    assert_eq!(retained_observation.carrier_ecn_rate, Some(0.025));
    let retained_published = path_metrics_from_snapshot(
        path_snapshot(&path, 0, retained_observation),
        retained_observation,
        PathMetricDirection::ClientToServer,
    );
    assert!(retained_published.loss_observed);
    assert_eq!(retained_published.loss_ppm, 125_000);
    assert!(retained_published.ecn_observed);
    assert_eq!(retained_published.ecn_ppm, 25_000);

    let unknown_instance = crate::model::path::next_carrier_path_instance_id();
    let mut unknown = ClientPathHealthRecord::default();
    unknown.install_peer_usage(unknown_instance, 0, PathUsage::Available);
    unknown.mark_quic_path_metrics(unknown_instance, metrics);
    let unknown_observation = unknown.observation_at(Instant::now());
    let unknown_published = path_metrics_from_snapshot(
        path_snapshot(&path, 0, unknown_observation),
        unknown_observation,
        PathMetricDirection::ClientToServer,
    );
    assert_eq!(unknown_observation.carrier_loss_rate, None);
    assert_eq!(unknown_observation.carrier_ecn_rate, None);
    assert!(!unknown_published.loss_observed);
    assert!(!unknown_published.ecn_observed);

    metrics.loss_ppm = Some(0);
    metrics.ecn_ppm = Some(0);
    unknown.mark_quic_path_metrics(unknown_instance, metrics);
    let zero_observation = unknown.observation_at(Instant::now());
    let zero_published = path_metrics_from_snapshot(
        path_snapshot(&path, 0, zero_observation),
        zero_observation,
        PathMetricDirection::ClientToServer,
    );
    assert_eq!(zero_observation.carrier_loss_rate, Some(0.0));
    assert_eq!(zero_observation.carrier_ecn_rate, Some(0.0));
    assert!(zero_published.loss_observed);
    assert_eq!(zero_published.loss_ppm, 0);
    assert!(zero_published.ecn_observed);
    assert_eq!(zero_published.ecn_ppm, 0);
}

#[test]
fn stale_quic_metrics_cannot_overwrite_a_reconnected_carrier() {
    let now = Instant::now();
    let mut record = ClientPathHealthRecord::default();
    let stale_instance = crate::model::path::next_carrier_path_instance_id();
    let live_instance = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(live_instance, 0, PathUsage::Available);

    let stale_metrics = request_quic_path_metrics(now, now + Duration::from_secs(2));
    record.mark_quic_path_metrics(stale_instance, stale_metrics);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_bulk_proof_expires_at, None);
}

#[test]
fn terminal_carrier_failure_rejects_delayed_native_metrics() {
    let now = Instant::now();
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    assert!(record.mark_data_plane_failure(path_instance_id, now, true));

    record.mark_quic_path_metrics(
        path_instance_id,
        request_quic_path_metrics(now, now + Duration::from_secs(2)),
    );
    assert!(!record.mark_tcp_transport_state(path_instance_id, request_tcp_native_observation(0),));

    assert_eq!(record.state, SchedulerPathState::Failed);
    assert_eq!(record.carrier_srtt_ms, None);
    assert_eq!(record.carrier_delivery_rate_bps, None);
}

#[test]
fn tcp_transport_state_updates_native_rtt_without_rate_authority() {
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let observation = request_tcp_native_observation(2);

    assert!(record.mark_tcp_transport_state(path_instance_id, observation));

    assert_eq!(record.carrier_srtt_ms, Some(180.0));
    assert_eq!(record.carrier_rttvar_ms, Some(10.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert!(record.native_drain_observed);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert!(!record.carrier_ack_derived_data_seen);
}

#[test]
fn tcp_transport_state_retains_non_app_limited_ack_window_without_data_ack_authority() {
    let baseline = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 180_000,
            rttvar_us: Some(10_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(128 * 1_024),
            inflight_limit_bytes: 512 * 1_024,
            inflight_hi_bytes: Some(512 * 1_024),
        }),
        notsent_bytes: Some(4_096),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        pacing_rate_bytes_per_second: Some(31_250_000),
        delivery_rate_bytes_per_second: Some(25_000_000),
        app_limited: Some(false),
    };
    let current = TcpNativeSnapshot {
        bytes_acked: Some(100 + 1024 * 1024),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        current,
    );
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    let sample_at = Instant::now();

    assert!(record.mark_tcp_transport_state_at(path_instance_id, observation, sample_at));

    let native_window = record
        .carrier_native_window_sample
        .expect("TCP native C keeps its own observation epoch");
    assert_eq!(native_window.inflight_limit_bytes, 512 * 1024);
    assert_eq!(native_window.observed_at, sample_at);
    assert!(native_window.fresh_at(sample_at));
    assert_eq!(record.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 1024 * 1024);
    assert!(record.carrier_delivery_window_covered);
    assert!(record.carrier_last_delivery_at.is_some());
    let frozen_expires_at = record
        .carrier_bulk_proof_expires_at
        .expect("TCP ACK sample freezes its three-PTO deadline");
    assert!(!record.carrier_app_limited);
    assert_eq!(record.carrier_current_app_limited, Some(false));
    assert!(!record.carrier_ack_derived_data_seen);

    let app_limited_current = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 2_000_000,
            rttvar_us: Some(500_000),
        }),
        bytes_acked: current.bytes_acked.map(|bytes| bytes + 512 * 1024),
        delivery_rate_bytes_per_second: Some(1_000_000),
        pacing_rate_bytes_per_second: Some(2_000_000),
        app_limited: Some(true),
        ..current
    };
    let app_limited = TcpSenderMetricTracker::new(current).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        app_limited_current,
    );
    assert!(record.mark_tcp_transport_state_at(
        path_instance_id,
        app_limited,
        sample_at + Duration::from_millis(1),
    ));
    assert_eq!(record.carrier_delivery_rate_bps, Some(200_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, Some(250_000_000.0));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 1024 * 1024);
    assert!(!record.carrier_app_limited);
    assert_eq!(
        record.carrier_current_app_limited,
        Some(true),
        "the latest same-socket native poll is currently application-limited"
    );
    let tcp_path = "tcp://127.0.0.1:10000"
        .parse::<PathSpec>()
        .expect("TCP path");
    assert!(
        crate::runtime::path::model::bulk_candidate_has_native_carrier_rate_evidence(
            &tcp_path,
            record.observation_at(sample_at + Duration::from_millis(1)),
        ),
        "current native underfill must not revoke the retained qualified delivery epoch"
    );
    assert_eq!(
        record.carrier_bulk_proof_expires_at,
        Some(frozen_expires_at)
    );

    let app_limited_shrink_current = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 1_000,
            rttvar_us: Some(100),
        }),
        bytes_acked: app_limited_current
            .bytes_acked
            .map(|bytes| bytes + 512 * 1024),
        ..app_limited_current
    };
    let app_limited_shrink = TcpSenderMetricTracker::new(app_limited_current).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        app_limited_shrink_current,
    );
    assert!(record.mark_tcp_transport_state_at(
        path_instance_id,
        app_limited_shrink,
        sample_at + Duration::from_millis(2),
    ));
    assert_eq!(
        record.carrier_bulk_proof_expires_at,
        Some(frozen_expires_at)
    );
    assert_eq!(
        record
            .observation_at(frozen_expires_at - Duration::from_nanos(1))
            .carrier_delivery_rate_bps,
        Some(200_000_000.0),
    );
    assert_eq!(
        record
            .observation_at(frozen_expires_at)
            .carrier_delivery_rate_bps,
        None,
    );

    // An ACK at the frozen boundary starts a new accumulation epoch even after
    // the app-limited RTT update greatly increased the live RTT shape.
    assert!(record.mark_tcp_transport_state_at(path_instance_id, observation, frozen_expires_at,));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 1024 * 1024);
    assert_eq!(record.carrier_last_delivery_at, Some(frozen_expires_at));
}

#[test]
fn qualifying_tcp_epoch_without_pacing_clears_prior_epoch_pacing() {
    let baseline = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 20_000,
            rttvar_us: Some(2_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(128 * 1_024),
            inflight_limit_bytes: 512 * 1_024,
            inflight_hi_bytes: Some(512 * 1_024),
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
        loss: Some(TcpNativeLossCounters {
            retransmits: 0,
            data_segments_out: 10,
        }),
        pacing_rate_bytes_per_second: Some(25_000_000),
        delivery_rate_bytes_per_second: Some(20_000_000),
        app_limited: Some(false),
    };
    let first_snapshot = TcpNativeSnapshot {
        bytes_acked: Some(100 + 1024 * 1024),
        ..baseline
    };
    let first = TcpSenderMetricTracker::new(baseline).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        first_snapshot,
    );
    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(instance, 0, PathUsage::Available);
    let first_at = Instant::now();
    assert!(record.mark_tcp_transport_state_at(instance, first, first_at));
    assert_eq!(record.carrier_pacing_rate_bps, Some(200_000_000.0));

    let second_snapshot = TcpNativeSnapshot {
        bytes_acked: first_snapshot.bytes_acked.map(|bytes| bytes + 64 * 1024),
        delivery_rate_bytes_per_second: Some(10_000_000),
        // Linux uses this sentinel when the current native pacing value is
        // unavailable; it must not inherit the preceding sample's value.
        pacing_rate_bytes_per_second: Some(u64::MAX),
        ..first_snapshot
    };
    let second = TcpSenderMetricTracker::new(first_snapshot).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        second_snapshot,
    );
    let second_at = first_at + Duration::from_millis(1);
    assert!(record.mark_tcp_transport_state_at(instance, second, second_at));
    assert_eq!(record.carrier_delivery_rate_bps, Some(80_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, None);
    assert_eq!(record.carrier_last_delivery_at, Some(second_at));
    assert_eq!(
        record.observation_at(second_at).carrier_pacing_rate_bps,
        None,
        "an unavailable value cannot be relabelled into the new delivery epoch",
    );
}

#[test]
fn first_tcp_ack_after_a_stale_gap_starts_a_new_native_evidence_epoch() {
    let baseline = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 20_000,
            rttvar_us: Some(5_000),
        }),
        flight: Some(TcpNativeFlight {
            bytes_in_flight: Some(64 * 1024),
            inflight_limit_bytes: 512 * 1024,
            inflight_hi_bytes: Some(512 * 1024),
        }),
        notsent_bytes: Some(0),
        bytes_acked: Some(100),
        retransmission_counter: Some(0),
        delivery_rate_bytes_per_second: Some(2_000_000),
        pacing_rate_bytes_per_second: Some(3_000_000),
        app_limited: Some(false),
        ..TcpNativeSnapshot::default()
    };
    let current = TcpNativeSnapshot {
        bytes_acked: Some(100 + 4_096),
        ..baseline
    };
    let observation = TcpSenderMetricTracker::new(baseline).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        current,
    );
    assert!(!observation.delivery_window_covered());

    let instance = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(instance, 0, PathUsage::Available);
    record.carrier_srtt_ms = Some(20.0);
    record.carrier_rttvar_ms = Some(5.0);
    record.carrier_delivery_rate_bps = Some(1_000_000_000.0);
    record.carrier_pacing_rate_bps = Some(1_100_000_000.0);
    record.carrier_delivery_samples = 100;
    record.carrier_delivery_sample_bytes = 8 * 1024 * 1024;
    record.carrier_delivery_window_covered = true;
    record.carrier_last_delivery_at = Some(Instant::now() - Duration::from_secs(60));
    record.carrier_app_limited = false;

    assert!(record.mark_tcp_transport_state(instance, observation));

    assert_eq!(record.carrier_delivery_rate_bps, Some(16_000_000.0));
    assert_eq!(record.carrier_pacing_rate_bps, Some(24_000_000.0));
    assert_eq!(record.carrier_delivery_samples, 1);
    assert_eq!(record.carrier_delivery_sample_bytes, 4_096);
    assert!(!record.carrier_delivery_window_covered);
    assert!(!record.carrier_app_limited);
    assert!(record.carrier_bulk_proof_expires_at.is_some());
}

#[test]
fn replacement_tcp_carrier_rejects_stale_native_observation_and_clears_credit() {
    let original = crate::model::path::next_carrier_path_instance_id();
    let replacement = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_peer_usage(original, 0, PathUsage::Available);
    record.measured_srtt_ms = Some(25.0);
    record.measured_jitter_ms = Some(3.0);
    record.measured_rate_bps = Some(180_000_000.0);
    record.measured_loss_rate = Some(0.01);
    record.delivery_samples = 4;
    record.delivery_sample_bytes = 1024 * 1024;
    record.product_delivery_rate_bps = Some(175_000_000.0);
    record.product_delivery_samples = 3;
    record.product_delivery_sample_bytes = 2 * 1024 * 1024;
    record.datagram_feedback_samples = 2;
    let product_sample_at = Instant::now();
    record.last_delivery_at = Some(product_sample_at);
    record.delivery_rate_expires_at = Some(product_sample_at + Duration::from_secs(1));
    record.product_last_delivery_at = Some(product_sample_at);
    record.product_delivery_rate_expires_at = Some(product_sample_at + Duration::from_secs(1));
    record.relay_bytes_in_flight = 64 * 1024;
    record.relay_queue_bytes = 32 * 1024;
    record.carrier_delivery_rate_bps = Some(200_000_000.0);
    record.carrier_pacing_rate_bps = Some(250_000_000.0);
    record.carrier_loss_rate = Some(0.05);
    record.carrier_bytes_in_flight = 128 * 1024;
    record.carrier_inflight_limit_bytes = 512 * 1024;
    record.carrier_delivery_samples = 3;
    record.carrier_delivery_sample_bytes = 1024 * 1024;
    record.carrier_delivery_window_covered = true;
    record.carrier_last_delivery_at = Some(Instant::now());
    record.carrier_app_limited = false;
    record.active_flows = 2;
    record.active_latency_sensitive_flows = 1;
    record.install_peer_usage(replacement, 0, PathUsage::Available);

    assert_eq!(record.measured_srtt_ms, None);
    assert_eq!(record.measured_jitter_ms, None);
    assert_eq!(record.measured_rate_bps, None);
    assert_eq!(record.measured_loss_rate, None);
    assert_eq!(record.delivery_samples, 0);
    assert_eq!(record.delivery_sample_bytes, 0);
    assert_eq!(record.product_delivery_rate_bps, None);
    assert_eq!(record.product_delivery_samples, 0);
    assert_eq!(record.product_delivery_sample_bytes, 0);
    assert_eq!(record.datagram_feedback_samples, 0);
    assert_eq!(record.last_delivery_at, None);
    assert_eq!(record.delivery_rate_expires_at, None);
    assert_eq!(record.product_last_delivery_at, None);
    assert_eq!(record.product_delivery_rate_expires_at, None);
    assert_eq!(record.relay_bytes_in_flight, 0);
    assert_eq!(record.relay_queue_bytes, 0);
    assert_eq!(record.carrier_delivery_rate_bps, None);
    assert_eq!(record.carrier_pacing_rate_bps, None);
    assert_eq!(record.carrier_loss_rate, None);
    assert_eq!(record.carrier_bytes_in_flight, 0);
    assert_eq!(record.carrier_inflight_limit_bytes, 0);
    assert_eq!(record.carrier_native_window_sample, None);
    assert_eq!(record.carrier_delivery_samples, 0);
    assert!(!record.carrier_delivery_window_covered);
    assert_eq!(record.carrier_last_delivery_at, None);
    assert_eq!(record.active_flows, 0);
    assert_eq!(record.active_latency_sensitive_flows, 0);
    assert!(!record.mark_tcp_transport_state(original, request_tcp_native_observation(0)));
    assert_eq!(record.carrier_srtt_ms, None);

    record.begin_planned_retirement();
    assert_eq!(record.state, SchedulerPathState::Draining);
    assert!(record.retire_planned_instance(replacement));
    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.consecutive_failures, 0);
    assert_eq!(record.failed_until, None);
    assert_eq!(record.active_flows, 0);
    assert_eq!(record.active_latency_sensitive_flows, 0);
    assert!(!record.retire_planned_instance(replacement));
}

#[test]
fn attachment_evidence_and_open_success_are_owned_by_the_exact_tcp_carrier() {
    let now = Instant::now();
    let original = crate::model::path::next_carrier_path_instance_id();
    let replacement = crate::model::path::next_carrier_path_instance_id();
    let mut record = ClientPathHealthRecord::default();
    record.install_tcp_peer_usage(PathId(3), original, 0, PathUsage::Available);
    record.install_tcp_peer_usage(PathId(7), replacement, 0, PathUsage::Available);
    record.state = SchedulerPathState::Suspect;

    assert!(record.observation_for_instance_at(original, now).is_none());
    assert_eq!(
        record
            .observation_for_instance_at(replacement, now)
            .and_then(|observation| observation.wire_path_id),
        Some(PathId(7))
    );

    assert!(!record.mark_reserved_open_success_for_instance(original, Duration::from_millis(20)));
    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert!(!record.mark_open_success_for_instance(
        original,
        Duration::from_millis(20),
        TrafficClass::Latency,
    ));
    assert_eq!(record.active_flows, 0);

    assert!(record.mark_reserved_open_success_for_instance(replacement, Duration::from_millis(20)));
    assert_eq!(record.state, SchedulerPathState::Active);
    assert!(record.mark_open_success_for_instance(
        replacement,
        Duration::from_millis(20),
        TrafficClass::Latency,
    ));
    assert_eq!(record.active_flows, 0);
    assert_eq!(record.active_latency_sensitive_flows, 0);
}

#[test]
fn partial_tcp_transport_state_does_not_clear_unknown_fields() {
    let mut record = ClientPathHealthRecord::default();
    let path_instance_id = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(path_instance_id, 0, PathUsage::Available);
    record.carrier_bytes_in_flight = 64 * 1024;
    record.carrier_inflight_limit_bytes = 512 * 1024;
    record.carrier_queue_bytes = 8 * 1024;
    record.native_drain_observed = true;
    let snapshot = TcpNativeSnapshot {
        rtt: Some(TcpNativeRtt {
            srtt_us: 30_000,
            rttvar_us: Some(3_000),
        }),
        ..TcpNativeSnapshot::default()
    };
    let observation = TcpSenderMetricTracker::new(snapshot).observe(
        PathId(0),
        PathMetricDirection::ClientToServer,
        snapshot,
    );

    assert!(record.mark_tcp_transport_state(path_instance_id, observation));

    assert_eq!(record.carrier_srtt_ms, Some(30.0));
    assert_eq!(record.carrier_bytes_in_flight, 64 * 1024);
    assert_eq!(record.carrier_inflight_limit_bytes, 512 * 1024);
    assert_eq!(record.carrier_queue_bytes, 8 * 1024);
    assert!(!record.native_drain_observed);
}

#[test]
fn observation_projects_deadlines_without_applying_lifecycle_transitions() {
    let deadline = Instant::now();
    let mut record = ClientPathHealthRecord {
        state: SchedulerPathState::Failed,
        failed_until: Some(deadline),
        ..ClientPathHealthRecord::default()
    };

    let observation = record.observation_at(deadline);

    assert_eq!(observation.state, SchedulerPathState::Suspect);
    assert_eq!(record.state, SchedulerPathState::Failed);
    assert_eq!(record.failed_until, Some(deadline));

    record.maintain(deadline);
    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.failed_until, None);
}

#[test]
fn data_plane_failure_is_published_once_until_liveness_recovers() {
    let mut record = ClientPathHealthRecord::default();
    let now = Instant::now();
    let original = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(original, 0, PathUsage::Available);

    assert!(record.mark_data_plane_failure(original, now, true));
    assert!(!record.mark_data_plane_failure(original, now, true));

    // A separate reachability probe may recover logical health, but it must not
    // make the retired carrier's delayed cleanup count a second time.
    record.mark_liveness_success();
    assert!(!record.mark_data_plane_failure(original, now, true));

    assert_eq!(record.state, SchedulerPathState::Active);
    assert_eq!(record.consecutive_failures, 0);

    let replacement = crate::model::path::next_carrier_path_instance_id();
    record.install_peer_usage(replacement, 0, PathUsage::Available);
    assert!(!record.mark_data_plane_failure(original, now, false));
    assert_eq!(record.state, SchedulerPathState::Active);
    assert!(record.mark_data_plane_failure(replacement, now, false));

    assert_eq!(record.state, SchedulerPathState::Suspect);
    assert_eq!(record.consecutive_failures, 1);
}
