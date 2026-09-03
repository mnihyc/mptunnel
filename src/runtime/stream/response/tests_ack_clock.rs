use super::super::test_support::binding_for_underlay;
use super::*;
use crate::model::capacity::reliable_path_startup_sample_limit_bytes;
use crate::model::timing::transport_rate_sample_freshness_horizon;
use crate::mux::MuxLimits;
use crate::protocol::{PathMetricDirection, PathMetrics};
use crate::runtime::path::CarrierNativeWindowSample;
use crate::runtime::stream::response::attachment::ResponseProductRateEpoch;
use crate::runtime::stream::response::evidence::{
    ServerPathMetricsEntry, ServerPathMetricsSource, server_output_has_bulk_rate_evidence_at,
};

fn assert_rate_close(actual: f64, expected: f64) {
    let relative_error = (actual - expected).abs() / expected.max(1.0);
    assert!(
        relative_error < 1e-6,
        "expected {expected} bps, got {actual} bps"
    );
}

fn exact_path_sample(
    key: CarrierPathKey,
    incarnation: u64,
    bytes: u64,
    first_sent_at: Instant,
    last_sent_at: Instant,
) -> HashMap<(CarrierPathKey, u64), (u64, u64, Instant, Instant)> {
    HashMap::from([(
        (key, incarnation),
        (bytes, bytes, first_sent_at, last_sent_at),
    )])
}

fn local_timing_metrics(
    key: CarrierPathKey,
    srtt: Duration,
    rttvar: Duration,
) -> ServerPathMetricsEntry {
    let recorded_at = Instant::now();
    let metrics = PathMetrics {
        path_id: key.path_id,
        underlay: key.underlay,
        direction: PathMetricDirection::ServerToClient,
        metric_epoch: 1,
        metric_age_us: 0,
        rate_valid_for_us: 1_000_000,
        rate_observed: true,
        srtt_us: srtt.as_micros().try_into().expect("test SRTT"),
        rttvar_us: rttvar.as_micros().try_into().expect("test RTTVAR"),
        jitter_us: rttvar.as_micros().try_into().expect("test jitter"),
        delivery_rate_bps: 100_000_000,
        pacing_rate_bps: 100_000_000,
        pacing_rate_observed: true,
        loss_ppm: 0,
        ecn_ppm: 0,
        loss_observed: false,
        ecn_observed: false,
        bytes_in_flight_observed: false,
        queue_observed: false,
        bytes_in_flight: 0,
        queue_bytes: 0,
        inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
        inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
        confidence_ppm: 1_000_000,
        app_limited: false,
        has_ack_derived_data_sample: true,
        data_sample_count: 1,
        data_sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
    };
    ServerPathMetricsEntry {
        metrics,
        source: ServerPathMetricsSource::LocalSender,
        native_drain_observed: false,
        carrier_native_window_sample: CarrierNativeWindowSample::from_path_metrics_at(
            metrics,
            recorded_at,
        ),
        carrier_delivery_rate_sample: None,
        recorded_at,
    }
}

#[test]
fn goodput_requires_a_second_data_ack_and_minimum_elapsed_time() {
    let started_at = Instant::now();
    let bytes = 64 * 1024;
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);

    let _ = evidence.observe(bytes, started_at, started_at, started_at);
    assert!(
        evidence.goodput_sample().is_none(),
        "the first Data ACK only establishes the per-output clock"
    );

    let _ = evidence.observe(
        bytes,
        started_at,
        started_at,
        started_at + RESPONSE_ACK_CLOCK_GOODPUT_MIN_ELAPSED - Duration::from_millis(1),
    );
    assert!(
        evidence.goodput_sample().is_none(),
        "a short callback interval is not a stable goodput sample"
    );
}

#[test]
fn goodput_rolls_bytes_and_elapsed_across_data_ack_callbacks() {
    let started_at = Instant::now();
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);

    let _ = evidence.observe(32 * 1024, started_at, started_at, started_at);
    let _ = evidence.observe(
        64 * 1024,
        started_at,
        started_at,
        started_at + Duration::from_millis(100),
    );
    let first = evidence.goodput_sample().expect("first goodput sample");
    assert_eq!(first.bytes(), 64 * 1024);
    assert_eq!(first.elapsed(), Duration::from_millis(100));

    let _ = evidence.observe(
        128 * 1024,
        started_at,
        started_at,
        started_at + Duration::from_millis(250),
    );
    let rolling = evidence.goodput_sample().expect("rolling goodput sample");
    assert_eq!(rolling.bytes(), 192 * 1024);
    assert_eq!(rolling.elapsed(), Duration::from_millis(250));
    assert_rate_close(
        rolling.rate_bps(),
        192.0 * 1024.0 * 8.0 / Duration::from_millis(250).as_secs_f64(),
    );
}

#[test]
fn product_rate_epoch_expires_at_the_exact_frozen_boundary() {
    let sampled_at = Instant::now();
    let horizon = Duration::from_millis(300);
    let epoch = ResponseProductRateEpoch::new(42_000_000.0, 1, 64 * 1024, sampled_at, horizon)
        .expect("valid Product rate epoch");
    assert_eq!(
        epoch.fresh_rate_at(sampled_at + horizon - Duration::from_nanos(1)),
        Some(42_000_000.0)
    );
    assert_eq!(epoch.fresh_rate_at(sampled_at + horizon), None);
}

#[test]
fn release_sample_requires_the_exact_output_incarnation() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let started_at = Instant::now();
    let incarnation = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .incarnation;

    {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        apply_response_ack_clock_release_samples(
            &mut outputs,
            exact_path_sample(key, incarnation + 1, 64 * 1024, started_at, started_at),
            started_at,
        );
        assert!(outputs.entries[0].tcp_product_rate_evidence.is_none());
    }

    for ack_offset in [Duration::ZERO, Duration::from_millis(100)] {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        apply_response_ack_clock_release_samples(
            &mut outputs,
            exact_path_sample(key, incarnation, 64 * 1024, started_at, started_at),
            started_at + ack_offset,
        );
    }

    let outputs = binding.outputs.lock().expect("response outputs lock");
    let entry = &outputs.entries[0];
    let expected = 64.0 * 1024.0 * 8.0 / Duration::from_millis(100).as_secs_f64();
    let epoch = entry.product_rate_epoch.expect("TCP Product rate epoch");
    assert_rate_close(epoch.rate_bps, expected);
    assert_eq!(epoch.sample_count, 1);
    assert_eq!(epoch.sample_bytes, 64 * 1024);
    assert_eq!(epoch.observed_at, started_at + Duration::from_millis(100));
}

#[test]
fn excluded_ambiguous_or_reinjected_ranges_do_not_change_goodput() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let started_at = Instant::now();
    let incarnation = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .incarnation;

    for ack_offset in [Duration::ZERO, Duration::from_millis(100)] {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        apply_response_ack_clock_release_samples(
            &mut outputs,
            exact_path_sample(key, incarnation, 64 * 1024, started_at, started_at),
            started_at + ack_offset,
        );
    }
    let established_epoch = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch;

    // The release ledger classifies ranges. It omits ambiguous ownership and
    // reinjected copies instead of assigning their Data ACK to either output.
    let mut outputs = binding.outputs.lock().expect("response outputs lock");
    apply_response_ack_clock_release_samples(
        &mut outputs,
        HashMap::new(),
        started_at + Duration::from_millis(200),
    );
    assert_eq!(outputs.entries[0].product_rate_epoch, established_epoch);
}

#[test]
fn later_rtt_mutation_cannot_rewrite_product_epoch_deadline() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let observed_at = Instant::now();
    let srtt = Duration::from_millis(20);
    let rttvar = Duration::from_millis(2);
    let incarnation = {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        let entry = &mut outputs.entries[0];
        entry.local_path_metrics = Some(local_timing_metrics(key, srtt, rttvar));
        entry.incarnation
    };
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            125_000,
            observed_at - Duration::from_millis(100),
            observed_at - Duration::from_millis(100),
        ),
        observed_at,
    );

    let expected_expires_at = observed_at + transport_rate_sample_freshness_horizon(srtt, rttvar);
    let mut outputs = binding.outputs.lock().expect("response outputs lock");
    let entry = &mut outputs.entries[0];
    let epoch = entry.product_rate_epoch.expect("UDP Product epoch");
    assert_eq!(epoch.expires_at, expected_expires_at);
    entry.local_path_metrics = Some(local_timing_metrics(
        key,
        Duration::from_secs(10),
        Duration::from_secs(2),
    ));
    assert_eq!(
        entry
            .product_rate_epoch
            .expect("retained immutable epoch")
            .expires_at,
        expected_expires_at
    );
    assert!(
        epoch
            .fresh_rate_at(expected_expires_at - Duration::from_nanos(1))
            .is_some()
    );
    assert_eq!(epoch.fresh_rate_at(expected_expires_at), None);
}

#[test]
fn tcp_first_post_expiry_ack_seeds_and_second_ack_qualifies_new_epoch() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let started_at = Instant::now();
    let incarnation = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .incarnation;
    for ack_offset in [Duration::ZERO, Duration::from_millis(100)] {
        apply_response_ack_clock_release_samples(
            &mut binding.outputs.lock().expect("response outputs lock"),
            exact_path_sample(key, incarnation, 64 * 1024, started_at, started_at),
            started_at + ack_offset,
        );
    }
    let old_epoch = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("established Product epoch");

    let first_new_ack = old_epoch.expires_at;
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            64 * 1024,
            first_new_ack - Duration::from_millis(10),
            first_new_ack - Duration::from_millis(10),
        ),
        first_new_ack,
    );
    let after_first = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch;
    assert_eq!(after_first, None, "the new TCP epoch has no one-ACK rate");

    let second_new_ack = first_new_ack + Duration::from_millis(100);
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(key, incarnation, 64 * 1024, first_new_ack, first_new_ack),
        second_new_ack,
    );
    let rebuilt = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("second ACK qualifies the new epoch");
    assert_eq!(rebuilt.observed_at, second_new_ack);
    assert_eq!(rebuilt.sample_count, 1);
    assert_eq!(rebuilt.sample_bytes, 64 * 1024);
    assert_rate_close(
        rebuilt.rate_bps,
        64.0 * 1024.0 * 8.0 / Duration::from_millis(100).as_secs_f64(),
    );
}

#[test]
fn udp_post_expiry_sample_does_not_ewma_with_stale_epoch() {
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Udp);
    let first_ack = Instant::now();
    let incarnation = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .incarnation;
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            1_250_000,
            first_ack - Duration::from_millis(100),
            first_ack - Duration::from_millis(100),
        ),
        first_ack,
    );
    let old_epoch = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("first UDP epoch");
    assert_rate_close(old_epoch.rate_bps, 100_000_000.0);

    let second_ack = old_epoch.expires_at;
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            125_000,
            second_ack - Duration::from_millis(100),
            second_ack - Duration::from_millis(100),
        ),
        second_ack,
    );
    let new_epoch = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("post-expiry UDP epoch");
    assert_eq!(new_epoch.observed_at, second_ack);
    assert_eq!(new_epoch.sample_count, 1);
    assert_eq!(new_epoch.sample_bytes, 125_000);
    assert_rate_close(new_epoch.rate_bps, 10_000_000.0);

    let third_ack = second_ack + Duration::from_millis(50);
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            125_000,
            third_ack - Duration::from_millis(100),
            third_ack - Duration::from_millis(100),
        ),
        third_ack,
    );
    let accumulated = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("fresh UDP chain accumulates evidence");
    assert_eq!(accumulated.sample_count, 2);
    assert_eq!(accumulated.sample_bytes, 250_000);
}

#[test]
fn post_expiry_tcp_epoch_must_earn_its_own_bulk_sample_floor() {
    let mux_limits = MuxLimits::default();
    let sample_floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let small_sample = PATH_OPEN_SCORE_BYTES as u64;
    assert!(sample_floor > small_sample);
    let (binding, key, _receivers) = binding_for_underlay(UnderlayProtocol::Tcp);
    let started_at = Instant::now();
    let incarnation = {
        let mut outputs = binding.outputs.lock().expect("response outputs lock");
        outputs.entries[0].original_data_acked_bytes = sample_floor;
        outputs.entries[0].incarnation
    };

    for (bytes, acked_at) in [
        (1, started_at),
        (sample_floor, started_at + Duration::from_millis(100)),
    ] {
        apply_response_ack_clock_release_samples(
            &mut binding.outputs.lock().expect("response outputs lock"),
            exact_path_sample(key, incarnation, bytes, started_at, started_at),
            acked_at,
        );
    }
    let old_epoch = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .product_rate_epoch
        .expect("mature old Product epoch");
    assert_eq!(old_epoch.sample_count, 1);
    assert_eq!(old_epoch.sample_bytes, sample_floor);
    assert!(server_output_has_bulk_rate_evidence_at(
        &binding
            .outputs
            .lock()
            .expect("response outputs lock")
            .entries[0],
        mux_limits,
        old_epoch.observed_at,
    ));

    let first_new_ack = old_epoch.expires_at;
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            small_sample,
            first_new_ack - Duration::from_millis(10),
            first_new_ack - Duration::from_millis(10),
        ),
        first_new_ack,
    );
    assert!(
        binding
            .outputs
            .lock()
            .expect("response outputs lock")
            .entries[0]
            .product_rate_epoch
            .is_none(),
        "the first post-expiry ACK only seeds the new clock"
    );

    let second_new_ack = first_new_ack + Duration::from_millis(100);
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(key, incarnation, small_sample, first_new_ack, first_new_ack),
        second_new_ack,
    );
    {
        let outputs = binding.outputs.lock().expect("response outputs lock");
        let entry = &outputs.entries[0];
        let tiny_epoch = entry.product_rate_epoch.expect("fresh tiny Product rate");
        assert_eq!(tiny_epoch.sample_count, 1);
        assert_eq!(tiny_epoch.sample_bytes, small_sample);
        assert!(tiny_epoch.fresh_rate_at(second_new_ack).is_some());
        assert_eq!(entry.original_data_acked_bytes, sample_floor);
        assert!(
            !server_output_has_bulk_rate_evidence_at(entry, mux_limits, second_new_ack),
            "lifetime progress cannot mature a tiny new rate epoch"
        );
    }

    let third_new_ack = second_new_ack + Duration::from_millis(100);
    apply_response_ack_clock_release_samples(
        &mut binding.outputs.lock().expect("response outputs lock"),
        exact_path_sample(
            key,
            incarnation,
            sample_floor - small_sample,
            second_new_ack,
            second_new_ack,
        ),
        third_new_ack,
    );
    let outputs = binding.outputs.lock().expect("response outputs lock");
    let entry = &outputs.entries[0];
    let mature_epoch = entry.product_rate_epoch.expect("mature new Product epoch");
    assert_eq!(mature_epoch.sample_count, 2);
    assert_eq!(mature_epoch.sample_bytes, sample_floor);
    assert!(server_output_has_bulk_rate_evidence_at(
        entry,
        mux_limits,
        third_new_ack,
    ));
}

#[test]
fn zero_or_stale_ack_intervals_do_not_publish_invalid_rates() {
    let started_at = Instant::now();
    let bytes = 64 * 1024;
    let mut zero_interval = ResponseAckClockRateEvidence::new(started_at);

    let _ = zero_interval.observe(bytes, started_at, started_at, started_at);
    let _ = zero_interval.observe(bytes, started_at, started_at, started_at);
    assert!(zero_interval.goodput_sample().is_none());

    let _ = zero_interval.observe(
        bytes,
        started_at,
        started_at,
        started_at + Duration::from_millis(100),
    );
    let sample = zero_interval
        .goodput_sample()
        .expect("later elapsed time makes the rolling sample valid");
    assert!(sample.rate_bps().is_finite());

    let mut stale = ResponseAckClockRateEvidence::new(started_at);
    let _ = stale.observe(bytes, started_at, started_at, started_at);
    let _ = stale.observe(
        bytes,
        started_at,
        started_at,
        started_at + RESPONSE_ACK_CLOCK_GOODPUT_MAX_ELAPSED + Duration::from_millis(1),
    );
    assert!(
        stale.goodput_sample().is_none(),
        "an idle gap resets the rolling goodput window"
    );
}
