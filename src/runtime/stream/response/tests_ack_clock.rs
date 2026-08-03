use super::super::test_support::binding_for_underlay;
use super::*;

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
fn goodput_sample_expires_on_the_supplied_transport_horizon() {
    let started_at = Instant::now();
    let mut evidence = ResponseAckClockRateEvidence::new(started_at);

    let _ = evidence.observe(64 * 1024, started_at, started_at, started_at);
    let sampled_at = started_at + Duration::from_millis(100);
    let _ = evidence.observe(64 * 1024, started_at, started_at, sampled_at);

    let horizon = Duration::from_millis(300);
    assert!(
        evidence
            .fresh_goodput_sample(sampled_at + Duration::from_millis(299), horizon)
            .is_some()
    );
    assert!(
        evidence
            .fresh_goodput_sample(sampled_at + horizon, horizon)
            .is_none()
    );
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
    assert_rate_close(
        entry.tcp_ack_clock_rate_bps.expect("TCP Data ACK rate"),
        expected,
    );
    assert_rate_close(
        entry
            .product_progress_rate_bps
            .expect("product progress rate"),
        expected,
    );
    assert_rate_close(entry.delivery_rate_bps.expect("delivery rate"), expected);
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
    let established_rate = binding
        .outputs
        .lock()
        .expect("response outputs lock")
        .entries[0]
        .tcp_ack_clock_rate_bps;

    // The release ledger classifies ranges. It omits ambiguous ownership and
    // reinjected copies instead of assigning their Data ACK to either output.
    let mut outputs = binding.outputs.lock().expect("response outputs lock");
    apply_response_ack_clock_release_samples(
        &mut outputs,
        HashMap::new(),
        started_at + Duration::from_millis(200),
    );
    assert_eq!(outputs.entries[0].tcp_ack_clock_rate_bps, established_rate);
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
