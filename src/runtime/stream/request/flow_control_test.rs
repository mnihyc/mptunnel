use super::{RequestOutstandingWindow, request_outstanding_resource_ceiling};
use crate::model::capacity::{MIN_RATE_SAMPLE_BYTES, reliable_relay_buffer_len};
use crate::model::request_evidence::RequestWindowGrowthEvidence;
use crate::mux::MuxLimits;
use crate::scheduler::TrafficClass;
use std::time::{Duration, Instant};

#[test]
fn request_window_grows_from_connection_ack_credits_and_resets_on_demotion() {
    let limits = MuxLimits::default();
    let ceiling = request_outstanding_resource_ceiling(limits);
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);

    let latency_startup = window.limit_bytes_at(
        TrafficClass::Latency,
        MIN_RATE_SAMPLE_BYTES as usize,
        limits,
        now,
    );
    assert_eq!(
        latency_startup,
        reliable_relay_buffer_len(limits).min(ceiling).max(1)
    );

    let promoted_at = now + Duration::from_millis(1);
    let bulk_startup =
        window.limit_bytes_at(TrafficClass::Throughput, 64 * 1024, limits, promoted_at);
    assert!(bulk_startup >= latency_startup);
    assert!(bulk_startup < ceiling);

    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::AckCredits {
            bytes: bulk_startup,
            growth_interval: Duration::from_secs(1),
            observed_at: promoted_at + Duration::from_millis(1),
        },
        TrafficClass::Throughput,
        limits,
    );
    assert_eq!(
        window.product_limit_bytes,
        bulk_startup.saturating_mul(2).min(ceiling),
        "ACK credits grow the one connection-level product window"
    );

    let demoted = window.limit_bytes_at(
        TrafficClass::Latency,
        MIN_RATE_SAMPLE_BYTES as usize,
        limits,
        promoted_at + Duration::from_millis(2),
    );
    assert_eq!(demoted, latency_startup);
    assert_eq!(window.acked_in_epoch, 0);
    assert_eq!(
        window.limit_bytes_at(
            TrafficClass::Throughput,
            64 * 1024,
            limits,
            promoted_at + Duration::from_millis(3),
        ),
        bulk_startup,
        "promotion restarts at the bounded startup window instead of reviving old growth"
    );
}

#[test]
fn request_window_discards_expired_ack_credit_and_stops_at_resource_ceiling() {
    let limits = MuxLimits {
        max_stream_window_bytes: 1024 * 1024,
        ..MuxLimits::default()
    };
    let ceiling = request_outstanding_resource_ceiling(limits);
    let now = Instant::now();
    let mut window = RequestOutstandingWindow::new_at(now);
    let startup = window.limit_bytes_at(TrafficClass::Throughput, 64 * 1024, limits, now);
    assert!(startup < ceiling);

    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::AckCredits {
            bytes: startup,
            growth_interval: Duration::from_millis(10),
            observed_at: now + Duration::from_secs(1),
        },
        TrafficClass::Throughput,
        limits,
    );
    assert_eq!(window.product_limit_bytes, startup);

    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::AckCredits {
            bytes: startup,
            growth_interval: Duration::from_millis(10),
            observed_at: now + Duration::from_secs(1) + Duration::from_millis(1),
        },
        TrafficClass::Throughput,
        limits,
    );
    assert_eq!(window.product_limit_bytes, ceiling);

    window.apply_growth_evidence(
        RequestWindowGrowthEvidence::AckCredits {
            bytes: ceiling,
            growth_interval: Duration::from_secs(1),
            observed_at: now + Duration::from_secs(1) + Duration::from_millis(2),
        },
        TrafficClass::Throughput,
        limits,
    );
    assert_eq!(window.product_limit_bytes, ceiling);
}
