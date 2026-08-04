use super::*;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::capacity::adaptive_reliable_relay_inflight_bytes;
use crate::model::path::next_carrier_path_instance_id;
use crate::transport::PathSpec;

#[test]
fn authenticated_output_uses_startup_prior_before_exact_measurement() {
    let path = "udp://127.0.0.1:12700"
        .parse::<PathSpec>()
        .expect("test UDP path");
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("test secret"),
    );
    let context = ClientPathContext::new(vec![path], security, ResourceLimits::default())
        .expect("test path context");
    let instance = RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        },
        path_instance_id: next_carrier_path_instance_id(),
        attachment_id: 1,
    };

    let admission = context.reliable_stream_source_admission(
        [(instance, false)],
        TrafficClass::Latency,
        PATH_OPEN_SCORE_BYTES,
    );
    let snapshot = admission
        .selected_path
        .expect("authenticated output remains available before measurement");

    assert_eq!(snapshot.state, SchedulerPathState::Suspect);
    assert_eq!(snapshot.id, PathId(0));
    assert_eq!(
        admission.window_bytes,
        adaptive_reliable_relay_inflight_bytes(
            Some(snapshot),
            TrafficClass::Latency,
            context.mux_limits,
        )
    );
    assert!(admission.window_bytes > 0);
}
