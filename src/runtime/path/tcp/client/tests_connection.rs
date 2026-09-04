use super::connection::heartbeat_renewal_delay;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::protocol::{ConfiguredMemberSlot, PathId};
use crate::runtime::path::ClientPathContext;
use crate::transport::PathSpec;
use std::time::Duration;

#[test]
fn configured_member_slot_is_unique_per_flattened_member_and_stable_across_replacement() {
    let path = "tcp://127.0.0.1:12700?max-tcp-carriers=3"
        .parse::<PathSpec>()
        .expect("bounded TCP carrier pool");
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
            .expect("test transport secret"),
    );
    let context = ClientPathContext::new(vec![path], security, ResourceLimits::default())
        .expect("expanded TCP carrier pool");

    assert_eq!(
        context
            .tcp_sessions
            .iter()
            .map(|member| member.runtime.configured_member_slot())
            .collect::<Vec<_>>(),
        [
            ConfiguredMemberSlot(0),
            ConfiguredMemberSlot(1),
            ConfiguredMemberSlot(2),
        ],
        "each flattened configured pool member owns one distinct underlay-local slot",
    );

    let member = &context.tcp_sessions[1].runtime;
    let predecessor = member.for_carrier(PathId(17), Some(12_700));
    let successor = member.for_carrier(PathId(23), Some(12_701));
    assert_ne!(predecessor.path_id(), successor.path_id());
    assert_eq!(
        predecessor.configured_member_slot(),
        successor.configured_member_slot(),
        "physical PathId and port replacement must not remint configured-member identity",
    );
    assert_eq!(successor.configured_member_slot(), ConfiguredMemberSlot(1),);
}

#[test]
fn heartbeat_renewal_preserves_the_rfc_bounds_and_distribution() {
    let maximum = Duration::from_secs(10);
    let minimum = Duration::from_secs(8);
    assert_eq!(heartbeat_renewal_delay(maximum, 0), minimum);
    assert_eq!(heartbeat_renewal_delay(maximum, u64::MAX), maximum);

    let samples = 65_536_u64;
    let mut total_nanos = 0_u128;
    let mut previous = Duration::ZERO;
    for index in 0..samples {
        let sample = index.saturating_mul(u64::MAX / (samples - 1));
        let delay = heartbeat_renewal_delay(maximum, sample);
        assert!((minimum..=maximum).contains(&delay));
        assert!(delay >= previous);
        previous = delay;
        total_nanos += delay.as_nanos();
    }
    let mean = Duration::from_nanos((total_nanos / u128::from(samples)) as u64);
    assert!(mean.abs_diff(Duration::from_secs(9)) < Duration::from_millis(1));
}
