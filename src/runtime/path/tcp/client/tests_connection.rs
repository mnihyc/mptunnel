use super::ClientTcpPathSessionSlot;
use super::connection::heartbeat_renewal_delay;
use crate::config::{ClientSecurityConfig, ResourceLimits, SharedSecret};
use crate::model::path::next_carrier_path_instance_id;
use crate::protocol::{
    ConfiguredMemberSlot, PathId, ResetReason, StreamDemandHint, StreamId, TargetAddr,
};
use crate::runtime::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::commands::{
    ClientTcpOpenDeadlines, ClientTcpOpenResponse, ReliablePathCommand, recv_reliable_path_command,
    reliable_path_command_channels,
};
use crate::scheduler::TrafficClass;
use crate::transport::PathSpec;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[path = "tests_accepted_reset.rs"]
mod accepted_reset;

#[tokio::test]
async fn tcp_open_preserves_terminal_reset_after_slot_replacement() {
    tokio::time::timeout(Duration::from_secs(2), async {
        for (failure, terminal) in [
            (RuntimeError::RemoteReset(ResetReason::RemoteClosed), true),
            (RuntimeError::PathOpenTimedOut, false),
        ] {
            let path = "tcp://127.0.0.1:12700".parse::<PathSpec>().expect("test path");
            let security = ClientSecurityConfig::for_test(
                SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec())
                    .expect("test transport secret"),
            );
            let context = ClientPathContext::new(vec![path], security, ResourceLimits::default())
                .expect("client context");
            let handle = &context.tcp_sessions[0];
            let (predecessor_commands, mut predecessor_receivers) = reliable_path_command_channels(8);
            handle.member.lock().expect("test carrier member").current = Some(ClientTcpPathSessionSlot {
                commands: predecessor_commands,
                terminal: Arc::new(AtomicBool::new(false)),
                path_id: PathId(17),
            });
            handle.ready_carrier_instance.store(next_carrier_path_instance_id().as_u64(), Ordering::Release);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            let opening = handle.open_stream_with_deadlines(
                StreamId(11),
                TargetAddr::Ip(([127, 0, 0, 1], 80).into()),
                TrafficClass::Latency,
                StreamDemandHint::Latency,
                Default::default(),
                ClientTcpOpenDeadlines::fixed(deadline),
                context.mux_limits.max_stream_window_bytes,
            );
            tokio::pin!(opening);
            let request = tokio::select! {
                command = recv_reliable_path_command(&mut predecessor_receivers) => command.expect("queued predecessor OPEN"),
                _ = &mut opening => panic!("open completed before its carrier response"),
            };
            let ReliablePathCommand::OpenStream { response, .. } = request else {
                panic!("expected predecessor OPEN");
            };
            // The old actor has already decoded its response. Publish the
            // successor before the awaiting Product task can observe it.
            let (successor_commands, _successor_receivers) = reliable_path_command_channels(8);
            handle.member.lock().expect("test carrier member").current = Some(ClientTcpPathSessionSlot {
                commands: successor_commands,
                terminal: Arc::new(AtomicBool::new(false)),
                path_id: PathId(18),
            });
            handle.ready_carrier_instance.store(next_carrier_path_instance_id().as_u64(), Ordering::Release);
            assert!(response.send(ClientTcpOpenResponse::FailedAfterOpen(failure)).is_ok());
            let result = opening.await;
            if terminal {
                assert!(matches!(result, Err(RuntimeError::RemoteReset(ResetReason::RemoteClosed))));
            } else {
                assert!(matches!(result, Err(RuntimeError::ReliablePathRetired)), "ordinary carrier failures retain replacement semantics");
            }
            assert!(handle.session_slot_is_current(PathId(18)), "terminal stream failure leaves successor carrier intact");
        }
    })
    .await
    .expect("queued TCP terminal authority settles promptly across slot replacement");
}

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
