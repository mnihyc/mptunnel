use super::*;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::sender::response::test_support::response_target;

#[test]
fn tcp_capacity_probe_does_not_wait_for_product_subflow_graduation() {
    let mux_limits = MuxLimits::default();
    let mut service = response_target(0, UnderlayProtocol::Tcp, 20.0, 0, 64 * 1024, true);
    let mut cold = response_target(1, UnderlayProtocol::Tcp, 80.0, 0, 64 * 1024, false);
    service.has_bulk_rate_evidence = false;
    cold.has_bulk_rate_evidence = false;
    let (cold_commands, _cold_receivers) = reliable_path_command_channels(4);
    cold.commands = cold_commands;

    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold.clone()],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::default(),
            mux_limits,
        )
        .is_none()
    );

    service.has_bulk_rate_evidence = true;
    let (selected, train_bytes) = select_response_tcp_capacity_probe_target(
        &[service.clone(), cold.clone()],
        FlowLane::Throughput,
        Some(service.key),
        ResponseServiceFamilyLoads::default(),
        mux_limits,
    )
    .expect("proven Service opens offset-free discovery");
    assert_eq!(selected.key, cold.key);
    assert_eq!(train_bytes, 2 * 1024 * 1024);

    let udp = response_target(2, UnderlayProtocol::Udp, 10.0, 0, 64 * 1024, false);
    assert!(
        select_response_tcp_capacity_probe_target(
            &[service.clone(), cold, udp],
            FlowLane::Throughput,
            Some(service.key),
            ResponseServiceFamilyLoads::new(2, 0),
            mux_limits,
        )
        .is_none(),
        "a measured cross-family handoff must outrank optional TCP discovery"
    );
}
