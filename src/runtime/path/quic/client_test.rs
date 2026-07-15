use super::*;
use crate::protocol::StreamOpenRole;

#[test]
fn nonblocking_udp_open_uses_zero_initial_window_without_accept() {
    let options = UdpStreamOpenOptions {
        wait_for_accept: false,
        role: StreamOpenRole::Validation,
    };

    assert_eq!(udp_stream_open_initial_max_offset(options, None), 0);
}

#[test]
fn blocking_udp_open_uses_accepted_initial_window() {
    assert_eq!(
        udp_stream_open_initial_max_offset(UdpStreamOpenOptions::ACTIVE_WAIT, Some(8192)),
        8192
    );
}

#[test]
fn address_retry_uses_rfc_delay_for_normal_budgets() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_secs(4), 3),
        QUIC_ADDRESS_ATTEMPT_DELAY
    );
}

#[test]
fn address_retry_fits_alternates_inside_short_budget() {
    assert_eq!(
        quic_address_attempt_delay(Duration::from_millis(120), 3),
        Duration::from_millis(30)
    );
    assert_eq!(
        quic_address_attempt_delay(Duration::from_nanos(1), 1),
        Duration::ZERO
    );
}
