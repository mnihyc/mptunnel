use super::*;

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
