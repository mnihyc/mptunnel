use super::unsupported_winsock_capability;
use std::io;

#[test]
fn optional_winsock_capability_errors_select_the_basic_udp_adapter() {
    for error in [
        io::Error::new(io::ErrorKind::Unsupported, "unsupported socket feature"),
        io::Error::from_raw_os_error(10042),
        io::Error::from_raw_os_error(10045),
    ] {
        assert!(unsupported_winsock_capability(&error));
    }
}

#[test]
fn operational_socket_errors_remain_fatal() {
    for error in [
        io::Error::from_raw_os_error(10013),
        io::Error::from_raw_os_error(10048),
        io::Error::from_raw_os_error(10054),
    ] {
        assert!(!unsupported_winsock_capability(&error));
    }
}
