use super::*;
use crate::platform::{AddressFamily, ProcessNativeNetwork, ProcessNativeRoute};

fn route(family: AddressFamily, interface_index: u32) -> ProcessNativeRoute {
    ProcessNativeRoute::new(family, interface_index, None, 10).expect("native route")
}

#[test]
fn socket_binding_uses_longest_prefix_native_route() {
    let default = route(AddressFamily::Ipv4, 7);
    let specific = route(AddressFamily::Ipv4, 9);
    let network =
        ProcessNativeNetwork::new("198.51.100.0/24".parse().expect("network"), specific, true)
            .expect("native network");
    let environment =
        Arc::new(ProcessVpnEnvironment::new([default], vec![network]).expect("environment"));
    let binder = WindowsNativeSocketBinder::new(environment);

    assert_eq!(
        binder
            .interface_index_for("198.51.100.42".parse().expect("address"), None)
            .expect("specific route"),
        9
    );
    assert_eq!(
        binder
            .interface_index_for("203.0.113.42".parse().expect("address"), None)
            .expect("default route"),
        7
    );
}

#[test]
fn explicit_source_binding_selects_its_native_interface() {
    let default = route(AddressFamily::Ipv4, 7);
    let source_route = route(AddressFamily::Ipv4, 11);
    let source_network =
        ProcessNativeNetwork::new("192.0.2.0/24".parse().expect("network"), source_route, true)
            .expect("source network");
    let environment =
        Arc::new(ProcessVpnEnvironment::new([default], vec![source_network]).expect("environment"));
    let binder = WindowsNativeSocketBinder::new(environment);

    assert_eq!(
        binder
            .interface_index_for(
                "203.0.113.42".parse().expect("remote"),
                Some("192.0.2.10".parse().expect("source")),
            )
            .expect("source-selected route"),
        11
    );
}

#[test]
fn interface_socket_options_use_windows_required_byte_order() {
    let index = 0x0102_0304;
    let (level, option, encoded) =
        windows_unicast_interface_option("192.0.2.1".parse().expect("IPv4"), index);
    assert_eq!((level, option), (IPPROTO_IP, IP_UNICAST_IF));
    assert_eq!(encoded, index.to_be_bytes());

    let (level, option, encoded) =
        windows_unicast_interface_option("2001:db8::1".parse().expect("IPv6"), index);
    assert_eq!((level, option), (IPPROTO_IPV6, IPV6_UNICAST_IF));
    assert_eq!(encoded, index.to_ne_bytes());
}
