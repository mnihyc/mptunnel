use super::*;

fn path(spec: &str) -> PathSpec {
    spec.parse().expect("path spec")
}

#[test]
fn system_udp_socket_is_bound_for_quic_handoff() {
    let path = path("udp://192.0.2.1:443");
    let carrier = CarrierSocket::system(CarrierSocketRequest {
        path: &path,
        identity: CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        remote_addr: "192.0.2.1:443".parse().expect("remote address"),
    })
    .expect("carrier socket");
    let socket = carrier.into_udp_socket().expect("UDP socket");

    let local_addr = socket.local_addr().expect("local address");
    assert_ne!(local_addr.port(), 0);
    assert!(local_addr.is_ipv4());
}

#[test]
fn system_socket_defers_source_family_check_until_resolution() {
    let path = path("tcp://example.test:443?source-ip=192.0.2.10");
    let error = CarrierSocket::system(CarrierSocketRequest {
        path: &path,
        identity: CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        remote_addr: "[2001:db8::1]:443".parse().expect("remote address"),
    })
    .expect_err("source family mismatch");

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn carrier_conversion_rejects_the_wrong_transport() {
    let path = path("udp://192.0.2.1:443");
    let carrier = CarrierSocket::system(CarrierSocketRequest {
        path: &path,
        identity: CarrierPathIdentity {
            group_ordinal: 0,
            path_ordinal: 0,
        },
        remote_addr: "192.0.2.1:443".parse().expect("remote address"),
    })
    .expect("carrier socket");

    assert_eq!(
        carrier
            .into_tcp_socket()
            .expect_err("wrong transport")
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn carrier_resolution_interleaves_grouped_address_families() {
    let v6_first = SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 1], 443));
    let v6_second = SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, 2], 443));
    let v4_first = SocketAddr::from(([192, 0, 2, 1], 443));
    let v4_second = SocketAddr::from(([192, 0, 2, 2], 443));

    assert_eq!(
        interleave_socket_addr_families(vec![v6_first, v6_second, v4_first, v4_second]),
        vec![v6_first, v4_first, v6_second, v4_second]
    );
}
