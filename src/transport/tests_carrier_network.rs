use super::*;
use crate::transport::{
    HostSocketHandle, HostSocketProtectionRequest, HostSocketProtector, HostSocketPurpose,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn path(spec: &str) -> PathSpec {
    spec.parse().expect("path spec")
}

struct RecordingProtector {
    requests: Mutex<Vec<HostSocketProtectionRequest>>,
    reject: bool,
}

impl RecordingProtector {
    fn new(reject: bool) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            reject,
        })
    }
}

impl HostSocketProtector for RecordingProtector {
    fn protect(
        &self,
        socket: HostSocketHandle<'_>,
        request: HostSocketProtectionRequest,
    ) -> io::Result<()> {
        #[cfg(unix)]
        assert!(socket.as_raw_fd() >= 0);
        #[cfg(windows)]
        assert_ne!(socket.as_raw_socket(), std::os::windows::io::RawSocket::MAX);
        self.requests.lock().expect("requests").push(request);
        if self.reject {
            Err(io::Error::other("test host protection rejection"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn unified_host_callback_protects_tcp_and_udp_carriers_once() {
    let protector = RecordingProtector::new(false);
    let provider = ProtectedCarrierNetworkProvider::new(
        Arc::new(SystemCarrierNetworkProvider),
        protector.clone(),
    );
    let tcp_path = path("tcp://192.0.2.1:443");
    let udp_path = path("quic://192.0.2.2:443");
    let tcp_identity = CarrierPathIdentity {
        group_ordinal: 3,
        path_ordinal: 4,
    };
    let udp_identity = CarrierPathIdentity {
        group_ordinal: 3,
        path_ordinal: 5,
    };
    let tcp_remote = "192.0.2.1:443".parse().expect("TCP remote");
    let udp_remote = "192.0.2.2:443".parse().expect("UDP remote");

    provider
        .create_socket(CarrierSocketRequest {
            path: &tcp_path,
            identity: tcp_identity,
            remote_addr: tcp_remote,
        })
        .expect("protected TCP carrier");
    provider
        .create_socket(CarrierSocketRequest {
            path: &udp_path,
            identity: udp_identity,
            remote_addr: udp_remote,
        })
        .expect("protected UDP carrier");

    assert_eq!(
        *protector.requests.lock().expect("requests"),
        vec![
            HostSocketProtectionRequest {
                remote_addr: tcp_remote,
                purpose: HostSocketPurpose::MppCarrier {
                    underlay: UnderlayProtocol::Tcp,
                    group_ordinal: 3,
                    path_ordinal: 4,
                },
            },
            HostSocketProtectionRequest {
                remote_addr: udp_remote,
                purpose: HostSocketPurpose::MppCarrier {
                    underlay: UnderlayProtocol::Udp,
                    group_ordinal: 3,
                    path_ordinal: 5,
                },
            },
        ]
    );
}

#[tokio::test]
async fn carrier_protection_rejection_prevents_tcp_connect() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("TCP listener");
    let remote = listener.local_addr().expect("TCP address");
    let configured = path(&format!("tcp://{remote}"));
    let identity = CarrierPathIdentity {
        group_ordinal: 7,
        path_ordinal: 8,
    };
    let prepared =
        PreparedCarrierPath::new(identity, configured.clone(), [remote]).expect("prepared path");
    let protector = RecordingProtector::new(true);
    let provider = ProtectedCarrierNetworkProvider::new(
        Arc::new(PreparedCarrierNetworkProvider::new(vec![prepared]).expect("prepared provider")),
        protector.clone(),
    );

    let error = crate::transport::tcp::connect_path_with_provider(
        &configured,
        identity,
        crate::transport::tcp::TcpConnectOptions {
            timeout: Duration::from_millis(100),
            ..crate::transport::tcp::TcpConnectOptions::default()
        },
        &provider,
    )
    .await
    .expect_err("host rejection");
    assert!(matches!(
        error,
        crate::transport::tcp::TcpTransportError::Io(_)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "rejected carrier socket reached the listener"
    );
    assert_eq!(
        *protector.requests.lock().expect("requests"),
        vec![HostSocketProtectionRequest {
            remote_addr: remote,
            purpose: HostSocketPurpose::MppCarrier {
                underlay: UnderlayProtocol::Tcp,
                group_ordinal: 7,
                path_ordinal: 8,
            },
        }]
    );
}

#[test]
fn system_udp_socket_is_bound_for_quic_handoff() {
    let path = path("quic://192.0.2.1:443");
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
    let path = path("tcp://example.test:443?source-address=192.0.2.10");
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
    let path = path("quic://192.0.2.1:443");
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

#[tokio::test]
async fn prepared_provider_never_falls_back_to_runtime_dns() {
    let configured = path("tcp://carrier.invalid:440-450");
    let identity = CarrierPathIdentity {
        group_ordinal: 3,
        path_ordinal: 2,
    };
    let first = "192.0.2.10:443".parse().expect("first");
    let second = "[2001:db8::10]:443".parse().expect("second");
    let prepared = PreparedCarrierPath::new(identity, configured.clone(), [first, second, first])
        .expect("prepared path");
    let provider = PreparedCarrierNetworkProvider::new(vec![prepared]).expect("prepared provider");

    assert_eq!(
        provider
            .resolve(CarrierResolutionRequest {
                path: &configured,
                identity,
                remote_port: 447,
            })
            .await
            .expect("prepared resolution"),
        vec![
            "192.0.2.10:447".parse().expect("remapped IPv4"),
            "[2001:db8::10]:447".parse().expect("remapped IPv6"),
        ]
    );
    assert_eq!(
        provider.endpoint_addresses(),
        vec![
            "192.0.2.10".parse::<IpAddr>().expect("IPv4"),
            "2001:db8::10".parse::<IpAddr>().expect("IPv6"),
        ]
    );
    assert_eq!(
        provider
            .resolve(CarrierResolutionRequest {
                path: &configured,
                identity,
                remote_port: 449,
            })
            .await
            .expect("later carrier selects independently"),
        vec![
            "192.0.2.10:449".parse().expect("later IPv4"),
            "[2001:db8::10]:449".parse().expect("later IPv6"),
        ]
    );
    let error = provider
        .resolve(CarrierResolutionRequest {
            path: &configured,
            identity,
            remote_port: 451,
        })
        .await
        .expect_err("out-of-range carrier port");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

    let changed = path("tcp://other.invalid:443");
    let error = provider
        .resolve(CarrierResolutionRequest {
            path: &changed,
            identity,
            remote_port: 443,
        })
        .await
        .expect_err("unprepared generation path");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[test]
fn prepared_provider_rejects_identity_aliases_and_incompatible_answers() {
    let configured = path("quic://carrier.invalid:443?source-address=192.0.2.5");
    let identity = CarrierPathIdentity {
        group_ordinal: 0,
        path_ordinal: 0,
    };
    assert!(
        PreparedCarrierPath::new(
            identity,
            configured.clone(),
            ["[2001:db8::1]:443".parse().expect("IPv6")]
        )
        .is_err()
    );

    let prepared = PreparedCarrierPath::new(
        identity,
        configured,
        ["192.0.2.1:443".parse().expect("IPv4")],
    )
    .expect("prepared path");
    let error = PreparedCarrierNetworkProvider::new(vec![prepared.clone(), prepared])
        .expect_err("duplicate identity");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}
