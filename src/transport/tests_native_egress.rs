use super::*;
use crate::transport::tcp::{TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{UdpConnectOptions, UdpTransportError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct RejectingConfigurator {
    requests: Mutex<Vec<NativeSocketRequest>>,
}

struct RecordingHostProtector {
    requests: Mutex<Vec<HostSocketProtectionRequest>>,
    reject: bool,
}

impl RecordingHostProtector {
    fn accepting() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            reject: false,
        })
    }

    fn rejecting() -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            reject: true,
        })
    }
}

impl HostSocketProtector for RecordingHostProtector {
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
            Err(io::Error::other("host rejected socket protection"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn unified_host_callback_covers_every_native_purpose_once_per_socket() {
    let protector = RecordingHostProtector::accepting();
    let configurator = ProtectedNativeSocketConfigurator::new(protector.clone());
    let remote = SocketAddr::from(([192, 0, 2, 1], 443));
    for purpose in [
        NativeEgressPurpose::Target,
        NativeEgressPurpose::Proxy,
        NativeEgressPurpose::Dns,
    ] {
        let tcp = TcpSocket::new_v4().expect("TCP socket");
        configurator
            .configure_tcp(
                &tcp,
                NativeSocketRequest {
                    remote_addr: remote,
                    purpose,
                },
            )
            .expect("TCP protection");
        let udp = StdUdpSocket::bind("127.0.0.1:0").expect("UDP socket");
        configurator
            .configure_udp(
                &udp,
                NativeSocketRequest {
                    remote_addr: remote,
                    purpose,
                },
            )
            .expect("UDP protection");
    }

    assert_eq!(
        *protector.requests.lock().expect("requests"),
        vec![
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Target,
            },
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Target,
            },
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Proxy,
            },
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Proxy,
            },
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Dns,
            },
            HostSocketProtectionRequest {
                remote_addr: remote,
                purpose: HostSocketPurpose::Dns,
            },
        ]
    );
}

#[tokio::test]
async fn unified_host_rejection_prevents_tcp_connect_and_udp_send() {
    let protector = RecordingHostProtector::rejecting();
    let configurator = ProtectedNativeSocketConfigurator::new(protector.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("TCP listener");
    let tcp_remote = listener.local_addr().expect("TCP address");
    let tcp_error = crate::transport::tcp::connect_addr_with_configurator(
        tcp_remote,
        TcpConnectOptions {
            timeout: Duration::from_millis(100),
            ..TcpConnectOptions::default()
        },
        NativeEgressPurpose::Target,
        &configurator,
    )
    .await
    .expect_err("host rejection");
    assert!(matches!(tcp_error, TcpTransportError::Io(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "rejected TCP socket reached the listener"
    );

    let receiver = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("UDP receiver");
    let udp_remote = receiver.local_addr().expect("UDP address");
    let udp_error = crate::transport::udp::connect_addr_with_configurator(
        udp_remote,
        UdpConnectOptions {
            timeout: Duration::from_millis(100),
            ..UdpConnectOptions::default()
        },
        NativeEgressPurpose::Dns,
        &configurator,
    )
    .await
    .expect_err("host rejection");
    assert!(matches!(udp_error, UdpTransportError::Io(_)));
    let mut payload = [0_u8; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), receiver.recv(&mut payload))
            .await
            .is_err(),
        "rejected UDP socket emitted a packet"
    );

    assert_eq!(
        *protector.requests.lock().expect("requests"),
        vec![
            HostSocketProtectionRequest {
                remote_addr: tcp_remote,
                purpose: HostSocketPurpose::Target,
            },
            HostSocketProtectionRequest {
                remote_addr: udp_remote,
                purpose: HostSocketPurpose::Dns,
            },
        ]
    );
}

#[cfg(target_os = "linux")]
#[test]
fn linux_marker_uses_the_unified_protector_contract() {
    fn assert_protector<T: HostSocketProtector>() {}
    assert_protector::<LinuxSocketMarker>();
}

impl NativeSocketConfigurator for RejectingConfigurator {
    fn configure_tcp(&self, _socket: &TcpSocket, request: NativeSocketRequest) -> io::Result<()> {
        self.requests.lock().expect("requests").push(request);
        Err(io::Error::other("test pre-connect rejection"))
    }

    fn configure_udp(
        &self,
        _socket: &StdUdpSocket,
        request: NativeSocketRequest,
    ) -> io::Result<()> {
        self.requests.lock().expect("requests").push(request);
        Err(io::Error::other("test pre-connect rejection"))
    }
}

#[tokio::test]
async fn tcp_configurator_runs_before_connect() {
    let remote = SocketAddr::from(([127, 0, 0, 1], 9));
    let configurator = RejectingConfigurator::default();
    let error = crate::transport::tcp::connect_addr_with_configurator(
        remote,
        TcpConnectOptions {
            timeout: Duration::from_secs(1),
            ..TcpConnectOptions::default()
        },
        NativeEgressPurpose::Proxy,
        &configurator,
    )
    .await
    .expect_err("configurator rejects before connect");
    assert!(matches!(error, TcpTransportError::Io(_)));
    assert_eq!(
        *configurator.requests.lock().expect("requests"),
        vec![NativeSocketRequest {
            remote_addr: remote,
            purpose: NativeEgressPurpose::Proxy,
        }]
    );
}

#[tokio::test]
async fn udp_configurator_runs_before_connect() {
    let remote = SocketAddr::from(([127, 0, 0, 1], 9));
    let configurator = RejectingConfigurator::default();
    let error = crate::transport::udp::connect_addr_with_configurator(
        remote,
        UdpConnectOptions {
            timeout: Duration::from_secs(1),
            ..UdpConnectOptions::default()
        },
        NativeEgressPurpose::Target,
        &configurator,
    )
    .await
    .expect_err("configurator rejects before connect");
    assert!(matches!(error, UdpTransportError::Io(_)));
    assert_eq!(
        *configurator.requests.lock().expect("requests"),
        vec![NativeSocketRequest {
            remote_addr: remote,
            purpose: NativeEgressPurpose::Target,
        }]
    );
}
