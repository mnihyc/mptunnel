//! Host-owned pre-connect protection for every process-created egress socket.
//!
//! The hook runs after socket creation/source binding and before TCP connect or
//! UDP connect/first send. Android hosts can synchronously call
//! `VpnService.protect`; Apple packet-tunnel hosts can apply their native
//! exclusion or network binding. A borrowed handle never transfers ownership
//! to the callback.

use crate::protocol::UnderlayProtocol;
use std::io;
use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use tokio::net::TcpSocket;

/// A socket handle borrowed only for the duration of
/// [`HostSocketProtector::protect`].
///
/// The host must not close the handle. A raw descriptor/socket obtained from
/// this value must not be retained after the callback returns. Hosts that need
/// independent ownership must duplicate the handle themselves.
#[derive(Clone, Copy)]
pub struct HostSocketHandle<'a> {
    #[cfg(unix)]
    descriptor: std::os::fd::BorrowedFd<'a>,
    #[cfg(windows)]
    socket: std::os::windows::io::BorrowedSocket<'a>,
}

impl std::fmt::Debug for HostSocketHandle<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            formatter
                .debug_tuple("HostSocketHandle")
                .field(&self.descriptor.as_raw_fd())
                .finish()
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            formatter
                .debug_tuple("HostSocketHandle")
                .field(&self.socket.as_raw_socket())
                .finish()
        }
    }
}

impl<'a> HostSocketHandle<'a> {
    #[cfg(unix)]
    pub fn as_fd(self) -> std::os::fd::BorrowedFd<'a> {
        self.descriptor
    }

    /// Raw descriptor for a synchronous Android/JNI bridge.
    ///
    /// The descriptor remains borrowed: do not close it or retain it beyond
    /// the protection callback.
    #[cfg(unix)]
    pub fn as_raw_fd(self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.descriptor.as_raw_fd()
    }

    #[cfg(windows)]
    pub fn as_socket(self) -> std::os::windows::io::BorrowedSocket<'a> {
        self.socket
    }

    /// Raw socket for a synchronous native bridge. Ownership is not
    /// transferred and the value must not outlive the callback.
    #[cfg(windows)]
    pub fn as_raw_socket(self) -> std::os::windows::io::RawSocket {
        use std::os::windows::io::AsRawSocket;
        self.socket.as_raw_socket()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostSocketPurpose {
    MppCarrier {
        underlay: UnderlayProtocol,
        group_ordinal: usize,
        path_ordinal: usize,
    },
    Target,
    Proxy,
    Dns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostSocketProtectionRequest {
    pub remote_addr: SocketAddr,
    pub purpose: HostSocketPurpose,
}

/// One platform-neutral callback for carrier and native egress sockets.
///
/// Success certifies that this socket may bypass the embedded VPN. Returning
/// an error fails socket creation closed; MPTunnel drops it without connecting
/// or sending. The callback is synchronous and runs exactly once for each
/// socket created by the corresponding protected adapters.
pub trait HostSocketProtector: Send + Sync + 'static {
    fn protect(
        &self,
        socket: HostSocketHandle<'_>,
        request: HostSocketProtectionRequest,
    ) -> io::Result<()>;
}

#[cfg(unix)]
pub(crate) fn protect_socket<S>(
    protector: &dyn HostSocketProtector,
    socket: &S,
    request: HostSocketProtectionRequest,
) -> io::Result<()>
where
    S: std::os::fd::AsFd + ?Sized,
{
    protector.protect(
        HostSocketHandle {
            descriptor: socket.as_fd(),
        },
        request,
    )
}

#[cfg(windows)]
pub(crate) fn protect_socket<S>(
    protector: &dyn HostSocketProtector,
    socket: &S,
    request: HostSocketProtectionRequest,
) -> io::Result<()>
where
    S: std::os::windows::io::AsSocket + ?Sized,
{
    protector.protect(
        HostSocketHandle {
            socket: socket.as_socket(),
        },
        request,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeEgressPurpose {
    Target,
    Proxy,
    Dns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeSocketRequest {
    pub remote_addr: SocketAddr,
    pub purpose: NativeEgressPurpose,
}

pub trait NativeSocketConfigurator: Send + Sync {
    fn configure_tcp(&self, socket: &TcpSocket, request: NativeSocketRequest) -> io::Result<()>;

    fn configure_udp(&self, socket: &StdUdpSocket, request: NativeSocketRequest) -> io::Result<()>;
}

/// Adapts the unified host callback to native target/proxy/DNS socket opens.
#[derive(Clone)]
pub struct ProtectedNativeSocketConfigurator {
    protector: Arc<dyn HostSocketProtector>,
}

impl ProtectedNativeSocketConfigurator {
    pub fn new(protector: Arc<dyn HostSocketProtector>) -> Self {
        Self { protector }
    }

    pub fn protector(&self) -> &Arc<dyn HostSocketProtector> {
        &self.protector
    }
}

impl std::fmt::Debug for ProtectedNativeSocketConfigurator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtectedNativeSocketConfigurator")
            .finish_non_exhaustive()
    }
}

impl NativeSocketConfigurator for ProtectedNativeSocketConfigurator {
    fn configure_tcp(&self, socket: &TcpSocket, request: NativeSocketRequest) -> io::Result<()> {
        protect_socket(
            self.protector.as_ref(),
            socket,
            HostSocketProtectionRequest {
                remote_addr: request.remote_addr,
                purpose: request.purpose.into(),
            },
        )
    }

    fn configure_udp(&self, socket: &StdUdpSocket, request: NativeSocketRequest) -> io::Result<()> {
        protect_socket(
            self.protector.as_ref(),
            socket,
            HostSocketProtectionRequest {
                remote_addr: request.remote_addr,
                purpose: request.purpose.into(),
            },
        )
    }
}

impl From<NativeEgressPurpose> for HostSocketPurpose {
    fn from(value: NativeEgressPurpose) -> Self {
        match value {
            NativeEgressPurpose::Target => Self::Target,
            NativeEgressPurpose::Proxy => Self::Proxy,
            NativeEgressPurpose::Dns => Self::Dns,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemNativeSocketConfigurator;

impl NativeSocketConfigurator for SystemNativeSocketConfigurator {
    fn configure_tcp(&self, _socket: &TcpSocket, _request: NativeSocketRequest) -> io::Result<()> {
        Ok(())
    }

    fn configure_udp(
        &self,
        _socket: &StdUdpSocket,
        _request: NativeSocketRequest,
    ) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
pub struct LinuxSocketMarker {
    mark: crate::platform::LinuxSocketMark,
}

#[cfg(target_os = "linux")]
impl LinuxSocketMarker {
    pub const fn new(mark: crate::platform::LinuxSocketMark) -> Self {
        Self { mark }
    }

    pub const fn mark(self) -> crate::platform::LinuxSocketMark {
        self.mark
    }
}

#[cfg(target_os = "linux")]
impl HostSocketProtector for LinuxSocketMarker {
    fn protect(
        &self,
        socket: HostSocketHandle<'_>,
        _request: HostSocketProtectionRequest,
    ) -> io::Result<()> {
        crate::platform::apply_linux_socket_mark(socket.as_raw_fd(), self.mark)
            .map_err(io::Error::other)
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
pub struct LinuxMarkedNativeSocketConfigurator {
    marker: LinuxSocketMarker,
}

#[cfg(target_os = "linux")]
impl LinuxMarkedNativeSocketConfigurator {
    pub const fn new(mark: crate::platform::LinuxSocketMark) -> Self {
        Self {
            marker: LinuxSocketMarker::new(mark),
        }
    }

    pub const fn mark(self) -> crate::platform::LinuxSocketMark {
        self.marker.mark()
    }
}

#[cfg(target_os = "linux")]
impl NativeSocketConfigurator for LinuxMarkedNativeSocketConfigurator {
    fn configure_tcp(&self, socket: &TcpSocket, request: NativeSocketRequest) -> io::Result<()> {
        protect_socket(
            &self.marker,
            socket,
            HostSocketProtectionRequest {
                remote_addr: request.remote_addr,
                purpose: request.purpose.into(),
            },
        )
    }

    fn configure_udp(&self, socket: &StdUdpSocket, request: NativeSocketRequest) -> io::Result<()> {
        protect_socket(
            &self.marker,
            socket,
            HostSocketProtectionRequest {
                remote_addr: request.remote_addr,
                purpose: request.purpose.into(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
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
        fn configure_tcp(
            &self,
            _socket: &TcpSocket,
            request: NativeSocketRequest,
        ) -> io::Result<()> {
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
}
