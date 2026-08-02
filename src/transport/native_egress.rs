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
#[path = "tests_native_egress.rs"]
mod tests;
