//! QUIC UDP socket construction and compatibility fallback.
//!
//! Quinn's native UDP adapter remains authoritative. The basic adapter is only
//! selected when a Windows compatibility layer lacks optional Winsock features.

use quinn::{Endpoint as QuinnEndpoint, EndpointConfig, ServerConfig};
#[cfg(windows)]
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
#[cfg(windows)]
use std::net::SocketAddr;
#[cfg(windows)]
use std::sync::Arc;

#[cfg(windows)]
pub(super) fn bind_server_udp_socket(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    std::net::UdpSocket::bind(addr)
}

#[cfg(windows)]
pub(super) fn bind_client_udp_socket(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    if addr.is_ipv6() {
        // Match Quinn's client helper: prefer dual stack but retain IPv6-only
        // operation when the host does not permit changing the socket policy.
        let _ = socket.set_only_v6(false);
    }
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

pub(super) fn endpoint_from_udp_socket(
    socket: std::net::UdpSocket,
    server_config: Option<ServerConfig>,
) -> io::Result<QuinnEndpoint> {
    let runtime =
        quinn::default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;

    #[cfg(not(windows))]
    return QuinnEndpoint::new(EndpointConfig::default(), server_config, socket, runtime);

    #[cfg(windows)]
    let socket = wrap_udp_socket(socket, &runtime)?;
    #[cfg(windows)]
    QuinnEndpoint::new_with_abstract_socket(
        EndpointConfig::default(),
        server_config,
        socket,
        runtime,
    )
}

#[cfg(windows)]
fn wrap_udp_socket(
    socket: std::net::UdpSocket,
    runtime: &Arc<dyn quinn::Runtime>,
) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
    // `wrap_udp_socket` takes ownership even when capability probing fails.
    // Keep the original handle for the compatibility adapter.
    let native_socket = socket.try_clone()?;
    match runtime.wrap_udp_socket(native_socket) {
        Ok(socket) => Ok(socket),
        Err(err) if unsupported_winsock_capability(&err) => {
            PORTABLE_UDP_WARNING.call_once(|| {
                eprintln!(
                    "warning: native Windows QUIC UDP features are unavailable ({err}); \
                     using portable UDP without ECN or segmentation offload; \
                     QUIC performance may be reduced"
                );
            });
            Ok(Arc::new(PortableUdpSocket::new(socket)?))
        }
        Err(err) => Err(err),
    }
}

#[cfg(windows)]
fn unsupported_winsock_capability(err: &io::Error) -> bool {
    const WSAENOPROTOOPT: i32 = 10042;
    const WSAEOPNOTSUPP: i32 = 10045;

    err.kind() == io::ErrorKind::Unsupported
        || matches!(err.raw_os_error(), Some(WSAENOPROTOOPT | WSAEOPNOTSUPP))
}

#[cfg(windows)]
static PORTABLE_UDP_WARNING: std::sync::Once = std::sync::Once::new();

#[cfg(windows)]
#[derive(Debug)]
struct PortableUdpSocket {
    io: tokio::net::UdpSocket,
    last_send_error: std::sync::Mutex<std::time::Instant>,
}

#[cfg(windows)]
impl PortableUdpSocket {
    fn new(socket: std::net::UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        let now = std::time::Instant::now();
        Ok(Self {
            io: tokio::net::UdpSocket::from_std(socket)?,
            last_send_error: std::sync::Mutex::new(
                now.checked_sub(std::time::Duration::from_secs(120))
                    .unwrap_or(now),
            ),
        })
    }

    fn log_send_error(&self, err: &io::Error, transmit: &quinn::udp::Transmit<'_>) {
        const LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let now = std::time::Instant::now();
        let mut last_error = self
            .last_send_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if now.saturating_duration_since(*last_error) >= LOG_INTERVAL {
            *last_error = now;
            eprintln!(
                "warning: portable QUIC UDP send failed: {err}; destination={}, bytes={}",
                transmit.destination,
                transmit.contents.len()
            );
        }
    }
}

#[cfg(windows)]
impl quinn::AsyncUdpSocket for PortableUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> std::pin::Pin<Box<dyn quinn::UdpPoller>> {
        Box::pin(PortableUdpPoller {
            socket: self,
            writable: std::sync::Mutex::new(None),
        })
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        match self.io.try_send_to(transmit.contents, transmit.destination) {
            Ok(written) if written == transmit.contents.len() => Ok(()),
            Ok(written) => {
                let err = io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "UDP socket accepted {written} of {} bytes",
                        transmit.contents.len()
                    ),
                );
                self.log_send_error(&err, transmit);
                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Err(err),
            Err(err) => {
                // UDP delivery errors are non-fatal. QUIC owns retransmission and
                // timeout handling, matching quinn-udp's fallback adapter.
                self.log_send_error(&err, transmit);
                Ok(())
            }
        }
    }

    fn poll_recv(
        &self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> std::task::Poll<io::Result<usize>> {
        let mut read = tokio::io::ReadBuf::new(&mut bufs[0]);
        match self.io.poll_recv_from(cx, &mut read) {
            std::task::Poll::Ready(Ok(addr)) => {
                let len = read.filled().len();
                meta[0] = quinn::udp::RecvMeta {
                    addr,
                    len,
                    stride: len,
                    ecn: None,
                    dst_ip: None,
                };
                std::task::Poll::Ready(Ok(1))
            }
            std::task::Poll::Ready(Err(err)) => std::task::Poll::Ready(Err(err)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    fn may_fragment(&self) -> bool {
        true
    }
}

#[cfg(windows)]
type WritableFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + 'static>>;

#[cfg(windows)]
struct PortableUdpPoller {
    socket: Arc<PortableUdpSocket>,
    writable: std::sync::Mutex<Option<WritableFuture>>,
}

#[cfg(windows)]
impl std::fmt::Debug for PortableUdpPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PortableUdpPoller").finish_non_exhaustive()
    }
}

#[cfg(windows)]
impl quinn::UdpPoller for PortableUdpPoller {
    fn poll_writable(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let mut writable = self
            .writable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if writable.is_none() {
            let socket = self.socket.clone();
            *writable = Some(Box::pin(async move { socket.io.writable().await }));
        }
        let result = writable
            .as_mut()
            .expect("writable future was initialized")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            *writable = None;
        }
        result
    }
}

#[cfg(all(test, windows))]
#[path = "socket_test.rs"]
mod tests;
