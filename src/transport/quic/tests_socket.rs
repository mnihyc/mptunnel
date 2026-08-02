use super::RemotePortMappedUdpSocket;
use quinn::AsyncUdpSocket;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[cfg(windows)]
#[test]
fn optional_winsock_capability_errors_select_the_basic_udp_adapter() {
    use super::unsupported_winsock_capability;

    for error in [
        io::Error::new(io::ErrorKind::Unsupported, "unsupported socket feature"),
        io::Error::from_raw_os_error(10042),
        io::Error::from_raw_os_error(10045),
    ] {
        assert!(unsupported_winsock_capability(&error));
    }
}

#[cfg(windows)]
#[test]
fn operational_socket_errors_remain_fatal() {
    use super::unsupported_winsock_capability;

    for error in [
        io::Error::from_raw_os_error(10013),
        io::Error::from_raw_os_error(10048),
        io::Error::from_raw_os_error(10054),
    ] {
        assert!(!unsupported_winsock_capability(&error));
    }
}

#[derive(Debug)]
struct TestUdpSocket {
    local_addr: SocketAddr,
    sent_to: Mutex<Vec<SocketAddr>>,
    received_from: Mutex<Option<SocketAddr>>,
}

impl TestUdpSocket {
    fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr,
            sent_to: Mutex::new(Vec::new()),
            received_from: Mutex::new(None),
        }
    }
}

#[derive(Debug)]
struct ReadyPoller;

impl quinn::UdpPoller for ReadyPoller {
    fn poll_writable(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl quinn::AsyncUdpSocket for TestUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> std::pin::Pin<Box<dyn quinn::UdpPoller>> {
        Box::pin(ReadyPoller)
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        self.sent_to
            .lock()
            .expect("sent destination lock")
            .push(transmit.destination);
        Ok(())
    }

    fn poll_recv(
        &self,
        _cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let Some(source) = self
            .received_from
            .lock()
            .expect("received source lock")
            .take()
        else {
            return Poll::Pending;
        };
        bufs[0][0] = 7;
        meta[0] = quinn::udp::RecvMeta {
            addr: source,
            len: 1,
            stride: 1,
            ecn: None,
            dst_ip: None,
        };
        Poll::Ready(Ok(1))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn max_transmit_segments(&self) -> usize {
        8
    }

    fn max_receive_segments(&self) -> usize {
        16
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

#[test]
fn destination_port_mapping_preserves_quic_peer_identity_and_socket_capabilities() {
    let local_addr = "127.0.0.1:41000".parse().expect("local address");
    let canonical_remote = "127.0.0.1:443".parse().expect("canonical remote");
    let selected_remote = "127.0.0.1:8443".parse().expect("selected remote");
    let other_remote = "127.0.0.2:443".parse().expect("other remote");
    let socket = Arc::new(TestUdpSocket::new(local_addr));
    let (mapped, receipt) =
        RemotePortMappedUdpSocket::new(socket.clone(), canonical_remote, selected_remote)
            .expect("same-IP destination mapping");

    mapped
        .try_send(&quinn::udp::Transmit {
            destination: canonical_remote,
            ecn: None,
            contents: &[1, 2, 3],
            segment_size: None,
            src_ip: None,
        })
        .expect("mapped transmit");
    mapped
        .try_send(&quinn::udp::Transmit {
            destination: other_remote,
            ecn: None,
            contents: &[1, 2, 3],
            segment_size: None,
            src_ip: None,
        })
        .expect("QUIC-owned alternate peer locator");
    assert_eq!(
        *socket.sent_to.lock().expect("sent destination lock"),
        vec![selected_remote, other_remote]
    );

    let mut packet = [0_u8; 8];
    let mut bufs = [io::IoSliceMut::new(&mut packet)];
    let mut meta = [quinn::udp::RecvMeta::default()];
    *socket.received_from.lock().expect("received source lock") = Some(selected_remote);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    *socket.received_from.lock().expect("received source lock") = Some(other_remote);
    assert!(matches!(
        mapped.poll_recv(&mut context, &mut bufs, &mut meta),
        Poll::Ready(Ok(1))
    ));
    assert_eq!(meta[0].addr, other_remote);
    assert!(
        !receipt
            .observation
            .observed
            .load(std::sync::atomic::Ordering::Acquire)
    );
    *socket.received_from.lock().expect("received source lock") = Some(selected_remote);
    assert!(matches!(
        mapped.poll_recv(&mut context, &mut bufs, &mut meta),
        Poll::Ready(Ok(1))
    ));
    assert_eq!(meta[0].addr, canonical_remote);
    assert!(
        receipt
            .observation
            .observed
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(mapped.local_addr().expect("local address"), local_addr);
    assert_eq!(mapped.max_transmit_segments(), 8);
    assert_eq!(mapped.max_receive_segments(), 16);
    assert!(!mapped.may_fragment());
}

#[test]
fn destination_port_mapping_rejects_server_ip_changes() {
    let socket: Arc<dyn quinn::AsyncUdpSocket> = Arc::new(TestUdpSocket::new(
        "127.0.0.1:41000".parse().expect("local address"),
    ));
    let error = RemotePortMappedUdpSocket::new(
        socket,
        "127.0.0.1:443".parse().expect("canonical remote"),
        "127.0.0.2:8443".parse().expect("different-IP remote"),
    )
    .expect_err("server IP changes are not port migration");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}
