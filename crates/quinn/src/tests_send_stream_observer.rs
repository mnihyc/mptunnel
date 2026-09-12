//! Exercise observer futures against Quinn's actual native packet constructor.

use super::*;
use crate::{Endpoint, SendStreamObservationError, TokioRuntime};
use std::{net::UdpSocket, sync::atomic::AtomicUsize, task::Wake};

fn endpoint() -> Endpoint {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let server = crate::ServerConfig::with_single_cert(vec![cert.cert.der().clone()], key).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut endpoint = Endpoint::new(
        Default::default(),
        Some(server),
        UdpSocket::bind("127.0.0.1:0").unwrap(),
        Arc::new(TokioRuntime),
    )
    .unwrap();
    endpoint.set_default_client_config(
        crate::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );
    endpoint
}

async fn established() -> (Endpoint, Connection, Connection) {
    let endpoint = endpoint();
    let connecting = endpoint
        .connect(endpoint.local_addr().unwrap(), "localhost")
        .unwrap();
    let (client, server) =
        tokio::join!(connecting, async { endpoint.accept().await.unwrap().await });
    (endpoint, client.unwrap(), server.unwrap())
}

#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll<T>(future: Pin<&mut impl Future<Output = T>>, count: &Arc<WakeCount>) -> Poll<T> {
    future.poll(&mut Context::from_waker(&Waker::from(count.clone())))
}

fn constructed_packet(connection: &Connection, when: Instant) {
    let mut state = connection.0.state.lock("test_packetization_producer");
    let mut bytes = Vec::new();
    // One actual QUIC datagram is constructed and deliberately not delivered.
    // Thus no receiver/native ACK can cause the observation to succeed.
    assert!(state.inner.poll_transmit(when, 1, &mut bytes).is_some());
    assert!(!bytes.is_empty());
    state.forward_app_events(&connection.0.shared);
}

#[tokio::test(flavor = "current_thread")]
async fn send_stream_observer_targets_concurrency_cancellation_and_native_packetization() {
    let (endpoint, connection, _peer) = established().await;
    let mut sender = connection.open_uni().await.unwrap();
    let mut other = connection.open_uni().await.unwrap();
    other.set_priority(1).unwrap();
    let observer = connection.observe_send_stream(sender.id()).unwrap();
    let second_observer = connection.observe_send_stream(sender.id()).unwrap();
    let other_observer = connection.observe_send_stream(other.id()).unwrap();
    assert_eq!(
        connection
            .0
            .state
            .lock("observer_count")
            .packetization_observers
            .len(),
        2
    );

    // No await after these immediately ready writes: the current-thread driver
    // cannot have packetized the new bytes before we inspect/arm the futures.
    let main_wakes = Arc::new(WakeCount::default());
    let other_wakes = Arc::new(WakeCount::default());
    assert!(matches!(
        poll(Box::pin(sender.write(&[1; 4096])).as_mut(), &main_wakes),
        Poll::Ready(Ok(4096))
    ));
    assert!(matches!(
        poll(Box::pin(other.write(&[2; 64])).as_mut(), &other_wakes),
        Poll::Ready(Ok(64))
    ));
    let end = observer.snapshot().unwrap().accepted_end;
    assert_eq!(observer.snapshot().unwrap().first_unpacketized, 0);
    let mut main_wait = Box::pin(observer.wait_until_packetized(end));
    let mut other_wait = Box::pin(other_observer.wait_until_packetized(64));
    assert!(poll(main_wait.as_mut(), &main_wakes).is_pending());
    assert!(poll(other_wait.as_mut(), &other_wakes).is_pending());
    let ack_count = connection.stats().frame_rx.acks;

    // Fixture timestamps make pacing eligible without invoking any loss timer.
    // The accepted payload fits the initial send window; no ACK is delivered.
    let mut when = Instant::now() + Duration::from_secs(1);
    while other_observer.snapshot().unwrap().first_unpacketized < 64 {
        constructed_packet(&connection, when);
        when += Duration::from_secs(1);
    }
    assert!(matches!(
        poll(other_wait.as_mut(), &other_wakes),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(
        main_wakes.0.load(Ordering::SeqCst),
        0,
        "unrelated stream must not wake the pending end"
    );
    let below = observer.snapshot().unwrap();
    assert!(below.first_unpacketized < end);

    // A cancelled earlier goal must not withdraw either later observer. The
    // surviving intermediate goal consumes the shared minimum; the final goal
    // must then rearm rather than lose its later crossing.
    let intermediate = below.first_unpacketized + 1;
    let mut cancelled = Box::pin(observer.wait_until_packetized(intermediate));
    let mut middle = Box::pin(second_observer.wait_until_packetized(intermediate));
    let middle_wakes = Arc::new(WakeCount::default());
    assert!(poll(cancelled.as_mut(), &middle_wakes).is_pending());
    assert!(poll(middle.as_mut(), &middle_wakes).is_pending());
    drop(cancelled);
    constructed_packet(&connection, when);
    when += Duration::from_secs(1);
    assert!(matches!(
        poll(middle.as_mut(), &middle_wakes),
        Poll::Ready(Ok(_))
    ));
    assert!(observer.snapshot().unwrap().first_unpacketized < end);
    assert!(main_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(poll(main_wait.as_mut(), &main_wakes).is_pending());

    let mut concurrent = Box::pin(second_observer.wait_until_packetized(end));
    let concurrent_wakes = Arc::new(WakeCount::default());
    assert!(poll(concurrent.as_mut(), &concurrent_wakes).is_pending());
    while observer.snapshot().unwrap().first_unpacketized < end {
        constructed_packet(&connection, when);
        when += Duration::from_secs(1);
    }
    assert!(
        matches!(poll(main_wait.as_mut(), &main_wakes), Poll::Ready(Ok(progress)) if progress.first_unpacketized >= end)
    );
    assert!(matches!(
        poll(concurrent.as_mut(), &concurrent_wakes),
        Poll::Ready(Ok(_))
    ));
    assert!(concurrent_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll(
            Box::pin(observer.wait_until_packetized(end)).as_mut(),
            &main_wakes
        ),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(connection.stats().frame_rx.acks, ack_count);

    drop((main_wait, concurrent, middle, other_wait));
    drop((observer, second_observer, other_observer));
    assert!(
        connection
            .0
            .state
            .lock("observer_cleanup")
            .packetization_observers
            .is_empty()
    );
    endpoint.close(0u32.into(), b"done");
}

#[tokio::test(flavor = "current_thread")]
async fn send_stream_observer_reset_terminal_and_unaccepted_end() {
    let (endpoint, connection, _peer) = established().await;
    let mut sender = connection.open_uni().await.unwrap();
    let mut other = connection.open_uni().await.unwrap();
    let observer = connection.observe_send_stream(sender.id()).unwrap();
    let idle_observer = connection.observe_send_stream(other.id()).unwrap();
    let wakes = Arc::new(WakeCount::default());
    assert!(matches!(
        poll(Box::pin(sender.write(&[3; 128])).as_mut(), &wakes),
        Poll::Ready(Ok(128))
    ));
    assert!(matches!(
        poll(
            Box::pin(observer.wait_until_packetized(129)).as_mut(),
            &wakes
        ),
        Poll::Ready(Err(SendStreamObservationError::UnacceptedEnd {
            accepted_end: 128
        }))
    ));
    let mut wait = Box::pin(observer.wait_until_packetized(128));
    assert!(poll(wait.as_mut(), &wakes).is_pending());
    sender.reset(0u32.into()).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll(wait.as_mut(), &wakes),
        Poll::Ready(Err(SendStreamObservationError::ClosedStream))
    ));
    assert_eq!(
        connection
            .0
            .state
            .lock("reset_cleanup")
            .packetization_observers
            .len(),
        1
    );

    assert!(matches!(
        poll(Box::pin(other.write(&[4; 64])).as_mut(), &wakes),
        Poll::Ready(Ok(64))
    ));
    let mut terminal_wait = Box::pin(idle_observer.wait_until_packetized(64));
    let terminal_wakes = Arc::new(WakeCount::default());
    assert!(poll(terminal_wait.as_mut(), &terminal_wakes).is_pending());
    // Termination removes cells even while callers retain the old observers.
    connection.close(0u32.into(), b"done");
    assert!(terminal_wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        poll(terminal_wait.as_mut(), &terminal_wakes),
        Poll::Ready(Err(SendStreamObservationError::ConnectionLost(_)))
    ));
    assert!(matches!(
        idle_observer.snapshot(),
        Err(SendStreamObservationError::ConnectionLost(_))
    ));
    assert!(
        connection
            .0
            .state
            .lock("terminal_cleanup")
            .packetization_observers
            .is_empty()
    );
    endpoint.close(0u32.into(), b"done");
}

#[tokio::test(flavor = "current_thread")]
async fn send_stream_observer_rejects_pre_handshake_scope() {
    let endpoint = endpoint();
    let connecting = endpoint
        .connect(endpoint.local_addr().unwrap(), "localhost")
        .unwrap();
    let early = Connection(connecting.conn.as_ref().unwrap().clone());
    assert!(matches!(
        early.observe_send_stream(StreamId::new(Side::Client, Dir::Uni, 0)),
        Err(SendStreamObservationError::NotEstablished)
    ));
    endpoint.close(0u32.into(), b"done");
}

#[tokio::test(flavor = "current_thread")]
async fn send_stream_observer_idle_stop_only_wakes_exact_send_half() {
    let (endpoint, connection, peer) = established().await;
    let (mut sender, mut receiver) = connection.open_bi().await.unwrap();
    sender.write_all(b"open").await.unwrap();
    let (mut peer_sender, mut peer_receiver) = peer.accept_bi().await.unwrap();
    let mut opening = [0; 4];
    peer_receiver.read_exact(&mut opening).await.unwrap();
    assert_eq!(&opening, b"open");
    let observer = connection.observe_send_stream(sender.id()).unwrap();
    let idle = observer.snapshot().unwrap();
    assert_eq!(idle.accepted_end, idle.first_unpacketized);

    let (mut sibling, _sibling_receiver) = connection.open_bi().await.unwrap();
    let sibling_observer = connection.observe_send_stream(sibling.id()).unwrap();
    assert_eq!(sibling_observer.snapshot().unwrap().accepted_end, 0);
    let wakes = Arc::new(WakeCount::default());
    let sibling_wakes = Arc::new(WakeCount::default());
    let mut wait = Box::pin(observer.wait_until_terminated());
    let mut sibling_wait = Box::pin(sibling_observer.wait_until_terminated());
    assert!(poll(wait.as_mut(), &wakes).is_pending());
    assert!(poll(sibling_wait.as_mut(), &sibling_wakes).is_pending());
    {
        let mut cancelled = Box::pin(observer.wait_until_terminated());
        assert!(poll(cancelled.as_mut(), &wakes).is_pending());
    }
    // The pending wait owns its observer and needs neither new bytes nor an
    // offset arm. Keep the opposite half and entire connection open at STOP.
    drop(observer);
    let stop_code = VarInt::from_u32(17);
    peer_receiver.stop(stop_code).unwrap();
    // A watchdog bounds a broken wake test; it is not a protocol timing input.
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("idle observer must receive the exact send-half STOP"),
        SendStreamObservationError::Stopped(stop_code)
    );
    assert!(connection.close_reason().is_none());
    assert!(peer.close_reason().is_none());
    assert_eq!(sibling_wakes.0.load(Ordering::SeqCst), 0);
    assert!(poll(sibling_wait.as_mut(), &sibling_wakes).is_pending());

    peer_sender.write_all(b"opposite").await.unwrap();
    let mut opposite = [0; 8];
    receiver.read_exact(&mut opposite).await.unwrap();
    assert_eq!(&opposite, b"opposite");
    sibling.write_all(b"sibling").await.unwrap();
    let (_peer_sibling_sender, mut peer_sibling_receiver) = peer.accept_bi().await.unwrap();
    let mut sibling_payload = [0; 7];
    peer_sibling_receiver
        .read_exact(&mut sibling_payload)
        .await
        .unwrap();
    assert_eq!(&sibling_payload, b"sibling");
    assert!(poll(sibling_wait.as_mut(), &sibling_wakes).is_pending());
    assert!(connection.close_reason().is_none());
    endpoint.close(0u32.into(), b"done");
}

#[derive(Debug)]
struct FailedSendSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    fail_readiness: bool,
}

#[derive(Debug)]
struct FailedSendPoller(bool);

impl crate::UdpPoller for FailedSendPoller {
    fn poll_writable(self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(if self.0 {
            Err(io::Error::other("fixture fatal write-readiness error"))
        } else {
            Ok(())
        })
    }
}

impl AsyncUdpSocket for FailedSendSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn crate::UdpPoller>> {
        Box::pin(FailedSendPoller(self.fail_readiness))
    }

    fn try_send(&self, _: &udp::Transmit) -> io::Result<()> {
        Err(io::Error::other("fixture fatal socket send error"))
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn send_stream_observer_fatal_socket_error_wakes_retained_waiter() {
    for fail_readiness in [false, true] {
        let (endpoint, connection, _peer) = established().await;
        let mut sender = connection.open_uni().await.unwrap();
        let observer = connection.observe_send_stream(sender.id()).unwrap();
        let wakes = Arc::new(WakeCount::default());
        assert!(matches!(
            poll(Box::pin(sender.write(&[5; 64])).as_mut(), &wakes),
            Poll::Ready(Ok(64))
        ));
        let mut waiter = Box::pin(observer.wait_until_packetized(64));
        assert!(poll(waiter.as_mut(), &wakes).is_pending());
        {
            let mut state = connection.0.state.lock("fatal_socket_fixture");
            let mut bytes = Vec::new();
            let transmit = state
                .inner
                .poll_transmit(Instant::now() + Duration::from_secs(1), 1, &mut bytes)
                .expect("actual native packet construction");
            assert_eq!(
                state
                    .inner
                    .send_stream(sender.id())
                    .packetization_progress()
                    .unwrap()
                    .first_unpacketized,
                64
            );
            // Preserve the real native transmit for the production driver to
            // attempt, with its crossing event still waiting to be forwarded.
            state.buffered_transmit = Some(transmit);
            state.send_buffer = bytes;
            state.socket = Arc::new(FailedSendSocket {
                inner: state.socket.clone(),
                fail_readiness,
            });
            state.io_poller = state.socket.clone().create_io_poller();
        }
        let (_source_tx, source_rx) = mpsc::unbounded_channel();
        let mut driver = Box::pin(ConnectionDriver {
            conn: connection.0.clone(),
            sources: TransmitSources::new(source_rx),
        });
        assert!(matches!(poll(driver.as_mut(), &wakes), Poll::Ready(Err(_))));
        assert!(
            connection.close_reason().is_some(),
            "fatal driver exit must publish termination"
        );
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            poll(waiter.as_mut(), &wakes),
            Poll::Ready(Err(SendStreamObservationError::ConnectionLost(_)))
        ));
        assert!(
            connection
                .0
                .state
                .lock("fatal_cleanup")
                .packetization_observers
                .is_empty()
        );
        endpoint.close(0u32.into(), b"done");
    }
}
