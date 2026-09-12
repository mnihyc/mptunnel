use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

const LIFECYCLE_GUARD: Duration = Duration::from_secs(5);

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_once<F: Future + Unpin>(future: &mut F, wake: &Arc<WakeCount>) -> Poll<F::Output> {
    let waker = Waker::from(wake.clone());
    Pin::new(future).poll(&mut Context::from_waker(&waker))
}

struct LastDrop(Arc<AtomicBool>);

impl Drop for LastDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

// The marker is a field after the real future, so reaching Ready in the inner
// future does not set it. Even an unwind from OwnedActor::drop must destroy it.
struct OwnedActor<F> {
    future: Pin<Box<F>>,
    panic_on_drop: bool,
    _last_drop: LastDrop,
}

impl<F> OwnedActor<F> {
    fn new(future: F, dropped: Arc<AtomicBool>, panic_on_drop: bool) -> Self {
        Self {
            future: Box::pin(future),
            panic_on_drop,
            _last_drop: LastDrop(dropped),
        }
    }
}

impl<F: Future> Future for OwnedActor<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

impl<F> Drop for OwnedActor<F> {
    fn drop(&mut self) {
        assert!(!self.panic_on_drop, "owned actor destructor panic");
    }
}

struct DropGate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    done: Arc<AtomicBool>,
}

impl Drop for DropGate {
    fn drop(&mut self) {
        self.entered.send(()).expect("drop entry observer");
        self.release
            .recv_timeout(LIFECYCLE_GUARD)
            .expect("release controlled destructor");
        self.done.store(true, Ordering::SeqCst);
    }
}

struct GatedActor {
    entered: Option<mpsc::Sender<()>>,
    release: Option<mpsc::Receiver<()>>,
    polls: Arc<AtomicUsize>,
    ready: bool,
    _drop_gate: DropGate,
}

impl Future for GatedActor {
    type Output = u32;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<u32> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = self.entered.take() {
            entered.send(()).expect("poll entry observer");
            self.release
                .take()
                .expect("one controlled poll")
                .recv_timeout(LIFECYCLE_GUARD)
                .expect("release finite actor poll");
        }
        if self.ready {
            Poll::Ready(17)
        } else {
            Poll::Pending
        }
    }
}

#[test]
fn driven_source_pending_wake_retains_actor_until_completion() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (input_tx, input_rx) = tokio::sync::oneshot::channel();
    let (mut handle, mut proxy) = source_pair(OwnedActor::new(
        async move { input_rx.await.expect("actual source input") },
        dropped.clone(),
        false,
    ));
    let owner_wake = Arc::new(WakeCount::default());
    let native_wake = Arc::new(WakeCount::default());
    assert!(poll_once(&mut handle, &owner_wake).is_pending());
    assert!(poll_once(&mut proxy, &native_wake).is_pending());
    assert!(!dropped.load(Ordering::SeqCst));
    input_tx.send(41u32).expect("publish new source input");
    assert!(native_wake.0.load(Ordering::SeqCst) > 0);
    assert_eq!(owner_wake.0.load(Ordering::SeqCst), 0);
    assert!(poll_once(&mut proxy, &native_wake).is_ready());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        poll_once(&mut handle, &owner_wake),
        Poll::Ready(Ok(41))
    ));
}

#[test]
fn driven_source_ready_waits_for_complete_destruction_before_publication() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (drop_entered_tx, drop_entered_rx) = mpsc::channel();
    let (drop_release_tx, drop_release_rx) = mpsc::channel();
    let (mut handle, mut proxy) = source_pair(GatedActor {
        entered: None,
        release: None,
        polls: Arc::new(AtomicUsize::new(0)),
        ready: true,
        _drop_gate: DropGate {
            entered: drop_entered_tx,
            release: drop_release_rx,
            done: dropped.clone(),
        },
    });
    let owner_wake = Arc::new(WakeCount::default());
    assert!(poll_once(&mut handle, &owner_wake).is_pending());
    let poll_thread =
        std::thread::spawn(move || poll_once(&mut proxy, &Arc::new(WakeCount::default())));
    drop_entered_rx
        .recv_timeout(LIFECYCLE_GUARD)
        .expect("Ready reached actual destruction");
    assert!(!dropped.load(Ordering::SeqCst));
    // The constructor's result future was armed before the producer ran. A
    // completion published before Drop finishes would positively wake it here.
    assert_eq!(owner_wake.0.load(Ordering::SeqCst), 0);
    drop_release_tx
        .send(())
        .expect("complete actor destruction");
    assert!(
        poll_thread
            .join()
            .expect("native proxy poll did not unwind")
            .is_ready()
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        poll_once(&mut handle, &owner_wake),
        Poll::Ready(Ok(17))
    ));
}

#[test]
fn driven_source_cancel_racing_finite_poll_waits_for_owned_destructor() {
    let dropped = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let (poll_entered_tx, poll_entered_rx) = mpsc::channel();
    let (poll_release_tx, poll_release_rx) = mpsc::channel();
    let (drop_entered_tx, drop_entered_rx) = mpsc::channel();
    let (drop_release_tx, drop_release_rx) = mpsc::channel();
    let (handle, mut proxy) = source_pair(GatedActor {
        entered: Some(poll_entered_tx),
        release: Some(poll_release_rx),
        polls: polls.clone(),
        ready: false,
        _drop_gate: DropGate {
            entered: drop_entered_tx,
            release: drop_release_rx,
            done: dropped.clone(),
        },
    });
    let native_wake = Arc::new(WakeCount::default());
    let poll_wake = native_wake.clone();
    let poll_thread = std::thread::spawn(move || {
        assert!(poll_once(&mut proxy, &poll_wake).is_pending());
        // Keep the native proxy alive in the JoinHandle result. Its destructor
        // must not become the producer of this test's cancellation observation.
        proxy
    });
    poll_entered_rx
        .recv_timeout(LIFECYCLE_GUARD)
        .expect("actual actor poll entered");
    let (cancel_started_tx, cancel_started_rx) = mpsc::channel();
    let (cancel_returned_tx, cancel_returned_rx) = mpsc::channel();
    let cancel_dropped = dropped.clone();
    let cancel_thread = std::thread::spawn(move || {
        cancel_started_tx.send(()).expect("cancel entry observer");
        drop(handle);
        assert!(
            cancel_dropped.load(Ordering::SeqCst),
            "cancel returned before actual actor destruction"
        );
        cancel_returned_tx.send(()).expect("cancel return observer");
    });
    cancel_started_rx
        .recv_timeout(LIFECYCLE_GUARD)
        .expect("concurrent cancellation started");
    assert!(matches!(
        cancel_returned_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    poll_release_tx
        .send(())
        .expect("finish the already running actor poll");
    let mut proxy = poll_thread.join().expect("finite native poll");
    drop_entered_rx
        .recv_timeout(LIFECYCLE_GUARD)
        .expect("synchronous cancellation entered destruction");
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(matches!(
        cancel_returned_rx.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(
        native_wake.0.load(Ordering::SeqCst),
        0,
        "retirement wake preceded destruction"
    );
    drop_release_tx.send(()).expect("finish owned destruction");
    cancel_returned_rx
        .recv_timeout(LIFECYCLE_GUARD)
        .expect("cancel returned after destruction");
    cancel_thread.join().expect("cancelling owner");
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(poll_once(&mut proxy, &native_wake).is_ready());
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "cancelled actor was polled again"
    );
}

#[test]
fn driven_source_native_proxy_drop_publishes_stopped_after_destruction() {
    let dropped = Arc::new(AtomicBool::new(false));
    let (mut handle, mut proxy) = source_pair(OwnedActor::new(
        std::future::pending::<()>(),
        dropped.clone(),
        false,
    ));
    let wake = Arc::new(WakeCount::default());
    assert!(poll_once(&mut handle, &wake).is_pending());
    assert!(poll_once(&mut proxy, &wake).is_pending());
    drop(proxy);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        poll_once(&mut handle, &wake),
        Poll::Ready(Err(NativeSourceStopped { cause: None }))
    ));
}

#[test]
fn driven_source_poll_and_destructor_panics_resume_only_at_owner() {
    for panic_on_drop in [false, true] {
        let dropped = Arc::new(AtomicBool::new(false));
        let (mut handle, mut proxy) = source_pair(OwnedActor::new(
            async move {
                assert!(panic_on_drop, "owned actor poll panic");
            },
            dropped.clone(),
            panic_on_drop,
        ));
        let wake = Arc::new(WakeCount::default());
        assert!(poll_once(&mut handle, &wake).is_pending());
        let producer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_once(&mut proxy, &wake)
        }));
        assert!(
            producer
                .expect("actor panic escaped into native producer")
                .is_ready()
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "owner must observe panic after complete destruction"
        );
        let owner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            poll_once(&mut handle, &wake)
        }));
        let panic = owner.expect_err("actor panic must resume at original owner");
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .expect("test panic text");
        assert!(text.contains(if panic_on_drop {
            "destructor panic"
        } else {
            "poll panic"
        }));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_driven_bridge_panics_leave_real_h3_sibling_serviceable() {
    use crate::mux::MuxLimits;
    use crate::protocol::Frame;
    use crate::protocol::codec::CodecLimits;
    use crate::transport::quic::{Endpoint, finish_stream, read_frame, write_frame};
    use tokio::time::timeout;

    let limits = CodecLimits::default();
    let server = Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server address"),
        &crate::transport::encrypted::test_server_tls_config(),
        super::super::test_candidate_verifier(),
        MuxLimits::default(),
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server address");
    let (client_done_tx, client_done_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server.accept().await.expect("server connection");
        let (mut send, mut recv) = connection.accept_bi().await.expect("H3 request");
        for nonce in [1, 42] {
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("actual request"),
                Frame::Ping { nonce }
            );
            write_frame(&mut send, &Frame::Pong { nonce }, limits)
                .await
                .expect("actual response");
        }
        client_done_rx
            .await
            .expect("receiver completed before server exit");
    });
    let client = Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client address"),
        &crate::transport::encrypted::test_client_tls_config(),
        super::super::test_candidate_selector(),
        MuxLimits::default(),
    )
    .await
    .expect("client endpoint");
    let connection = client
        .connect(server_addr)
        .await
        .expect("client connection");
    let (mut send, mut recv) = connection.open_bi().await.expect("client H3 request");
    write_frame(&mut send, &Frame::Ping { nonce: 1 }, limits)
        .await
        .expect("warmup request");
    assert_eq!(
        timeout(LIFECYCLE_GUARD, read_frame(&mut recv, limits))
            .await
            .expect("warmup timeout")
            .expect("warmup response"),
        Frame::Pong { nonce: 1 }
    );
    let native = send.connection.clone();
    let backlog = send.write_backlog.clone();
    let terminal_registration = send.native_source_registration();
    let poll_dropped = Arc::new(AtomicBool::new(false));
    let drop_dropped = Arc::new(AtomicBool::new(false));
    let poll_owner = send
        .native_source_registration()
        .register(OwnedActor::new(
            async {
                panic!("owned actor poll panic");
            },
            poll_dropped.clone(),
            false,
        ))
        .expect("register poll panic source");
    let drop_owner = send
        .native_source_registration()
        .register(OwnedActor::new(
            std::future::ready(()),
            drop_dropped.clone(),
            true,
        ))
        .expect("register destructor panic source");
    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
    let sibling = send
        .native_source_registration()
        .register(async move {
            resume_rx.await.expect("real source availability wake");
            write_frame(&mut send, &Frame::Ping { nonce: 42 }, limits)
                .await
                .expect("sibling request after panics");
            let response = read_frame(&mut recv, limits)
                .await
                .expect("sibling response after panics");
            finish_stream(&mut send)
                .await
                .expect("finite sibling source");
            response
        })
        .expect("register live H3 sibling");
    for owner in [tokio::spawn(poll_owner), tokio::spawn(drop_owner)] {
        let failure = timeout(LIFECYCLE_GUARD, owner)
            .await
            .expect("owner panic timeout")
            .expect_err("original Tokio owner must receive actor panic");
        assert!(failure.is_panic());
    }
    assert!(poll_dropped.load(Ordering::SeqCst));
    assert!(drop_dropped.load(Ordering::SeqCst));
    assert!(
        native.close_reason().is_none(),
        "actor panic killed shared native connection"
    );
    resume_tx.send(()).expect("wake actual H3 sibling");
    assert_eq!(
        timeout(LIFECYCLE_GUARD, sibling)
            .await
            .expect("native sibling progress timeout")
            .expect("native source remained live"),
        Frame::Pong { nonce: 42 }
    );
    assert_eq!(backlog.load(Ordering::Relaxed), 0);

    // Keep the successful sibling exchange above independent of termination.
    // This actor has actually entered Pending and has no native-close branch of
    // its own, so only dropping the registered native proxy can stop it.
    let terminal_dropped = Arc::new(AtomicBool::new(false));
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let terminal_owner = terminal_registration
        .register(OwnedActor::new(
            async move {
                entered_tx.send(()).expect("pending actor entry observer");
                std::future::pending::<()>().await;
            },
            terminal_dropped.clone(),
            false,
        ))
        .expect("register actor awaiting actual native termination");
    timeout(LIFECYCLE_GUARD, entered_rx)
        .await
        .expect("pending actor entry timeout")
        .expect("native driver polled pending actor");
    assert!(!terminal_dropped.load(Ordering::SeqCst));
    assert!(native.close_reason().is_none());
    native.close(0u32.into(), b"test actual native source termination");
    let observed_cause = native
        .close_reason()
        .expect("native published its actual close cause");
    let stopped = timeout(LIFECYCLE_GUARD, terminal_owner)
        .await
        .expect("pending actor termination timeout")
        .expect_err("native termination must stop the pending actor");
    assert!(
        terminal_dropped.load(Ordering::SeqCst),
        "stop result preceded actual actor destruction"
    );
    assert_eq!(stopped.cause, Some(observed_cause));
    assert_eq!(stopped.cause, native.close_reason());
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
    client_done_tx.send(()).expect("server receiver release");
    timeout(LIFECYCLE_GUARD, server_task)
        .await
        .expect("server task timeout")
        .expect("server task");
    connection.close();
}
