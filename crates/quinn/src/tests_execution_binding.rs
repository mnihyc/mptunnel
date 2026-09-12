use super::ExecutionBinding;
use crate::ExecutionDomain;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

// Deadlines bound a broken test's lifetime; they do not model Native timing.
const DEADLINE: Duration = Duration::from_secs(5);
const STILL_BLOCKED: Duration = Duration::from_millis(100);

struct ReleaseOnDrop(Option<Sender<()>>);

impl ReleaseOnDrop {
    fn release(mut self) {
        self.0.take().unwrap().send(()).unwrap();
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

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

struct PollGate {
    entered: Sender<()>,
    release: Receiver<()>,
}

struct Probe {
    first_poll: Option<PollGate>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    drop_entered: Option<Sender<()>>,
}

impl Future for Probe {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = self.first_poll.take() {
            gate.entered.send(()).unwrap();
            gate.release.recv_timeout(DEADLINE).unwrap();
        }
        Poll::Pending
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = self.drop_entered.take() {
            let _ = entered.send(());
        }
    }
}

fn hold_domain(domain: ExecutionDomain) -> (ReleaseOnDrop, thread::JoinHandle<()>) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder = thread::spawn(move || {
        domain.with_exclusive(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(DEADLINE).unwrap();
        });
    });
    let release = ReleaseOnDrop(Some(release_tx));
    entered_rx.recv_timeout(DEADLINE).unwrap();
    (release, holder)
}

#[test]
fn binding_fences_the_actual_unbound_poll_then_joins_domain() {
    let binding = Arc::new(ExecutionBinding::default());
    let domain = ExecutionDomain::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_poll = ReleaseOnDrop(Some(release_tx));
    let mut driver = binding.wrap(Probe {
        first_poll: Some(PollGate {
            entered: entered_tx,
            release: release_rx,
        }),
        polls: polls.clone(),
        drops: drops.clone(),
        drop_entered: None,
    });
    let (polled_tx, polled_rx) = mpsc::channel();
    let poller = thread::spawn(move || {
        let wake = Waker::from(Arc::new(WakeCount::default()));
        let result = Pin::new(&mut driver).poll(&mut Context::from_waker(&wake));
        assert!(polled_tx.send((driver, result)).is_ok());
    });
    entered_rx.recv_timeout(DEADLINE).unwrap();
    assert!(binding.unbound_poll.try_lock().is_err());
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    let (binding_started_tx, binding_started_rx) = mpsc::channel();
    let (bound_tx, bound_rx) = mpsc::channel();
    let binder_binding = binding.clone();
    let binder_domain = domain.clone();
    let binder = thread::spawn(move || {
        binding_started_tx.send(()).unwrap();
        bound_tx.send(binder_binding.bind(binder_domain)).unwrap();
    });
    binding_started_rx.recv_timeout(DEADLINE).unwrap();
    assert_eq!(
        bound_rx.recv_timeout(STILL_BLOCKED),
        Err(RecvTimeoutError::Timeout),
        "binding returned while the old driver poll was still executing"
    );
    assert!(binding.domain().is_none());

    release_poll.release();
    let (driver, result) = polled_rx.recv_timeout(DEADLINE).unwrap();
    assert!(result.is_pending());
    assert_eq!(bound_rx.recv_timeout(DEADLINE).unwrap(), Ok(()));
    poller.join().unwrap();
    binder.join().unwrap();
    assert!(binding.domain().unwrap().same_domain(&domain));

    // A later Native poll must join the domain before touching its actor. It
    // returns Pending instead of blocking a worker, retaining its parent wake.
    let (release_domain, holder) = hold_domain(domain);
    let wake_count = Arc::new(WakeCount::default());
    let contender_wake = wake_count.clone();
    let (contended_tx, contended_rx) = mpsc::channel();
    let contender = thread::spawn(move || {
        let mut driver = driver;
        let wake = Waker::from(contender_wake);
        let result = Pin::new(&mut driver).poll(&mut Context::from_waker(&wake));
        assert!(contended_tx.send((driver, result)).is_ok());
    });
    let (mut driver, result) = contended_rx.recv_timeout(DEADLINE).unwrap();
    assert!(result.is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    contender.join().unwrap();
    release_domain.release();
    holder.join().unwrap();
    assert!(wake_count.0.load(Ordering::SeqCst) > 0);
    let wake = Waker::from(wake_count);
    assert!(
        Pin::new(&mut driver)
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    drop(driver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn binding_accepts_only_the_existing_domain_identity() {
    let binding = ExecutionBinding::default();
    let original = ExecutionDomain::default();
    assert_eq!(binding.bind(original.clone()), Ok(()));
    assert_eq!(binding.bind(original.clone()), Ok(()));
    assert_eq!(
        binding.bind(ExecutionDomain::default()),
        Err(super::ExecutionDomainConflict)
    );
    assert!(binding.domain().unwrap().same_domain(&original));
}

#[test]
fn binding_between_last_unbound_poll_and_drop_gates_actual_destructor() {
    let binding = Arc::new(ExecutionBinding::default());
    let domain = ExecutionDomain::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (drop_entered_tx, drop_entered_rx) = mpsc::channel();
    let mut driver = binding.wrap(Probe {
        first_poll: None,
        polls: polls.clone(),
        drops: drops.clone(),
        drop_entered: Some(drop_entered_tx),
    });
    let wake = Waker::from(Arc::new(WakeCount::default()));
    assert!(
        Pin::new(&mut driver)
            .poll(&mut Context::from_waker(&wake))
            .is_pending()
    );
    binding.bind(domain.clone()).unwrap();

    // No post-binding poll is allowed to transition the wrapper for this case.
    let (release_domain, holder) = hold_domain(domain);
    let (dropping_tx, dropping_rx) = mpsc::channel();
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let dropper = thread::spawn(move || {
        dropping_tx.send(()).unwrap();
        drop(driver);
        dropped_tx.send(()).unwrap();
    });
    dropping_rx.recv_timeout(DEADLINE).unwrap();
    assert_eq!(
        drop_entered_rx.recv_timeout(STILL_BLOCKED),
        Err(RecvTimeoutError::Timeout),
        "actor destruction escaped the domain after a successful bind"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    release_domain.release();
    drop_entered_rx.recv_timeout(DEADLINE).unwrap();
    dropped_rx.recv_timeout(DEADLINE).unwrap();
    holder.join().unwrap();
    dropper.join().unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
