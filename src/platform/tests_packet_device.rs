use super::*;

#[test]
fn managed_guard_signals_ready_and_reports_drop() {
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let state = Arc::new(ManagedPacketDeviceState {
        live: AtomicBool::new(true),
        ready: Mutex::new(Some(ready_tx)),
    });
    let mut guard = ManagedPacketDeviceGuard {
        state: state.clone(),
    };

    guard.signal_ready();
    assert_eq!(ready_rx.try_recv(), Ok(()));
    assert!(state.live.load(Ordering::Acquire));
    drop(guard);
    assert!(!state.live.load(Ordering::Acquire));
}

#[test]
fn dropping_unready_guard_cancels_publication_barrier() {
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let state = Arc::new(ManagedPacketDeviceState {
        live: AtomicBool::new(true),
        ready: Mutex::new(Some(ready_tx)),
    });

    drop(ManagedPacketDeviceGuard { state });
    assert!(matches!(
        ready_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
}
