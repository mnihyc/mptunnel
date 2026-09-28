#![warn(rust_2018_idioms)]
#![cfg(all(feature = "rt-multi-thread", feature = "time", not(target_os = "wasi")))]

use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::Poll;
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

// A finite poll that crosses the one millisecond service budget. Runtime
// maintenance cannot interrupt a poll already in progress.
const HOT_POLL_WORK: Duration = Duration::from_millis(2);
const TIMER_DELAY: Duration = Duration::from_millis(20);
const TEST_WATCHDOG: Duration = Duration::from_secs(5);

// The production default remains 61 event turns. The regression tests raise
// that interval to isolate elapsed-time service and use 61 as a poll watchdog.
const ORIGINAL_EVENT_INTERVAL: usize = 61;

#[derive(Debug)]
enum ServiceSignal {
    TimerFired(usize),
    SocketReady(usize),
    PollLimitReached(usize),
}

#[test]
fn elapsed_service_reaches_due_timer_with_one_worker() {
    check_elapsed_service(1);
}

#[test]
fn elapsed_service_reaches_due_timer_with_two_workers() {
    check_elapsed_service(2);
}

fn check_elapsed_service(worker_count: usize) {
    let runtime = runtime_with_long_event_interval(worker_count);
    let (signal_tx, signal_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    let (hot_tasks, hot_started_rx) = spawn_hot_tasks(
        &runtime,
        worker_count,
        Arc::clone(&stop),
        Arc::clone(&polls),
        signal_tx.clone(),
    );

    // Start the timer only after the hot tasks have begun self-waking. The
    // timer deadline therefore becomes ready while the workers stay busy.
    receive_task_starts(&hot_started_rx, hot_tasks.len());
    let (timer_started_tx, timer_started_rx) = mpsc::channel();
    let timer_polls = Arc::clone(&polls);
    let timer_stop = Arc::clone(&stop);
    let timer_signal = signal_tx;
    let timer_task = runtime.spawn(async move {
        let mut sleep = Box::pin(tokio::time::sleep(TIMER_DELAY));
        let mut started_tx = Some(timer_started_tx);

        poll_fn(move |cx| {
            let result = sleep.as_mut().poll(cx);
            if let Some(started_tx) = started_tx.take() {
                let _ = started_tx.send(());
            }
            result
        })
        .await;

        let observed_polls = timer_polls.load(Ordering::Relaxed);
        timer_stop.store(true, Ordering::Release);
        let _ = timer_signal.send(ServiceSignal::TimerFired(observed_polls));
    });
    let signal = match timer_started_rx.recv_timeout(TEST_WATCHDOG) {
        Ok(()) => signal_rx.recv_timeout(TEST_WATCHDOG),
        Err(error) => Err(error),
    };
    stop.store(true, Ordering::Release);
    timer_task.abort();
    finish_hot_tasks(&runtime, hot_tasks);
    let _ = runtime.block_on(timer_task);
    finish_idle_and_shutdown(runtime);

    assert_service_before_poll_limit(signal, "timer");
}

fn runtime_with_long_event_interval(worker_count: usize) -> Runtime {
    let mut builder = Builder::new_multi_thread();
    builder
        .worker_threads(worker_count)
        // Admit new tasks promptly while hot tasks refill local queues, so
        // this test isolates event-driver service in maintenance.
        .global_queue_interval(1)
        .event_interval(1_000_000)
        .enable_all();
    builder.build().unwrap()
}

fn spawn_hot_tasks(
    runtime: &Runtime,
    worker_count: usize,
    stop: Arc<AtomicBool>,
    polls: Arc<AtomicUsize>,
    signal: mpsc::Sender<ServiceSignal>,
) -> (Vec<JoinHandle<()>>, mpsc::Receiver<()>) {
    let (started_tx, started_rx) = mpsc::channel();
    let mut handles = Vec::with_capacity(worker_count * 2);

    // Two loops per worker keep ready work available even while a worker
    // handles another task or transfers its core through block_in_place.
    for _ in 0..(worker_count * 2) {
        let stop = Arc::clone(&stop);
        let polls = Arc::clone(&polls);
        let signal = signal.clone();
        let mut started_tx = Some(started_tx.clone());

        handles.push(runtime.spawn(poll_fn(move |cx| {
            if stop.load(Ordering::Acquire) {
                return Poll::Ready(());
            }

            if let Some(started_tx) = started_tx.take() {
                let _ = started_tx.send(());
            }

            let observed_polls = polls.fetch_add(1, Ordering::Relaxed) + 1;
            if observed_polls >= ORIGINAL_EVENT_INTERVAL {
                stop.store(true, Ordering::Release);
                let _ = signal.send(ServiceSignal::PollLimitReached(observed_polls));
                return Poll::Ready(());
            }

            let poll_started = Instant::now();
            while poll_started.elapsed() < HOT_POLL_WORK {
                std::hint::spin_loop();
            }

            if stop.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                // `yield_now` defers its wake and could let the worker park,
                // which would exercise the ordinary event path instead.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })));
    }

    (handles, started_rx)
}

fn receive_task_starts(started_rx: &mpsc::Receiver<()>, count: usize) {
    for _ in 0..count {
        started_rx
            .recv_timeout(TEST_WATCHDOG)
            .expect("each hot task should begin polling");
    }
}

fn finish_hot_tasks(runtime: &Runtime, hot_tasks: Vec<JoinHandle<()>>) {
    for task in hot_tasks {
        runtime
            .block_on(task)
            .expect("hot task should stop after the shared flag is set");
    }
}

fn finish_idle_and_shutdown(runtime: Runtime) {
    runtime.block_on(async {
        tokio::time::sleep(Duration::from_millis(5)).await;
    });
    drop(runtime);
}

fn assert_service_before_poll_limit(
    signal: Result<ServiceSignal, mpsc::RecvTimeoutError>,
    event_name: &str,
) {
    match signal {
        Ok(ServiceSignal::TimerFired(observed_polls))
        | Ok(ServiceSignal::SocketReady(observed_polls)) => {
            assert!(observed_polls > 0, "hot tasks should have made progress");
            assert!(
                observed_polls < ORIGINAL_EVENT_INTERVAL,
                "{event_name} should be serviced before {ORIGINAL_EVENT_INTERVAL} hot polls; observed {observed_polls}"
            );
        }
        Ok(ServiceSignal::PollLimitReached(observed_polls)) => {
            panic!(
                "{event_name} service did not occur before the {ORIGINAL_EVENT_INTERVAL}-poll limit (observed {observed_polls})"
            );
        }
        Err(error) => panic!("{event_name} or hot-poll watchdog expired: {error}"),
    }
}

#[cfg(feature = "net")]
#[test]
fn elapsed_service_handles_socket_readiness_after_block_in_place() {
    check_socket_readiness_after_block_in_place(1);
    check_socket_readiness_after_block_in_place(2);
}

#[cfg(feature = "net")]
fn check_socket_readiness_after_block_in_place(worker_count: usize) {
    use std::future::poll_fn;
    use std::io::Write;
    use std::pin::Pin;

    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::net::TcpStream;

    let runtime = runtime_with_long_event_interval(worker_count);
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind a local TCP listener");
    let address = listener.local_addr().unwrap();
    let mut writer = std::net::TcpStream::connect(address).expect("connect to local listener");
    let (reader, _) = listener.accept().expect("accept local connection");
    reader.set_nonblocking(true).unwrap();
    writer.set_write_timeout(Some(TEST_WATCHDOG)).unwrap();
    let reader = {
        let _entered = runtime.enter();
        TcpStream::from_std(reader).expect("register local reader with Tokio")
    };

    let (signal_tx, signal_rx) = mpsc::channel();
    let (read_registered_tx, read_registered_rx) = mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let read_polls = Arc::clone(&polls);
    let read_signal = signal_tx.clone();
    let reader_task = runtime.spawn(async move {
        let mut reader = reader;
        let mut buffer = [0; 1];
        let mut registered_tx = Some(read_registered_tx);

        let result = poll_fn(move |cx| {
            let mut read_buf = ReadBuf::new(&mut buffer);
            match Pin::new(&mut reader).poll_read(cx, &mut read_buf) {
                Poll::Pending => {
                    if let Some(registered_tx) = registered_tx.take() {
                        let _ = registered_tx.send(());
                    }
                    Poll::Pending
                }
                Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().first().copied())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            }
        })
        .await;

        if matches!(result, Ok(Some(_))) {
            let _ = read_signal.send(ServiceSignal::SocketReady(
                read_polls.load(Ordering::Relaxed),
            ));
        }
        result
    });
    read_registered_rx
        .recv_timeout(TEST_WATCHDOG)
        .expect("reader should register interest before data is written");

    let stop = Arc::new(AtomicBool::new(false));
    let (hot_tasks, hot_started_rx) = spawn_hot_tasks(
        &runtime,
        worker_count,
        Arc::clone(&stop),
        Arc::clone(&polls),
        signal_tx.clone(),
    );
    receive_task_starts(&hot_started_rx, hot_tasks.len());

    // Exercise the scheduler's core handoff while ready work is continuous.
    let (handoff_tx, handoff_rx) = mpsc::channel();
    let handoff_task = runtime.spawn(async move {
        tokio::task::block_in_place(|| {
            std::thread::sleep(Duration::from_millis(2));
        });
        let _ = handoff_tx.send(());
    });
    handoff_rx
        .recv_timeout(TEST_WATCHDOG)
        .expect("block_in_place should hand the core back without hanging");

    writer.write_all(b"x").unwrap();
    let signal = signal_rx.recv_timeout(TEST_WATCHDOG);

    stop.store(true, Ordering::Release);
    reader_task.abort();
    finish_hot_tasks(&runtime, hot_tasks);
    let read_result = runtime.block_on(reader_task);
    let _ = runtime.block_on(handoff_task);
    finish_idle_and_shutdown(runtime);

    assert_service_before_poll_limit(signal, "socket readiness");
    assert_eq!(read_result.unwrap().unwrap(), Some(b'x'));
}
