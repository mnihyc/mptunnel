//! Exact-socket native TCP refill admission.
//!
//! The low-water setting limits unsent socket admission, not congestion flight
//! or Product credit. Readiness permits an imminent write attempt; it does not
//! reserve room for a complete protected frame. The carrier remains responsible
//! for its existing partial-write and terminal lifetimes.

use std::io;
use tokio::net::TcpStream;

#[cfg(any(target_os = "linux", target_os = "android"))]
use std::os::fd::{AsFd, AsRawFd, OwnedFd};
#[cfg(any(target_os = "linux", target_os = "android"))]
use tokio::io::{Interest, unix::AsyncFd};

/// One reactor registration owned by the same physical carrier as its writer.
/// A cancelled writable future does not close or replace this socket owner.
#[derive(Debug)]
pub(crate) struct TcpWriteAdmission {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket: AsyncFd<OwnedFd>,
}

impl TcpWriteAdmission {
    /// Captures an already nonblocking carrier socket inside its Tokio runtime.
    ///
    /// Linux wakes writers below half the unsent low-water threshold. Twice the
    /// caller's native-byte refill quantum supplies that reserve; this is not a
    /// protected-frame size or an estimate of congestion-window capacity.
    pub(crate) fn capture(
        stream: &TcpStream,
        refill_quantum_bytes: usize,
    ) -> io::Result<Option<Self>> {
        let low_water = low_water_bytes(refill_quantum_bytes)?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            // Finish every fallible ownership/registration step before changing
            // the original socket's option. Failure cannot leave a low-water
            // policy behind without its readiness owner.
            let fd = stream.as_fd().try_clone_to_owned()?;
            let socket = AsyncFd::with_interest(fd, Interest::WRITABLE)?;
            // SAFETY: this borrows the owned socket and an initialized u32 for
            // exactly its size. setsockopt retains neither pointer.
            let result = unsafe {
                libc::setsockopt(
                    socket.get_ref().as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    (&low_water as *const u32).cast(),
                    std::mem::size_of::<u32>() as libc::socklen_t,
                )
            };
            if result != 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(libc::ENOPROTOOPT) {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
            Ok(Some(Self { socket }))
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            let _ = (stream, low_water);
            Ok(None)
        }
    }

    /// Fresh native readiness, never a cached writable event or sampled queue.
    pub(crate) fn is_ready(&self) -> io::Result<bool> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            poll_write_ready(self.socket.get_ref())
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native TCP refill admission is unavailable",
            ))
        }
    }

    /// Waits for actual socket readiness without a timer or native-queue poll
    /// loop. A stale reactor event is cleared only after a fresh negative poll.
    pub(crate) async fn writable(&self) -> io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            loop {
                let mut ready = self.socket.writable().await?;
                match ready.try_io(|socket| {
                    if poll_write_ready(socket.get_ref())? {
                        Ok(())
                    } else {
                        Err(io::Error::from(io::ErrorKind::WouldBlock))
                    }
                }) {
                    Ok(result) => return result,
                    Err(_stale_readiness) => continue,
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native TCP refill admission is unavailable",
            ))
        }
    }
}

fn low_water_bytes(refill_quantum_bytes: usize) -> io::Result<u32> {
    refill_quantum_bytes
        .checked_mul(2)
        .and_then(|bytes| u32::try_from(bytes).ok())
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP refill quantum must have a positive u32 twice-quantum threshold",
            )
        })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn poll_write_ready(socket: &OwnedFd) -> io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: socket.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    loop {
        // SAFETY: poll borrows one initialized pollfd for this synchronous call;
        // timeout zero only checks the kernel's current writable predicate.
        let result = unsafe { libc::poll(&mut descriptor, 1, 0) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if descriptor.revents & libc::POLLNVAL != 0 {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        // Terminal readiness wakes the existing lifecycle owner but cannot
        // authorize a new Original claim. Do not consume SO_ERROR here, or
        // confuse a peer write-half-close (POLLRDHUP) with a full hangup.
        if descriptor.revents & (libc::POLLERR | libc::POLLHUP) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "native TCP write admission observed a socket error or full hangup",
            ));
        }
        return Ok(descriptor.revents & libc::POLLOUT != 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refill_threshold_requires_checked_nonzero_native_bytes() {
        assert_eq!(low_water_bytes(64 * 1024).unwrap(), 128 * 1024);
        assert_eq!(
            low_water_bytes(0).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            low_water_bytes(usize::MAX).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
        );
        assert_eq!(
            low_water_bytes(u32::MAX as usize).unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    mod native {
        use super::*;
        use std::time::Duration;
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        // A test cleanup bound, not native admission or refill policy.
        const TEST_TIMEOUT: Duration = Duration::from_secs(5);
        const REFILL_QUANTUM: usize = 64 * 1024;

        async fn connected_pair() -> (TcpStream, TcpStream) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (sender, receiver) = tokio::join!(TcpStream::connect(address), listener.accept());
            (sender.unwrap(), receiver.unwrap().0)
        }

        fn socket_u32(stream: &TcpStream, level: i32, option: i32) -> u32 {
            let mut value = 0_u32;
            let mut length = std::mem::size_of::<u32>() as libc::socklen_t;
            // SAFETY: getsockopt borrows this exact socket and initialized
            // storage sized by length; it retains neither output pointer.
            let result = unsafe {
                libc::getsockopt(
                    stream.as_raw_fd(),
                    level,
                    option,
                    (&mut value as *mut u32).cast(),
                    &mut length,
                )
            };
            assert_eq!(result, 0, "socket option: {}", io::Error::last_os_error());
            assert_eq!(length as usize, std::mem::size_of::<u32>());
            value
        }

        fn fill_until_native_blocked(
            sender: &TcpStream,
            admission: &TcpWriteAdmission,
            byte_limit: usize,
        ) -> usize {
            let payload = [0x5a_u8; REFILL_QUANTUM];
            let mut accepted = 0;
            // Bound fixture work if an unexpected platform never backpressures
            // an unread peer. No socket buffer or congestion option is altered.
            while accepted < byte_limit {
                if !admission.is_ready().unwrap() {
                    return accepted;
                }
                let wanted = payload.len().min(byte_limit - accepted);
                // SAFETY: send borrows this nonblocking socket and the valid
                // payload slice. Direct native send avoids the *test producer's*
                // separate cached Tokio writable bit obscuring the adapter.
                let written = unsafe {
                    libc::send(
                        sender.as_raw_fd(),
                        payload.as_ptr().cast(),
                        wanted,
                        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                    )
                };
                if written > 0 {
                    accepted += written as usize;
                } else {
                    let error = io::Error::last_os_error();
                    if written < 0 && error.kind() == io::ErrorKind::WouldBlock {
                        return accepted;
                    }
                    panic!("fixture native write failed: result={written}, error={error}");
                }
            }
            panic!("unread TCP peer did not backpressure within 64 MiB of fixture writes");
        }

        async fn fill_until_wait_parks(sender: &TcpStream, admission: &TcpWriteAdmission) -> usize {
            let mut accepted = 0;
            // The kernel may drain an early low-water crossing into the unread
            // peer's receive window between poll and await. Refill on that real
            // progress until a wait actually parks; do not mistake it for a bug.
            for _ in 0..1024 {
                accepted +=
                    fill_until_native_blocked(sender, admission, 64 * 1024 * 1024 - accepted);
                let mut first_wait = Box::pin(admission.writable());
                match futures::poll!(&mut first_wait) {
                    std::task::Poll::Pending => {
                        assert!(accepted > 0, "the actual socket accepted fixture data");
                        // Cancel this wait without closing the reactor owner.
                        return accepted;
                    }
                    std::task::Poll::Ready(result) => result.unwrap(),
                }
            }
            panic!("the native wait did not park within bounded fixture work");
        }

        #[tokio::test]
        async fn low_water_is_exact_socket_option_without_send_buffer_change() {
            let (sender, receiver) = connected_pair().await;
            let sender_buffer = socket_u32(&sender, libc::SOL_SOCKET, libc::SO_SNDBUF);
            let peer_low_water = socket_u32(&receiver, libc::IPPROTO_TCP, libc::TCP_NOTSENT_LOWAT);
            let admission = TcpWriteAdmission::capture(&sender, REFILL_QUANTUM)
                .unwrap()
                .expect("Linux TCP_NOTSENT_LOWAT");
            assert_eq!(
                socket_u32(&sender, libc::IPPROTO_TCP, libc::TCP_NOTSENT_LOWAT),
                2 * REFILL_QUANTUM as u32,
            );
            assert_eq!(
                socket_u32(&sender, libc::SOL_SOCKET, libc::SO_SNDBUF),
                sender_buffer
            );
            assert_eq!(
                socket_u32(&receiver, libc::IPPROTO_TCP, libc::TCP_NOTSENT_LOWAT),
                peer_low_water,
                "the opposite socket is unchanged",
            );
            assert!(admission.is_ready().unwrap());
            // No peer application read or Product ACK is required for writable
            // admission. This does not assert a particular native flight size.
            assert_eq!(sender.try_write(b"unconsumed").unwrap(), 10);
            assert!(admission.is_ready().unwrap());
        }

        #[tokio::test]
        async fn native_blocked_wait_clears_stale_readiness_and_survives_cancellation() {
            let (sender, mut receiver) = connected_pair().await;
            let admission = TcpWriteAdmission::capture(&sender, REFILL_QUANTUM)
                .unwrap()
                .expect("Linux TCP_NOTSENT_LOWAT");
            tokio::time::timeout(TEST_TIMEOUT, admission.writable())
                .await
                .unwrap()
                .unwrap();
            let accepted = fill_until_wait_parks(&sender, &admission).await;
            let drain = async {
                let mut remaining = accepted;
                let mut buffer = [0_u8; REFILL_QUANTUM];
                while remaining > 0 {
                    let wanted = remaining.min(buffer.len());
                    let read = receiver.read(&mut buffer[..wanted]).await.unwrap();
                    assert!(read > 0);
                    remaining -= read;
                }
            };
            tokio::time::timeout(TEST_TIMEOUT, async {
                let (ready, ()) = tokio::join!(admission.writable(), drain);
                ready.unwrap();
            })
            .await
            .expect("native drain alone wakes admission, without another sender write");
            assert!(admission.is_ready().unwrap());
        }

        #[tokio::test]
        async fn peer_close_does_not_strand_native_write_admission() {
            let (mut sender, receiver) = connected_pair().await;
            let admission = TcpWriteAdmission::capture(&sender, REFILL_QUANTUM)
                .unwrap()
                .expect("Linux TCP_NOTSENT_LOWAT");
            tokio::time::timeout(TEST_TIMEOUT, admission.writable())
                .await
                .unwrap()
                .unwrap();
            let _accepted = fill_until_wait_parks(&sender, &admission).await;
            drop(receiver);
            let mut byte = [0_u8; 1];
            let terminal = tokio::time::timeout(TEST_TIMEOUT, sender.read(&mut byte))
                .await
                .expect("the original I/O owner can observe peer terminal state");
            assert!(matches!(terminal, Ok(0) | Err(_)));
            assert_eq!(
                admission.is_ready().unwrap_err().kind(),
                io::ErrorKind::ConnectionAborted,
                "observed terminal state must not authorize a new Original claim",
            );
            assert_eq!(
                tokio::time::timeout(TEST_TIMEOUT, admission.writable())
                    .await
                    .expect("peer terminal state wakes native admission")
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::ConnectionAborted,
            );
        }

        #[tokio::test]
        async fn duplicate_owner_is_retained_until_the_carrier_admission_is_dropped() {
            let (sender, mut receiver) = connected_pair().await;
            let admission = TcpWriteAdmission::capture(&sender, REFILL_QUANTUM)
                .unwrap()
                .expect("Linux TCP_NOTSENT_LOWAT");
            drop(sender);
            let mut byte = [0_u8; 1];
            let mut peer_read = Box::pin(receiver.read(&mut byte));
            assert!(futures::poll!(&mut peer_read).is_pending());
            assert!(admission.is_ready().unwrap());
            drop(admission);
            assert_eq!(
                tokio::time::timeout(TEST_TIMEOUT, peer_read)
                    .await
                    .unwrap()
                    .unwrap(),
                0,
                "dropping the last exact socket owner releases the native connection",
            );
        }
    }
}
