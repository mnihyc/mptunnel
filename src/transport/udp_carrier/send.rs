use bytes::Bytes;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GsoSendOutcome {
    Sent,
    Unsupported,
}

#[cfg(target_os = "linux")]
pub(super) async fn send_udp_segments(
    socket: &UdpSocket,
    peer: SocketAddr,
    segments: &[Bytes],
    segment_len: usize,
) -> io::Result<GsoSendOutcome> {
    use std::os::fd::AsRawFd;
    use std::ptr;
    use tokio::io::Interest;

    if segments.len() < 2 || segment_len == 0 {
        return Ok(GsoSendOutcome::Unsupported);
    }
    if segments
        .iter()
        .any(|segment| segment.is_empty() || segment.len() != segment_len)
    {
        return Ok(GsoSendOutcome::Unsupported);
    }
    let segment_len = match u16::try_from(segment_len) {
        Ok(segment_len) => segment_len,
        Err(_) => return Ok(GsoSendOutcome::Unsupported),
    };
    let mut payload = Vec::with_capacity(segments.iter().map(Bytes::len).sum());
    for segment in segments {
        payload.extend_from_slice(segment);
    }

    let fd = socket.as_raw_fd();
    let addr = socket2::SockAddr::from(peer);
    let result = socket
        .async_io(Interest::WRITABLE, || {
            let mut iov = libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            };
            let mut control = [0u8; unsafe {
                libc::CMSG_SPACE(std::mem::size_of::<u16>() as libc::c_uint) as usize
            }];
            let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
            message.msg_name = addr.as_ptr() as *mut libc::c_void;
            message.msg_namelen = addr.len();
            message.msg_iov = &mut iov;
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr() as *mut libc::c_void;
            message.msg_controllen = control.len();

            // SAFETY: message points to live stack control storage and a live
            // payload buffer. CMSG_FIRSTHDR returns a header inside that storage
            // because msg_controllen was sized with CMSG_SPACE for u16.
            unsafe {
                let cmsg = libc::CMSG_FIRSTHDR(&message);
                if cmsg.is_null() {
                    return Err(io::Error::from(io::ErrorKind::InvalidInput));
                }
                (*cmsg).cmsg_level = libc::SOL_UDP;
                (*cmsg).cmsg_type = libc::UDP_SEGMENT;
                (*cmsg).cmsg_len =
                    libc::CMSG_LEN(std::mem::size_of::<u16>() as libc::c_uint) as libc::size_t;
                ptr::write_unaligned(libc::CMSG_DATA(cmsg) as *mut u16, segment_len);
            }

            // SAFETY: the msghdr references live address, iovec, control, and
            // payload storage for the duration of this syscall. The fd belongs to
            // the Tokio socket and is accessed only after WRITABLE readiness.
            let written = unsafe { libc::sendmsg(fd, &message, 0) };
            if written < 0 {
                return Err(io::Error::last_os_error());
            }
            if written as usize == payload.len() {
                Ok(GsoSendOutcome::Sent)
            } else {
                Err(io::Error::from(io::ErrorKind::WriteZero))
            }
        })
        .await;

    match result {
        Ok(outcome) => Ok(outcome),
        Err(err) if is_gso_unsupported(&err) => Ok(GsoSendOutcome::Unsupported),
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) async fn send_udp_segments(
    _socket: &UdpSocket,
    _peer: SocketAddr,
    _segments: &[Bytes],
    _segment_len: usize,
) -> io::Result<GsoSendOutcome> {
    Ok(GsoSendOutcome::Unsupported)
}

#[cfg(target_os = "linux")]
fn is_gso_unsupported(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EINVAL | libc::ENOPROTOOPT | libc::EOPNOTSUPP | libc::EMSGSIZE)
    )
}
