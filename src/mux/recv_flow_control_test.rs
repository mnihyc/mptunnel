use super::{ReliableRecvStream, StreamError};
use crate::mux::MuxLimits;
use crate::protocol::{OffsetRange, StreamId};
use bytes::Bytes;

#[test]
fn receive_data_rejects_unpublished_credit_without_mutation() {
    let mut recv =
        ReliableRecvStream::new_with_initial_max_offset(StreamId(41), MuxLimits::default(), 4);

    let error = recv
        .receive_data(3, Bytes::from_static(b"xx"))
        .expect_err("range crossing the published ceiling must fail");

    assert_eq!(error, StreamError::FlowControlViolation { end: 5, max: 4 });
    assert_eq!(recv.next_offset(), 0);
    assert_eq!(recv.reorder_bytes(), 0);
    assert!(recv.ack_ranges().is_empty());
}

#[test]
fn committed_credit_is_monotonic_and_allows_exact_boundary_data() {
    let mut recv =
        ReliableRecvStream::new_with_initial_max_offset(StreamId(42), MuxLimits::default(), 4);

    recv.receive_data(0, Bytes::from_static(b"head"))
        .expect("initial published credit");
    recv.commit_max_data(8);
    recv.commit_max_data(6);
    recv.receive_data(4, Bytes::from_static(b"tail"))
        .expect("extended published credit");

    assert_eq!(recv.published_max_offset(), 8);
    assert_eq!(recv.next_offset(), 8);
    assert_eq!(
        recv.ack_ranges(),
        vec![OffsetRange::new(0, 8).expect("nonempty range")]
    );
}

#[test]
fn zero_credit_receive_stream_stays_blocked_until_commit() {
    let mut recv =
        ReliableRecvStream::new_with_initial_max_offset(StreamId(43), MuxLimits::default(), 0);

    assert_eq!(
        recv.receive_data(0, Bytes::from_static(b"x")),
        Err(StreamError::FlowControlViolation { end: 1, max: 0 })
    );
    recv.commit_max_data(1);
    assert!(recv.receive_data(0, Bytes::from_static(b"x")).is_ok());
}
