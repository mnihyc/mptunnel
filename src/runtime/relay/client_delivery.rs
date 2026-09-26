//! Client-side ordered response delivery and receive-credit frontier.

use super::io::pending_stream_fin_ready;
use super::server_delivery::ServerTargetIo;
use crate::model::capacity::reliable_stream_advertised_window_bytes;
use crate::mux::MuxLimits;
use crate::mux::stream::ReliableRecvStream;
use crate::runtime::sender::RelayRecvProgressSend;
use crate::scheduler::{PathSnapshot, TrafficClass};

/// Construct every client response MAX_DATA request from the flushed local
/// sink frontier. Receipt ACK state remains in `ReliableRecvProgress`.
pub(super) fn recv_progress_send(
    delivery: &ServerTargetIo,
    recv_stream: &ReliableRecvStream,
    path: Option<PathSnapshot>,
    lane: TrafficClass,
    mux_limits: MuxLimits,
    force_max_data: bool,
) -> RelayRecvProgressSend {
    if delivery.shutdown_requested() {
        return RelayRecvProgressSend::ack_only(path, lane);
    }

    let window = reliable_stream_advertised_window_bytes(path, lane, mux_limits);
    let flushed_max_offset = delivery.delivered_offset().saturating_add(window);
    let max_offset = flushed_max_offset.max(recv_stream.published_max_offset());
    RelayRecvProgressSend::new(path, lane, force_max_data, max_offset)
}

/// A FIN is lifecycle-ready only when its contiguous receive extent has also
/// crossed the target's successful flush frontier. The caller separately waits
/// for final-ACK admission before requesting local half-close.
pub(super) fn remote_fin_delivery_ready(
    delivery: &ServerTargetIo,
    recv_stream: &ReliableRecvStream,
    pending_final_offset: Option<u64>,
) -> bool {
    let Some(final_offset) = pending_final_offset else {
        return false;
    };
    !delivery.shutdown_requested()
        && pending_stream_fin_ready(recv_stream, Some(final_offset))
        && delivery.delivered_offset() == final_offset
        && delivery.pending_bytes() == 0
        && !delivery.has_work()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::capacity::reliable_stream_initial_advertised_window_bytes;
    use crate::mux::MuxLimits;
    use crate::protocol::StreamId;
    use crate::protocol::UnderlayProtocol;
    use bytes::Bytes;
    use smallvec::smallvec;
    use std::future::poll_fn;
    use tokio::io::duplex;

    #[tokio::test]
    async fn receive_credit_and_fin_follow_the_flushed_target_frontier() {
        let stream_id = StreamId(0xD311);
        let limits = MuxLimits::default();
        let window = reliable_stream_initial_advertised_window_bytes(
            UnderlayProtocol::Tcp,
            TrafficClass::Latency,
            limits,
        );
        let mut recv = ReliableRecvStream::new_with_initial_max_offset(stream_id, limits, window);
        recv.receive_data(0, Bytes::from_static(b"four"))
            .expect("within initial receive credit");
        let mut delivery = ServerTargetIo::new(2, usize::try_from(window).unwrap());
        delivery
            .append_batch(smallvec![Bytes::from_static(b"four")])
            .expect("within delivery window");

        assert_eq!(recv.next_offset(), 4);
        assert_eq!(
            recv_progress_send(&delivery, &recv, None, TrafficClass::Latency, limits, false,)
                .max_data_offset(),
            Some(window),
            "receipt alone cannot advance the grant"
        );
        assert!(
            !remote_fin_delivery_ready(&delivery, &recv, Some(4)),
            "the receive frontier is ahead of local flush"
        );

        let (mut sink, _peer) = duplex(8);
        while delivery.delivered_offset() < 4 {
            poll_fn(|cx| delivery.poll_io(cx, &mut sink))
                .await
                .expect("target write and flush");
        }
        assert_eq!(delivery.delivered_offset(), 4);
        assert!(remote_fin_delivery_ready(&delivery, &recv, Some(4)));
        assert_eq!(
            recv_progress_send(&delivery, &recv, None, TrafficClass::Latency, limits, false,)
                .max_data_offset(),
            Some(window + 4),
            "only successful flush advances the absolute receive grant"
        );
    }
}
