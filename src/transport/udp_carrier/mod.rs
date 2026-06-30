mod ack;
mod assembly;
mod controller;
mod crypto;
mod endpoint;
mod error;
mod packet;
mod send;
mod stream;
mod window;

use crate::mux::MuxLimits;
use crate::protocol::codec::{CodecLimits, FRAME_HEADER_LEN};

pub use endpoint::{Connection, Endpoint, UdpCarrierPathMetrics};
pub use error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
pub use stream::{RecvStream, SendStream, finish_stream, read_frame, write_frame};

const STREAM_DATA_PAYLOAD_PREFIX_LEN: usize = 8 + 8 + 1 + 4;

fn stream_frame_overhead() -> usize {
    FRAME_HEADER_LEN.saturating_add(STREAM_DATA_PAYLOAD_PREFIX_LEN)
}

fn stream_payload_for_fragment(fragment_payload: usize) -> usize {
    fragment_payload
        .saturating_sub(stream_frame_overhead())
        .max(1)
}

pub fn safe_stream_payload_bytes(mux_limits: MuxLimits) -> usize {
    let fragment_payload = packet::max_frame_fragment_payload();
    stream_payload_for_fragment(fragment_payload)
        .min(mux_limits.max_tcp_relay_chunk_bytes)
        .max(1)
}

pub fn max_stream_payload_bytes(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    let safe_payload = safe_stream_payload_bytes(mux_limits);
    let ack_horizon_fragments = mux_limits.max_ack_ranges.max(1);
    let bulk_payload = safe_payload.saturating_mul(ack_horizon_fragments);
    mux_limits
        .max_tcp_relay_chunk_bytes
        .min(codec_limits.max_payload_bytes)
        .min(bulk_payload)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_stream_payload_ceiling_fits_one_udp_packet() {
        let codec_limits = CodecLimits::default();
        let compact = MuxLimits {
            max_tcp_relay_chunk_bytes: 64 * 1024,
            ..MuxLimits::default()
        };
        let high_bdp = MuxLimits {
            max_tcp_relay_chunk_bytes: 512 * 1024,
            ..MuxLimits::default()
        };

        let compact_payload = safe_stream_payload_bytes(compact);
        let high_bdp_payload = safe_stream_payload_bytes(high_bdp);
        let fragment_payload = packet::max_frame_fragment_payload();
        let packet_fit_payload = fragment_payload.saturating_sub(stream_frame_overhead());

        assert_eq!(compact_payload, high_bdp_payload);
        assert_eq!(high_bdp_payload, packet_fit_payload);
        assert!(high_bdp_payload <= codec_limits.max_payload_bytes);
    }

    #[test]
    fn udp_bulk_stream_payload_ceiling_amortizes_across_ack_horizon() {
        let codec_limits = CodecLimits::default();
        let limits = MuxLimits {
            max_tcp_relay_chunk_bytes: 64 * 1024,
            max_ack_ranges: 16,
            ..MuxLimits::default()
        };
        let safe_payload = safe_stream_payload_bytes(limits);
        let bulk_payload = max_stream_payload_bytes(codec_limits, limits);

        assert!(bulk_payload > safe_payload);
        assert_eq!(
            bulk_payload,
            (safe_payload * limits.max_ack_ranges).min(limits.max_tcp_relay_chunk_bytes)
        );
    }
}
