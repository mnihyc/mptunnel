mod crypto;
mod endpoint;
mod error;
mod packet;
mod stream;

use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;

pub use endpoint::{Connection, Endpoint};
pub use error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
pub use stream::{RecvStream, SendStream, finish_stream, read_frame, write_frame};

const MIN_STREAM_FRAME_PACKET_BATCH: usize = 32;
const MAX_STREAM_FRAME_PACKET_BATCH: usize = 512;
const STREAM_FRAME_CODEC_OVERHEAD_ALLOWANCE: usize = 256;

pub fn max_stream_payload_bytes(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    let fragment_payload = packet::max_frame_fragment_payload();
    let target_payload = mux_limits
        .max_tcp_relay_chunk_bytes
        .min(codec_limits.max_payload_bytes)
        .max(1);
    let packet_batch = target_payload
        .div_ceil(fragment_payload)
        .clamp(MIN_STREAM_FRAME_PACKET_BATCH, MAX_STREAM_FRAME_PACKET_BATCH);
    fragment_payload
        .saturating_mul(packet_batch)
        .saturating_sub(128)
        .saturating_sub(STREAM_FRAME_CODEC_OVERHEAD_ALLOWANCE)
        .clamp(1, codec_limits.max_payload_bytes.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_payload_ceiling_tracks_mux_chunk_budget() {
        let codec_limits = CodecLimits::default();
        let compact = MuxLimits {
            max_tcp_relay_chunk_bytes: 64 * 1024,
            ..MuxLimits::default()
        };
        let high_bdp = MuxLimits {
            max_tcp_relay_chunk_bytes: 512 * 1024,
            ..MuxLimits::default()
        };

        let compact_payload = max_stream_payload_bytes(codec_limits, compact);
        let high_bdp_payload = max_stream_payload_bytes(codec_limits, high_bdp);

        assert!(compact_payload >= packet::max_frame_fragment_payload() * 32 - 384);
        assert!(high_bdp_payload > compact_payload.saturating_mul(4));
        assert!(high_bdp_payload <= codec_limits.max_payload_bytes);
    }
}
