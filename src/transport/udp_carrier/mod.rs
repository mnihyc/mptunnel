mod crypto;
mod endpoint;
mod error;
mod packet;
mod stream;

use crate::protocol::codec::CodecLimits;

pub use endpoint::{Connection, Endpoint};
pub use error::{UdpCarrierConnectionError, UdpCarrierFrameError, UdpCarrierTransportError};
pub use stream::{RecvStream, SendStream, finish_stream, read_frame, write_frame};

const STREAM_FRAME_PACKET_BATCH: usize = 32;
const STREAM_FRAME_CODEC_OVERHEAD_ALLOWANCE: usize = 256;

pub fn max_stream_payload_bytes(limits: CodecLimits) -> usize {
    packet::max_frame_fragment_payload()
        .saturating_mul(STREAM_FRAME_PACKET_BATCH)
        .saturating_sub(128)
        .saturating_sub(STREAM_FRAME_CODEC_OVERHEAD_ALLOWANCE)
        .clamp(1, limits.max_payload_bytes.max(1))
}
