use super::{
    AuthNonce, AuthTag, CloseReason, DatagramFlowId, DatagramId, Frame, OffsetRange, PathId,
    PathMetricDirection, PathMetrics, PathUsage, PeerPathState, PeerPathStatus, PeerStatusCode,
    ResetReason, SessionId, StreamDemandHint, StreamId, TargetAddr, UnderlayProtocol,
};
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const MAGIC: &[u8; 4] = b"MPTF";
const VERSION: u8 = 2;
pub const FRAME_HEADER_LEN: usize = 10;
const PATH_METRICS_ENCODED_LEN: usize = 104;
const PEER_PATH_STATUS_ENCODED_LEN: usize = 2 + PATH_METRICS_ENCODED_LEN;
const PEER_STATUS_RESPONSE_FIXED_PAYLOAD_LEN: usize = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
    pub max_host_bytes: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_host_bytes: 255,
        }
    }
}

pub(crate) fn peer_status_response_path_limit(limits: CodecLimits) -> usize {
    let frame_limit = limits
        .max_frame_bytes
        .saturating_sub(FRAME_HEADER_LEN + PEER_STATUS_RESPONSE_FIXED_PAYLOAD_LEN)
        / PEER_PATH_STATUS_ENCODED_LEN;
    frame_limit.min(usize::from(u16::MAX))
}

pub fn encode_frame(frame: &Frame, limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + encoded_payload_capacity_hint(frame));
    encode_frame_into(frame, limits, &mut out)?;
    Ok(out)
}

pub fn encode_frames(frames: &[Frame], limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    let capacity_hint = frames.iter().fold(0usize, |total, frame| {
        total.saturating_add(FRAME_HEADER_LEN + encoded_payload_capacity_hint(frame))
    });
    let mut out = Vec::with_capacity(capacity_hint);
    encode_frames_into(frames, limits, &mut out)?;
    Ok(out)
}

pub fn encode_frames_into(
    frames: &[Frame],
    limits: CodecLimits,
    out: &mut Vec<u8>,
) -> Result<(), CodecError> {
    out.clear();
    let capacity_hint = frames.iter().fold(0usize, |total, frame| {
        total.saturating_add(encoded_frame_capacity_hint(frame))
    });
    out.reserve(capacity_hint);
    for frame in frames {
        encode_frame_into(frame, limits, out)?;
        if out.len() > limits.max_frame_bytes {
            return Err(CodecError::FrameTooLarge {
                actual: out.len(),
                limit: limits.max_frame_bytes,
            });
        }
    }
    Ok(())
}

pub fn encode_frame_into(
    frame: &Frame,
    limits: CodecLimits,
    out: &mut Vec<u8>,
) -> Result<(), CodecError> {
    out.reserve(encoded_frame_capacity_hint(frame));
    let frame_start = out.len();
    out.resize(frame_start + FRAME_HEADER_LEN, 0);
    let kind = encode_payload(frame, limits, &mut *out)?;
    let payload_len = out
        .len()
        .checked_sub(frame_start)
        .and_then(|len| len.checked_sub(FRAME_HEADER_LEN))
        .ok_or(CodecError::LengthOverflow)?;
    if payload_len > u32::MAX as usize {
        return Err(CodecError::LengthOverflow);
    }
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(CodecError::LengthOverflow)?;
    if frame_len > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: frame_len,
            limit: limits.max_frame_bytes,
        });
    }

    out[frame_start..frame_start + 4].copy_from_slice(MAGIC);
    out[frame_start + 4] = VERSION;
    out[frame_start + 5] = kind as u8;
    out[frame_start + 6..frame_start + 10].copy_from_slice(&(payload_len as u32).to_be_bytes());
    Ok(())
}

pub(crate) fn encoded_frame_capacity_hint(frame: &Frame) -> usize {
    FRAME_HEADER_LEN.saturating_add(encoded_payload_capacity_hint(frame))
}

fn encoded_payload_capacity_hint(frame: &Frame) -> usize {
    match frame {
        Frame::StreamData { payload, .. } | Frame::DatagramData { payload, .. } => {
            payload.len().saturating_add(32)
        }
        Frame::PathProofData { payload, .. } | Frame::PathCapacityData { payload, .. } => {
            payload.len().saturating_add(16)
        }
        Frame::StreamAck { ranges, .. } => 16usize.saturating_add(ranges.len().saturating_mul(16)),
        Frame::PeerStatusResponse { paths, .. } => PEER_STATUS_RESPONSE_FIXED_PAYLOAD_LEN
            .saturating_add(paths.len().saturating_mul(PEER_PATH_STATUS_ENCODED_LEN)),
        Frame::OpenStream { .. } => 128,
        _ => 64,
    }
}

pub fn decode_frame_bytes(bytes: Bytes, limits: CodecLimits) -> Result<Frame, CodecError> {
    if bytes.len() < FRAME_HEADER_LEN {
        return Err(CodecError::UnexpectedEof);
    }
    if bytes.len() > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: bytes.len(),
            limit: limits.max_frame_bytes,
        });
    }
    let payload_len = decode_payload_len_from_header(&bytes[..FRAME_HEADER_LEN], limits)?;
    let expected_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(CodecError::LengthOverflow)?;
    match bytes.len().cmp(&expected_len) {
        std::cmp::Ordering::Less => return Err(CodecError::UnexpectedEof),
        std::cmp::Ordering::Greater => return Err(CodecError::TrailingBytes),
        std::cmp::Ordering::Equal => {}
    }

    let kind = FrameKind::from_u8(bytes[5])?;
    let mut reader = Reader::with_source(&bytes, FRAME_HEADER_LEN, bytes.len());
    let frame = decode_payload(kind, limits, &mut reader)?;
    reader.finish()?;
    Ok(frame)
}

pub fn decode_frames_bytes(bytes: Bytes, limits: CodecLimits) -> Result<Vec<Frame>, CodecError> {
    if bytes.is_empty() {
        return Err(CodecError::UnexpectedEof);
    }
    let mut frames = Vec::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let header_end = pos
            .checked_add(FRAME_HEADER_LEN)
            .ok_or(CodecError::LengthOverflow)?;
        if header_end > bytes.len() {
            return Err(CodecError::UnexpectedEof);
        }
        let payload_len = decode_payload_len_from_header(&bytes[pos..header_end], limits)?;
        let frame_end = header_end
            .checked_add(payload_len)
            .ok_or(CodecError::LengthOverflow)?;
        if frame_end > bytes.len() {
            return Err(CodecError::UnexpectedEof);
        }
        let kind = FrameKind::from_u8(bytes[pos + 5])?;
        let mut reader = Reader::with_source(&bytes, header_end, frame_end);
        let frame = decode_payload(kind, limits, &mut reader)?;
        reader.finish()?;
        frames.push(frame);
        pos = frame_end;
    }
    Ok(frames)
}

pub fn decode_payload_len_from_header(
    header: &[u8],
    limits: CodecLimits,
) -> Result<usize, CodecError> {
    if header.len() != FRAME_HEADER_LEN {
        return Err(CodecError::UnexpectedEof);
    }
    if &header[0..4] != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    if header[4] != VERSION {
        return Err(CodecError::UnsupportedVersion(header[4]));
    }
    let _ = FrameKind::from_u8(header[5])?;
    let payload_len = u32::from_be_bytes(header[6..10].try_into().expect("slice length")) as usize;
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(CodecError::LengthOverflow)?;
    if frame_len > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: frame_len,
            limit: limits.max_frame_bytes,
        });
    }
    Ok(payload_len)
}

fn encode_payload(
    frame: &Frame,
    limits: CodecLimits,
    out: &mut Vec<u8>,
) -> Result<FrameKind, CodecError> {
    match frame {
        Frame::SessionHello { session_id } => {
            put_u64(out, session_id.0);
            Ok(FrameKind::SessionHello)
        }
        Frame::SessionAuth {
            session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } => {
            put_u64(out, session_id.0);
            encode_nonce(out, *nonce);
            put_u64(out, *issued_at_unix_secs);
            encode_auth_tag(out, *auth_tag);
            Ok(FrameKind::SessionAuth)
        }
        Frame::SessionReady => Ok(FrameKind::SessionReady),
        Frame::SessionClose { reason } => {
            put_u8(out, close_reason_to_u8(*reason));
            Ok(FrameKind::SessionClose)
        }
        Frame::PathJoin {
            session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } => {
            put_u64(out, session_id.0);
            put_u16(out, path_id.0);
            put_u8(out, underlay_to_u8(*underlay));
            encode_nonce(out, *nonce);
            put_u64(out, *issued_at_unix_secs);
            encode_auth_tag(out, *auth_tag);
            Ok(FrameKind::PathJoin)
        }
        Frame::PathStatus {
            path_id,
            sequence,
            usage,
        } => {
            put_u16(out, path_id.0);
            put_u64(out, *sequence);
            put_u8(out, path_usage_to_u8(*usage));
            Ok(FrameKind::PathStatus)
        }
        Frame::PathDrain { path_id } => {
            put_u16(out, path_id.0);
            Ok(FrameKind::PathDrain)
        }
        Frame::PathClose { path_id, reason } => {
            put_u16(out, path_id.0);
            put_u8(out, close_reason_to_u8(*reason));
            Ok(FrameKind::PathClose)
        }
        Frame::PathProofData {
            path_id,
            proof_id,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u16(out, path_id.0);
            put_u64(out, *proof_id);
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::PathProofData)
        }
        Frame::PathProofAck {
            path_id,
            proof_id,
            payload_bytes,
        } => {
            put_u16(out, path_id.0);
            put_u64(out, *proof_id);
            put_u32(out, *payload_bytes);
            Ok(FrameKind::PathProofAck)
        }
        Frame::PathCapacityData {
            path_id,
            measurement_id,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u16(out, path_id.0);
            put_u64(out, *measurement_id);
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::PathCapacityData)
        }
        Frame::PathCapacityFinish {
            path_id,
            measurement_id,
            payload_bytes,
        } => {
            put_u16(out, path_id.0);
            put_u64(out, *measurement_id);
            put_u64(out, *payload_bytes);
            Ok(FrameKind::PathCapacityFinish)
        }
        Frame::PathCapacityReceipt {
            path_id,
            measurement_id,
            received_payload_bytes,
        } => {
            put_u16(out, path_id.0);
            put_u64(out, *measurement_id);
            put_u64(out, *received_payload_bytes);
            Ok(FrameKind::PathCapacityReceipt)
        }
        Frame::OpenStream {
            stream_id,
            target,
            demand,
        } => {
            put_u64(out, stream_id.0);
            encode_target(out, target, limits)?;
            encode_stream_demand_hint(out, *demand);
            Ok(FrameKind::OpenStream)
        }
        Frame::StreamData {
            stream_id,
            offset,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u64(out, stream_id.0);
            put_u64(out, *offset);
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::StreamData)
        }
        Frame::StreamAck {
            stream_id,
            complete,
            ranges,
        } => {
            if ranges.len() > limits.max_ack_ranges {
                return Err(CodecError::TooManyAckRanges {
                    actual: ranges.len(),
                    limit: limits.max_ack_ranges,
                });
            }
            if ranges.len() > u16::MAX as usize {
                return Err(CodecError::LengthOverflow);
            }
            put_u64(out, stream_id.0);
            put_u8(out, u8::from(*complete));
            put_u16(out, ranges.len() as u16);
            for range in ranges {
                if range.is_empty() {
                    return Err(CodecError::InvalidRange);
                }
                put_u64(out, range.start);
                put_u64(out, range.end);
            }
            Ok(FrameKind::StreamAck)
        }
        Frame::StreamMaxData {
            stream_id,
            max_offset,
        } => {
            put_u64(out, stream_id.0);
            put_u64(out, *max_offset);
            Ok(FrameKind::StreamMaxData)
        }
        Frame::StreamFin {
            stream_id,
            final_offset,
        } => {
            put_u64(out, stream_id.0);
            put_u64(out, *final_offset);
            Ok(FrameKind::StreamFin)
        }
        Frame::StreamDetach { stream_id } => {
            put_u64(out, stream_id.0);
            Ok(FrameKind::StreamDetach)
        }
        Frame::StreamReset { stream_id, reason } => {
            put_u64(out, stream_id.0);
            put_u8(out, reset_reason_to_u8(*reason));
            Ok(FrameKind::StreamReset)
        }
        Frame::OpenDatagramFlow { flow_id, target } => {
            put_u64(out, flow_id.0);
            encode_target(out, target, limits)?;
            Ok(FrameKind::OpenDatagramFlow)
        }
        Frame::DatagramData {
            flow_id,
            datagram_id,
            ttl_ms,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u64(out, flow_id.0);
            put_u64(out, datagram_id.0);
            put_u32(out, *ttl_ms);
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::DatagramData)
        }
        Frame::DatagramClose { flow_id } => {
            put_u64(out, flow_id.0);
            Ok(FrameKind::DatagramClose)
        }
        Frame::DatagramFeedback { flow_id, received } => {
            put_u64(out, flow_id.0);
            encode_offset_ranges(out, received, limits)?;
            Ok(FrameKind::DatagramFeedback)
        }
        Frame::PathMetrics { metrics } => {
            encode_path_metrics(out, *metrics);
            Ok(FrameKind::PathMetrics)
        }
        Frame::PeerStatusRequest { request_id } => {
            put_u64(out, *request_id);
            Ok(FrameKind::PeerStatusRequest)
        }
        Frame::PeerStatusResponse {
            request_id,
            code,
            paths,
        } => {
            let path_count = u16::try_from(paths.len()).map_err(|_| CodecError::LengthOverflow)?;
            put_u64(out, *request_id);
            put_u8(out, peer_status_code_to_u8(*code));
            put_u16(out, path_count);
            for path in paths {
                put_u8(out, peer_path_state_to_u8(path.state));
                put_u8(out, path_usage_to_u8(path.usage));
                encode_path_metrics(out, path.metrics);
            }
            Ok(FrameKind::PeerStatusResponse)
        }
        Frame::Ping { nonce } => {
            put_u64(out, *nonce);
            Ok(FrameKind::Ping)
        }
        Frame::Pong { nonce } => {
            put_u64(out, *nonce);
            Ok(FrameKind::Pong)
        }
    }
}

fn decode_payload(
    kind: FrameKind,
    limits: CodecLimits,
    reader: &mut Reader<'_>,
) -> Result<Frame, CodecError> {
    match kind {
        FrameKind::SessionHello => Ok(Frame::SessionHello {
            session_id: SessionId(reader.get_u64()?),
        }),
        FrameKind::SessionAuth => Ok(Frame::SessionAuth {
            session_id: SessionId(reader.get_u64()?),
            nonce: decode_nonce(reader)?,
            issued_at_unix_secs: reader.get_u64()?,
            auth_tag: decode_auth_tag(reader)?,
        }),
        FrameKind::SessionReady => Ok(Frame::SessionReady),
        FrameKind::SessionClose => Ok(Frame::SessionClose {
            reason: close_reason_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::PathJoin => Ok(Frame::PathJoin {
            session_id: SessionId(reader.get_u64()?),
            path_id: PathId(reader.get_u16()?),
            underlay: underlay_from_u8(reader.get_u8()?)?,
            nonce: decode_nonce(reader)?,
            issued_at_unix_secs: reader.get_u64()?,
            auth_tag: decode_auth_tag(reader)?,
        }),
        FrameKind::PathStatus => Ok(Frame::PathStatus {
            path_id: PathId(reader.get_u16()?),
            sequence: reader.get_u64()?,
            usage: path_usage_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::PathDrain => Ok(Frame::PathDrain {
            path_id: PathId(reader.get_u16()?),
        }),
        FrameKind::PathClose => Ok(Frame::PathClose {
            path_id: PathId(reader.get_u16()?),
            reason: close_reason_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::PathProofData => {
            let path_id = PathId(reader.get_u16()?);
            let proof_id = reader.get_u64()?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::PathProofData {
                path_id,
                proof_id,
                payload,
            })
        }
        FrameKind::PathProofAck => Ok(Frame::PathProofAck {
            path_id: PathId(reader.get_u16()?),
            proof_id: reader.get_u64()?,
            payload_bytes: reader.get_u32()?,
        }),
        FrameKind::PathCapacityData => {
            let path_id = PathId(reader.get_u16()?);
            let measurement_id = reader.get_u64()?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::PathCapacityData {
                path_id,
                measurement_id,
                payload,
            })
        }
        FrameKind::PathCapacityFinish => Ok(Frame::PathCapacityFinish {
            path_id: PathId(reader.get_u16()?),
            measurement_id: reader.get_u64()?,
            payload_bytes: reader.get_u64()?,
        }),
        FrameKind::PathCapacityReceipt => Ok(Frame::PathCapacityReceipt {
            path_id: PathId(reader.get_u16()?),
            measurement_id: reader.get_u64()?,
            received_payload_bytes: reader.get_u64()?,
        }),
        FrameKind::OpenStream => Ok(Frame::OpenStream {
            stream_id: StreamId(reader.get_u64()?),
            target: decode_target(reader, limits)?,
            demand: decode_stream_demand_hint(reader)?,
        }),
        FrameKind::StreamData => {
            let stream_id = StreamId(reader.get_u64()?);
            let offset = reader.get_u64()?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::StreamData {
                stream_id,
                offset,
                payload,
            })
        }
        FrameKind::StreamAck => {
            let stream_id = StreamId(reader.get_u64()?);
            let complete = match reader.get_u8()? {
                0 => false,
                1 => true,
                _ => return Err(CodecError::InvalidEnum),
            };
            let range_count = reader.get_u16()? as usize;
            if range_count > limits.max_ack_ranges {
                return Err(CodecError::TooManyAckRanges {
                    actual: range_count,
                    limit: limits.max_ack_ranges,
                });
            }
            let mut ranges = Vec::with_capacity(range_count);
            for _ in 0..range_count {
                let start = reader.get_u64()?;
                let end = reader.get_u64()?;
                let Some(range) = OffsetRange::new(start, end) else {
                    return Err(CodecError::InvalidRange);
                };
                ranges.push(range);
            }
            Ok(Frame::StreamAck {
                stream_id,
                complete,
                ranges,
            })
        }
        FrameKind::StreamMaxData => Ok(Frame::StreamMaxData {
            stream_id: StreamId(reader.get_u64()?),
            max_offset: reader.get_u64()?,
        }),
        FrameKind::StreamFin => Ok(Frame::StreamFin {
            stream_id: StreamId(reader.get_u64()?),
            final_offset: reader.get_u64()?,
        }),
        FrameKind::StreamDetach => Ok(Frame::StreamDetach {
            stream_id: StreamId(reader.get_u64()?),
        }),
        FrameKind::StreamReset => Ok(Frame::StreamReset {
            stream_id: StreamId(reader.get_u64()?),
            reason: reset_reason_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::OpenDatagramFlow => Ok(Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(reader.get_u64()?),
            target: decode_target(reader, limits)?,
        }),
        FrameKind::DatagramData => {
            let flow_id = DatagramFlowId(reader.get_u64()?);
            let datagram_id = DatagramId(reader.get_u64()?);
            let ttl_ms = reader.get_u32()?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::DatagramData {
                flow_id,
                datagram_id,
                ttl_ms,
                payload,
            })
        }
        FrameKind::DatagramClose => Ok(Frame::DatagramClose {
            flow_id: DatagramFlowId(reader.get_u64()?),
        }),
        FrameKind::DatagramFeedback => Ok(Frame::DatagramFeedback {
            flow_id: DatagramFlowId(reader.get_u64()?),
            received: decode_offset_ranges(reader, limits)?,
        }),
        FrameKind::PathMetrics => Ok(Frame::PathMetrics {
            metrics: decode_path_metrics(reader)?,
        }),
        FrameKind::PeerStatusRequest => Ok(Frame::PeerStatusRequest {
            request_id: reader.get_u64()?,
        }),
        FrameKind::PeerStatusResponse => {
            let request_id = reader.get_u64()?;
            let code = peer_status_code_from_u8(reader.get_u8()?)?;
            let path_count = reader.get_u16()? as usize;
            let required_path_bytes = path_count
                .checked_mul(PEER_PATH_STATUS_ENCODED_LEN)
                .ok_or(CodecError::LengthOverflow)?;
            if required_path_bytes > reader.remaining() {
                return Err(CodecError::UnexpectedEof);
            }
            let mut paths = Vec::with_capacity(path_count);
            for _ in 0..path_count {
                paths.push(PeerPathStatus {
                    state: peer_path_state_from_u8(reader.get_u8()?)?,
                    usage: path_usage_from_u8(reader.get_u8()?)?,
                    metrics: decode_path_metrics(reader)?,
                });
            }
            Ok(Frame::PeerStatusResponse {
                request_id,
                code,
                paths,
            })
        }
        FrameKind::Ping => Ok(Frame::Ping {
            nonce: reader.get_u64()?,
        }),
        FrameKind::Pong => Ok(Frame::Pong {
            nonce: reader.get_u64()?,
        }),
    }
}

fn encode_target(
    out: &mut Vec<u8>,
    target: &TargetAddr,
    limits: CodecLimits,
) -> Result<(), CodecError> {
    match target {
        TargetAddr::Domain { host, port } => {
            encode_host(out, 1, host, *port, limits)?;
        }
        TargetAddr::Ip(addr) => {
            encode_socket_addr(out, addr)?;
        }
    }
    Ok(())
}

fn decode_target(reader: &mut Reader<'_>, limits: CodecLimits) -> Result<TargetAddr, CodecError> {
    match reader.get_u8()? {
        1 => {
            let host = reader.get_string_u16(limits.max_host_bytes)?;
            let port = reader.get_u16()?;
            if port == 0 {
                return Err(CodecError::InvalidPort);
            }
            Ok(TargetAddr::Domain { host, port })
        }
        2 => {
            let octets = reader.get_array::<4>()?;
            let port = reader.get_u16()?;
            if port == 0 {
                return Err(CodecError::InvalidPort);
            }
            Ok(TargetAddr::Ip(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(octets)),
                port,
            )))
        }
        3 => {
            let octets = reader.get_array::<16>()?;
            let port = reader.get_u16()?;
            if port == 0 {
                return Err(CodecError::InvalidPort);
            }
            Ok(TargetAddr::Ip(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(octets)),
                port,
            )))
        }
        _ => Err(CodecError::InvalidEnum),
    }
}

fn encode_socket_addr(out: &mut Vec<u8>, addr: &SocketAddr) -> Result<(), CodecError> {
    match addr {
        SocketAddr::V4(addr) => {
            put_u8(out, 2);
            out.extend_from_slice(&addr.ip().octets());
            put_u16(out, addr.port());
        }
        SocketAddr::V6(addr) => {
            put_u8(out, 3);
            out.extend_from_slice(&addr.ip().octets());
            put_u16(out, addr.port());
        }
    }
    Ok(())
}

fn encode_host(
    out: &mut Vec<u8>,
    kind: u8,
    host: &str,
    port: u16,
    limits: CodecLimits,
) -> Result<(), CodecError> {
    if host.len() > limits.max_host_bytes {
        return Err(CodecError::HostTooLong {
            actual: host.len(),
            limit: limits.max_host_bytes,
        });
    }
    if host.len() > u16::MAX as usize {
        return Err(CodecError::LengthOverflow);
    }
    if port == 0 {
        return Err(CodecError::InvalidPort);
    }
    put_u8(out, kind);
    put_u16(out, host.len() as u16);
    out.extend_from_slice(host.as_bytes());
    put_u16(out, port);
    Ok(())
}

fn encode_nonce(out: &mut Vec<u8>, nonce: AuthNonce) {
    out.extend_from_slice(&nonce.0);
}

fn decode_nonce(reader: &mut Reader<'_>) -> Result<AuthNonce, CodecError> {
    Ok(AuthNonce(reader.get_array::<16>()?))
}

fn encode_auth_tag(out: &mut Vec<u8>, tag: AuthTag) {
    out.extend_from_slice(&tag.0);
}

fn decode_auth_tag(reader: &mut Reader<'_>) -> Result<AuthTag, CodecError> {
    Ok(AuthTag(reader.get_array::<32>()?))
}

fn encode_path_metrics(out: &mut Vec<u8>, metrics: PathMetrics) {
    put_u16(out, metrics.path_id.0);
    put_u8(out, underlay_to_u8(metrics.underlay));
    put_u8(out, path_metric_direction_to_u8(metrics.direction));
    put_u64(out, metrics.metric_epoch);
    put_u32(out, metrics.metric_age_us);
    put_u32(out, metrics.srtt_us);
    put_u32(out, metrics.rttvar_us);
    put_u32(out, metrics.jitter_us);
    put_u64(out, metrics.delivery_rate_bps);
    put_u64(out, metrics.pacing_rate_bps);
    put_u32(out, metrics.loss_ppm);
    put_u32(out, metrics.ecn_ppm);
    put_u8(out, u8::from(metrics.loss_observed));
    put_u8(out, u8::from(metrics.ecn_observed));
    put_u64(out, metrics.bytes_in_flight);
    put_u64(out, metrics.queue_bytes);
    put_u64(out, metrics.inflight_limit_bytes);
    put_u64(out, metrics.inflight_hi_bytes);
    put_u32(out, metrics.confidence_ppm);
    put_u8(out, u8::from(metrics.app_limited));
    put_u8(out, u8::from(metrics.has_ack_derived_data_sample));
    put_u32(out, metrics.data_sample_count);
    put_u64(out, metrics.data_sample_bytes);
}

fn decode_path_metrics(reader: &mut Reader<'_>) -> Result<PathMetrics, CodecError> {
    Ok(PathMetrics {
        path_id: PathId(reader.get_u16()?),
        underlay: underlay_from_u8(reader.get_u8()?)?,
        direction: path_metric_direction_from_u8(reader.get_u8()?)?,
        metric_epoch: reader.get_u64()?,
        metric_age_us: reader.get_u32()?,
        srtt_us: reader.get_u32()?,
        rttvar_us: reader.get_u32()?,
        jitter_us: reader.get_u32()?,
        delivery_rate_bps: reader.get_u64()?,
        pacing_rate_bps: reader.get_u64()?,
        loss_ppm: reader.get_u32()?,
        ecn_ppm: reader.get_u32()?,
        loss_observed: decode_bool(reader.get_u8()?)?,
        ecn_observed: decode_bool(reader.get_u8()?)?,
        bytes_in_flight: reader.get_u64()?,
        queue_bytes: reader.get_u64()?,
        inflight_limit_bytes: reader.get_u64()?,
        inflight_hi_bytes: reader.get_u64()?,
        confidence_ppm: reader.get_u32()?,
        app_limited: decode_bool(reader.get_u8()?)?,
        has_ack_derived_data_sample: decode_bool(reader.get_u8()?)?,
        data_sample_count: reader.get_u32()?,
        data_sample_bytes: reader.get_u64()?,
    })
}

fn encode_offset_ranges(
    out: &mut Vec<u8>,
    ranges: &[OffsetRange],
    limits: CodecLimits,
) -> Result<(), CodecError> {
    if ranges.len() > limits.max_ack_ranges {
        return Err(CodecError::TooManyAckRanges {
            actual: ranges.len(),
            limit: limits.max_ack_ranges,
        });
    }
    if ranges.len() > u16::MAX as usize {
        return Err(CodecError::LengthOverflow);
    }
    put_u16(out, ranges.len() as u16);
    for range in ranges {
        if range.is_empty() {
            return Err(CodecError::InvalidRange);
        }
        put_u64(out, range.start);
        put_u64(out, range.end);
    }
    Ok(())
}

fn encode_stream_demand_hint(out: &mut Vec<u8>, demand: StreamDemandHint) {
    put_u8(
        out,
        match demand {
            StreamDemandHint::Latency => 1,
            StreamDemandHint::Throughput => 2,
            StreamDemandHint::Realtime => 3,
        },
    );
}

fn decode_stream_demand_hint(reader: &mut Reader<'_>) -> Result<StreamDemandHint, CodecError> {
    match reader.get_u8()? {
        1 => Ok(StreamDemandHint::Latency),
        2 => Ok(StreamDemandHint::Throughput),
        3 => Ok(StreamDemandHint::Realtime),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn decode_offset_ranges(
    reader: &mut Reader<'_>,
    limits: CodecLimits,
) -> Result<Vec<OffsetRange>, CodecError> {
    let range_count = reader.get_u16()? as usize;
    if range_count > limits.max_ack_ranges {
        return Err(CodecError::TooManyAckRanges {
            actual: range_count,
            limit: limits.max_ack_ranges,
        });
    }
    let mut ranges = Vec::with_capacity(range_count);
    for _ in 0..range_count {
        let start = reader.get_u64()?;
        let end = reader.get_u64()?;
        let Some(range) = OffsetRange::new(start, end) else {
            return Err(CodecError::InvalidRange);
        };
        ranges.push(range);
    }
    Ok(ranges)
}

fn encode_payload_bytes_len(len: usize, limits: CodecLimits) -> Result<(), CodecError> {
    if len > limits.max_payload_bytes {
        return Err(CodecError::PayloadTooLarge {
            actual: len,
            limit: limits.max_payload_bytes,
        });
    }
    if len > u32::MAX as usize {
        return Err(CodecError::LengthOverflow);
    }
    Ok(())
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
    source: &'a Bytes,
    absolute_start: usize,
}

impl<'a> Reader<'a> {
    fn with_source(source: &'a Bytes, start: usize, end: usize) -> Self {
        Self {
            bytes: &source[start..end],
            pos: 0,
            source,
            absolute_start: start,
        }
    }

    fn finish(&self) -> Result<(), CodecError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn get_u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.get_exact(1)?[0])
    }

    fn get_u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.get_exact(2)?.try_into().expect("slice length"),
        ))
    }

    fn get_u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.get_exact(4)?.try_into().expect("slice length"),
        ))
    }

    fn get_u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.get_exact(8)?.try_into().expect("slice length"),
        ))
    }

    fn get_array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        Ok(self.get_exact(N)?.try_into().expect("slice length"))
    }

    fn get_string_u16(&mut self, limit: usize) -> Result<String, CodecError> {
        let len = self.get_u16()? as usize;
        if len > limit {
            return Err(CodecError::HostTooLong { actual: len, limit });
        }
        let bytes = self.get_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    fn get_bytes_u32(&mut self, limit: usize) -> Result<Bytes, CodecError> {
        let len = self.get_u32()? as usize;
        if len > limit {
            return Err(CodecError::PayloadTooLarge { actual: len, limit });
        }
        let start = self.pos;
        let _ = self.get_exact(len)?;
        let absolute_start = self
            .absolute_start
            .checked_add(start)
            .ok_or(CodecError::LengthOverflow)?;
        let absolute_end = absolute_start
            .checked_add(len)
            .ok_or(CodecError::LengthOverflow)?;
        Ok(self.source.slice(absolute_start..absolute_end))
    }

    fn get_exact(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(CodecError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(CodecError::UnexpectedEof);
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FrameKind {
    SessionHello = 1,
    SessionReady = 2,
    SessionClose = 3,
    PathJoin = 4,
    // 5 and 6 are reserved; an abandoned challenge exchange never had a sender.
    OpenStream = 7,
    StreamData = 8,
    StreamAck = 9,
    StreamMaxData = 10,
    StreamReset = 11,
    OpenDatagramFlow = 12,
    DatagramData = 13,
    DatagramClose = 14,
    // 15 is reserved; connection flow control is owned by the carrier.
    Ping = 16,
    Pong = 17,
    SessionAuth = 18,
    // 19 is reserved for the unused PATH_JOIN_OK draft frame.
    PathStatus = 20,
    PathDrain = 21,
    PathClose = 22,
    DatagramFeedback = 23,
    PathMetrics = 24,
    // 25 is reserved for a removed hint; 26 has never been allocated.
    StreamFin = 27,
    // 28 and 29 are reserved for a removed product-PMTU experiment.
    StreamDetach = 30,
    PathProofData = 31,
    PathProofAck = 32,
    PathCapacityData = 33,
    PathCapacityFinish = 34,
    PathCapacityReceipt = 35,
    PeerStatusRequest = 36,
    PeerStatusResponse = 37,
}

impl FrameKind {
    fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::SessionHello),
            2 => Ok(Self::SessionReady),
            3 => Ok(Self::SessionClose),
            4 => Ok(Self::PathJoin),
            7 => Ok(Self::OpenStream),
            8 => Ok(Self::StreamData),
            9 => Ok(Self::StreamAck),
            10 => Ok(Self::StreamMaxData),
            11 => Ok(Self::StreamReset),
            12 => Ok(Self::OpenDatagramFlow),
            13 => Ok(Self::DatagramData),
            14 => Ok(Self::DatagramClose),
            16 => Ok(Self::Ping),
            17 => Ok(Self::Pong),
            18 => Ok(Self::SessionAuth),
            20 => Ok(Self::PathStatus),
            21 => Ok(Self::PathDrain),
            22 => Ok(Self::PathClose),
            23 => Ok(Self::DatagramFeedback),
            24 => Ok(Self::PathMetrics),
            27 => Ok(Self::StreamFin),
            30 => Ok(Self::StreamDetach),
            31 => Ok(Self::PathProofData),
            32 => Ok(Self::PathProofAck),
            33 => Ok(Self::PathCapacityData),
            34 => Ok(Self::PathCapacityFinish),
            35 => Ok(Self::PathCapacityReceipt),
            36 => Ok(Self::PeerStatusRequest),
            37 => Ok(Self::PeerStatusResponse),
            _ => Err(CodecError::UnknownKind(value)),
        }
    }
}

fn underlay_to_u8(value: UnderlayProtocol) -> u8 {
    match value {
        UnderlayProtocol::Tcp => 1,
        UnderlayProtocol::Udp => 2,
    }
}

fn underlay_from_u8(value: u8) -> Result<UnderlayProtocol, CodecError> {
    match value {
        1 => Ok(UnderlayProtocol::Tcp),
        2 => Ok(UnderlayProtocol::Udp),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn path_metric_direction_to_u8(value: PathMetricDirection) -> u8 {
    match value {
        PathMetricDirection::ClientToServer => 1,
        PathMetricDirection::ServerToClient => 2,
    }
}

fn path_metric_direction_from_u8(value: u8) -> Result<PathMetricDirection, CodecError> {
    match value {
        1 => Ok(PathMetricDirection::ClientToServer),
        2 => Ok(PathMetricDirection::ServerToClient),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn decode_bool(value: u8) -> Result<bool, CodecError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn path_usage_to_u8(value: PathUsage) -> u8 {
    match value {
        PathUsage::Available => 0,
        PathUsage::Backup => 1,
    }
}

fn path_usage_from_u8(value: u8) -> Result<PathUsage, CodecError> {
    match value {
        0 => Ok(PathUsage::Available),
        1 => Ok(PathUsage::Backup),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn peer_status_code_to_u8(value: PeerStatusCode) -> u8 {
    match value {
        PeerStatusCode::Ok => 0,
        PeerStatusCode::Disabled => 1,
        PeerStatusCode::Unavailable => 2,
    }
}

fn peer_status_code_from_u8(value: u8) -> Result<PeerStatusCode, CodecError> {
    match value {
        0 => Ok(PeerStatusCode::Ok),
        1 => Ok(PeerStatusCode::Disabled),
        2 => Ok(PeerStatusCode::Unavailable),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn peer_path_state_to_u8(value: PeerPathState) -> u8 {
    match value {
        PeerPathState::Active => 0,
        PeerPathState::Suspect => 1,
        PeerPathState::Draining => 2,
        PeerPathState::Failed => 3,
    }
}

fn peer_path_state_from_u8(value: u8) -> Result<PeerPathState, CodecError> {
    match value {
        0 => Ok(PeerPathState::Active),
        1 => Ok(PeerPathState::Suspect),
        2 => Ok(PeerPathState::Draining),
        3 => Ok(PeerPathState::Failed),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn close_reason_to_u8(value: CloseReason) -> u8 {
    match value {
        CloseReason::Normal => 0,
        CloseReason::ProtocolError => 1,
        CloseReason::AuthenticationFailed => 2,
        CloseReason::PolicyRejected => 3,
    }
}

fn close_reason_from_u8(value: u8) -> Result<CloseReason, CodecError> {
    match value {
        0 => Ok(CloseReason::Normal),
        1 => Ok(CloseReason::ProtocolError),
        2 => Ok(CloseReason::AuthenticationFailed),
        3 => Ok(CloseReason::PolicyRejected),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn reset_reason_to_u8(value: ResetReason) -> u8 {
    match value {
        ResetReason::Refused => 1,
        ResetReason::TimedOut => 2,
        ResetReason::RemoteClosed => 3,
        ResetReason::PolicyRejected => 4,
    }
}

fn reset_reason_from_u8(value: u8) -> Result<ResetReason, CodecError> {
    match value {
        1 => Ok(ResetReason::Refused),
        2 => Ok(ResetReason::TimedOut),
        3 => Ok(ResetReason::RemoteClosed),
        4 => Ok(ResetReason::PolicyRejected),
        _ => Err(CodecError::InvalidEnum),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    InvalidMagic,
    UnsupportedVersion(u8),
    UnknownKind(u8),
    UnexpectedEof,
    TrailingBytes,
    FrameTooLarge { actual: usize, limit: usize },
    PayloadTooLarge { actual: usize, limit: usize },
    HostTooLong { actual: usize, limit: usize },
    TooManyAckRanges { actual: usize, limit: usize },
    InvalidUtf8,
    InvalidEnum,
    InvalidRange,
    InvalidPort,
    LengthOverflow,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid frame magic"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported frame version {version}"),
            Self::UnknownKind(kind) => write!(f, "unknown frame kind {kind}"),
            Self::UnexpectedEof => write!(f, "unexpected end of frame"),
            Self::TrailingBytes => write!(f, "frame has trailing bytes"),
            Self::FrameTooLarge { actual, limit } => {
                write!(f, "frame is {actual} bytes, limit is {limit}")
            }
            Self::PayloadTooLarge { actual, limit } => {
                write!(f, "payload is {actual} bytes, limit is {limit}")
            }
            Self::HostTooLong { actual, limit } => {
                write!(f, "host is {actual} bytes, limit is {limit}")
            }
            Self::TooManyAckRanges { actual, limit } => {
                write!(f, "ACK has {actual} ranges, limit is {limit}")
            }
            Self::InvalidUtf8 => write!(f, "string field is not valid UTF-8"),
            Self::InvalidEnum => write!(f, "invalid enum value"),
            Self::InvalidRange => write!(f, "invalid offset range"),
            Self::InvalidPort => write!(f, "port must be in 1..=65535"),
            Self::LengthOverflow => write!(f, "frame length overflow"),
        }
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
#[path = "codec_test.rs"]
mod tests;
