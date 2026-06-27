use super::{
    AuthNonce, AuthTag, CloseReason, DatagramFlowId, DatagramId, Frame, IngressKind, OffsetRange,
    OutboundPolicy, PathCapabilities, PathId, PathMetrics, PathStatus, RateHint, ResetReason,
    SessionId, StreamFlags, StreamId, TargetAddr, TrafficClass, UnderlayProtocol,
};
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const MAGIC: &[u8; 4] = b"MPTF";
const VERSION: u8 = 1;
pub const FRAME_HEADER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_ack_ranges: usize,
    pub max_host_bytes: usize,
    pub max_udp_replay_window_packets: u64,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_payload_bytes: 1_048_512,
            max_ack_ranges: 256,
            max_host_bytes: 255,
            max_udp_replay_window_packets: 16_384,
        }
    }
}

pub fn encode_frame(frame: &Frame, limits: CodecLimits) -> Result<Vec<u8>, CodecError> {
    let mut payload = Vec::new();
    let kind = encode_payload(frame, limits, &mut payload)?;
    if payload.len() > u32::MAX as usize {
        return Err(CodecError::LengthOverflow);
    }
    let frame_len = FRAME_HEADER_LEN
        .checked_add(payload.len())
        .ok_or(CodecError::LengthOverflow)?;
    if frame_len > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: frame_len,
            limit: limits.max_frame_bytes,
        });
    }

    let mut out = Vec::with_capacity(frame_len);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.push(kind as u8);
    put_u32(&mut out, payload.len() as u32);
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn decode_frame(bytes: &[u8], limits: CodecLimits) -> Result<Frame, CodecError> {
    if bytes.len() < FRAME_HEADER_LEN {
        return Err(CodecError::UnexpectedEof);
    }
    if bytes.len() > limits.max_frame_bytes {
        return Err(CodecError::FrameTooLarge {
            actual: bytes.len(),
            limit: limits.max_frame_bytes,
        });
    }
    if &bytes[0..4] != MAGIC {
        return Err(CodecError::InvalidMagic);
    }
    if bytes[4] != VERSION {
        return Err(CodecError::UnsupportedVersion(bytes[4]));
    }

    let kind = FrameKind::from_u8(bytes[5])?;
    let payload_len = decode_payload_len_from_header(&bytes[..FRAME_HEADER_LEN], limits)?;
    let expected_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or(CodecError::LengthOverflow)?;
    match bytes.len().cmp(&expected_len) {
        std::cmp::Ordering::Less => return Err(CodecError::UnexpectedEof),
        std::cmp::Ordering::Greater => return Err(CodecError::TrailingBytes),
        std::cmp::Ordering::Equal => {}
    }

    let mut reader = Reader::new(&bytes[FRAME_HEADER_LEN..]);
    let frame = decode_payload(kind, limits, &mut reader)?;
    reader.finish()?;
    Ok(frame)
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
            auth_tag,
        } => {
            put_u64(out, session_id.0);
            encode_nonce(out, *nonce);
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
            capabilities,
            auth_tag,
        } => {
            put_u64(out, session_id.0);
            put_u16(out, path_id.0);
            put_u8(out, underlay_to_u8(*underlay));
            encode_nonce(out, *nonce);
            encode_path_capabilities(out, *capabilities);
            encode_auth_tag(out, *auth_tag);
            Ok(FrameKind::PathJoin)
        }
        Frame::PathJoinOk {
            path_id,
            nonce,
            auth_tag,
        } => {
            put_u16(out, path_id.0);
            encode_nonce(out, *nonce);
            encode_auth_tag(out, *auth_tag);
            Ok(FrameKind::PathJoinOk)
        }
        Frame::PathChallenge { path_id, nonce } => {
            put_u16(out, path_id.0);
            put_u64(out, *nonce);
            Ok(FrameKind::PathChallenge)
        }
        Frame::PathResponse { path_id, nonce } => {
            put_u16(out, path_id.0);
            put_u64(out, *nonce);
            Ok(FrameKind::PathResponse)
        }
        Frame::PathStatus {
            path_id,
            status,
            capabilities,
        } => {
            put_u16(out, path_id.0);
            put_u8(out, path_status_to_u8(*status));
            encode_path_capabilities(out, *capabilities);
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
        Frame::PathMtuProbe {
            path_id,
            probe_id,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u16(out, path_id.0);
            put_u64(out, *probe_id);
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::PathMtuProbe)
        }
        Frame::PathMtuAck {
            path_id,
            probe_id,
            payload_bytes,
        } => {
            put_u16(out, path_id.0);
            put_u64(out, *probe_id);
            put_u32(out, *payload_bytes);
            Ok(FrameKind::PathMtuAck)
        }
        Frame::OpenStream {
            stream_id,
            target,
            ingress,
            outbound,
            class,
        } => {
            put_u64(out, stream_id.0);
            encode_target(out, target, limits)?;
            put_u8(out, ingress_to_u8(*ingress));
            encode_outbound(out, outbound, limits)?;
            put_u8(out, traffic_class_to_u8(*class));
            Ok(FrameKind::OpenStream)
        }
        Frame::StreamData {
            stream_id,
            offset,
            flags,
            payload,
        } => {
            encode_payload_bytes_len(payload.len(), limits)?;
            put_u64(out, stream_id.0);
            put_u64(out, *offset);
            put_u8(out, stream_flags_to_u8(*flags));
            put_u32(out, payload.len() as u32);
            out.extend_from_slice(payload);
            Ok(FrameKind::StreamData)
        }
        Frame::StreamAck { stream_id, ranges } => {
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
        Frame::StreamFin { stream_id } => {
            put_u64(out, stream_id.0);
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
        Frame::OpenDatagramFlow {
            flow_id,
            target,
            ingress,
            outbound,
            class,
        } => {
            put_u64(out, flow_id.0);
            encode_target(out, target, limits)?;
            put_u8(out, ingress_to_u8(*ingress));
            encode_outbound(out, outbound, limits)?;
            put_u8(out, traffic_class_to_u8(*class));
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
        Frame::RxRateHint { path_id, hint } => {
            put_u16(out, path_id.0);
            encode_rate_hint(out, *hint);
            Ok(FrameKind::RxRateHint)
        }
        Frame::MaxConnectionData { max_bytes } => {
            put_u64(out, *max_bytes);
            Ok(FrameKind::MaxConnectionData)
        }
        Frame::Ping { nonce } => {
            put_u64(out, *nonce);
            Ok(FrameKind::Ping)
        }
        Frame::Pong { nonce } => {
            put_u64(out, *nonce);
            Ok(FrameKind::Pong)
        }
        Frame::KeyUpdate {
            key_phase,
            nonce,
            auth_tag,
        } => {
            put_u64(out, *key_phase);
            encode_nonce(out, *nonce);
            encode_auth_tag(out, *auth_tag);
            Ok(FrameKind::KeyUpdate)
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
            capabilities: decode_path_capabilities(reader)?,
            auth_tag: decode_auth_tag(reader)?,
        }),
        FrameKind::PathJoinOk => Ok(Frame::PathJoinOk {
            path_id: PathId(reader.get_u16()?),
            nonce: decode_nonce(reader)?,
            auth_tag: decode_auth_tag(reader)?,
        }),
        FrameKind::PathChallenge => Ok(Frame::PathChallenge {
            path_id: PathId(reader.get_u16()?),
            nonce: reader.get_u64()?,
        }),
        FrameKind::PathResponse => Ok(Frame::PathResponse {
            path_id: PathId(reader.get_u16()?),
            nonce: reader.get_u64()?,
        }),
        FrameKind::PathStatus => Ok(Frame::PathStatus {
            path_id: PathId(reader.get_u16()?),
            status: path_status_from_u8(reader.get_u8()?)?,
            capabilities: decode_path_capabilities(reader)?,
        }),
        FrameKind::PathDrain => Ok(Frame::PathDrain {
            path_id: PathId(reader.get_u16()?),
        }),
        FrameKind::PathClose => Ok(Frame::PathClose {
            path_id: PathId(reader.get_u16()?),
            reason: close_reason_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::PathMtuProbe => {
            let path_id = PathId(reader.get_u16()?);
            let probe_id = reader.get_u64()?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::PathMtuProbe {
                path_id,
                probe_id,
                payload,
            })
        }
        FrameKind::PathMtuAck => Ok(Frame::PathMtuAck {
            path_id: PathId(reader.get_u16()?),
            probe_id: reader.get_u64()?,
            payload_bytes: reader.get_u32()?,
        }),
        FrameKind::OpenStream => Ok(Frame::OpenStream {
            stream_id: StreamId(reader.get_u64()?),
            target: decode_target(reader, limits)?,
            ingress: ingress_from_u8(reader.get_u8()?)?,
            outbound: decode_outbound(reader, limits)?,
            class: traffic_class_from_u8(reader.get_u8()?)?,
        }),
        FrameKind::StreamData => {
            let stream_id = StreamId(reader.get_u64()?);
            let offset = reader.get_u64()?;
            let flags = stream_flags_from_u8(reader.get_u8()?)?;
            let payload = reader.get_bytes_u32(limits.max_payload_bytes)?;
            Ok(Frame::StreamData {
                stream_id,
                offset,
                flags,
                payload,
            })
        }
        FrameKind::StreamAck => {
            let stream_id = StreamId(reader.get_u64()?);
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
            Ok(Frame::StreamAck { stream_id, ranges })
        }
        FrameKind::StreamMaxData => Ok(Frame::StreamMaxData {
            stream_id: StreamId(reader.get_u64()?),
            max_offset: reader.get_u64()?,
        }),
        FrameKind::StreamFin => Ok(Frame::StreamFin {
            stream_id: StreamId(reader.get_u64()?),
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
            ingress: ingress_from_u8(reader.get_u8()?)?,
            outbound: decode_outbound(reader, limits)?,
            class: traffic_class_from_u8(reader.get_u8()?)?,
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
        FrameKind::RxRateHint => Ok(Frame::RxRateHint {
            path_id: PathId(reader.get_u16()?),
            hint: decode_rate_hint(reader)?,
        }),
        FrameKind::MaxConnectionData => Ok(Frame::MaxConnectionData {
            max_bytes: reader.get_u64()?,
        }),
        FrameKind::Ping => Ok(Frame::Ping {
            nonce: reader.get_u64()?,
        }),
        FrameKind::Pong => Ok(Frame::Pong {
            nonce: reader.get_u64()?,
        }),
        FrameKind::KeyUpdate => Ok(Frame::KeyUpdate {
            key_phase: reader.get_u64()?,
            nonce: decode_nonce(reader)?,
            auth_tag: decode_auth_tag(reader)?,
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
            encode_socket_addr(out, addr, limits)?;
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

fn encode_outbound(
    out: &mut Vec<u8>,
    outbound: &OutboundPolicy,
    limits: CodecLimits,
) -> Result<(), CodecError> {
    match outbound {
        OutboundPolicy::Direct => put_u8(out, 0),
        OutboundPolicy::BindSourceIp(ip) => {
            put_u8(out, 1);
            encode_ip_addr(out, *ip);
        }
        OutboundPolicy::Socks5 { proxy } => {
            put_u8(out, 2);
            encode_socket_addr(out, proxy, limits)?;
        }
        OutboundPolicy::HttpConnect { proxy } => {
            put_u8(out, 3);
            encode_socket_addr(out, proxy, limits)?;
        }
    }
    Ok(())
}

fn decode_outbound(
    reader: &mut Reader<'_>,
    limits: CodecLimits,
) -> Result<OutboundPolicy, CodecError> {
    match reader.get_u8()? {
        0 => Ok(OutboundPolicy::Direct),
        1 => Ok(OutboundPolicy::BindSourceIp(decode_ip_addr(reader)?)),
        2 => Ok(OutboundPolicy::Socks5 {
            proxy: decode_socket_addr(reader, limits)?,
        }),
        3 => Ok(OutboundPolicy::HttpConnect {
            proxy: decode_socket_addr(reader, limits)?,
        }),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn encode_socket_addr(
    out: &mut Vec<u8>,
    addr: &SocketAddr,
    _limits: CodecLimits,
) -> Result<(), CodecError> {
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

fn decode_socket_addr(
    reader: &mut Reader<'_>,
    _limits: CodecLimits,
) -> Result<SocketAddr, CodecError> {
    match reader.get_u8()? {
        2 => {
            let octets = reader.get_array::<4>()?;
            let port = reader.get_u16()?;
            if port == 0 {
                return Err(CodecError::InvalidPort);
            }
            Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        3 => {
            let octets = reader.get_array::<16>()?;
            let port = reader.get_u16()?;
            if port == 0 {
                return Err(CodecError::InvalidPort);
            }
            Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        _ => Err(CodecError::InvalidEnum),
    }
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

fn encode_ip_addr(out: &mut Vec<u8>, ip: IpAddr) {
    match ip {
        IpAddr::V4(ip) => {
            put_u8(out, 2);
            out.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            put_u8(out, 3);
            out.extend_from_slice(&ip.octets());
        }
    }
}

fn decode_ip_addr(reader: &mut Reader<'_>) -> Result<IpAddr, CodecError> {
    match reader.get_u8()? {
        2 => Ok(IpAddr::V4(Ipv4Addr::from(reader.get_array::<4>()?))),
        3 => Ok(IpAddr::V6(Ipv6Addr::from(reader.get_array::<16>()?))),
        _ => Err(CodecError::InvalidEnum),
    }
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

fn encode_path_capabilities(out: &mut Vec<u8>, caps: PathCapabilities) {
    put_u16(out, caps.to_bits());
}

fn decode_path_capabilities(reader: &mut Reader<'_>) -> Result<PathCapabilities, CodecError> {
    let bits = reader.get_u16()?;
    PathCapabilities::from_bits(bits).ok_or(CodecError::InvalidEnum)
}

fn encode_path_metrics(out: &mut Vec<u8>, metrics: PathMetrics) {
    put_u16(out, metrics.path_id.0);
    put_u32(out, metrics.min_rtt_us);
    put_u32(out, metrics.srtt_us);
    put_u32(out, metrics.rttvar_us);
    put_u32(out, metrics.jitter_us);
    put_u64(out, metrics.delivery_rate_bps);
    put_u32(out, metrics.loss_ppm);
    put_u32(out, metrics.ecn_ppm);
    put_u64(out, metrics.bytes_in_flight);
    put_u64(out, metrics.queue_bytes);
}

fn decode_path_metrics(reader: &mut Reader<'_>) -> Result<PathMetrics, CodecError> {
    Ok(PathMetrics {
        path_id: PathId(reader.get_u16()?),
        min_rtt_us: reader.get_u32()?,
        srtt_us: reader.get_u32()?,
        rttvar_us: reader.get_u32()?,
        jitter_us: reader.get_u32()?,
        delivery_rate_bps: reader.get_u64()?,
        loss_ppm: reader.get_u32()?,
        ecn_ppm: reader.get_u32()?,
        bytes_in_flight: reader.get_u64()?,
        queue_bytes: reader.get_u64()?,
    })
}

fn encode_rate_hint(out: &mut Vec<u8>, hint: RateHint) {
    match hint {
        RateHint::Unknown => put_u8(out, 0),
        RateHint::Unlimited => put_u8(out, 1),
        RateHint::BitsPerSecond(rate) => {
            put_u8(out, 2);
            put_u64(out, rate);
        }
    }
}

fn decode_rate_hint(reader: &mut Reader<'_>) -> Result<RateHint, CodecError> {
    match reader.get_u8()? {
        0 => Ok(RateHint::Unknown),
        1 => Ok(RateHint::Unlimited),
        2 => Ok(RateHint::BitsPerSecond(reader.get_u64()?)),
        _ => Err(CodecError::InvalidEnum),
    }
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
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn finish(&self) -> Result<(), CodecError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes)
        }
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
        Ok(Bytes::copy_from_slice(self.get_exact(len)?))
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
    PathChallenge = 5,
    PathResponse = 6,
    OpenStream = 7,
    StreamData = 8,
    StreamAck = 9,
    StreamMaxData = 10,
    StreamReset = 11,
    OpenDatagramFlow = 12,
    DatagramData = 13,
    DatagramClose = 14,
    MaxConnectionData = 15,
    Ping = 16,
    Pong = 17,
    SessionAuth = 18,
    PathJoinOk = 19,
    PathStatus = 20,
    PathDrain = 21,
    PathClose = 22,
    DatagramFeedback = 23,
    PathMetrics = 24,
    RxRateHint = 25,
    KeyUpdate = 26,
    StreamFin = 27,
    PathMtuProbe = 28,
    PathMtuAck = 29,
    StreamDetach = 30,
}

impl FrameKind {
    fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            1 => Ok(Self::SessionHello),
            2 => Ok(Self::SessionReady),
            3 => Ok(Self::SessionClose),
            4 => Ok(Self::PathJoin),
            5 => Ok(Self::PathChallenge),
            6 => Ok(Self::PathResponse),
            7 => Ok(Self::OpenStream),
            8 => Ok(Self::StreamData),
            9 => Ok(Self::StreamAck),
            10 => Ok(Self::StreamMaxData),
            11 => Ok(Self::StreamReset),
            12 => Ok(Self::OpenDatagramFlow),
            13 => Ok(Self::DatagramData),
            14 => Ok(Self::DatagramClose),
            15 => Ok(Self::MaxConnectionData),
            16 => Ok(Self::Ping),
            17 => Ok(Self::Pong),
            18 => Ok(Self::SessionAuth),
            19 => Ok(Self::PathJoinOk),
            20 => Ok(Self::PathStatus),
            21 => Ok(Self::PathDrain),
            22 => Ok(Self::PathClose),
            23 => Ok(Self::DatagramFeedback),
            24 => Ok(Self::PathMetrics),
            25 => Ok(Self::RxRateHint),
            26 => Ok(Self::KeyUpdate),
            27 => Ok(Self::StreamFin),
            28 => Ok(Self::PathMtuProbe),
            29 => Ok(Self::PathMtuAck),
            30 => Ok(Self::StreamDetach),
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

fn path_status_to_u8(value: PathStatus) -> u8 {
    match value {
        PathStatus::Active => 1,
        PathStatus::Suspect => 2,
        PathStatus::Draining => 3,
        PathStatus::Failed => 4,
    }
}

fn path_status_from_u8(value: u8) -> Result<PathStatus, CodecError> {
    match value {
        1 => Ok(PathStatus::Active),
        2 => Ok(PathStatus::Suspect),
        3 => Ok(PathStatus::Draining),
        4 => Ok(PathStatus::Failed),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn ingress_to_u8(value: IngressKind) -> u8 {
    match value {
        IngressKind::Socks5 => 1,
        IngressKind::HttpConnect => 2,
        IngressKind::TunTcp => 3,
        IngressKind::TunUdp => 4,
    }
}

fn ingress_from_u8(value: u8) -> Result<IngressKind, CodecError> {
    match value {
        1 => Ok(IngressKind::Socks5),
        2 => Ok(IngressKind::HttpConnect),
        3 => Ok(IngressKind::TunTcp),
        4 => Ok(IngressKind::TunUdp),
        _ => Err(CodecError::InvalidEnum),
    }
}

fn traffic_class_to_u8(value: TrafficClass) -> u8 {
    match value {
        TrafficClass::Control => 1,
        TrafficClass::Interactive => 2,
        TrafficClass::Bulk => 3,
        TrafficClass::RealtimeDatagram => 4,
        TrafficClass::Background => 5,
    }
}

fn traffic_class_from_u8(value: u8) -> Result<TrafficClass, CodecError> {
    match value {
        1 => Ok(TrafficClass::Control),
        2 => Ok(TrafficClass::Interactive),
        3 => Ok(TrafficClass::Bulk),
        4 => Ok(TrafficClass::RealtimeDatagram),
        5 => Ok(TrafficClass::Background),
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

fn stream_flags_to_u8(flags: StreamFlags) -> u8 {
    u8::from(flags.fin) | (u8::from(flags.early_data) << 1)
}

fn stream_flags_from_u8(value: u8) -> Result<StreamFlags, CodecError> {
    if value & !0x03 != 0 {
        return Err(CodecError::InvalidEnum);
    }
    Ok(StreamFlags {
        fin: value & 0x01 != 0,
        early_data: value & 0x02 != 0,
    })
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
mod tests {
    use super::*;

    fn round_trip(frame: Frame) {
        let encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
        let decoded = decode_frame(&encoded, CodecLimits::default()).expect("decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn stream_frames_round_trip() {
        round_trip(Frame::OpenStream {
            stream_id: StreamId(7),
            target: TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            ingress: IngressKind::Socks5,
            outbound: OutboundPolicy::Direct,
            class: TrafficClass::Interactive,
        });
        round_trip(Frame::StreamData {
            stream_id: StreamId(7),
            offset: 1024,
            flags: StreamFlags {
                fin: true,
                early_data: false,
            },
            payload: Bytes::from_static(b"hello"),
        });
        round_trip(Frame::StreamAck {
            stream_id: StreamId(7),
            ranges: vec![
                OffsetRange::new(0, 5).expect("range"),
                OffsetRange::new(10, 12).expect("range"),
            ],
        });
        round_trip(Frame::StreamFin {
            stream_id: StreamId(7),
        });
        round_trip(Frame::StreamDetach {
            stream_id: StreamId(7),
        });
    }

    #[test]
    fn datagram_flow_uses_compact_flow_id_after_open() {
        round_trip(Frame::OpenDatagramFlow {
            flow_id: DatagramFlowId(9),
            target: TargetAddr::Ip("192.0.2.10:53".parse().expect("addr")),
            ingress: IngressKind::TunUdp,
            outbound: OutboundPolicy::Socks5 {
                proxy: "127.0.0.1:1080".parse().expect("proxy"),
            },
            class: TrafficClass::RealtimeDatagram,
        });
        let data = Frame::DatagramData {
            flow_id: DatagramFlowId(9),
            datagram_id: DatagramId(11),
            ttl_ms: 250,
            payload: Bytes::from_static(b"dns"),
        };
        let encoded = encode_frame(&data, CodecLimits::default()).expect("encode");
        assert!(encoded.len() < 40);
        assert_eq!(
            decode_frame(&encoded, CodecLimits::default()).expect("decode"),
            data
        );
    }

    #[test]
    fn control_frames_round_trip_auth_path_metrics_and_key_update() {
        let nonce = AuthNonce([7; 16]);
        let auth_tag = AuthTag([9; 32]);
        let caps = PathCapabilities {
            backup: true,
            expensive: true,
            low_latency: true,
            bulk_allowed: false,
            probe_only: true,
            no_udp: true,
        };

        round_trip(Frame::SessionAuth {
            session_id: SessionId(42),
            nonce,
            auth_tag,
        });
        round_trip(Frame::PathJoin {
            session_id: SessionId(42),
            path_id: PathId(3),
            underlay: UnderlayProtocol::Udp,
            nonce,
            capabilities: caps,
            auth_tag,
        });
        round_trip(Frame::PathJoinOk {
            path_id: PathId(3),
            nonce,
            auth_tag,
        });
        round_trip(Frame::PathStatus {
            path_id: PathId(3),
            status: PathStatus::Suspect,
            capabilities: caps,
        });
        round_trip(Frame::PathDrain { path_id: PathId(3) });
        round_trip(Frame::PathClose {
            path_id: PathId(3),
            reason: CloseReason::ProtocolError,
        });
        round_trip(Frame::PathMtuProbe {
            path_id: PathId(3),
            probe_id: 99,
            payload: Bytes::from_static(b"mtu-probe"),
        });
        round_trip(Frame::PathMtuAck {
            path_id: PathId(3),
            probe_id: 99,
            payload_bytes: 9,
        });
        round_trip(Frame::DatagramFeedback {
            flow_id: DatagramFlowId(10),
            received: vec![
                OffsetRange::new(1, 2).expect("range"),
                OffsetRange::new(8, 12).expect("range"),
            ],
        });
        round_trip(Frame::PathMetrics {
            metrics: PathMetrics {
                path_id: PathId(3),
                min_rtt_us: 18_000,
                srtt_us: 25_000,
                rttvar_us: 3_000,
                jitter_us: 1_200,
                delivery_rate_bps: 125_000_000,
                loss_ppm: 1_500,
                ecn_ppm: 25,
                bytes_in_flight: 64 * 1024,
                queue_bytes: 16 * 1024,
            },
        });
        round_trip(Frame::RxRateHint {
            path_id: PathId(3),
            hint: RateHint::Unknown,
        });
        round_trip(Frame::RxRateHint {
            path_id: PathId(3),
            hint: RateHint::Unlimited,
        });
        round_trip(Frame::RxRateHint {
            path_id: PathId(3),
            hint: RateHint::BitsPerSecond(300_000_000),
        });
        round_trip(Frame::KeyUpdate {
            key_phase: 2,
            nonce,
            auth_tag,
        });
    }

    #[test]
    fn codec_rejects_oversize_payloads_and_ack_ranges() {
        let limits = CodecLimits {
            max_payload_bytes: 4,
            max_ack_ranges: 1,
            ..CodecLimits::default()
        };
        let oversized = Frame::StreamData {
            stream_id: StreamId(1),
            offset: 0,
            flags: StreamFlags::NONE,
            payload: Bytes::from_static(b"hello"),
        };
        assert!(matches!(
            encode_frame(&oversized, limits),
            Err(CodecError::PayloadTooLarge { .. })
        ));

        let too_many_ranges = Frame::StreamAck {
            stream_id: StreamId(1),
            ranges: vec![
                OffsetRange::new(0, 1).expect("range"),
                OffsetRange::new(2, 3).expect("range"),
            ],
        };
        assert!(matches!(
            encode_frame(&too_many_ranges, limits),
            Err(CodecError::TooManyAckRanges { .. })
        ));

        let too_many_datagram_ranges = Frame::DatagramFeedback {
            flow_id: DatagramFlowId(1),
            received: vec![
                OffsetRange::new(0, 1).expect("range"),
                OffsetRange::new(2, 3).expect("range"),
            ],
        };
        assert!(matches!(
            encode_frame(&too_many_datagram_ranges, limits),
            Err(CodecError::TooManyAckRanges { .. })
        ));
    }

    #[test]
    fn decoder_rejects_unknown_path_capability_bits() {
        let frame = Frame::PathJoin {
            session_id: SessionId(1),
            path_id: PathId(1),
            underlay: UnderlayProtocol::Tcp,
            nonce: AuthNonce([0; 16]),
            capabilities: PathCapabilities::default(),
            auth_tag: AuthTag([0; 32]),
        };
        let mut encoded = encode_frame(&frame, CodecLimits::default()).expect("encode");
        let capability_offset = FRAME_HEADER_LEN + 8 + 2 + 1 + 16;
        encoded[capability_offset] = 0x80;

        assert_eq!(
            decode_frame(&encoded, CodecLimits::default()),
            Err(CodecError::InvalidEnum)
        );
    }

    #[test]
    fn decoder_rejects_trailing_bytes() {
        let mut encoded =
            encode_frame(&Frame::Ping { nonce: 42 }, CodecLimits::default()).expect("encode");
        encoded.push(0);

        assert_eq!(
            decode_frame(&encoded, CodecLimits::default()),
            Err(CodecError::TrailingBytes)
        );
    }
}
