use super::crypto::{DIR_CLIENT_TO_SERVER, DIR_SERVER_TO_CLIENT, PacketCipher};
use super::error::UdpCarrierFrameError;
use crate::transport::aead::AEAD_TAG_LEN;
use bytes::Bytes;

const VERSION: u8 = 1;
const HEADER_LEN: usize = 18;
const KIND_ACK: u8 = 1;
const KIND_FRAME_FRAGMENT: u8 = 2;
const KIND_CLOSE_STREAM: u8 = 3;
const KIND_UNORDERED_FRAME_FRAGMENT: u8 = 4;
const KIND_RELIABLE_UNORDERED_FRAME_FRAGMENT: u8 = 5;
const ACK_PREFIX_LEN: usize = 3;
const ACK_RANGE_LEN: usize = 16;
const FRAME_FRAGMENT_PREFIX_LEN: usize = 25;
const CLOSE_STREAM_LEN: usize = 9;
pub(super) const SAFE_TARGET_DATAGRAM_BYTES: usize = 1_200;
pub(super) const MAX_PROBED_DATAGRAM_BYTES: usize = 1_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PacketHeader {
    pub(super) direction: u8,
    pub(super) connection_id: u64,
    pub(super) packet_number: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PacketPayload {
    Ack {
        ranges: Vec<PacketAckRange>,
    },
    FrameFragment {
        ordered: bool,
        ack_eliciting: bool,
        stream_id: u64,
        frame_id: u64,
        offset: u32,
        total_len: u32,
        payload: Bytes,
    },
    CloseStream {
        stream_id: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PacketAckRange {
    pub(super) start: u64,
    pub(super) end: u64,
}

pub(super) fn max_frame_fragment_payload() -> usize {
    max_frame_fragment_payload_for_datagram(SAFE_TARGET_DATAGRAM_BYTES)
}

pub(super) fn max_frame_fragment_payload_for_datagram(datagram_bytes: usize) -> usize {
    datagram_bytes
        .saturating_sub(HEADER_LEN)
        .saturating_sub(AEAD_TAG_LEN)
        .saturating_sub(FRAME_FRAGMENT_PREFIX_LEN)
        .max(1)
}

pub(super) fn encode_packet(
    cipher: &PacketCipher,
    header: PacketHeader,
    payload: &PacketPayload,
) -> Result<Vec<u8>, UdpCarrierFrameError> {
    let mut plaintext = encode_payload(payload)?;
    let aad = encode_header(header);
    let tag = cipher
        .encrypt(header.direction, header.packet_number, &aad, &mut plaintext)
        .map_err(|_| UdpCarrierFrameError::Crypto)?;
    let mut packet = Vec::with_capacity(aad.len() + plaintext.len() + tag.len());
    packet.extend_from_slice(&aad);
    packet.extend_from_slice(&plaintext);
    packet.extend_from_slice(&tag);
    Ok(packet)
}

pub(super) fn decode_packet(
    cipher: &PacketCipher,
    packet: &[u8],
    expected_direction: u8,
) -> Result<(PacketHeader, PacketPayload), UdpCarrierFrameError> {
    if packet.len() < HEADER_LEN + AEAD_TAG_LEN {
        return Err(UdpCarrierFrameError::InvalidPacket("packet too short"));
    }
    let header_bytes: [u8; HEADER_LEN] = packet[..HEADER_LEN]
        .try_into()
        .expect("header slice length checked");
    let header = decode_header(&header_bytes)?;
    if header.direction != expected_direction {
        return Err(UdpCarrierFrameError::InvalidPacket(
            "packet direction does not match receiver",
        ));
    }
    let tag_start = packet
        .len()
        .checked_sub(AEAD_TAG_LEN)
        .ok_or(UdpCarrierFrameError::Crypto)?;
    let mut payload = packet[HEADER_LEN..tag_start].to_vec();
    let tag: [u8; AEAD_TAG_LEN] = packet[tag_start..]
        .try_into()
        .expect("tag slice length checked");
    cipher
        .decrypt(
            header.direction,
            header.packet_number,
            &header_bytes,
            &mut payload,
            &tag,
        )
        .map_err(|_| UdpCarrierFrameError::Crypto)?;
    let decoded = decode_payload(&payload)?;
    Ok((header, decoded))
}

pub(super) fn peek_connection_id(packet: &[u8]) -> Option<u64> {
    if packet.len() < HEADER_LEN {
        return None;
    }
    if packet[0] != VERSION {
        return None;
    }
    Some(u64::from_be_bytes(packet[2..10].try_into().ok()?))
}

fn encode_header(header: PacketHeader) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[0] = VERSION;
    out[1] = header.direction;
    out[2..10].copy_from_slice(&header.connection_id.to_be_bytes());
    out[10..18].copy_from_slice(&header.packet_number.to_be_bytes());
    out
}

fn decode_header(header: &[u8; HEADER_LEN]) -> Result<PacketHeader, UdpCarrierFrameError> {
    if header[0] != VERSION {
        return Err(UdpCarrierFrameError::InvalidPacket(
            "unsupported packet version",
        ));
    }
    if !matches!(header[1], DIR_CLIENT_TO_SERVER | DIR_SERVER_TO_CLIENT) {
        return Err(UdpCarrierFrameError::InvalidPacket("invalid direction"));
    }
    Ok(PacketHeader {
        direction: header[1],
        connection_id: u64::from_be_bytes(header[2..10].try_into().expect("slice length")),
        packet_number: u64::from_be_bytes(header[10..18].try_into().expect("slice length")),
    })
}

fn encode_payload(payload: &PacketPayload) -> Result<Vec<u8>, UdpCarrierFrameError> {
    match payload {
        PacketPayload::Ack { ranges } => {
            let mut out = Vec::with_capacity(ACK_PREFIX_LEN + ranges.len() * ACK_RANGE_LEN);
            out.push(KIND_ACK);
            let count = u16::try_from(ranges.len())
                .map_err(|_| UdpCarrierFrameError::InvalidPacket("too many ACK ranges"))?;
            out.extend_from_slice(&count.to_be_bytes());
            for range in ranges {
                out.extend_from_slice(&range.start.to_be_bytes());
                out.extend_from_slice(&range.end.to_be_bytes());
            }
            Ok(out)
        }
        PacketPayload::FrameFragment {
            ordered,
            ack_eliciting,
            stream_id,
            frame_id,
            offset,
            total_len,
            payload,
        } => {
            let mut out = Vec::with_capacity(FRAME_FRAGMENT_PREFIX_LEN + payload.len());
            out.push(if *ordered {
                KIND_FRAME_FRAGMENT
            } else if *ack_eliciting {
                KIND_RELIABLE_UNORDERED_FRAME_FRAGMENT
            } else {
                KIND_UNORDERED_FRAME_FRAGMENT
            });
            out.extend_from_slice(&stream_id.to_be_bytes());
            out.extend_from_slice(&frame_id.to_be_bytes());
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&total_len.to_be_bytes());
            out.extend_from_slice(payload);
            Ok(out)
        }
        PacketPayload::CloseStream { stream_id } => {
            let mut out = Vec::with_capacity(CLOSE_STREAM_LEN);
            out.push(KIND_CLOSE_STREAM);
            out.extend_from_slice(&stream_id.to_be_bytes());
            Ok(out)
        }
    }
}

fn decode_payload(payload: &[u8]) -> Result<PacketPayload, UdpCarrierFrameError> {
    let Some(kind) = payload.first().copied() else {
        return Err(UdpCarrierFrameError::InvalidPacket("empty payload"));
    };
    match kind {
        KIND_ACK => {
            if payload.len() < ACK_PREFIX_LEN {
                return Err(UdpCarrierFrameError::InvalidPacket("invalid ACK length"));
            }
            let count =
                u16::from_be_bytes(payload[1..3].try_into().expect("slice length")) as usize;
            if payload.len() != ACK_PREFIX_LEN + count * ACK_RANGE_LEN {
                return Err(UdpCarrierFrameError::InvalidPacket(
                    "invalid ACK range length",
                ));
            }
            let mut ranges = Vec::with_capacity(count);
            let mut cursor = ACK_PREFIX_LEN;
            for _ in 0..count {
                let start = u64::from_be_bytes(
                    payload[cursor..cursor + 8]
                        .try_into()
                        .expect("slice length"),
                );
                cursor += 8;
                let end = u64::from_be_bytes(
                    payload[cursor..cursor + 8]
                        .try_into()
                        .expect("slice length"),
                );
                cursor += 8;
                if start >= end {
                    return Err(UdpCarrierFrameError::InvalidPacket("empty ACK range"));
                }
                ranges.push(PacketAckRange { start, end });
            }
            Ok(PacketPayload::Ack { ranges })
        }
        KIND_FRAME_FRAGMENT
        | KIND_UNORDERED_FRAME_FRAGMENT
        | KIND_RELIABLE_UNORDERED_FRAME_FRAGMENT => {
            if payload.len() < FRAME_FRAGMENT_PREFIX_LEN {
                return Err(UdpCarrierFrameError::InvalidPacket(
                    "invalid frame fragment length",
                ));
            }
            Ok(PacketPayload::FrameFragment {
                ordered: kind == KIND_FRAME_FRAGMENT,
                ack_eliciting: matches!(
                    kind,
                    KIND_FRAME_FRAGMENT | KIND_RELIABLE_UNORDERED_FRAME_FRAGMENT
                ),
                stream_id: u64::from_be_bytes(payload[1..9].try_into().expect("slice length")),
                frame_id: u64::from_be_bytes(payload[9..17].try_into().expect("slice length")),
                offset: u32::from_be_bytes(payload[17..21].try_into().expect("slice length")),
                total_len: u32::from_be_bytes(payload[21..25].try_into().expect("slice length")),
                payload: Bytes::copy_from_slice(&payload[25..]),
            })
        }
        KIND_CLOSE_STREAM => {
            if payload.len() != CLOSE_STREAM_LEN {
                return Err(UdpCarrierFrameError::InvalidPacket(
                    "invalid stream-close length",
                ));
            }
            Ok(PacketPayload::CloseStream {
                stream_id: u64::from_be_bytes(payload[1..9].try_into().expect("slice length")),
            })
        }
        _ => Err(UdpCarrierFrameError::InvalidPacket("unknown payload kind")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CipherSuite;
    use crate::transport::udp_carrier::crypto::PacketCipher;

    #[test]
    fn packet_round_trips_without_visible_product_metadata() {
        let secret = b"mptunnel integration test secret with enough entropy";
        let cipher = PacketCipher::new(secret, CipherSuite::Aes256Gcm, 7).expect("cipher");
        let header = PacketHeader {
            direction: DIR_CLIENT_TO_SERVER,
            connection_id: 7,
            packet_number: 9,
        };
        let encoded = encode_packet(
            &cipher,
            header,
            &PacketPayload::FrameFragment {
                ordered: true,
                ack_eliciting: true,
                stream_id: 1,
                frame_id: 2,
                offset: 0,
                total_len: 5,
                payload: Bytes::from_static(b"hello"),
            },
        )
        .expect("encode");

        for token in [b"mptunnel".as_slice(), b"udp-carrier".as_slice()] {
            assert!(!encoded.windows(token.len()).any(|window| window == token));
        }
        let (decoded_header, decoded_payload) =
            decode_packet(&cipher, &encoded, DIR_CLIENT_TO_SERVER).expect("decode");
        assert_eq!(decoded_header, header);
        assert_eq!(
            decoded_payload,
            PacketPayload::FrameFragment {
                ordered: true,
                ack_eliciting: true,
                stream_id: 1,
                frame_id: 2,
                offset: 0,
                total_len: 5,
                payload: Bytes::from_static(b"hello"),
            }
        );
    }
}
