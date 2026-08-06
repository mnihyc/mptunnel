//! Allocation-independent parsing for the layer-3 packet service.
//!
//! The parser extracts only immutable routing and flow-affinity facts. It
//! neither validates transport checksums nor mutates the inner packet.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IpPacketFlowKey {
    pub(crate) source: IpAddr,
    pub(crate) destination: IpAddr,
    pub(crate) next_header: u8,
    discriminator: FlowDiscriminator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FlowDiscriminator {
    Ports { source: u16, destination: u16 },
    Fragment(u32),
    Icmp { kind: u8, code: u8, identifier: u16 },
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IpPacketMetadata {
    pub(crate) source: IpAddr,
    pub(crate) destination: IpAddr,
    pub(crate) flow_key: IpPacketFlowKey,
}

pub(crate) fn parse_ip_packet(packet: &[u8]) -> Result<IpPacketMetadata, IpPacketError> {
    let version = packet
        .first()
        .map(|byte| byte >> 4)
        .ok_or(IpPacketError::Truncated)?;
    match version {
        4 => parse_ipv4_packet(packet),
        6 => parse_ipv6_packet(packet),
        value => Err(IpPacketError::UnsupportedVersion(value)),
    }
}

fn parse_ipv4_packet(packet: &[u8]) -> Result<IpPacketMetadata, IpPacketError> {
    if packet.len() < 20 {
        return Err(IpPacketError::Truncated);
    }
    let header_len = usize::from(packet[0] & 0x0f) * 4;
    if header_len < 20 || header_len > packet.len() {
        return Err(IpPacketError::InvalidHeader);
    }
    let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if total_len != packet.len() || total_len < header_len {
        return Err(IpPacketError::InvalidLength);
    }
    let source = IpAddr::V4(Ipv4Addr::new(
        packet[12], packet[13], packet[14], packet[15],
    ));
    let destination = IpAddr::V4(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ));
    let next_header = packet[9];
    let fragment = u16::from_be_bytes([packet[6], packet[7]]);
    let fragment_offset = fragment & 0x1fff;
    let more_fragments = fragment & 0x2000 != 0;
    let discriminator = if fragment_offset != 0 || more_fragments {
        FlowDiscriminator::Fragment(u32::from(u16::from_be_bytes([packet[4], packet[5]])))
    } else {
        transport_discriminator(next_header, &packet[header_len..])
    };
    Ok(metadata(source, destination, next_header, discriminator))
}

fn parse_ipv6_packet(packet: &[u8]) -> Result<IpPacketMetadata, IpPacketError> {
    if packet.len() < 40 {
        return Err(IpPacketError::Truncated);
    }
    let payload_len = usize::from(u16::from_be_bytes([packet[4], packet[5]]));
    if payload_len == 0 {
        if packet.len() != 40 {
            return Err(IpPacketError::UnsupportedJumbogram);
        }
    } else if packet.len() != 40 + payload_len {
        return Err(IpPacketError::InvalidLength);
    }
    let source = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[8..24]).expect("IPv6 source slice"),
    ));
    let destination = IpAddr::V6(Ipv6Addr::from(
        <[u8; 16]>::try_from(&packet[24..40]).expect("IPv6 destination slice"),
    ));
    let mut next_header = packet[6];
    let mut offset = 40usize;
    loop {
        let extension_len = match next_header {
            0 | 43 | 60 => {
                let header = packet
                    .get(offset..offset + 2)
                    .ok_or(IpPacketError::Truncated)?;
                next_header = header[0];
                (usize::from(header[1]) + 1) * 8
            }
            44 => {
                let header = packet
                    .get(offset..offset + 8)
                    .ok_or(IpPacketError::Truncated)?;
                next_header = header[0];
                let fragment_id =
                    u32::from_be_bytes(header[4..8].try_into().expect("IPv6 fragment ID slice"));
                return Ok(metadata(
                    source,
                    destination,
                    next_header,
                    FlowDiscriminator::Fragment(fragment_id),
                ));
            }
            51 => {
                let header = packet
                    .get(offset..offset + 2)
                    .ok_or(IpPacketError::Truncated)?;
                next_header = header[0];
                (usize::from(header[1]) + 2) * 4
            }
            _ => break,
        };
        offset = offset
            .checked_add(extension_len)
            .filter(|offset| *offset <= packet.len())
            .ok_or(IpPacketError::Truncated)?;
    }
    let discriminator = transport_discriminator(next_header, &packet[offset..]);
    Ok(metadata(source, destination, next_header, discriminator))
}

fn transport_discriminator(next_header: u8, payload: &[u8]) -> FlowDiscriminator {
    match next_header {
        6 | 17 | 132 if payload.len() >= 4 => FlowDiscriminator::Ports {
            source: u16::from_be_bytes([payload[0], payload[1]]),
            destination: u16::from_be_bytes([payload[2], payload[3]]),
        },
        1 | 58 if payload.len() >= 8 => FlowDiscriminator::Icmp {
            kind: payload[0],
            code: payload[1],
            identifier: u16::from_be_bytes([payload[4], payload[5]]),
        },
        _ => FlowDiscriminator::None,
    }
}

fn metadata(
    source: IpAddr,
    destination: IpAddr,
    next_header: u8,
    discriminator: FlowDiscriminator,
) -> IpPacketMetadata {
    IpPacketMetadata {
        source,
        destination,
        flow_key: IpPacketFlowKey {
            source,
            destination,
            next_header,
            discriminator,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpPacketError {
    Truncated,
    UnsupportedVersion(u8),
    InvalidHeader,
    InvalidLength,
    UnsupportedJumbogram,
}

#[cfg(test)]
#[path = "tests_tun_l3.rs"]
mod tests;
