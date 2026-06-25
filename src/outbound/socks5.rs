use crate::outbound::{OutboundError, validate_target};
use crate::protocol::TargetAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const VERSION: u8 = 0x05;
const METHOD_NO_AUTH: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const CMD_UDP_ASSOCIATE: u8 = 0x03;
const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_IPV6: u8 = 0x04;

pub fn no_auth_greeting() -> [u8; 3] {
    [VERSION, 0x01, METHOD_NO_AUTH]
}

pub fn connect_request(target: &TargetAddr) -> Result<Vec<u8>, OutboundError> {
    validate_target(target)?;
    request(CMD_CONNECT, target)
}

pub fn udp_associate_request(client_udp_addr: SocketAddr) -> Result<Vec<u8>, OutboundError> {
    let target = TargetAddr::Ip(client_udp_addr);
    request(CMD_UDP_ASSOCIATE, &target)
}

pub fn parse_method_selection(input: &[u8]) -> Result<Socks5MethodSelection, Socks5ClientError> {
    if input.len() < 2 {
        return Err(Socks5ClientError::Incomplete);
    }
    if input[0] != VERSION {
        return Err(Socks5ClientError::UnsupportedVersion(input[0]));
    }
    Ok(Socks5MethodSelection { method: input[1] })
}

pub fn parse_connect_reply(input: &[u8]) -> Result<Socks5ConnectReply, Socks5ClientError> {
    if input.len() < 4 {
        return Err(Socks5ClientError::Incomplete);
    }
    if input[0] != VERSION {
        return Err(Socks5ClientError::UnsupportedVersion(input[0]));
    }
    let status = input[1];
    if input[2] != 0 {
        return Err(Socks5ClientError::InvalidReservedByte);
    }
    let (bind, consumed) = parse_addr(&input[3..])?;
    Ok(Socks5ConnectReply {
        status,
        bind,
        consumed: 3 + consumed,
    })
}

fn request(command: u8, target: &TargetAddr) -> Result<Vec<u8>, OutboundError> {
    let mut out = vec![VERSION, command, 0x00];
    encode_addr(&mut out, target)?;
    Ok(out)
}

fn encode_addr(out: &mut Vec<u8>, target: &TargetAddr) -> Result<(), OutboundError> {
    match target {
        TargetAddr::Domain { host, port } => {
            if host.len() > u8::MAX as usize {
                return Err(OutboundError::DomainTooLong);
            }
            out.push(ATYP_DOMAIN);
            out.push(host.len() as u8);
            out.extend_from_slice(host.as_bytes());
            out.extend_from_slice(&port.to_be_bytes());
        }
        TargetAddr::Ip(SocketAddr::V4(addr)) => {
            out.push(ATYP_IPV4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        TargetAddr::Ip(SocketAddr::V6(addr)) => {
            out.push(ATYP_IPV6);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    Ok(())
}

fn parse_addr(input: &[u8]) -> Result<(TargetAddr, usize), Socks5ClientError> {
    if input.is_empty() {
        return Err(Socks5ClientError::Incomplete);
    }
    match input[0] {
        ATYP_IPV4 => {
            if input.len() < 7 {
                return Err(Socks5ClientError::Incomplete);
            }
            let port = u16::from_be_bytes([input[5], input[6]]);
            Ok((
                TargetAddr::Ip(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(input[1], input[2], input[3], input[4])),
                    port,
                )),
                7,
            ))
        }
        ATYP_IPV6 => {
            if input.len() < 19 {
                return Err(Socks5ClientError::Incomplete);
            }
            let octets: [u8; 16] = input[1..17].try_into().expect("slice length");
            let port = u16::from_be_bytes([input[17], input[18]]);
            Ok((
                TargetAddr::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port)),
                19,
            ))
        }
        ATYP_DOMAIN => {
            if input.len() < 2 {
                return Err(Socks5ClientError::Incomplete);
            }
            let host_len = input[1] as usize;
            if host_len == 0 {
                return Err(Socks5ClientError::InvalidDomain);
            }
            let total_len = 2usize
                .checked_add(host_len)
                .and_then(|value| value.checked_add(2))
                .ok_or(Socks5ClientError::MessageTooLong)?;
            if input.len() < total_len {
                return Err(Socks5ClientError::Incomplete);
            }
            let host = std::str::from_utf8(&input[2..2 + host_len])
                .map_err(|_| Socks5ClientError::InvalidDomain)?;
            let port_start = 2 + host_len;
            let port = u16::from_be_bytes([input[port_start], input[port_start + 1]]);
            Ok((
                TargetAddr::Domain {
                    host: host.to_string(),
                    port,
                },
                total_len,
            ))
        }
        atyp => Err(Socks5ClientError::UnsupportedAddressType(atyp)),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Socks5MethodSelection {
    pub method: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5ConnectReply {
    pub status: u8,
    pub bind: TargetAddr,
    pub consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5ClientError {
    Incomplete,
    UnsupportedVersion(u8),
    UnsupportedAddressType(u8),
    InvalidReservedByte,
    InvalidDomain,
    MessageTooLong,
}

impl std::fmt::Display for Socks5ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete SOCKS5 response"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported SOCKS version {version}")
            }
            Self::UnsupportedAddressType(atyp) => {
                write!(f, "unsupported SOCKS5 address type {atyp}")
            }
            Self::InvalidReservedByte => write!(f, "invalid SOCKS5 reserved byte"),
            Self::InvalidDomain => write!(f, "invalid SOCKS5 domain"),
            Self::MessageTooLong => write!(f, "SOCKS5 message is too long"),
        }
    }
}

impl std::error::Error for Socks5ClientError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_connect_request_for_domain() {
        let request = connect_request(&TargetAddr::Domain {
            host: "example.com".to_string(),
            port: 443,
        })
        .expect("request");

        let mut expected = vec![0x05, 0x01, 0x00, 0x03, 11];
        expected.extend_from_slice(b"example.com");
        expected.extend_from_slice(&443u16.to_be_bytes());
        assert_eq!(request, expected);
    }

    #[test]
    fn builds_udp_associate_request() {
        let request = udp_associate_request("0.0.0.0:0".parse().expect("addr")).expect("request");

        assert_eq!(request, vec![0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn parses_method_selection_and_connect_reply() {
        assert_eq!(
            parse_method_selection(&[0x05, 0x00]).expect("method"),
            Socks5MethodSelection { method: 0x00 }
        );

        let reply = parse_connect_reply(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0x1f, 0x90])
            .expect("reply");
        assert_eq!(reply.status, 0);
        assert_eq!(
            reply.bind,
            TargetAddr::Ip("127.0.0.1:8080".parse().expect("addr"))
        );
        assert_eq!(reply.consumed, 10);
    }
}
