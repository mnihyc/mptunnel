use crate::protocol::TargetAddr;
use bytes::Bytes;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const VERSION: u8 = 0x05;
const USERNAME_PASSWORD_VERSION: u8 = 0x01;
const METHOD_NO_AUTH: u8 = 0x00;
const METHOD_USERNAME_PASSWORD: u8 = 0x02;
const METHOD_NO_ACCEPTABLE: u8 = 0xff;
const CMD_CONNECT: u8 = 0x01;
const CMD_UDP_ASSOCIATE: u8 = 0x03;
const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_IPV6: u8 = 0x04;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthRequest {
    pub methods: Vec<u8>,
}

impl AuthRequest {
    pub fn supports_no_auth(&self) -> bool {
        self.methods.contains(&METHOD_NO_AUTH)
    }

    pub fn supports_username_password(&self) -> bool {
        self.methods.contains(&METHOD_USERNAME_PASSWORD)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct UsernamePasswordAuthRequest {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for UsernamePasswordAuthRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UsernamePasswordAuthRequest")
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub target: TargetAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpAssociateRequest {
    pub client_endpoint: TargetAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRequest {
    pub command: Socks5Command,
    pub target: TargetAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Socks5Command {
    Connect,
    UdpAssociate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpDatagram {
    pub target: TargetAddr,
    pub payload: Bytes,
}

pub fn parse_auth_request(input: &[u8]) -> Result<(AuthRequest, usize), Socks5Error> {
    if input.len() < 2 {
        return Err(Socks5Error::Incomplete);
    }
    if input[0] != VERSION {
        return Err(Socks5Error::UnsupportedVersion(input[0]));
    }
    let method_count = input[1] as usize;
    let total_len = 2usize
        .checked_add(method_count)
        .ok_or(Socks5Error::MessageTooLong)?;
    if input.len() < total_len {
        return Err(Socks5Error::Incomplete);
    }
    Ok((
        AuthRequest {
            methods: input[2..total_len].to_vec(),
        },
        total_len,
    ))
}

pub fn no_auth_response() -> [u8; 2] {
    [VERSION, METHOD_NO_AUTH]
}

pub fn username_password_method_response() -> [u8; 2] {
    [VERSION, METHOD_USERNAME_PASSWORD]
}

pub fn username_password_auth_response(success: bool) -> [u8; 2] {
    [USERNAME_PASSWORD_VERSION, u8::from(!success)]
}

pub fn no_acceptable_methods_response() -> [u8; 2] {
    [VERSION, METHOD_NO_ACCEPTABLE]
}

pub fn parse_username_password_auth_request(
    input: &[u8],
) -> Result<(UsernamePasswordAuthRequest, usize), Socks5Error> {
    if input.len() < 2 {
        return Err(Socks5Error::Incomplete);
    }
    if input[0] != USERNAME_PASSWORD_VERSION {
        return Err(Socks5Error::UnsupportedAuthVersion(input[0]));
    }
    let username_len = input[1] as usize;
    let password_len_offset = 2usize
        .checked_add(username_len)
        .ok_or(Socks5Error::MessageTooLong)?;
    if input.len() <= password_len_offset {
        return Err(Socks5Error::Incomplete);
    }
    let password_len = input[password_len_offset] as usize;
    let total_len = password_len_offset
        .checked_add(1)
        .and_then(|value| value.checked_add(password_len))
        .ok_or(Socks5Error::MessageTooLong)?;
    if input.len() < total_len {
        return Err(Socks5Error::Incomplete);
    }
    let username = std::str::from_utf8(&input[2..password_len_offset])
        .map_err(|_| Socks5Error::InvalidAuthEncoding)?;
    let password_start = password_len_offset + 1;
    let password = std::str::from_utf8(&input[password_start..total_len])
        .map_err(|_| Socks5Error::InvalidAuthEncoding)?;
    Ok((
        UsernamePasswordAuthRequest {
            username: username.to_string(),
            password: password.to_string(),
        },
        total_len,
    ))
}

pub fn parse_connect_request(input: &[u8]) -> Result<(ConnectRequest, usize), Socks5Error> {
    let (request, consumed) = parse_command_request(input)?;
    if request.command != Socks5Command::Connect {
        return Err(Socks5Error::UnsupportedCommand(command_code(
            request.command,
        )));
    }
    Ok((
        ConnectRequest {
            target: request.target,
        },
        consumed,
    ))
}

pub fn parse_udp_associate_request(
    input: &[u8],
) -> Result<(UdpAssociateRequest, usize), Socks5Error> {
    let (request, consumed) = parse_command_request(input)?;
    if request.command != Socks5Command::UdpAssociate {
        return Err(Socks5Error::UnsupportedCommand(command_code(
            request.command,
        )));
    }
    Ok((
        UdpAssociateRequest {
            client_endpoint: request.target,
        },
        consumed,
    ))
}

pub fn parse_command_request(input: &[u8]) -> Result<(CommandRequest, usize), Socks5Error> {
    if input.len() < 4 {
        return Err(Socks5Error::Incomplete);
    }
    if input[0] != VERSION {
        return Err(Socks5Error::UnsupportedVersion(input[0]));
    }
    if input[2] != 0 {
        return Err(Socks5Error::InvalidReservedByte);
    }
    let command = match input[1] {
        CMD_CONNECT => Socks5Command::Connect,
        CMD_UDP_ASSOCIATE => Socks5Command::UdpAssociate,
        command => return Err(Socks5Error::UnsupportedCommand(command)),
    };

    let allow_zero_port = command == Socks5Command::UdpAssociate;
    let (target, consumed) = parse_addr(&input[3..], allow_zero_port)?;
    Ok((CommandRequest { command, target }, 3 + consumed))
}

pub fn connect_reply(reply: Socks5Reply, bind: SocketAddr) -> Vec<u8> {
    let mut out = vec![VERSION, reply as u8, 0x00];
    match bind {
        SocketAddr::V4(addr) => {
            out.push(ATYP_IPV4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        SocketAddr::V6(addr) => {
            out.push(ATYP_IPV6);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    out
}

pub fn parse_udp_datagram(input: &[u8]) -> Result<(UdpDatagram, usize), Socks5Error> {
    let parts = parse_udp_datagram_parts(input)?;
    Ok((
        UdpDatagram {
            target: parts.target,
            payload: Bytes::copy_from_slice(&input[parts.payload_offset..parts.consumed]),
        },
        parts.consumed,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpDatagramParts {
    pub target: TargetAddr,
    pub payload_offset: usize,
    pub consumed: usize,
}

pub fn parse_udp_datagram_parts(input: &[u8]) -> Result<UdpDatagramParts, Socks5Error> {
    if input.len() < 4 {
        return Err(Socks5Error::Incomplete);
    }
    if input[0] != 0 || input[1] != 0 {
        return Err(Socks5Error::InvalidReservedByte);
    }
    if input[2] != 0 {
        return Err(Socks5Error::UnsupportedFragment(input[2]));
    }
    let (target, consumed) = parse_addr(&input[3..], false)?;
    let payload_offset = 3usize
        .checked_add(consumed)
        .ok_or(Socks5Error::MessageTooLong)?;
    Ok(UdpDatagramParts {
        target,
        payload_offset,
        consumed: input.len(),
    })
}

pub fn udp_datagram(target: &TargetAddr, payload: &[u8]) -> Result<Vec<u8>, Socks5Error> {
    let mut out = Vec::with_capacity(3 + 1 + payload.len());
    out.extend_from_slice(&[0x00, 0x00, 0x00]);
    append_addr(&mut out, target)?;
    out.extend_from_slice(payload);
    Ok(out)
}

fn parse_addr(input: &[u8], allow_zero_port: bool) -> Result<(TargetAddr, usize), Socks5Error> {
    if input.is_empty() {
        return Err(Socks5Error::Incomplete);
    }
    match input[0] {
        ATYP_IPV4 => {
            if input.len() < 1 + 4 + 2 {
                return Err(Socks5Error::Incomplete);
            }
            let ip = Ipv4Addr::new(input[1], input[2], input[3], input[4]);
            let port = u16::from_be_bytes([input[5], input[6]]);
            if port == 0 && !allow_zero_port {
                return Err(Socks5Error::InvalidPort);
            }
            Ok((TargetAddr::Ip(SocketAddr::new(IpAddr::V4(ip), port)), 7))
        }
        ATYP_IPV6 => {
            if input.len() < 1 + 16 + 2 {
                return Err(Socks5Error::Incomplete);
            }
            let octets: [u8; 16] = input[1..17].try_into().expect("slice length");
            let port = u16::from_be_bytes([input[17], input[18]]);
            if port == 0 && !allow_zero_port {
                return Err(Socks5Error::InvalidPort);
            }
            Ok((
                TargetAddr::Ip(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port)),
                19,
            ))
        }
        ATYP_DOMAIN => {
            if input.len() < 2 {
                return Err(Socks5Error::Incomplete);
            }
            let host_len = input[1] as usize;
            if host_len == 0 {
                return Err(Socks5Error::InvalidDomain);
            }
            let total_len = 2usize
                .checked_add(host_len)
                .and_then(|value| value.checked_add(2))
                .ok_or(Socks5Error::MessageTooLong)?;
            if input.len() < total_len {
                return Err(Socks5Error::Incomplete);
            }
            let host = std::str::from_utf8(&input[2..2 + host_len])
                .map_err(|_| Socks5Error::InvalidDomain)?;
            let port_start = 2 + host_len;
            let port = u16::from_be_bytes([input[port_start], input[port_start + 1]]);
            if port == 0 && !allow_zero_port {
                return Err(Socks5Error::InvalidPort);
            }
            Ok((
                TargetAddr::Domain {
                    host: host.to_string(),
                    port,
                },
                total_len,
            ))
        }
        atyp => Err(Socks5Error::UnsupportedAddressType(atyp)),
    }
}

fn append_addr(out: &mut Vec<u8>, target: &TargetAddr) -> Result<(), Socks5Error> {
    match target {
        TargetAddr::Ip(SocketAddr::V4(addr)) => {
            if addr.port() == 0 {
                return Err(Socks5Error::InvalidPort);
            }
            out.push(ATYP_IPV4);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        TargetAddr::Ip(SocketAddr::V6(addr)) => {
            if addr.port() == 0 {
                return Err(Socks5Error::InvalidPort);
            }
            out.push(ATYP_IPV6);
            out.extend_from_slice(&addr.ip().octets());
            out.extend_from_slice(&addr.port().to_be_bytes());
        }
        TargetAddr::Domain { host, port } => {
            if host.is_empty() {
                return Err(Socks5Error::InvalidDomain);
            }
            if host.len() > u8::MAX as usize {
                return Err(Socks5Error::MessageTooLong);
            }
            if *port == 0 {
                return Err(Socks5Error::InvalidPort);
            }
            out.push(ATYP_DOMAIN);
            out.push(host.len() as u8);
            out.extend_from_slice(host.as_bytes());
            out.extend_from_slice(&port.to_be_bytes());
        }
    }
    Ok(())
}

fn command_code(command: Socks5Command) -> u8 {
    match command {
        Socks5Command::Connect => CMD_CONNECT,
        Socks5Command::UdpAssociate => CMD_UDP_ASSOCIATE,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Socks5Reply {
    Succeeded = 0x00,
    GeneralFailure = 0x01,
    ConnectionNotAllowed = 0x02,
    NetworkUnreachable = 0x03,
    HostUnreachable = 0x04,
    ConnectionRefused = 0x05,
    TtlExpired = 0x06,
    CommandNotSupported = 0x07,
    AddressTypeNotSupported = 0x08,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5Error {
    Incomplete,
    UnsupportedVersion(u8),
    UnsupportedCommand(u8),
    UnsupportedAddressType(u8),
    InvalidReservedByte,
    InvalidPort,
    InvalidDomain,
    UnsupportedAuthVersion(u8),
    InvalidAuthEncoding,
    UnsupportedFragment(u8),
    MessageTooLong,
}

impl std::fmt::Display for Socks5Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete SOCKS5 message"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported SOCKS version {version}")
            }
            Self::UnsupportedCommand(command) => write!(f, "unsupported SOCKS5 command {command}"),
            Self::UnsupportedAddressType(atyp) => {
                write!(f, "unsupported SOCKS5 address type {atyp}")
            }
            Self::InvalidReservedByte => write!(f, "invalid SOCKS5 reserved byte"),
            Self::InvalidPort => write!(f, "SOCKS5 target port must be greater than zero"),
            Self::InvalidDomain => write!(f, "invalid SOCKS5 domain"),
            Self::UnsupportedAuthVersion(version) => {
                write!(f, "unsupported SOCKS5 auth version {version}")
            }
            Self::InvalidAuthEncoding => write!(f, "invalid SOCKS5 username/password encoding"),
            Self::UnsupportedFragment(fragment) => {
                write!(f, "SOCKS5 UDP fragment {fragment} is not supported")
            }
            Self::MessageTooLong => write!(f, "SOCKS5 message is too long"),
        }
    }
}

impl std::error::Error for Socks5Error {}

#[cfg(test)]
#[path = "socks5_test.rs"]
mod tests;
