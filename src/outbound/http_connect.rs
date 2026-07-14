use crate::outbound::{OutboundError, validate_target};
use crate::protocol::TargetAddr;
use crate::transport::Endpoint;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use quinn_proto::{VarInt, coding::Codec};
use tokio::io::{AsyncRead, AsyncReadExt};

const DATAGRAM_CAPSULE_TYPE: u64 = 0;
const UDP_CONTEXT_ID: u64 = 0;
const MAX_CONNECT_UDP_PAYLOAD_BYTES: usize = 65_527;
const MAX_CONNECT_UDP_CAPSULE_PAYLOAD_BYTES: usize = MAX_CONNECT_UDP_PAYLOAD_BYTES + 8;
const MAX_HTTP_RESPONSE_HEADERS: usize = 64;
const CONNECT_UDP_TEMPLATE_VAR_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

pub fn connect_request(
    target: &TargetAddr,
    host_header: Option<&str>,
) -> Result<Vec<u8>, OutboundError> {
    validate_target(target)?;
    let authority = target.authority();
    let host = host_header.unwrap_or(&authority);
    Ok(format!(
        "CONNECT {authority} HTTP/1.1\r\nHost: {host}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    )
    .into_bytes())
}

pub fn connect_udp_request(
    proxy: &Endpoint,
    target: &TargetAddr,
) -> Result<Vec<u8>, OutboundError> {
    validate_target(target)?;
    let proxy_authority = proxy.authority();
    let target_host = connect_udp_target_host(target);
    let target_port = target.port();
    let target_host =
        utf8_percent_encode(&target_host, CONNECT_UDP_TEMPLATE_VAR_ENCODE_SET).to_string();
    let uri =
        format!("http://{proxy_authority}/.well-known/masque/udp/{target_host}/{target_port}/");
    Ok(format!(
        "GET {uri} HTTP/1.1\r\nHost: {proxy_authority}\r\nConnection: Upgrade\r\nUpgrade: connect-udp\r\nCapsule-Protocol: ?1\r\n\r\n"
    )
    .into_bytes())
}

pub fn parse_connect_response(input: &[u8]) -> Result<HttpConnectResponse, HttpConnectClientError> {
    let parsed = parse_http_response_headers(input)?;
    Ok(HttpConnectResponse {
        status: parsed.status,
        header_len: parsed.header_len,
    })
}

pub fn parse_connect_udp_response(
    input: &[u8],
) -> Result<HttpConnectUdpResponse, HttpConnectClientError> {
    let parsed = parse_http_response_headers(input)?;
    if parsed.status != 101 {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    if !parsed.header_has_token("connection", "upgrade") {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    if !parsed.header_has_value("upgrade", "connect-udp") {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    if !parsed.header_has_value("capsule-protocol", "?1") {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    Ok(HttpConnectUdpResponse {
        status: parsed.status,
        header_len: parsed.header_len,
    })
}

pub fn datagram_capsule(payload: &[u8]) -> Result<Vec<u8>, HttpConnectClientError> {
    if payload.len() > MAX_CONNECT_UDP_PAYLOAD_BYTES {
        return Err(HttpConnectClientError::DatagramPayloadTooLarge {
            actual: payload.len(),
            limit: MAX_CONNECT_UDP_PAYLOAD_BYTES,
        });
    }
    let mut http_datagram_payload = Vec::with_capacity(1 + payload.len());
    encode_capsule_varint(UDP_CONTEXT_ID, &mut http_datagram_payload)?;
    http_datagram_payload.extend_from_slice(payload);

    let mut capsule = Vec::with_capacity(2 + http_datagram_payload.len());
    encode_capsule_varint(DATAGRAM_CAPSULE_TYPE, &mut capsule)?;
    encode_capsule_varint(http_datagram_payload.len() as u64, &mut capsule)?;
    capsule.extend_from_slice(&http_datagram_payload);
    Ok(capsule)
}

pub async fn read_datagram_capsule<R>(reader: &mut R) -> Result<Vec<u8>, HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let capsule_type = read_capsule_varint(reader).await?.into_inner();
        let capsule_len = read_capsule_varint(reader).await?.into_inner();
        if capsule_len > MAX_CONNECT_UDP_CAPSULE_PAYLOAD_BYTES as u64 {
            return Err(HttpConnectClientError::CapsuleTooLarge {
                actual: capsule_len,
                limit: MAX_CONNECT_UDP_CAPSULE_PAYLOAD_BYTES,
            });
        }
        if capsule_type != DATAGRAM_CAPSULE_TYPE {
            discard_exact(reader, capsule_len as usize).await?;
            continue;
        }
        let mut payload = vec![0u8; capsule_len as usize];
        reader.read_exact(&mut payload).await?;
        let (context_id, consumed) = decode_capsule_varint(&payload)?;
        if context_id.into_inner() != UDP_CONTEXT_ID {
            continue;
        }
        let udp_payload_len = payload.len().saturating_sub(consumed);
        if udp_payload_len > MAX_CONNECT_UDP_PAYLOAD_BYTES {
            return Err(HttpConnectClientError::DatagramPayloadTooLarge {
                actual: udp_payload_len,
                limit: MAX_CONNECT_UDP_PAYLOAD_BYTES,
            });
        }
        payload.copy_within(consumed.., 0);
        payload.truncate(udp_payload_len);
        return Ok(payload);
    }
}

pub async fn read_datagram_capsule_into<R>(
    reader: &mut R,
    buffer: &mut [u8],
) -> Result<usize, HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    loop {
        let capsule_type = read_capsule_varint(reader).await?.into_inner();
        let capsule_len = read_capsule_varint(reader).await?.into_inner();
        if capsule_len > MAX_CONNECT_UDP_CAPSULE_PAYLOAD_BYTES as u64 {
            return Err(HttpConnectClientError::CapsuleTooLarge {
                actual: capsule_len,
                limit: MAX_CONNECT_UDP_CAPSULE_PAYLOAD_BYTES,
            });
        }
        let capsule_len = capsule_len as usize;
        if capsule_type != DATAGRAM_CAPSULE_TYPE {
            discard_exact(reader, capsule_len).await?;
            continue;
        }

        let (context_id, consumed) =
            read_capsule_payload_varint_with_len(reader, capsule_len).await?;
        let payload_len = capsule_len - consumed;
        if context_id.into_inner() != UDP_CONTEXT_ID {
            discard_exact(reader, payload_len).await?;
            continue;
        }
        if payload_len > MAX_CONNECT_UDP_PAYLOAD_BYTES {
            discard_exact(reader, payload_len).await?;
            return Err(HttpConnectClientError::DatagramPayloadTooLarge {
                actual: payload_len,
                limit: MAX_CONNECT_UDP_PAYLOAD_BYTES,
            });
        }
        if payload_len > buffer.len() {
            discard_exact(reader, payload_len).await?;
            return Err(HttpConnectClientError::DatagramPayloadTooLarge {
                actual: payload_len,
                limit: buffer.len(),
            });
        }
        reader.read_exact(&mut buffer[..payload_len]).await?;
        return Ok(payload_len);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpConnectResponse {
    pub status: u16,
    pub header_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpConnectUdpResponse {
    pub status: u16,
    pub header_len: usize,
}

struct ParsedHttpResponse<'a> {
    status: u16,
    header_len: usize,
    headers: Vec<(&'a str, &'a [u8])>,
}

fn parse_http_response_headers(
    input: &[u8],
) -> Result<ParsedHttpResponse<'_>, HttpConnectClientError> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_HTTP_RESPONSE_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let header_len = match response.parse(input) {
        Ok(httparse::Status::Complete(header_len)) => header_len,
        Ok(httparse::Status::Partial) => return Err(HttpConnectClientError::Incomplete),
        Err(_) => return Err(HttpConnectClientError::InvalidResponse),
    };
    if !matches!(response.version, Some(0 | 1)) {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    let status = response
        .code
        .ok_or(HttpConnectClientError::InvalidResponse)?;
    let headers = response
        .headers
        .iter()
        .map(|header| (header.name, header.value))
        .collect();
    Ok(ParsedHttpResponse {
        status,
        header_len,
        headers,
    })
}

impl ParsedHttpResponse<'_> {
    fn header_has_value(&self, name: &str, expected: &str) -> bool {
        self.headers.iter().any(|(header_name, value)| {
            header_name.eq_ignore_ascii_case(name)
                && header_value(value).is_some_and(|value| value.eq_ignore_ascii_case(expected))
        })
    }

    fn header_has_token(&self, name: &str, expected: &str) -> bool {
        self.headers.iter().any(|(header_name, value)| {
            header_name.eq_ignore_ascii_case(name)
                && header_value(value).is_some_and(|value| {
                    value
                        .split(',')
                        .any(|token| token.trim().eq_ignore_ascii_case(expected))
                })
        })
    }
}

fn header_value(value: &[u8]) -> Option<&str> {
    std::str::from_utf8(value).ok().map(str::trim)
}

fn connect_udp_target_host(target: &TargetAddr) -> String {
    match target {
        TargetAddr::Domain { host, .. } => host.clone(),
        TargetAddr::Ip(addr) => addr.ip().to_string(),
    }
}

fn encode_capsule_varint(value: u64, out: &mut Vec<u8>) -> Result<(), HttpConnectClientError> {
    let value = VarInt::from_u64(value).map_err(|_| HttpConnectClientError::InvalidVarint)?;
    value.encode(out);
    Ok(())
}

fn decode_capsule_varint(input: &[u8]) -> Result<(VarInt, usize), HttpConnectClientError> {
    let original_len = input.len();
    let mut cursor = input;
    let value = VarInt::decode(&mut cursor).map_err(|_| HttpConnectClientError::InvalidVarint)?;
    Ok((value, original_len - cursor.len()))
}

fn capsule_varint_len(first: u8) -> usize {
    1usize << (usize::from(first >> 6))
}

async fn read_capsule_varint<R>(reader: &mut R) -> Result<VarInt, HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    Ok(read_capsule_varint_with_len(reader).await?.0)
}

async fn read_capsule_varint_with_len<R>(
    reader: &mut R,
) -> Result<(VarInt, usize), HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    let mut first = [0u8; 1];
    reader.read_exact(&mut first).await?;
    let len = 1usize << usize::from(first[0] >> 6);
    let mut buf = [0u8; VarInt::MAX_SIZE];
    buf[0] = first[0];
    if len > 1 {
        reader.read_exact(&mut buf[1..len]).await?;
    }
    Ok((decode_capsule_varint(&buf[..len])?.0, len))
}

async fn read_capsule_payload_varint_with_len<R>(
    reader: &mut R,
    payload_len: usize,
) -> Result<(VarInt, usize), HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    if payload_len == 0 {
        return Err(HttpConnectClientError::InvalidVarint);
    }
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf[..1]).await?;
    let len = capsule_varint_len(buf[0]);
    if len > payload_len {
        discard_exact(reader, payload_len.saturating_sub(1)).await?;
        return Err(HttpConnectClientError::InvalidVarint);
    }
    if len > 1 {
        reader.read_exact(&mut buf[1..len]).await?;
    }
    Ok((decode_capsule_varint(&buf[..len])?.0, len))
}

async fn discard_exact<R>(reader: &mut R, mut len: usize) -> Result<(), HttpConnectClientError>
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; 1024];
    while len > 0 {
        let chunk = len.min(buf.len());
        reader.read_exact(&mut buf[..chunk]).await?;
        len -= chunk;
    }
    Ok(())
}

#[derive(Debug)]
pub enum HttpConnectClientError {
    Incomplete,
    InvalidResponse,
    Io(std::io::Error),
    InvalidVarint,
    DatagramPayloadTooLarge { actual: usize, limit: usize },
    CapsuleTooLarge { actual: u64, limit: usize },
}

impl From<std::io::Error> for HttpConnectClientError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for HttpConnectClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete HTTP CONNECT response"),
            Self::InvalidResponse => write!(f, "invalid HTTP CONNECT response"),
            Self::Io(err) => write!(f, "{err}"),
            Self::InvalidVarint => write!(f, "invalid HTTP capsule variable-length integer"),
            Self::DatagramPayloadTooLarge { actual, limit } => {
                write!(
                    f,
                    "CONNECT-UDP datagram payload is {actual} bytes, limit is {limit}"
                )
            }
            Self::CapsuleTooLarge { actual, limit } => {
                write!(
                    f,
                    "HTTP capsule payload is {actual} bytes, limit is {limit}"
                )
            }
        }
    }
}

impl std::error::Error for HttpConnectClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Incomplete
            | Self::InvalidResponse
            | Self::InvalidVarint
            | Self::DatagramPayloadTooLarge { .. }
            | Self::CapsuleTooLarge { .. } => None,
        }
    }
}

#[cfg(test)]
#[path = "http_connect_test.rs"]
mod tests;
