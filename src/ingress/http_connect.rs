use crate::product::ProtocolTarget;
use crate::protocol::TargetAddr;
use std::net::{IpAddr, SocketAddr};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 256;

#[derive(Clone, PartialEq, Eq)]
pub enum HttpProxyRequest {
    Connect(ConnectRequest),
    Forward(ForwardRequest),
}

impl HttpProxyRequest {
    pub fn target(&self) -> &TargetAddr {
        match self {
            Self::Connect(request) => &request.target,
            Self::Forward(request) => &request.target,
        }
    }

    pub fn proxy_authorization(&self) -> Option<&str> {
        match self {
            Self::Connect(request) => request.proxy_authorization.as_deref(),
            Self::Forward(request) => request.proxy_authorization.as_deref(),
        }
    }
}

impl std::fmt::Debug for HttpProxyRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(request) => formatter.debug_tuple("Connect").field(request).finish(),
            Self::Forward(request) => formatter.debug_tuple("Forward").field(request).finish(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub target: TargetAddr,
    pub header_len: usize,
    pub proxy_authorization: Option<String>,
}

impl std::fmt::Debug for ConnectRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectRequest")
            .field("target", &self.target)
            .field("header_len", &self.header_len)
            .field(
                "proxy_authorization",
                &self.proxy_authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ForwardRequest {
    pub target: TargetAddr,
    pub rewritten_header: Vec<u8>,
    pub body_len: u64,
    pub proxy_authorization: Option<String>,
}

impl std::fmt::Debug for ForwardRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForwardRequest")
            .field("target", &self.target)
            .field("rewritten_header_len", &self.rewritten_header.len())
            .field("body_len", &self.body_len)
            .field(
                "proxy_authorization",
                &self.proxy_authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

pub fn parse_proxy_request(input: &[u8]) -> Result<HttpProxyRequest, HttpConnectError> {
    if input.len() > MAX_HEADER_BYTES {
        return Err(HttpConnectError::HeaderTooLarge);
    }
    let Some(header_end) = find_header_end(input) else {
        return Err(HttpConnectError::Incomplete);
    };
    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADER_COUNT];
    let mut parsed = httparse::Request::new(&mut headers);
    match parsed.parse(&input[..header_end]) {
        Ok(httparse::Status::Complete(consumed)) if consumed == header_end => {}
        Ok(_) | Err(_) => return Err(HttpConnectError::InvalidRequest),
    }
    let method = parsed.method.ok_or(HttpConnectError::InvalidRequest)?;
    let request_target = parsed.path.ok_or(HttpConnectError::InvalidRequest)?;
    let version = match parsed.version {
        Some(0) => "HTTP/1.0",
        Some(1) => "HTTP/1.1",
        _ => return Err(HttpConnectError::InvalidRequest),
    };
    let proxy_authorization = unique_proxy_authorization(parsed.headers)?;

    if method == "CONNECT" {
        return Ok(HttpProxyRequest::Connect(ConnectRequest {
            target: parse_authority(request_target)?,
            header_len: header_end,
            proxy_authorization,
        }));
    }

    let (target, origin_form) = parse_absolute_http_target(request_target)?;
    let body_len = request_body_len(parsed.headers)?;
    let connection_headers = connection_named_headers(parsed.headers)?;
    if connection_headers.iter().any(|name| {
        matches!(
            name.as_str(),
            "content-length" | "host" | "transfer-encoding"
        )
    }) {
        return Err(HttpConnectError::InvalidRequest);
    }

    let mut rewritten_header = Vec::with_capacity(header_end);
    rewritten_header.extend_from_slice(method.as_bytes());
    rewritten_header.push(b' ');
    rewritten_header.extend_from_slice(origin_form.as_bytes());
    rewritten_header.push(b' ');
    rewritten_header.extend_from_slice(version.as_bytes());
    rewritten_header.extend_from_slice(b"\r\nHost: ");
    rewritten_header.extend_from_slice(target.authority().as_bytes());
    rewritten_header.extend_from_slice(b"\r\n");
    for header in parsed.headers.iter() {
        let name = header.name.to_ascii_lowercase();
        if name == "host"
            || name == "connection"
            || name == "keep-alive"
            || name == "proxy-authenticate"
            || name == "proxy-authorization"
            || name == "proxy-connection"
            || name == "te"
            || name == "trailer"
            || name == "upgrade"
            || connection_headers.contains(&name)
        {
            continue;
        }
        if name == "transfer-encoding" {
            return Err(HttpConnectError::UnsupportedTransferEncoding);
        }
        rewritten_header.extend_from_slice(header.name.as_bytes());
        rewritten_header.extend_from_slice(b": ");
        rewritten_header.extend_from_slice(header.value);
        rewritten_header.extend_from_slice(b"\r\n");
    }
    rewritten_header.extend_from_slice(b"Connection: close\r\n\r\n");
    if rewritten_header.len() > MAX_HEADER_BYTES {
        return Err(HttpConnectError::HeaderTooLarge);
    }
    Ok(HttpProxyRequest::Forward(ForwardRequest {
        target,
        rewritten_header,
        body_len,
        proxy_authorization,
    }))
}

pub fn parse_connect_request(input: &[u8]) -> Result<ConnectRequest, HttpConnectError> {
    let method = std::str::from_utf8(input)
        .ok()
        .and_then(|request| request.split_whitespace().next())
        .ok_or(HttpConnectError::InvalidRequest)?;
    if method != "CONNECT" {
        return Err(HttpConnectError::UnsupportedMethod(method.to_string()));
    }
    match parse_proxy_request(input)? {
        HttpProxyRequest::Connect(request) => Ok(request),
        HttpProxyRequest::Forward(_) => Err(HttpConnectError::InvalidRequest),
    }
}

pub fn success_response() -> &'static [u8] {
    b"HTTP/1.1 200 Connection Established\r\n\r\n"
}

pub fn error_response(status: HttpStatus) -> &'static [u8] {
    match status {
        HttpStatus::BadRequest => b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
        HttpStatus::BadGateway => b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n",
        HttpStatus::ServiceUnavailable => {
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"
        }
        HttpStatus::Forbidden => b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n",
        HttpStatus::MethodNotAllowed => {
            b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n"
        }
        HttpStatus::ProxyAuthenticationRequired => {
            b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"mptunnel\"\r\nContent-Length: 0\r\n\r\n"
        }
    }
}

fn unique_proxy_authorization(
    headers: &[httparse::Header<'_>],
) -> Result<Option<String>, HttpConnectError> {
    let mut value = None;
    for header in headers {
        if !header.name.eq_ignore_ascii_case("Proxy-Authorization") {
            continue;
        }
        if value.is_some() {
            return Err(HttpConnectError::InvalidRequest);
        }
        let parsed = std::str::from_utf8(header.value)
            .map_err(|_| HttpConnectError::InvalidRequest)?
            .trim();
        if !parsed.is_ascii() {
            return Err(HttpConnectError::InvalidRequest);
        }
        value = Some(parsed.to_string());
    }
    Ok(value)
}

fn find_header_end(input: &[u8]) -> Option<usize> {
    input
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_authority(authority: &str) -> Result<TargetAddr, HttpConnectError> {
    if authority.is_empty() {
        return Err(HttpConnectError::InvalidAuthority);
    }
    if authority.starts_with('[') {
        let addr = authority
            .parse::<SocketAddr>()
            .map_err(|_| HttpConnectError::InvalidAuthority)?;
        if addr.port() == 0 {
            return Err(HttpConnectError::InvalidPort);
        }
        return Ok(TargetAddr::Ip(addr));
    }

    if let Ok(addr) = authority.parse::<SocketAddr>() {
        if addr.port() == 0 {
            return Err(HttpConnectError::InvalidPort);
        }
        return Ok(TargetAddr::Ip(addr));
    }

    let Some((host, port)) = authority.rsplit_once(':') else {
        return Err(HttpConnectError::InvalidAuthority);
    };
    if host.is_empty() || host.contains('/') || host.contains('@') || host.contains(' ') {
        return Err(HttpConnectError::InvalidAuthority);
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| HttpConnectError::InvalidPort)?;
    if port == 0 {
        return Err(HttpConnectError::InvalidPort);
    }
    Ok(TargetAddr::Domain {
        host: host.to_string(),
        port,
    })
}

fn parse_absolute_http_target(
    request_target: &str,
) -> Result<(TargetAddr, String), HttpConnectError> {
    let Some(scheme) = request_target.get(..7) else {
        return Err(HttpConnectError::UnsupportedScheme);
    };
    if !scheme.eq_ignore_ascii_case("http://") {
        return Err(HttpConnectError::UnsupportedScheme);
    }
    let absolute = &request_target[7..];
    if absolute.contains('#') {
        return Err(HttpConnectError::InvalidRequest);
    }
    let authority_end = absolute.find(['/', '?']).unwrap_or(absolute.len());
    let authority = &absolute[..authority_end];
    let suffix = &absolute[authority_end..];
    let origin_form = if suffix.is_empty() {
        "/".to_string()
    } else if suffix.starts_with('?') {
        format!("/{suffix}")
    } else {
        suffix.to_string()
    };
    Ok((parse_http_authority(authority)?, origin_form))
}

fn parse_http_authority(authority: &str) -> Result<TargetAddr, HttpConnectError> {
    if authority.is_empty()
        || authority.contains(['/', '\\', '?', '#', '@'])
        || authority.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
    {
        return Err(HttpConnectError::InvalidAuthority);
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let closing = rest.find(']').ok_or(HttpConnectError::InvalidAuthority)?;
        let host = &rest[..closing];
        let trailing = &rest[closing + 1..];
        let port = if trailing.is_empty() {
            80
        } else {
            parse_explicit_port(
                trailing
                    .strip_prefix(':')
                    .ok_or(HttpConnectError::InvalidAuthority)?,
            )?
        };
        let address = host
            .parse::<std::net::Ipv6Addr>()
            .map_err(|_| HttpConnectError::InvalidAuthority)?;
        return Ok(TargetAddr::Ip(SocketAddr::new(IpAddr::V6(address), port)));
    } else {
        if authority.contains(['[', ']']) || authority.matches(':').count() > 1 {
            return Err(HttpConnectError::InvalidAuthority);
        }
        match authority.rsplit_once(':') {
            Some((host, port)) => (host, parse_explicit_port(port)?),
            None => (authority, 80),
        }
    };
    let normalized = ProtocolTarget::from_host_port(host, port)
        .map_err(|_| HttpConnectError::InvalidAuthority)?;
    match normalized.ip() {
        Some(address) => Ok(TargetAddr::Ip(SocketAddr::new(address, port))),
        None => Ok(TargetAddr::Domain {
            host: normalized
                .domain()
                .expect("non-IP HTTP target has a normalized domain")
                .as_str()
                .to_string(),
            port,
        }),
    }
}

fn parse_explicit_port(port: &str) -> Result<u16, HttpConnectError> {
    let port = port
        .parse::<u16>()
        .map_err(|_| HttpConnectError::InvalidPort)?;
    if port == 0 {
        return Err(HttpConnectError::InvalidPort);
    }
    Ok(port)
}

fn request_body_len(headers: &[httparse::Header<'_>]) -> Result<u64, HttpConnectError> {
    let mut body_len = None;
    for header in headers {
        if header.name.eq_ignore_ascii_case("Transfer-Encoding") {
            return Err(HttpConnectError::UnsupportedTransferEncoding);
        }
        if !header.name.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        if body_len.is_some() {
            return Err(HttpConnectError::InvalidRequest);
        }
        let value = std::str::from_utf8(header.value)
            .map_err(|_| HttpConnectError::InvalidRequest)?
            .trim();
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(HttpConnectError::InvalidRequest);
        }
        body_len = Some(
            value
                .parse::<u64>()
                .map_err(|_| HttpConnectError::InvalidRequest)?,
        );
    }
    Ok(body_len.unwrap_or(0))
}

fn connection_named_headers(
    headers: &[httparse::Header<'_>],
) -> Result<Vec<String>, HttpConnectError> {
    let mut named = Vec::new();
    for header in headers {
        if !header.name.eq_ignore_ascii_case("Connection") {
            continue;
        }
        let value =
            std::str::from_utf8(header.value).map_err(|_| HttpConnectError::InvalidRequest)?;
        for token in value.split(',').map(str::trim) {
            if token.is_empty() || !token.bytes().all(is_http_token_byte) {
                return Err(HttpConnectError::InvalidRequest);
            }
            let token = token.to_ascii_lowercase();
            if !named.contains(&token) {
                named.push(token);
            }
        }
    }
    Ok(named)
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpStatus {
    BadRequest,
    BadGateway,
    ServiceUnavailable,
    Forbidden,
    MethodNotAllowed,
    ProxyAuthenticationRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpConnectError {
    Incomplete,
    HeaderTooLarge,
    InvalidRequest,
    UnsupportedMethod(String),
    UnsupportedVersion(String),
    InvalidAuthority,
    InvalidPort,
    UnsupportedScheme,
    UnsupportedTransferEncoding,
}

impl std::fmt::Display for HttpConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete HTTP CONNECT request"),
            Self::HeaderTooLarge => write!(f, "HTTP CONNECT header is too large"),
            Self::InvalidRequest => write!(f, "invalid HTTP CONNECT request"),
            Self::UnsupportedMethod(method) => write!(f, "unsupported HTTP method {method:?}"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported HTTP version {version:?}")
            }
            Self::InvalidAuthority => write!(f, "invalid HTTP CONNECT authority"),
            Self::InvalidPort => write!(f, "HTTP CONNECT target port must be greater than zero"),
            Self::UnsupportedScheme => {
                write!(
                    f,
                    "HTTP forward proxy request must use an absolute http:// target"
                )
            }
            Self::UnsupportedTransferEncoding => {
                write!(
                    f,
                    "HTTP forward proxy request transfer encoding is unsupported"
                )
            }
        }
    }
}

impl std::error::Error for HttpConnectError {}

#[cfg(test)]
#[path = "tests_http_connect.rs"]
mod tests;
