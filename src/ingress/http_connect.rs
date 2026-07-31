use crate::protocol::TargetAddr;
use std::net::SocketAddr;

const MAX_HEADER_BYTES: usize = 64 * 1024;

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

pub fn parse_connect_request(input: &[u8]) -> Result<ConnectRequest, HttpConnectError> {
    if input.len() > MAX_HEADER_BYTES {
        return Err(HttpConnectError::HeaderTooLarge);
    }
    let Some(header_end) = find_header_end(input) else {
        return Err(HttpConnectError::Incomplete);
    };
    let request =
        std::str::from_utf8(&input[..header_end]).map_err(|_| HttpConnectError::InvalidRequest)?;
    let Some(request_line) = request.lines().next() else {
        return Err(HttpConnectError::InvalidRequest);
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or(HttpConnectError::InvalidRequest)?;
    let authority = parts.next().ok_or(HttpConnectError::InvalidRequest)?;
    let version = parts.next().ok_or(HttpConnectError::InvalidRequest)?;
    if parts.next().is_some() {
        return Err(HttpConnectError::InvalidRequest);
    }
    if method != "CONNECT" {
        return Err(HttpConnectError::UnsupportedMethod(method.to_string()));
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpConnectError::UnsupportedVersion(version.to_string()));
    }
    let proxy_authorization = proxy_authorization_header(request);
    Ok(ConnectRequest {
        target: parse_authority(authority)?,
        header_len: header_end,
        proxy_authorization,
    })
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

fn proxy_authorization_header(request: &str) -> Option<String> {
    request.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("Proxy-Authorization")
            .then(|| value.trim().to_string())
    })
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
        }
    }
}

impl std::error::Error for HttpConnectError {}

#[cfg(test)]
#[path = "http_connect_test.rs"]
mod tests;
