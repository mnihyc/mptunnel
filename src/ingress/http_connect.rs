use crate::protocol::TargetAddr;
use std::net::SocketAddr;

const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub target: TargetAddr,
    pub header_len: usize,
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
    Ok(ConnectRequest {
        target: parse_authority(authority)?,
        header_len: header_end,
    })
}

pub fn success_response() -> &'static [u8] {
    b"HTTP/1.1 200 Connection Established\r\n\r\n"
}

pub fn error_response(status: HttpStatus) -> &'static [u8] {
    match status {
        HttpStatus::BadRequest => b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n",
        HttpStatus::BadGateway => b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n",
        HttpStatus::MethodNotAllowed => {
            b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n"
        }
    }
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
    MethodNotAllowed,
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
mod tests {
    use super::*;

    #[test]
    fn parses_domain_connect_request() {
        let input = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\nextra";
        let request = parse_connect_request(input).expect("connect");

        let expected_header_len = input
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|pos| pos + 4)
            .expect("header delimiter");
        assert_eq!(request.header_len, expected_header_len);
        assert_eq!(
            request.target,
            TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443
            }
        );
    }

    #[test]
    fn parses_ip_authorities() {
        let ipv4 =
            parse_connect_request(b"CONNECT 192.0.2.10:8443 HTTP/1.1\r\n\r\n").expect("ipv4");
        assert_eq!(
            ipv4.target,
            TargetAddr::Ip("192.0.2.10:8443".parse().expect("addr"))
        );

        let ipv6 =
            parse_connect_request(b"CONNECT [2001:db8::1]:443 HTTP/1.1\r\n\r\n").expect("ipv6");
        assert_eq!(
            ipv6.target,
            TargetAddr::Ip("[2001:db8::1]:443".parse().expect("addr"))
        );
    }

    #[test]
    fn rejects_non_connect_and_bad_authority() {
        assert!(matches!(
            parse_connect_request(b"GET example.com:443 HTTP/1.1\r\n\r\n"),
            Err(HttpConnectError::UnsupportedMethod(_))
        ));
        assert_eq!(
            parse_connect_request(b"CONNECT example.com HTTP/1.1\r\n\r\n"),
            Err(HttpConnectError::InvalidAuthority)
        );
        assert_eq!(
            parse_connect_request(b"CONNECT example.com:0 HTTP/1.1\r\n\r\n"),
            Err(HttpConnectError::InvalidPort)
        );
    }

    #[test]
    fn builds_responses() {
        assert_eq!(
            success_response(),
            b"HTTP/1.1 200 Connection Established\r\n\r\n"
        );
        assert_eq!(
            error_response(HttpStatus::BadGateway),
            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"
        );
    }
}
