use crate::outbound::{OutboundError, validate_target};
use crate::protocol::TargetAddr;

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

pub fn parse_connect_response(input: &[u8]) -> Result<HttpConnectResponse, HttpConnectClientError> {
    let Some(header_end) = input
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
    else {
        return Err(HttpConnectClientError::Incomplete);
    };
    let header = std::str::from_utf8(&input[..header_end])
        .map_err(|_| HttpConnectClientError::InvalidResponse)?;
    let Some(status_line) = header.lines().next() else {
        return Err(HttpConnectClientError::InvalidResponse);
    };
    let mut parts = status_line.split_whitespace();
    let version = parts
        .next()
        .ok_or(HttpConnectClientError::InvalidResponse)?;
    let status = parts
        .next()
        .ok_or(HttpConnectClientError::InvalidResponse)?
        .parse::<u16>()
        .map_err(|_| HttpConnectClientError::InvalidResponse)?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpConnectClientError::InvalidResponse);
    }
    Ok(HttpConnectResponse {
        status,
        header_len: header_end,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpConnectResponse {
    pub status: u16,
    pub header_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpConnectClientError {
    Incomplete,
    InvalidResponse,
}

impl std::fmt::Display for HttpConnectClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incomplete => write!(f, "incomplete HTTP CONNECT response"),
            Self::InvalidResponse => write!(f, "invalid HTTP CONNECT response"),
        }
    }
}

impl std::error::Error for HttpConnectClientError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_http_connect_request() {
        let request = connect_request(
            &TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            None,
        )
        .expect("request");

        assert_eq!(
            request,
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nProxy-Connection: Keep-Alive\r\n\r\n"
        );
    }

    #[test]
    fn parses_http_connect_response() {
        let response =
            parse_connect_response(b"HTTP/1.1 200 Connection Established\r\n\r\npayload")
                .expect("response");

        assert_eq!(
            response,
            HttpConnectResponse {
                status: 200,
                header_len: 39
            }
        );
    }
}
