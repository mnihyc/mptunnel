use crate::outbound::{OutboundError, ProxyCredentials, validate_target};
use crate::protocol::TargetAddr;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

const MAX_HTTP_RESPONSE_HEADERS: usize = 64;
const MAX_HTTP_CONNECT_REQUEST_BYTES: usize = 16 * 1024;

pub fn connect_request(
    target: &TargetAddr,
    host_header: Option<&str>,
    credentials: Option<&ProxyCredentials>,
) -> Result<Vec<u8>, OutboundError> {
    validate_target(target)?;
    let authority = target.authority();
    let host = host_header.unwrap_or(&authority);
    validate_header_value(host)?;
    build_request(
        format!("CONNECT {authority} HTTP/1.1\r\nHost: {host}\r\n"),
        credentials,
        "Proxy-Connection: Keep-Alive\r\n\r\n",
    )
}

fn build_request(
    mut prefix: String,
    credentials: Option<&ProxyCredentials>,
    suffix: &str,
) -> Result<Vec<u8>, OutboundError> {
    if let Some(credentials) = credentials {
        let mut userinfo =
            String::with_capacity(credentials.username().len() + credentials.password().len() + 1);
        userinfo.push_str(credentials.username());
        userinfo.push(':');
        userinfo.push_str(credentials.password());
        let encoded = BASE64_STANDARD.encode(userinfo.as_bytes());
        prefix.push_str("Proxy-Authorization: Basic ");
        prefix.push_str(&encoded);
        prefix.push_str("\r\n");
    }
    prefix.push_str(suffix);
    if prefix.len() > MAX_HTTP_CONNECT_REQUEST_BYTES {
        return Err(OutboundError::ProxyRequestTooLarge);
    }
    Ok(prefix.into_bytes())
}

fn validate_header_value(value: &str) -> Result<(), OutboundError> {
    if value.is_empty() || value.bytes().any(|byte| byte <= b' ' || byte == 0x7f) {
        return Err(OutboundError::InvalidProxyHeaderValue);
    }
    Ok(())
}

pub fn parse_connect_response(input: &[u8]) -> Result<HttpConnectResponse, HttpConnectClientError> {
    let parsed = parse_http_response_headers(input)?;
    Ok(HttpConnectResponse {
        status: parsed.status,
        header_len: parsed.header_len,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpConnectResponse {
    pub status: u16,
    pub header_len: usize,
}

struct ParsedHttpResponse {
    status: u16,
    header_len: usize,
}

fn parse_http_response_headers(input: &[u8]) -> Result<ParsedHttpResponse, HttpConnectClientError> {
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
    Ok(ParsedHttpResponse { status, header_len })
}

#[derive(Debug)]
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
#[path = "http_connect_test.rs"]
mod tests;
