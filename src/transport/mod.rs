pub mod encrypted;
pub mod encrypted_udp;
pub mod framed;
pub mod tcp;
pub mod udp;

use crate::protocol::UnderlayProtocol;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, EndpointParseError> {
        let host = host.into();
        if host.is_empty() {
            return Err(EndpointParseError::EmptyHost);
        }
        if port == 0 {
            return Err(EndpointParseError::InvalidPort);
        }
        Ok(Self { host, port })
    }
}

impl FromStr for Endpoint {
    type Err = EndpointParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(EndpointParseError::Empty);
        }

        if let Some(rest) = value.strip_prefix('[') {
            let Some((host, tail)) = rest.split_once(']') else {
                return Err(EndpointParseError::InvalidIpv6);
            };
            let Some(port) = tail.strip_prefix(':') else {
                return Err(EndpointParseError::MissingPort);
            };
            return Self::new(host, parse_port(port)?);
        }

        let Some((host, port)) = value.rsplit_once(':') else {
            return Err(EndpointParseError::MissingPort);
        };
        Self::new(host, parse_port(port)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathSpec {
    pub underlay: UnderlayProtocol,
    pub endpoint: Endpoint,
}

impl FromStr for PathSpec {
    type Err = PathSpecParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((scheme, endpoint)) = value.split_once("://") else {
            return Err(PathSpecParseError::MissingScheme);
        };
        let underlay = match scheme {
            "tcp" => UnderlayProtocol::Tcp,
            "udp" => UnderlayProtocol::Udp,
            _ => return Err(PathSpecParseError::UnknownScheme(scheme.to_string())),
        };
        Ok(Self {
            underlay,
            endpoint: endpoint.parse()?,
        })
    }
}

fn parse_port(value: &str) -> Result<u16, EndpointParseError> {
    let port = value
        .parse::<u16>()
        .map_err(|_| EndpointParseError::InvalidPort)?;
    if port == 0 {
        return Err(EndpointParseError::InvalidPort);
    }
    Ok(port)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointParseError {
    Empty,
    EmptyHost,
    MissingPort,
    InvalidIpv6,
    InvalidPort,
}

impl std::fmt::Display for EndpointParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "endpoint is empty"),
            Self::EmptyHost => write!(f, "endpoint host is empty"),
            Self::MissingPort => write!(f, "endpoint must include a port"),
            Self::InvalidIpv6 => write!(f, "IPv6 endpoint must use [addr]:port syntax"),
            Self::InvalidPort => write!(f, "endpoint port must be in 1..=65535"),
        }
    }
}

impl std::error::Error for EndpointParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSpecParseError {
    MissingScheme,
    UnknownScheme(String),
    Endpoint(EndpointParseError),
}

impl From<EndpointParseError> for PathSpecParseError {
    fn from(value: EndpointParseError) -> Self {
        Self::Endpoint(value)
    }
}

impl std::fmt::Display for PathSpecParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingScheme => write!(f, "path must use tcp://host:port or udp://host:port"),
            Self::UnknownScheme(scheme) => write!(f, "unknown path scheme {scheme:?}"),
            Self::Endpoint(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for PathSpecParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_specs_parse_tcp_and_udp() {
        let tcp = "tcp://example.com:443".parse::<PathSpec>().expect("tcp");
        let udp = "udp://[2001:db8::1]:8443".parse::<PathSpec>().expect("udp");

        assert_eq!(tcp.underlay, UnderlayProtocol::Tcp);
        assert_eq!(tcp.endpoint.host, "example.com");
        assert_eq!(tcp.endpoint.port, 443);
        assert_eq!(udp.underlay, UnderlayProtocol::Udp);
        assert_eq!(udp.endpoint.host, "2001:db8::1");
        assert_eq!(udp.endpoint.port, 8443);
    }

    #[test]
    fn path_specs_reject_ambiguous_values() {
        assert!("example.com:443".parse::<PathSpec>().is_err());
        assert!("tcp://example.com".parse::<PathSpec>().is_err());
        assert!("udp://example.com:0".parse::<PathSpec>().is_err());
    }
}
