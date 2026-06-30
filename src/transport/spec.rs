use crate::protocol::{PathCapabilities, RateHint, UnderlayProtocol};
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
    pub metadata: PathMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpEngine {
    Quic,
    CustomLab,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathMetadata {
    pub capabilities: PathCapabilities,
    pub initial_srtt_ms: Option<u32>,
    pub initial_jitter_ms: Option<u32>,
    pub initial_rate: RateHint,
    pub initial_mtu_payload_bytes: Option<usize>,
    pub udp_engine: UdpEngine,
}

impl Default for PathMetadata {
    fn default() -> Self {
        Self {
            capabilities: PathCapabilities::default(),
            initial_srtt_ms: None,
            initial_jitter_ms: None,
            initial_rate: RateHint::Unknown,
            initial_mtu_payload_bytes: None,
            udp_engine: UdpEngine::Quic,
        }
    }
}

impl FromStr for PathSpec {
    type Err = PathSpecParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((scheme, path)) = value.split_once("://") else {
            return Err(PathSpecParseError::MissingScheme);
        };
        let underlay = match scheme {
            "tcp" => UnderlayProtocol::Tcp,
            "udp" => UnderlayProtocol::Udp,
            _ => return Err(PathSpecParseError::UnknownScheme(scheme.to_string())),
        };
        let (endpoint, query) = path
            .split_once('?')
            .map_or((path, None), |(endpoint, query)| (endpoint, Some(query)));
        let (metadata, udp_engine_set) = match query {
            Some(query) => parse_path_metadata(query)?,
            None => (PathMetadata::default(), false),
        };
        if underlay == UnderlayProtocol::Udp && metadata.capabilities.no_udp {
            return Err(PathSpecParseError::NoUdpOnUdpPath);
        }
        if underlay == UnderlayProtocol::Tcp && udp_engine_set {
            return Err(PathSpecParseError::UdpEngineOnTcpPath);
        }
        Ok(Self {
            underlay,
            endpoint: endpoint.parse()?,
            metadata,
        })
    }
}

fn parse_path_metadata(query: &str) -> Result<(PathMetadata, bool), PathSpecParseError> {
    if query.is_empty() {
        return Err(PathSpecParseError::EmptyQuery);
    }
    let mut metadata = PathMetadata::default();
    let mut srtt_set = false;
    let mut jitter_set = false;
    let mut rate_set = false;
    let mut mtu_set = false;
    let mut engine_set = false;
    for part in query.split('&') {
        if part.is_empty() {
            return Err(PathSpecParseError::EmptyQueryParam);
        }
        let (key, value) = part
            .split_once('=')
            .map_or((part, None), |(key, value)| (key, Some(value)));
        match key {
            "srtt-ms" | "rtt-ms" => {
                reject_duplicate(srtt_set, key)?;
                srtt_set = true;
                metadata.initial_srtt_ms = Some(parse_u32_param(key, value)?);
            }
            "jitter-ms" => {
                reject_duplicate(jitter_set, key)?;
                jitter_set = true;
                metadata.initial_jitter_ms = Some(parse_u32_param(key, value)?);
            }
            "rate-bps" => {
                reject_duplicate(rate_set, key)?;
                rate_set = true;
                metadata.initial_rate = RateHint::BitsPerSecond(parse_u64_param(key, value)?);
            }
            "rate-kbps" => {
                reject_duplicate(rate_set, key)?;
                rate_set = true;
                metadata.initial_rate = RateHint::BitsPerSecond(
                    parse_u64_param(key, value)?
                        .checked_mul(1_000)
                        .ok_or(PathSpecParseError::QueryParamOverflow(key.to_string()))?,
                );
            }
            "rate-mbps" => {
                reject_duplicate(rate_set, key)?;
                rate_set = true;
                metadata.initial_rate = RateHint::BitsPerSecond(
                    parse_u64_param(key, value)?
                        .checked_mul(1_000_000)
                        .ok_or(PathSpecParseError::QueryParamOverflow(key.to_string()))?,
                );
            }
            "rate" => {
                reject_duplicate(rate_set, key)?;
                rate_set = true;
                metadata.initial_rate = match value
                    .ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?
                {
                    "unknown" => RateHint::Unknown,
                    "unlimited" => RateHint::Unlimited,
                    value => {
                        return Err(PathSpecParseError::InvalidQueryParamValue(
                            key.to_string(),
                            value.to_string(),
                        ));
                    }
                };
            }
            "mtu" | "mtu-bytes" | "payload-mtu" => {
                reject_duplicate(mtu_set, key)?;
                mtu_set = true;
                metadata.initial_mtu_payload_bytes = Some(parse_mtu_param(key, value)?);
            }
            "engine" => {
                reject_duplicate(engine_set, key)?;
                engine_set = true;
                metadata.udp_engine = parse_udp_engine_param(key, value)?;
            }
            "backup" => metadata.capabilities.backup = parse_bool_param(key, value)?,
            "expensive" => metadata.capabilities.expensive = parse_bool_param(key, value)?,
            "low-latency" => metadata.capabilities.low_latency = parse_bool_param(key, value)?,
            "bulk-allowed" | "bulk" => {
                metadata.capabilities.bulk_allowed = parse_bool_param(key, value)?
            }
            "no-bulk" => metadata.capabilities.bulk_allowed = !parse_bool_param(key, value)?,
            "probe-only" => metadata.capabilities.probe_only = parse_bool_param(key, value)?,
            "no-udp" => metadata.capabilities.no_udp = parse_bool_param(key, value)?,
            _ => return Err(PathSpecParseError::UnknownQueryParam(key.to_string())),
        }
    }
    Ok((metadata, engine_set))
}

fn parse_udp_engine_param(key: &str, value: Option<&str>) -> Result<UdpEngine, PathSpecParseError> {
    match value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))? {
        "quic" => Ok(UdpEngine::Quic),
        "custom-lab" => Ok(UdpEngine::CustomLab),
        value => Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        )),
    }
}

fn reject_duplicate(seen: bool, key: &str) -> Result<(), PathSpecParseError> {
    if seen {
        Err(PathSpecParseError::DuplicateQueryParam(key.to_string()))
    } else {
        Ok(())
    }
}

fn parse_u32_param(key: &str, value: Option<&str>) -> Result<u32, PathSpecParseError> {
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    value
        .parse::<u32>()
        .map_err(|_| PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string()))
}

fn parse_u64_param(key: &str, value: Option<&str>) -> Result<u64, PathSpecParseError> {
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    value
        .parse::<u64>()
        .map_err(|_| PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string()))
}

fn parse_mtu_param(key: &str, value: Option<&str>) -> Result<usize, PathSpecParseError> {
    const MIN_MTU: usize = 512;
    const MAX_MTU: usize = 65_000;
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    let mtu = value.parse::<usize>().map_err(|_| {
        PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string())
    })?;
    if !(MIN_MTU..=MAX_MTU).contains(&mtu) {
        return Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        ));
    }
    Ok(mtu)
}

fn parse_bool_param(key: &str, value: Option<&str>) -> Result<bool, PathSpecParseError> {
    match value.unwrap_or("true") {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        value => Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        )),
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
    EmptyQuery,
    EmptyQueryParam,
    UnknownQueryParam(String),
    MissingQueryParamValue(String),
    InvalidQueryParamValue(String, String),
    DuplicateQueryParam(String),
    QueryParamOverflow(String),
    NoUdpOnUdpPath,
    UdpEngineOnTcpPath,
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
            Self::EmptyQuery => write!(f, "path query must not be empty"),
            Self::EmptyQueryParam => write!(f, "path query parameter must not be empty"),
            Self::UnknownQueryParam(key) => write!(f, "unknown path query parameter {key:?}"),
            Self::MissingQueryParamValue(key) => {
                write!(f, "path query parameter {key:?} requires a value")
            }
            Self::InvalidQueryParamValue(key, value) => {
                write!(
                    f,
                    "invalid value {value:?} for path query parameter {key:?}"
                )
            }
            Self::DuplicateQueryParam(key) => {
                write!(f, "duplicate path query parameter {key:?}")
            }
            Self::QueryParamOverflow(key) => {
                write!(f, "path query parameter {key:?} is too large")
            }
            Self::NoUdpOnUdpPath => write!(f, "udp:// paths cannot set no-udp=true"),
            Self::UdpEngineOnTcpPath => write!(f, "engine is only valid on udp:// paths"),
        }
    }
}

impl std::error::Error for PathSpecParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_specs_parse_tcp_and_udp() {
        let tcp = "tcp://example.com:443?srtt-ms=20&rate-mbps=30&low-latency=true"
            .parse::<PathSpec>()
            .expect("tcp");
        let udp = "udp://[2001:db8::1]:8443?jitter-ms=5&rate-bps=100000000&mtu=1400"
            .parse::<PathSpec>()
            .expect("udp");
        let udp_quic = "udp://example.com:8443?engine=quic"
            .parse::<PathSpec>()
            .expect("udp quic");

        assert_eq!(tcp.underlay, UnderlayProtocol::Tcp);
        assert_eq!(tcp.endpoint.host, "example.com");
        assert_eq!(tcp.endpoint.port, 443);
        assert_eq!(tcp.metadata.initial_srtt_ms, Some(20));
        assert_eq!(
            tcp.metadata.initial_rate,
            RateHint::BitsPerSecond(30_000_000)
        );
        assert!(tcp.metadata.capabilities.low_latency);
        assert_eq!(udp.underlay, UnderlayProtocol::Udp);
        assert_eq!(udp.endpoint.host, "2001:db8::1");
        assert_eq!(udp.endpoint.port, 8443);
        assert_eq!(udp.metadata.initial_jitter_ms, Some(5));
        assert_eq!(
            udp.metadata.initial_rate,
            RateHint::BitsPerSecond(100_000_000)
        );
        assert_eq!(udp.metadata.initial_mtu_payload_bytes, Some(1400));
        assert_eq!(udp.metadata.udp_engine, UdpEngine::Quic);
        assert_eq!(udp_quic.metadata.udp_engine, UdpEngine::Quic);
    }

    #[test]
    fn path_specs_reject_ambiguous_values() {
        assert!("example.com:443".parse::<PathSpec>().is_err());
        assert!("tcp://example.com".parse::<PathSpec>().is_err());
        assert!("udp://example.com:0".parse::<PathSpec>().is_err());
        assert!("tcp://example.com:443?".parse::<PathSpec>().is_err());
        assert!(
            "tcp://example.com:443?unknown=true"
                .parse::<PathSpec>()
                .is_err()
        );
        assert!(
            "udp://example.com:443?no-udp=true"
                .parse::<PathSpec>()
                .is_err()
        );
        assert!("udp://example.com:443?mtu=100".parse::<PathSpec>().is_err());
        assert!(
            "tcp://example.com:443?engine=quic"
                .parse::<PathSpec>()
                .is_err()
        );
        assert!(
            "udp://example.com:443?engine=raw"
                .parse::<PathSpec>()
                .is_err()
        );
        assert!(
            "udp://example.com:443?engine=quic&engine=custom-lab"
                .parse::<PathSpec>()
                .is_err()
        );
    }
}
