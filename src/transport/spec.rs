//! Endpoint-local carrier specifications.
//!
//! Binding, policy, and startup hints configure this endpoint only and never
//! become peer protocol authority or measured capacity evidence.

pub use crate::model::path::PathPolicy;
use crate::protocol::UnderlayProtocol;
use std::net::IpAddr;
use std::num::NonZeroU16;
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT_CARRIER_PORT_HOP_INTERVAL_MS: u32 = 5 * 60 * 1_000;
pub const MIN_CARRIER_PORT_HOP_INTERVAL_MS: u32 = 5 * 1_000;
pub const DEFAULT_TCP_CARRIER_MIN: u16 = 1;
pub const DEFAULT_TCP_CARRIER_MAX: u16 = 3;

/// Inclusive local concurrency bounds for one configured TCP endpoint.
///
/// These values never enter MPP frames. Each member remains an independently
/// authenticated carrier with its own `PathId` and physical lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCarrierRange {
    min: u16,
    max: u16,
}

impl TcpCarrierRange {
    pub fn new(min: u16, max: u16) -> Result<Self, TcpCarrierRangeError> {
        if min == 0 || min > max {
            return Err(TcpCarrierRangeError);
        }
        Ok(Self { min, max })
    }

    pub const fn min(self) -> u16 {
        self.min
    }

    pub const fn max(self) -> u16 {
        self.max
    }
}

impl Default for TcpCarrierRange {
    fn default() -> Self {
        Self {
            min: DEFAULT_TCP_CARRIER_MIN,
            max: DEFAULT_TCP_CARRIER_MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpCarrierRangeError;

impl std::fmt::Display for TcpCarrierRangeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TCP carrier range must satisfy 1 <= MIN <= MAX <= 65535")
    }
}

impl std::error::Error for TcpCarrierRangeError {}

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

/// Bounded inclusive destination-port set for one configured carrier path.
///
/// The interval is never expanded. A concrete port is selected once for each
/// new physical carrier establishment and remains fixed across that
/// establishment's DNS address race.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CarrierPortSet {
    first: NonZeroU16,
    last: NonZeroU16,
}

impl CarrierPortSet {
    pub fn new(first: u16, last: u16) -> Result<Self, CarrierEndpointParseError> {
        let first = NonZeroU16::new(first).ok_or(CarrierEndpointParseError::InvalidPort)?;
        let last = NonZeroU16::new(last).ok_or(CarrierEndpointParseError::InvalidPort)?;
        if first > last {
            return Err(CarrierEndpointParseError::InvalidPortRange);
        }
        Ok(Self { first, last })
    }

    pub fn single(port: u16) -> Result<Self, CarrierEndpointParseError> {
        Self::new(port, port)
    }

    pub const fn first(self) -> u16 {
        self.first.get()
    }

    pub const fn last(self) -> u16 {
        self.last.get()
    }

    pub const fn is_single(self) -> bool {
        self.first.get() == self.last.get()
    }

    pub const fn contains(self, port: u16) -> bool {
        self.first.get() <= port && port <= self.last.get()
    }

    /// Selects one unbiased port. Fixed endpoints keep the zero-syscall path.
    pub fn select(self) -> Result<u16, getrandom::Error> {
        if self.is_single() {
            return Ok(self.first());
        }
        Ok((u32::from(self.first()) + random_offset(self.width())?) as u16)
    }

    /// Selects uniformly from every configured port except `current`.
    pub fn select_other(self, current: u16) -> Result<u16, getrandom::Error> {
        if self.is_single() || !self.contains(current) {
            return self.select();
        }
        let current_offset = u32::from(current) - u32::from(self.first());
        let mut offset = random_offset(self.width() - 1)?;
        if offset >= current_offset {
            offset += 1;
        }
        Ok((u32::from(self.first()) + offset) as u16)
    }

    fn width(self) -> u32 {
        u32::from(self.last()) - u32::from(self.first()) + 1
    }
}

fn random_offset(width: u32) -> Result<u32, getrandom::Error> {
    debug_assert!((1..=u32::from(u16::MAX) + 1).contains(&width));
    let sample_space = u32::from(u16::MAX) + 1;
    let accepted = sample_space - sample_space % width;
    loop {
        let mut bytes = [0_u8; 2];
        getrandom::getrandom(&mut bytes)?;
        let sample = u32::from(u16::from_ne_bytes(bytes));
        if sample < accepted {
            return Ok(sample % width);
        }
    }
}

impl std::fmt::Display for CarrierPortSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_single() {
            write!(formatter, "{}", self.first())
        } else {
            write!(formatter, "{}-{}", self.first(), self.last())
        }
    }
}

/// Carrier-only endpoint whose port may be selected from one bounded interval.
///
/// Proxy, DNS, target, and management endpoints continue to use the
/// single-port [`Endpoint`] type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarrierEndpoint {
    pub host: String,
    ports: CarrierPortSet,
}

impl CarrierEndpoint {
    pub fn new(
        host: impl Into<String>,
        ports: CarrierPortSet,
    ) -> Result<Self, CarrierEndpointParseError> {
        let host = host.into();
        if host.is_empty() {
            return Err(CarrierEndpointParseError::EmptyHost);
        }
        Ok(Self { host, ports })
    }

    pub fn single(host: impl Into<String>, port: u16) -> Result<Self, CarrierEndpointParseError> {
        Self::new(host, CarrierPortSet::single(port)?)
    }

    pub const fn ports(&self) -> CarrierPortSet {
        self.ports
    }

    pub fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.ports)
        } else {
            format!("{}:{}", self.host, self.ports)
        }
    }

    pub fn first_endpoint(&self) -> Endpoint {
        Endpoint {
            host: self.host.clone(),
            port: self.ports.first(),
        }
    }
}

impl FromStr for CarrierEndpoint {
    type Err = CarrierEndpointParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            return Err(CarrierEndpointParseError::Empty);
        }

        if let Some(rest) = value.strip_prefix('[') {
            let Some((host, tail)) = rest.split_once(']') else {
                return Err(CarrierEndpointParseError::InvalidIpv6);
            };
            let Some(ports) = tail.strip_prefix(':') else {
                return Err(CarrierEndpointParseError::MissingPort);
            };
            return Self::new(host, parse_carrier_ports(ports)?);
        }

        let Some((host, ports)) = value.rsplit_once(':') else {
            return Err(CarrierEndpointParseError::MissingPort);
        };
        if host.contains(':') {
            return Err(CarrierEndpointParseError::InvalidIpv6);
        }
        Self::new(host, parse_carrier_ports(ports)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathSpec {
    pub underlay: UnderlayProtocol,
    pub endpoint: CarrierEndpoint,
    pub binding: PathBinding,
    pub metadata: PathMetadata,
}

/// Host routing constraints stay separate from path scheduling evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PathBinding {
    pub source_ip: Option<IpAddr>,
}

/// Endpoint-local startup capacity prior for a configured carrier path.
///
/// This is configuration, not peer evidence: it is never serialized and cannot
/// by itself authorize multipath ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateHint {
    Unknown,
    Unlimited,
    BitsPerSecond(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathMetadata {
    pub policy: PathPolicy,
    pub initial_srtt_ms: Option<u32>,
    pub initial_jitter_ms: Option<u32>,
    pub initial_rate: RateHint,
    /// Optional product datagram ceiling. QUIC owns transport PMTU discovery.
    pub max_datagram_payload_bytes: Option<usize>,
    /// Explicit TCP concurrency bounds. Absence selects the Product default.
    pub tcp_carriers: Option<TcpCarrierRange>,
    /// Override for ranged TCP or QUIC destination-port replacement.
    pub port_hop_interval_ms: Option<u32>,
}

impl Default for PathMetadata {
    fn default() -> Self {
        Self {
            policy: PathPolicy::default(),
            initial_srtt_ms: None,
            initial_jitter_ms: None,
            initial_rate: RateHint::Unknown,
            max_datagram_payload_bytes: None,
            tcp_carriers: None,
            port_hop_interval_ms: None,
        }
    }
}

impl PathSpec {
    pub fn tcp_carrier_range(&self) -> Option<TcpCarrierRange> {
        (self.underlay == UnderlayProtocol::Tcp)
            .then_some(self.metadata.tcp_carriers.unwrap_or_default())
    }

    /// A ranged QUIC carrier migrates in place. A ranged TCP carrier uses this
    /// interval to schedule bounded make-before-break replacement.
    pub fn port_hop_interval(&self) -> Option<Duration> {
        (!self.endpoint.ports().is_single()).then(|| {
            Duration::from_millis(u64::from(
                self.metadata
                    .port_hop_interval_ms
                    .unwrap_or(DEFAULT_CARRIER_PORT_HOP_INTERVAL_MS),
            ))
        })
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
        let endpoint: CarrierEndpoint = endpoint.parse()?;
        let (binding, metadata) = match query {
            Some(query) => parse_path_options(query)?,
            None => (PathBinding::default(), PathMetadata::default()),
        };
        if underlay == UnderlayProtocol::Udp && metadata.policy.no_udp {
            return Err(PathSpecParseError::NoUdpOnUdpPath);
        }
        if underlay != UnderlayProtocol::Tcp && metadata.tcp_carriers.is_some() {
            return Err(PathSpecParseError::TcpCarriersRequireTcpPath);
        }
        if metadata.port_hop_interval_ms.is_some() && endpoint.ports().is_single() {
            return Err(PathSpecParseError::PortHopIntervalRequiresRangedPath);
        }
        Ok(Self {
            underlay,
            endpoint,
            binding,
            metadata,
        })
    }
}

fn parse_path_options(query: &str) -> Result<(PathBinding, PathMetadata), PathSpecParseError> {
    if query.is_empty() {
        return Err(PathSpecParseError::EmptyQuery);
    }
    let mut binding = PathBinding::default();
    let mut metadata = PathMetadata::default();
    let mut source_ip_set = false;
    let mut srtt_set = false;
    let mut jitter_set = false;
    let mut rate_set = false;
    let mut datagram_payload_limit_set = false;
    let mut tcp_carriers_set = false;
    let mut port_hop_interval_set = false;
    for part in query.split('&') {
        if part.is_empty() {
            return Err(PathSpecParseError::EmptyQueryParam);
        }
        let (key, value) = part
            .split_once('=')
            .map_or((part, None), |(key, value)| (key, Some(value)));
        match key {
            "source-ip" => {
                reject_duplicate(source_ip_set, key)?;
                source_ip_set = true;
                binding.source_ip = Some(parse_ip_param(key, value)?);
            }
            "srtt-ms" => {
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
            "datagram-payload-limit" => {
                reject_duplicate(datagram_payload_limit_set, key)?;
                datagram_payload_limit_set = true;
                metadata.max_datagram_payload_bytes =
                    Some(parse_datagram_payload_limit(key, value)?);
            }
            "tcp-carriers" => {
                reject_duplicate(tcp_carriers_set, key)?;
                tcp_carriers_set = true;
                metadata.tcp_carriers = Some(parse_tcp_carrier_range(key, value)?);
            }
            "port-hop-interval-ms" => {
                reject_duplicate(port_hop_interval_set, key)?;
                port_hop_interval_set = true;
                metadata.port_hop_interval_ms = Some(parse_port_hop_interval(key, value)?);
            }
            "backup" => metadata.policy.backup = parse_bool_param(key, value)?,
            "expensive" => metadata.policy.expensive = parse_bool_param(key, value)?,
            "bulk-allowed" => metadata.policy.bulk_allowed = parse_bool_param(key, value)?,
            "probe-only" => metadata.policy.probe_only = parse_bool_param(key, value)?,
            "no-udp" => metadata.policy.no_udp = parse_bool_param(key, value)?,
            _ => return Err(PathSpecParseError::UnknownQueryParam(key.to_string())),
        }
    }
    Ok((binding, metadata))
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

fn parse_ip_param(key: &str, value: Option<&str>) -> Result<IpAddr, PathSpecParseError> {
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    value
        .parse::<IpAddr>()
        .map_err(|_| PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string()))
}

fn parse_u64_param(key: &str, value: Option<&str>) -> Result<u64, PathSpecParseError> {
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    value
        .parse::<u64>()
        .map_err(|_| PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string()))
}

fn parse_datagram_payload_limit(
    key: &str,
    value: Option<&str>,
) -> Result<usize, PathSpecParseError> {
    const MIN_PAYLOAD: usize = 512;
    const MAX_PAYLOAD: usize = 65_000;
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    let payload_limit = value.parse::<usize>().map_err(|_| {
        PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string())
    })?;
    if !(MIN_PAYLOAD..=MAX_PAYLOAD).contains(&payload_limit) {
        return Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        ));
    }
    Ok(payload_limit)
}

fn parse_port_hop_interval(key: &str, value: Option<&str>) -> Result<u32, PathSpecParseError> {
    let interval = parse_u32_param(key, value)?;
    if interval < MIN_CARRIER_PORT_HOP_INTERVAL_MS {
        return Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            interval.to_string(),
        ));
    }
    Ok(interval)
}

fn parse_tcp_carrier_range(
    key: &str,
    value: Option<&str>,
) -> Result<TcpCarrierRange, PathSpecParseError> {
    let value = value.ok_or_else(|| PathSpecParseError::MissingQueryParamValue(key.to_string()))?;
    let Some((min, max)) = value.split_once('-') else {
        return Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        ));
    };
    if min.is_empty() || max.is_empty() || max.contains('-') {
        return Err(PathSpecParseError::InvalidQueryParamValue(
            key.to_string(),
            value.to_string(),
        ));
    }
    let min = min.parse::<u16>().map_err(|_| {
        PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string())
    })?;
    let max = max.parse::<u16>().map_err(|_| {
        PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string())
    })?;
    TcpCarrierRange::new(min, max)
        .map_err(|_| PathSpecParseError::InvalidQueryParamValue(key.to_string(), value.to_string()))
}

fn parse_bool_param(key: &str, value: Option<&str>) -> Result<bool, PathSpecParseError> {
    match value.unwrap_or("true") {
        "true" => Ok(true),
        "false" => Ok(false),
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

fn parse_carrier_ports(value: &str) -> Result<CarrierPortSet, CarrierEndpointParseError> {
    let Some((first, last)) = value.split_once('-') else {
        return CarrierPortSet::single(parse_carrier_port(value)?);
    };
    if first.is_empty() || last.is_empty() || last.contains('-') {
        return Err(CarrierEndpointParseError::InvalidPortRange);
    }
    let first = parse_carrier_port(first)?;
    let last = parse_carrier_port(last)?;
    if first == last {
        return Err(CarrierEndpointParseError::NonCanonicalPortRange);
    }
    CarrierPortSet::new(first, last)
}

fn parse_carrier_port(value: &str) -> Result<u16, CarrierEndpointParseError> {
    let port = value
        .parse::<u16>()
        .map_err(|_| CarrierEndpointParseError::InvalidPort)?;
    if port == 0 {
        return Err(CarrierEndpointParseError::InvalidPort);
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
pub enum CarrierEndpointParseError {
    Empty,
    EmptyHost,
    MissingPort,
    InvalidIpv6,
    InvalidPort,
    InvalidPortRange,
    NonCanonicalPortRange,
}

impl std::fmt::Display for CarrierEndpointParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("carrier endpoint is empty"),
            Self::EmptyHost => formatter.write_str("carrier endpoint host is empty"),
            Self::MissingPort => formatter.write_str("carrier endpoint must include a port"),
            Self::InvalidIpv6 => {
                formatter.write_str("IPv6 carrier endpoint must use [addr]:port syntax")
            }
            Self::InvalidPort => formatter.write_str("carrier endpoint port must be in 1..=65535"),
            Self::InvalidPortRange => formatter.write_str(
                "carrier endpoint port range must be an ascending START-END in 1..=65535",
            ),
            Self::NonCanonicalPortRange => {
                formatter.write_str("a single carrier endpoint port must use PORT, not PORT-PORT")
            }
        }
    }
}

impl std::error::Error for CarrierEndpointParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSpecParseError {
    MissingScheme,
    UnknownScheme(String),
    Endpoint(CarrierEndpointParseError),
    EmptyQuery,
    EmptyQueryParam,
    UnknownQueryParam(String),
    MissingQueryParamValue(String),
    InvalidQueryParamValue(String, String),
    DuplicateQueryParam(String),
    QueryParamOverflow(String),
    NoUdpOnUdpPath,
    TcpCarriersRequireTcpPath,
    PortHopIntervalRequiresRangedPath,
}

impl From<CarrierEndpointParseError> for PathSpecParseError {
    fn from(value: CarrierEndpointParseError) -> Self {
        Self::Endpoint(value)
    }
}

impl std::fmt::Display for PathSpecParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingScheme => write!(
                f,
                "path must use tcp://host:PORT[-END] or udp://host:PORT[-END]"
            ),
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
            Self::TcpCarriersRequireTcpPath => {
                write!(f, "tcp-carriers requires a tcp:// carrier endpoint")
            }
            Self::PortHopIntervalRequiresRangedPath => {
                write!(f, "port-hop-interval-ms requires a ranged carrier endpoint")
            }
        }
    }
}

impl std::error::Error for PathSpecParseError {}

#[cfg(test)]
#[path = "spec_test.rs"]
mod tests;
