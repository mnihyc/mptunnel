use idna::domain_to_ascii_strict;
use std::borrow::Borrow;
use std::error::Error;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU16;
use std::str::FromStr;

const MAX_DOMAIN_BYTES: usize = 253;
const MAX_AUTHORITY_BYTES: usize = 512;
const MAX_POLICY_ID_BYTES: usize = 64;

/// A canonical DNS name in lower-case IDNA ASCII form without a trailing dot.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DomainName(String);

impl DomainName {
    pub fn parse(input: &str) -> Result<Self, FlowError> {
        validate_untrusted_text(input, MAX_AUTHORITY_BYTES)?;
        if input.is_empty() {
            return Err(FlowError::EmptyDomain);
        }
        if input.trim() != input {
            return Err(FlowError::InvalidDomain);
        }
        if input.contains(['/', '\\', '?', '#', '@', ':', '[', ']']) {
            return Err(FlowError::InvalidDomain);
        }

        let without_root = input.strip_suffix('.').unwrap_or(input);
        if without_root.is_empty() || without_root.ends_with('.') {
            return Err(FlowError::InvalidDomain);
        }
        let ascii = domain_to_ascii_strict(without_root).map_err(|_| FlowError::InvalidDomain)?;
        let canonical = ascii.to_ascii_lowercase();
        if canonical.is_empty() || canonical.len() > MAX_DOMAIN_BYTES {
            return Err(FlowError::DomainTooLong);
        }
        if canonical.split('.').any(|label| label.is_empty()) {
            return Err(FlowError::InvalidDomain);
        }
        if looks_like_numeric_address(&canonical) {
            return Err(FlowError::DomainLooksLikeIp);
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for DomainName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for DomainName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("DomainName").field(&self.0).finish()
    }
}

impl fmt::Display for DomainName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for DomainName {
    type Err = FlowError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

/// A non-zero destination port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TargetPort(NonZeroU16);

impl TargetPort {
    pub fn new(port: u16) -> Result<Self, FlowError> {
        NonZeroU16::new(port)
            .map(Self)
            .ok_or(FlowError::InvalidPort)
    }

    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl fmt::Display for TargetPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TargetHost {
    Domain(DomainName),
    Ip(IpAddr),
}

/// A normalized network destination suitable for policy and protocol mapping.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProtocolTarget {
    host: TargetHost,
    port: TargetPort,
}

impl ProtocolTarget {
    pub fn from_host_port(host: &str, port: u16) -> Result<Self, FlowError> {
        validate_untrusted_text(host, MAX_AUTHORITY_BYTES)?;
        let port = TargetPort::new(port)?;
        let host = match IpAddr::from_str(host) {
            Ok(address) => TargetHost::Ip(canonical_ip(address)),
            Err(_) => TargetHost::Domain(DomainName::parse(host)?),
        };
        Ok(Self { host, port })
    }

    pub fn from_ip(address: IpAddr, port: u16) -> Result<Self, FlowError> {
        Ok(Self {
            host: TargetHost::Ip(canonical_ip(address)),
            port: TargetPort::new(port)?,
        })
    }

    /// Parse an authority with a mandatory port.
    ///
    /// Domain/IPv4 authorities use `host:port`; IPv6 must use `[address]:port`.
    /// URI schemes, user-info, paths, fragments, whitespace, and control
    /// characters are rejected rather than interpreted.
    pub fn parse_authority(authority: &str) -> Result<Self, FlowError> {
        validate_untrusted_text(authority, MAX_AUTHORITY_BYTES)?;
        if authority.is_empty() || authority.trim() != authority {
            return Err(FlowError::InvalidAuthority);
        }
        if authority.contains(['/', '\\', '?', '#', '@']) {
            return Err(FlowError::InvalidAuthority);
        }

        if let Some(rest) = authority.strip_prefix('[') {
            let (address, port_text) = rest.split_once("]:").ok_or(FlowError::InvalidAuthority)?;
            if address.is_empty()
                || port_text.is_empty()
                || address.contains(['[', ']'])
                || port_text.contains(':')
            {
                return Err(FlowError::InvalidAuthority);
            }
            let address = Ipv6Addr::from_str(address).map_err(|_| FlowError::InvalidAuthority)?;
            return Self::from_ip(IpAddr::V6(address), parse_port(port_text)?);
        }

        if authority.contains(['[', ']']) {
            return Err(FlowError::InvalidAuthority);
        }
        let (host, port_text) = authority
            .rsplit_once(':')
            .ok_or(FlowError::InvalidAuthority)?;
        if host.is_empty() || host.contains(':') || port_text.is_empty() {
            return Err(FlowError::InvalidAuthority);
        }
        Self::from_host_port(host, parse_port(port_text)?)
    }

    pub fn host(&self) -> &TargetHost {
        &self.host
    }

    pub const fn port(&self) -> TargetPort {
        self.port
    }

    pub fn domain(&self) -> Option<&DomainName> {
        match &self.host {
            TargetHost::Domain(domain) => Some(domain),
            TargetHost::Ip(_) => None,
        }
    }

    pub const fn ip(&self) -> Option<IpAddr> {
        match self.host {
            TargetHost::Ip(address) => Some(address),
            TargetHost::Domain(_) => None,
        }
    }

    pub fn authority(&self) -> String {
        match &self.host {
            TargetHost::Domain(domain) => format!("{domain}:{}", self.port),
            TargetHost::Ip(IpAddr::V4(address)) => format!("{address}:{}", self.port),
            TargetHost::Ip(IpAddr::V6(address)) => format!("[{address}]:{}", self.port),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Network {
    Tcp,
    Udp,
}

impl fmt::Display for Network {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        })
    }
}

/// Compact, allocation-free network capabilities for one outbound leaf.
///
/// Capability checks happen only while opening a Product flow. The selected
/// leaf is then pinned for the flow lifetime, so this set never enters payload
/// forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NetworkSet(u8);

impl NetworkSet {
    const TCP_BIT: u8 = 1 << 0;
    const UDP_BIT: u8 = 1 << 1;

    pub const NONE: Self = Self(0);
    pub const TCP: Self = Self(Self::TCP_BIT);
    pub const UDP: Self = Self(Self::UDP_BIT);
    pub const TCP_UDP: Self = Self(Self::TCP_BIT | Self::UDP_BIT);

    pub const fn contains(self, network: Network) -> bool {
        let bit = match network {
            Network::Tcp => Self::TCP_BIT,
            Network::Udp => Self::UDP_BIT,
        };
        self.0 & bit != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl From<Network> for NetworkSet {
    fn from(network: Network) -> Self {
        match network {
            Network::Tcp => Self::TCP,
            Network::Udp => Self::UDP,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceEndpoint {
    address: IpAddr,
    port: u16,
}

impl SourceEndpoint {
    pub const fn new(address: IpAddr, port: u16) -> Self {
        Self {
            address: canonical_ip(address),
            port,
        }
    }

    pub const fn from_socket_addr(address: SocketAddr) -> Self {
        Self::new(address.ip(), address.port())
    }

    pub const fn address(self) -> IpAddr {
        self.address
    }

    pub const fn port(self) -> u16 {
        self.port
    }
}

macro_rules! policy_id {
    ($name:ident) => {
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn parse(input: &str) -> Result<Self, FlowError> {
                canonical_policy_id(input).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = FlowError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }
    };
}

policy_id!(PrincipalId);
policy_id!(CredentialId);
policy_id!(InboundId);

/// Immutable Product-owned identity for one TCP connection or UDP
/// association. All untrusted text is normalized before this value exists.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlowContext {
    network: Network,
    target: ProtocolTarget,
    source: SourceEndpoint,
    principal: PrincipalId,
    inbound: InboundId,
}

impl FlowContext {
    pub fn new(
        network: Network,
        target: ProtocolTarget,
        source: SourceEndpoint,
        principal: PrincipalId,
        inbound: InboundId,
    ) -> Self {
        Self {
            network,
            target,
            source,
            principal,
            inbound,
        }
    }

    pub const fn network(&self) -> Network {
        self.network
    }

    pub const fn target(&self) -> &ProtocolTarget {
        &self.target
    }

    pub const fn source(&self) -> SourceEndpoint {
        self.source
    }

    pub const fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    pub const fn inbound(&self) -> &InboundId {
        &self.inbound
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowError {
    EmptyDomain,
    DomainTooLong,
    DomainLooksLikeIp,
    InvalidDomain,
    InvalidAuthority,
    InvalidPort,
    InvalidPolicyId,
    ControlCharacter,
    TextTooLong,
}

impl fmt::Display for FlowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyDomain => "domain must not be empty",
            Self::DomainTooLong => "domain exceeds the DNS wire limit",
            Self::DomainLooksLikeIp => "numeric address text must be represented as an IP target",
            Self::InvalidDomain => "domain is not valid strict IDNA DNS text",
            Self::InvalidAuthority => "authority must be a host and non-zero port",
            Self::InvalidPort => "destination port must be between 1 and 65535",
            Self::InvalidPolicyId => "policy ID must be normalized ASCII name text",
            Self::ControlCharacter => "text contains a control character",
            Self::TextTooLong => "text exceeds its bounded policy length",
        })
    }
}

impl Error for FlowError {}

fn parse_port(input: &str) -> Result<u16, FlowError> {
    if !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(FlowError::InvalidPort);
    }
    let port = input.parse::<u16>().map_err(|_| FlowError::InvalidPort)?;
    TargetPort::new(port)?;
    Ok(port)
}

pub(crate) fn canonical_policy_id(input: &str) -> Result<String, FlowError> {
    validate_untrusted_text(input, MAX_POLICY_ID_BYTES)?;
    let mut bytes = input.bytes();
    let Some(first) = bytes.next() else {
        return Err(FlowError::InvalidPolicyId);
    };
    if !first.is_ascii_alphanumeric()
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(FlowError::InvalidPolicyId);
    }
    Ok(input.to_ascii_lowercase())
}

fn validate_untrusted_text(input: &str, max_bytes: usize) -> Result<(), FlowError> {
    if input.len() > max_bytes {
        return Err(FlowError::TextTooLong);
    }
    if input.chars().any(char::is_control) {
        return Err(FlowError::ControlCharacter);
    }
    Ok(())
}

const fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) if address.is_unspecified() || address.is_loopback() => {
            IpAddr::V6(address)
        }
        IpAddr::V6(address) => match address.to_ipv4() {
            Some(address) => IpAddr::V4(address),
            None => IpAddr::V6(address),
        },
        IpAddr::V4(address) => IpAddr::V4(address),
    }
}

fn looks_like_numeric_address(value: &str) -> bool {
    value.split('.').all(|component| {
        !component.is_empty()
            && (component.bytes().all(|byte| byte.is_ascii_digit())
                || component.strip_prefix("0x").is_some_and(|hex| {
                    !hex.is_empty() && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
                }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn domain_is_idna_lowercase_and_root_normalized() {
        let domain = DomainName::parse("BÜCHER.Example.").expect("valid IDN");
        assert_eq!(domain.as_str(), "xn--bcher-kva.example");
    }

    #[test]
    fn domain_rejects_injection_and_ambiguous_numeric_text() {
        for input in [
            "",
            ".",
            "example..com",
            "example.com..",
            "example.com\r\nhost: attacker",
            "user@example.com",
            "example.com:443",
            "127.0.0.1",
            "127.1",
            "0177.0.0.1",
            "0x7f000001",
            "0x7f.0.0.1",
            "[::1]",
            " example.com",
        ] {
            assert!(DomainName::parse(input).is_err(), "{input:?}");
        }
    }

    #[test]
    fn domain_enforces_dns_length_and_label_rules() {
        let label = "a".repeat(64);
        assert_eq!(
            DomainName::parse(&format!("{label}.example")),
            Err(FlowError::InvalidDomain)
        );
        let too_long = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(62)
        );
        assert!(DomainName::parse(&too_long).is_err());
    }

    #[test]
    fn authority_parses_domain_ipv4_and_bracketed_ipv6() {
        let domain = ProtocolTarget::parse_authority("EXAMPLE.com:443").expect("domain");
        assert_eq!(domain.authority(), "example.com:443");
        let ipv4 = ProtocolTarget::parse_authority("192.0.2.1:53").expect("IPv4");
        assert_eq!(ipv4.ip(), Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))));
        let ipv6 = ProtocolTarget::parse_authority("[2001:db8::1]:8443").expect("IPv6");
        assert_eq!(
            ipv6.ip(),
            Some(IpAddr::V6("2001:db8::1".parse().expect("address")))
        );
        assert_eq!(ipv6.authority(), "[2001:db8::1]:8443");
    }

    #[test]
    fn authority_rejects_ambiguous_or_injectable_forms() {
        for input in [
            "example.com",
            "https://example.com:443",
            "user@example.com:443",
            "example.com:0",
            "example.com:65536",
            "example.com:+80",
            "::1:443",
            "[::1]443",
            "[::1%25eth0]:443",
            "example.com:443/path",
            "example.com:443\r\nx: y",
        ] {
            assert!(ProtocolTarget::parse_authority(input).is_err(), "{input:?}");
        }
    }

    #[test]
    fn mapped_ipv6_is_canonicalized_to_ipv4() {
        let mapped = Ipv6Addr::from_str("::ffff:192.0.2.42").expect("mapped");
        let target = ProtocolTarget::from_ip(IpAddr::V6(mapped), 443).expect("target");
        assert_eq!(target.ip(), Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 42))));
    }

    #[test]
    fn compatible_ipv6_cannot_hide_an_ipv4_destination() {
        let compatible = Ipv6Addr::from_str("::127.0.0.1").expect("compatible");
        let target = ProtocolTarget::from_ip(IpAddr::V6(compatible), 443).expect("target");
        assert_eq!(target.ip(), Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));

        let loopback =
            ProtocolTarget::from_ip(IpAddr::V6(Ipv6Addr::LOCALHOST), 443).expect("IPv6 loopback");
        assert_eq!(loopback.ip(), Some(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn policy_ids_are_normalized_and_bounded() {
        assert_eq!(
            PrincipalId::parse("Alice-Mobile")
                .expect("principal")
                .as_str(),
            "alice-mobile"
        );
        for input in ["", ".bad", "bad name", "bad/name", "bad\r\nname"] {
            assert!(PrincipalId::parse(input).is_err(), "{input:?}");
        }
    }
}
