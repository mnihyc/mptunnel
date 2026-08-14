use super::tun::TunL4Config;
use crate::product::{FlowError, PrincipalId, ProtocolTarget, TargetHost};
use crate::protocol::TargetAddr;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_TCP_FORWARD_MAX_CONNECTIONS: usize = 1_024;
pub const MAX_TCP_FORWARD_CONNECTIONS: usize = 1_048_576;
pub const DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS: usize = 1_024;
pub const MAX_UDP_FORWARD_ASSOCIATIONS: usize = 1_048_576;
pub const DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS: u64 = 30_000;
pub const DEFAULT_UDP_FORWARD_IDLE_TIMEOUT: Duration =
    Duration::from_millis(DEFAULT_UDP_FORWARD_IDLE_TIMEOUT_MS);
pub const DEFAULT_UDP_FORWARD_DATAGRAM_TTL: Duration =
    Duration::from_millis(DEFAULT_UDP_FORWARD_DATAGRAM_TTL_MS);
pub const MAX_LOCAL_PROXY_USERS: usize = 64;
pub const DEFAULT_LOCAL_MAX_CONNECTIONS: usize = 1_024;
pub const DEFAULT_LOCAL_MAX_CONNECTIONS_PER_SOURCE: usize = 256;
pub const DEFAULT_LOCAL_MAX_CONNECTIONS_PER_PRINCIPAL: usize = 512;
pub const DEFAULT_LOCAL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_LOCAL_CONNECTIONS: usize = 1_048_576;

/// A canonical, non-zero fixed destination for a local port-forward inbound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardTarget(TargetAddr);

impl PortForwardTarget {
    pub fn parse(authority: &str) -> Result<Self, FlowError> {
        Ok(Self::from_protocol_target(ProtocolTarget::parse_authority(
            authority,
        )?))
    }

    pub fn try_from_target(target: TargetAddr) -> Result<Self, FlowError> {
        let normalized = match target {
            TargetAddr::Domain { host, port } => ProtocolTarget::from_host_port(&host, port)?,
            TargetAddr::Ip(address) => ProtocolTarget::from_ip(address.ip(), address.port())?,
        };
        Ok(Self::from_protocol_target(normalized))
    }

    pub const fn as_target(&self) -> &TargetAddr {
        &self.0
    }

    pub fn into_target(self) -> TargetAddr {
        self.0
    }

    fn from_protocol_target(target: ProtocolTarget) -> Self {
        let port = target.port().get();
        match target.host() {
            TargetHost::Domain(domain) => Self(TargetAddr::Domain {
                host: domain.as_str().to_string(),
                port,
            }),
            TargetHost::Ip(address) => Self(TargetAddr::Ip(SocketAddr::new(*address, port))),
        }
    }
}

impl FromStr for PortForwardTarget {
    type Err = FlowError;

    fn from_str(authority: &str) -> Result<Self, Self::Err> {
        Self::parse(authority)
    }
}

impl TryFrom<TargetAddr> for PortForwardTarget {
    type Error = FlowError;

    fn try_from(target: TargetAddr) -> Result<Self, Self::Error> {
        Self::try_from_target(target)
    }
}

impl fmt::Display for PortForwardTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.authority())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpForwardConfig {
    listen: Vec<SocketAddr>,
    target: PortForwardTarget,
    max_connections: usize,
}

impl TcpForwardConfig {
    pub fn new(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
        max_connections: usize,
    ) -> Result<Self, PortForwardConfigError> {
        validate_forward_listeners(&listen)?;
        if max_connections == 0 {
            return Err(PortForwardConfigError::ZeroMaxConnections);
        }
        if max_connections > MAX_TCP_FORWARD_CONNECTIONS {
            return Err(PortForwardConfigError::TooManyTcpConnections);
        }
        Ok(Self {
            listen,
            target,
            max_connections,
        })
    }

    pub fn with_defaults(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
    ) -> Result<Self, PortForwardConfigError> {
        Self::new(listen, target, DEFAULT_TCP_FORWARD_MAX_CONNECTIONS)
    }

    pub fn listen(&self) -> &[SocketAddr] {
        &self.listen
    }

    pub const fn target(&self) -> &PortForwardTarget {
        &self.target
    }

    pub const fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub fn into_parts(self) -> (Vec<SocketAddr>, PortForwardTarget, usize) {
        (self.listen, self.target, self.max_connections)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpForwardConfig {
    listen: Vec<SocketAddr>,
    target: PortForwardTarget,
    max_associations: usize,
    idle_timeout: Duration,
    datagram_ttl_ms: u32,
}

/// One fixed-target inbound that listens for TCP and UDP on the same addresses.
///
/// The contained protocol configurations deliberately reuse the dedicated
/// forward validation and limits so mixed forwarding behaves identically to
/// configuring matching `tcp-forward` and `udp-forward` inbounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MixedForwardConfig {
    tcp: TcpForwardConfig,
    udp: UdpForwardConfig,
}

impl MixedForwardConfig {
    pub fn new(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
        max_connections: usize,
        max_associations: usize,
        idle_timeout: Duration,
        datagram_ttl: Duration,
    ) -> Result<Self, PortForwardConfigError> {
        let tcp = TcpForwardConfig::new(listen.clone(), target.clone(), max_connections)?;
        let udp =
            UdpForwardConfig::new(listen, target, max_associations, idle_timeout, datagram_ttl)?;
        Ok(Self { tcp, udp })
    }

    pub fn with_defaults(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
    ) -> Result<Self, PortForwardConfigError> {
        Self::new(
            listen,
            target,
            DEFAULT_TCP_FORWARD_MAX_CONNECTIONS,
            DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS,
            DEFAULT_UDP_FORWARD_IDLE_TIMEOUT,
            DEFAULT_UDP_FORWARD_DATAGRAM_TTL,
        )
    }

    pub fn listen(&self) -> &[SocketAddr] {
        self.tcp.listen()
    }

    pub const fn target(&self) -> &PortForwardTarget {
        self.tcp.target()
    }

    pub const fn max_connections(&self) -> usize {
        self.tcp.max_connections()
    }

    pub const fn max_associations(&self) -> usize {
        self.udp.max_associations()
    }

    pub const fn idle_timeout(&self) -> Duration {
        self.udp.idle_timeout()
    }

    pub const fn datagram_ttl_ms(&self) -> u32 {
        self.udp.datagram_ttl_ms()
    }

    pub fn into_configs(self) -> (TcpForwardConfig, UdpForwardConfig) {
        (self.tcp, self.udp)
    }
}

impl UdpForwardConfig {
    pub fn new(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
        max_associations: usize,
        idle_timeout: Duration,
        datagram_ttl: Duration,
    ) -> Result<Self, PortForwardConfigError> {
        validate_forward_listeners(&listen)?;
        if max_associations == 0 {
            return Err(PortForwardConfigError::ZeroMaxAssociations);
        }
        if max_associations > MAX_UDP_FORWARD_ASSOCIATIONS {
            return Err(PortForwardConfigError::TooManyUdpAssociations);
        }
        let idle_timeout_ms = u32::try_from(idle_timeout.as_millis())
            .ok()
            .filter(|timeout| *timeout > 0)
            .ok_or(PortForwardConfigError::InvalidIdleTimeout)?;
        if Duration::from_millis(u64::from(idle_timeout_ms)) != idle_timeout {
            return Err(PortForwardConfigError::InvalidIdleTimeout);
        }
        let datagram_ttl_ms = u32::try_from(datagram_ttl.as_millis())
            .ok()
            .filter(|ttl| *ttl > 0)
            .ok_or(PortForwardConfigError::InvalidDatagramTtl)?;
        if Duration::from_millis(u64::from(datagram_ttl_ms)) != datagram_ttl {
            return Err(PortForwardConfigError::InvalidDatagramTtl);
        }
        Ok(Self {
            listen,
            target,
            max_associations,
            idle_timeout,
            datagram_ttl_ms,
        })
    }

    pub fn with_defaults(
        listen: Vec<SocketAddr>,
        target: PortForwardTarget,
    ) -> Result<Self, PortForwardConfigError> {
        Self::new(
            listen,
            target,
            DEFAULT_UDP_FORWARD_MAX_ASSOCIATIONS,
            DEFAULT_UDP_FORWARD_IDLE_TIMEOUT,
            DEFAULT_UDP_FORWARD_DATAGRAM_TTL,
        )
    }

    pub fn listen(&self) -> &[SocketAddr] {
        &self.listen
    }

    pub const fn target(&self) -> &PortForwardTarget {
        &self.target
    }

    pub const fn max_associations(&self) -> usize {
        self.max_associations
    }

    pub const fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    pub const fn datagram_ttl_ms(&self) -> u32 {
        self.datagram_ttl_ms
    }

    pub fn into_parts(self) -> (Vec<SocketAddr>, PortForwardTarget, usize, Duration, u32) {
        (
            self.listen,
            self.target,
            self.max_associations,
            self.idle_timeout,
            self.datagram_ttl_ms,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortForwardConfigError {
    NoListeners,
    ZeroListenPort(SocketAddr),
    DuplicateListener(SocketAddr),
    ZeroMaxConnections,
    TooManyTcpConnections,
    ZeroMaxAssociations,
    TooManyUdpAssociations,
    InvalidIdleTimeout,
    InvalidDatagramTtl,
}

impl fmt::Display for PortForwardConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoListeners => formatter.write_str("port-forward inbound needs a listener"),
            Self::ZeroListenPort(address) => {
                write!(
                    formatter,
                    "port-forward listener {address} must use a non-zero port"
                )
            }
            Self::DuplicateListener(address) => {
                write!(formatter, "duplicate port-forward listener {address}")
            }
            Self::ZeroMaxConnections => {
                formatter.write_str("TCP port-forward max connections must be non-zero")
            }
            Self::TooManyTcpConnections => write!(
                formatter,
                "TCP port-forward max connections exceeds {MAX_TCP_FORWARD_CONNECTIONS}"
            ),
            Self::ZeroMaxAssociations => {
                formatter.write_str("UDP port-forward max associations must be non-zero")
            }
            Self::TooManyUdpAssociations => write!(
                formatter,
                "UDP port-forward max associations exceeds {MAX_UDP_FORWARD_ASSOCIATIONS}"
            ),
            Self::InvalidIdleTimeout => formatter.write_str(
                "UDP port-forward idle timeout must be a whole number from 1 to 4294967295 milliseconds",
            ),
            Self::InvalidDatagramTtl => formatter.write_str(
                "UDP port-forward datagram TTL must be a whole number from 1 to 4294967295 milliseconds",
            ),
        }
    }
}

impl Error for PortForwardConfigError {}

fn validate_forward_listeners(listen: &[SocketAddr]) -> Result<(), PortForwardConfigError> {
    if listen.is_empty() {
        return Err(PortForwardConfigError::NoListeners);
    }
    if let Some(address) = listen.iter().find(|address| address.port() == 0) {
        return Err(PortForwardConfigError::ZeroListenPort(*address));
    }
    for (index, address) in listen.iter().enumerate() {
        if listen[..index].contains(address) {
            return Err(PortForwardConfigError::DuplicateListener(*address));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfig {
    Socks5 {
        listen: Vec<SocketAddr>,
        proxy_auth: ProxyAuthConfig,
        admission: LocalIngressAdmissionConfig,
    },
    HttpConnect {
        listen: Vec<SocketAddr>,
        proxy_auth: ProxyAuthConfig,
        admission: LocalIngressAdmissionConfig,
    },
    /// One TCP listener that dispatches SOCKS5 and HTTP CONNECT requests.
    Mixed {
        listen: Vec<SocketAddr>,
        proxy_auth: ProxyAuthConfig,
        admission: LocalIngressAdmissionConfig,
    },
    TcpForward(TcpForwardConfig),
    UdpForward(UdpForwardConfig),
    MixedForward(MixedForwardConfig),
    TunL4(TunL4Config),
}

/// Product-owned new-connection limits for one local proxy inbound.
///
/// These counters bound listener/source/principal work only. They do not
/// mirror or derive any MPP session, stream, datagram, queue, or flight limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalIngressAdmissionConfig {
    max_connections: usize,
    max_connections_per_source: usize,
    max_connections_per_principal: usize,
    handshake_timeout: Duration,
}

impl LocalIngressAdmissionConfig {
    pub fn new(
        max_connections: usize,
        max_connections_per_source: usize,
        max_connections_per_principal: usize,
        handshake_timeout: Duration,
    ) -> Result<Self, LocalIngressAdmissionConfigError> {
        for (boundary, value) in [
            ("listener", max_connections),
            ("source", max_connections_per_source),
            ("principal", max_connections_per_principal),
        ] {
            if value == 0 {
                return Err(LocalIngressAdmissionConfigError::Zero { boundary });
            }
            if value > MAX_LOCAL_CONNECTIONS {
                return Err(LocalIngressAdmissionConfigError::TooLarge {
                    boundary,
                    value,
                    limit: MAX_LOCAL_CONNECTIONS,
                });
            }
        }
        if max_connections_per_source > max_connections {
            return Err(LocalIngressAdmissionConfigError::ExceedsListener { boundary: "source" });
        }
        if max_connections_per_principal > max_connections {
            return Err(LocalIngressAdmissionConfigError::ExceedsListener {
                boundary: "principal",
            });
        }
        if handshake_timeout.is_zero() || handshake_timeout > Duration::from_secs(60) {
            return Err(LocalIngressAdmissionConfigError::InvalidHandshakeTimeout);
        }
        Ok(Self {
            max_connections,
            max_connections_per_source,
            max_connections_per_principal,
            handshake_timeout,
        })
    }

    pub const fn max_connections(self) -> usize {
        self.max_connections
    }

    pub const fn max_connections_per_source(self) -> usize {
        self.max_connections_per_source
    }

    pub const fn max_connections_per_principal(self) -> usize {
        self.max_connections_per_principal
    }

    pub const fn handshake_timeout(self) -> Duration {
        self.handshake_timeout
    }
}

impl Default for LocalIngressAdmissionConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_LOCAL_MAX_CONNECTIONS,
            max_connections_per_source: DEFAULT_LOCAL_MAX_CONNECTIONS_PER_SOURCE,
            max_connections_per_principal: DEFAULT_LOCAL_MAX_CONNECTIONS_PER_PRINCIPAL,
            handshake_timeout: DEFAULT_LOCAL_HANDSHAKE_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalIngressAdmissionConfigError {
    Zero {
        boundary: &'static str,
    },
    TooLarge {
        boundary: &'static str,
        value: usize,
        limit: usize,
    },
    ExceedsListener {
        boundary: &'static str,
    },
    InvalidHandshakeTimeout,
}

impl fmt::Display for LocalIngressAdmissionConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero { boundary } => {
                write!(
                    formatter,
                    "local proxy {boundary} connection limit must be non-zero"
                )
            }
            Self::TooLarge {
                boundary,
                value,
                limit,
            } => write!(
                formatter,
                "local proxy {boundary} connection limit {value} exceeds {limit}"
            ),
            Self::ExceedsListener { boundary } => write!(
                formatter,
                "local proxy {boundary} connection limit must not exceed the listener limit"
            ),
            Self::InvalidHandshakeTimeout => formatter.write_str(
                "local proxy handshake timeout must be from 1 millisecond through 60 seconds",
            ),
        }
    }
}

impl Error for LocalIngressAdmissionConfigError {}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct ProxyAuthConfig {
    users: Arc<[LocalProxyUser]>,
}

impl ProxyAuthConfig {
    pub fn disabled() -> Self {
        Self {
            users: Arc::from([]),
        }
    }

    pub fn required(
        users: impl IntoIterator<Item = LocalProxyUser>,
    ) -> Result<Self, ProxyAuthConfigError> {
        let users = users.into_iter().collect::<Vec<_>>();
        if users.is_empty() {
            return Err(ProxyAuthConfigError::NoUsers);
        }
        if users.len() > MAX_LOCAL_PROXY_USERS {
            return Err(ProxyAuthConfigError::TooManyUsers {
                actual: users.len(),
                limit: MAX_LOCAL_PROXY_USERS,
            });
        }
        for (index, user) in users.iter().enumerate() {
            if users[..index].iter().any(|other| other.id == user.id) {
                return Err(ProxyAuthConfigError::DuplicateUserId(user.id.clone()));
            }
            if users[..index]
                .iter()
                .any(|other| other.username == user.username)
            {
                return Err(ProxyAuthConfigError::DuplicateUsername);
            }
        }
        Ok(Self {
            users: users.into(),
        })
    }

    pub fn is_required(&self) -> bool {
        !self.users.is_empty()
    }

    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Principals that can authenticate on this local proxy inbound. An empty
    /// iterator means authentication is disabled and the runtime uses the
    /// fixed `anonymous` principal instead.
    pub(crate) fn principals(&self) -> impl ExactSizeIterator<Item = &PrincipalId> {
        self.users.iter().map(LocalProxyUser::principal)
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Option<PrincipalId> {
        let mut matched = None;
        for (index, user) in self.users.iter().enumerate() {
            if user.verify(username, password) {
                matched = Some(index);
            }
        }
        matched.map(|index| self.users[index].principal.clone())
    }

    pub fn authenticate_basic_header(&self, value: Option<&str>) -> Option<PrincipalId> {
        let value = value?;
        let encoded = value.trim().strip_prefix("Basic ")?;
        // Both supported proxy protocols cap each field at 255 bytes. Reject
        // oversized input before Base64 allocates.
        const MAX_ENCODED_CREDENTIAL_BYTES: usize = 684;
        if encoded.len() > MAX_ENCODED_CREDENTIAL_BYTES {
            return None;
        }
        let Ok(decoded) = BASE64_STANDARD.decode(encoded.trim()) else {
            return None;
        };
        let Ok(decoded) = String::from_utf8(decoded) else {
            return None;
        };
        let (username, password) = decoded.split_once(':')?;
        self.authenticate(username, password)
    }
}

impl std::fmt::Debug for ProxyAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyAuthConfig")
            .field("required", &self.is_required())
            .field("user_count", &self.users.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct LocalProxyUser {
    id: String,
    principal: PrincipalId,
    username: String,
    password: String,
}

impl LocalProxyUser {
    pub fn new(
        id: String,
        principal: PrincipalId,
        username: String,
        password: String,
    ) -> Result<Self, ProxyAuthConfigError> {
        if PrincipalId::parse(&id).is_err() {
            return Err(ProxyAuthConfigError::InvalidUserId(id));
        }
        if username.is_empty() {
            return Err(ProxyAuthConfigError::UsernameEmpty);
        }
        if username.len() > u8::MAX as usize {
            return Err(ProxyAuthConfigError::UsernameTooLong);
        }
        if username.contains(':') || username.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(ProxyAuthConfigError::InvalidUsername);
        }
        if password.is_empty() {
            return Err(ProxyAuthConfigError::PasswordEmpty);
        }
        if password.len() > u8::MAX as usize {
            return Err(ProxyAuthConfigError::PasswordTooLong);
        }
        if password.bytes().any(|byte| byte.is_ascii_control()) {
            return Err(ProxyAuthConfigError::InvalidPassword);
        }
        Ok(Self {
            id,
            principal,
            username,
            password,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn principal(&self) -> &PrincipalId {
        &self.principal
    }

    fn verify(&self, username: &str, password: &str) -> bool {
        constant_time_eq(self.username.as_bytes(), username.as_bytes())
            & constant_time_eq(self.password.as_bytes(), password.as_bytes())
    }
}

impl std::fmt::Debug for LocalProxyUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalProxyUser")
            .field("id", &self.id)
            .field("principal", &self.principal)
            .field("username", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyAuthConfigError {
    NoUsers,
    TooManyUsers { actual: usize, limit: usize },
    DuplicateUserId(String),
    DuplicateUsername,
    InvalidUserId(String),
    UsernameEmpty,
    UsernameTooLong,
    InvalidUsername,
    PasswordEmpty,
    PasswordTooLong,
    InvalidPassword,
}

impl fmt::Display for ProxyAuthConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoUsers => formatter.write_str("proxy authentication requires at least one user"),
            Self::TooManyUsers { actual, limit } => write!(
                formatter,
                "proxy authentication defines {actual} users; maximum is {limit}"
            ),
            Self::DuplicateUserId(id) => write!(formatter, "duplicate local proxy user ID {id:?}"),
            Self::DuplicateUsername => {
                formatter.write_str("duplicate local proxy authentication username")
            }
            Self::InvalidUserId(id) => {
                write!(formatter, "invalid local proxy user ID {id:?}")
            }
            Self::UsernameEmpty => {
                formatter.write_str("local proxy authentication username must not be empty")
            }
            Self::UsernameTooLong => {
                formatter.write_str("local proxy authentication username must fit in 255 bytes")
            }
            Self::InvalidUsername => formatter.write_str(
                "local proxy authentication username must not contain ':' or control characters",
            ),
            Self::PasswordEmpty => {
                formatter.write_str("local proxy authentication password must not be empty")
            }
            Self::PasswordTooLong => {
                formatter.write_str("local proxy authentication password must fit in 255 bytes")
            }
            Self::InvalidPassword => formatter.write_str(
                "local proxy authentication password must not contain control characters",
            ),
        }
    }
}

impl Error for ProxyAuthConfigError {}

fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    let max_len = expected.len().max(actual.len());
    let mut diff = expected.len() ^ actual.len();
    for index in 0..max_len {
        let lhs = expected.get(index).copied().unwrap_or(0);
        let rhs = actual.get(index).copied().unwrap_or(0);
        diff |= usize::from(lhs ^ rhs);
    }
    diff == 0
}

#[cfg(test)]
#[path = "tests_config.rs"]
mod tests;
