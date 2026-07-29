use crate::ingress::http_connect::HttpConnectError;
use crate::ingress::socks5::Socks5Error;
use crate::mux::datagram::DatagramError;
use crate::mux::stream::StreamError;
use crate::outbound;
use crate::platform;
use crate::protocol::auth::AuthError;
use crate::protocol::path_capacity::PathCapacityReceiveError;
use crate::protocol::{CloseReason, ResetReason};
use crate::transport::PathSpecParseError;
use crate::transport::encrypted::EncryptedFramedTransportError;
use crate::transport::quic::QuicCarrierError;
use crate::transport::tcp::TcpTransportError;
use crate::transport::udp::UdpTransportError;

#[derive(Debug)]
pub enum RuntimeError {
    Io(std::io::Error),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Encrypted(EncryptedFramedTransportError),
    QuicCarrier(QuicCarrierError),
    Auth(AuthError),
    Random(getrandom::Error),
    Socks5(Socks5Error),
    HttpConnect(HttpConnectError),
    Outbound(outbound::OutboundError),
    OutboundConnect(outbound::OutboundConnectError),
    Stream(StreamError),
    Datagram(DatagramError),
    PathSpec(PathSpecParseError),
    TunDevice(std::io::Error),
    TaskJoin(tokio::task::JoinError),
    NoTcpPath,
    NoDatagramPath,
    NoSchedulableReliablePath,
    NoSchedulableTcpPath,
    NoSchedulableUdpPath,
    PathIdOverflow,
    PathOpenTimedOut,
    PathHeartbeatTimeout,
    DatagramResponseTimedOut,
    SenderServiceBlocked,
    ReliablePathSessionClosed,
    SessionRetentionTimeout,
    ProductPolicy(String),
    GatewayStatePoisoned,
    GatewayUnavailable(String),
    DestinationDenied(String),
    RouteRejected,
    RouteDropped,
    RemoteReset(ResetReason),
    RemoteClosed(CloseReason),
    AuthenticationRejected(&'static str),
    CredentialAdmission(crate::product::CredentialAdmissionError),
    ProductAdmission(crate::product::ProductAdmissionError),
    Protocol(&'static str),
}

/// Carrier failures for which the product relay may preserve queued work and
/// seek another authenticated path.
pub(in crate::runtime) fn reliable_path_error_is_migratable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::PathHeartbeatTimeout
            | RuntimeError::PathOpenTimedOut
            | RuntimeError::ReliablePathSessionClosed
            | RuntimeError::Tcp(_)
            | RuntimeError::Encrypted(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
    )
}

impl From<std::io::Error> for RuntimeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<TcpTransportError> for RuntimeError {
    fn from(value: TcpTransportError) -> Self {
        Self::Tcp(value)
    }
}

impl From<UdpTransportError> for RuntimeError {
    fn from(value: UdpTransportError) -> Self {
        Self::Udp(value)
    }
}

impl From<EncryptedFramedTransportError> for RuntimeError {
    fn from(value: EncryptedFramedTransportError) -> Self {
        Self::Encrypted(value)
    }
}

impl From<QuicCarrierError> for RuntimeError {
    fn from(value: QuicCarrierError) -> Self {
        Self::QuicCarrier(value)
    }
}

impl From<AuthError> for RuntimeError {
    fn from(value: AuthError) -> Self {
        Self::Auth(value)
    }
}

impl From<Socks5Error> for RuntimeError {
    fn from(value: Socks5Error) -> Self {
        Self::Socks5(value)
    }
}

impl From<HttpConnectError> for RuntimeError {
    fn from(value: HttpConnectError) -> Self {
        Self::HttpConnect(value)
    }
}

impl From<outbound::OutboundError> for RuntimeError {
    fn from(value: outbound::OutboundError) -> Self {
        Self::Outbound(value)
    }
}

impl From<outbound::OutboundConnectError> for RuntimeError {
    fn from(value: outbound::OutboundConnectError) -> Self {
        Self::OutboundConnect(value)
    }
}

impl From<StreamError> for RuntimeError {
    fn from(value: StreamError) -> Self {
        Self::Stream(value)
    }
}

impl From<DatagramError> for RuntimeError {
    fn from(value: DatagramError) -> Self {
        Self::Datagram(value)
    }
}

impl From<PathSpecParseError> for RuntimeError {
    fn from(value: PathSpecParseError) -> Self {
        Self::PathSpec(value)
    }
}

impl From<PathCapacityReceiveError> for RuntimeError {
    fn from(value: PathCapacityReceiveError) -> Self {
        Self::Protocol(value.message())
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Tcp(err) => write!(f, "{err}"),
            Self::Udp(err) => write!(f, "{err}"),
            Self::Encrypted(err) => write!(f, "{err}"),
            Self::QuicCarrier(err) => write!(f, "{err}"),
            Self::Auth(err) => write!(f, "{err}"),
            Self::Random(err) => write!(f, "random source failed: {err}"),
            Self::Socks5(err) => write!(f, "{err}"),
            Self::HttpConnect(err) => write!(f, "{err}"),
            Self::Outbound(err) => write!(f, "{err}"),
            Self::OutboundConnect(err) => write!(f, "{err}"),
            Self::Stream(err) => write!(f, "{err}"),
            Self::Datagram(err) => write!(f, "{err}"),
            Self::PathSpec(err) => write!(f, "{err}"),
            Self::TunDevice(err) => write!(
                f,
                "failed to create TUN device: {err}; {}",
                platform::tun_privilege_hint()
            ),
            Self::TaskJoin(err) => write!(f, "runtime task failed: {err}"),
            Self::NoTcpPath => write!(f, "runtime operation requires at least one TCP path"),
            Self::NoDatagramPath => {
                write!(
                    f,
                    "runtime operation requires at least one TCP or UDP path for datagram relay"
                )
            }
            Self::NoSchedulableReliablePath => {
                write!(
                    f,
                    "no configured mptunnel path is schedulable for this reliable flow"
                )
            }
            Self::NoSchedulableTcpPath => {
                write!(f, "no configured TCP path is schedulable for this flow")
            }
            Self::NoSchedulableUdpPath => {
                write!(
                    f,
                    "no configured UDP path is schedulable for this datagram flow"
                )
            }
            Self::PathIdOverflow => write!(f, "configured paths exceed protocol path ID space"),
            Self::PathOpenTimedOut => write!(f, "path stream open timed out"),
            Self::PathHeartbeatTimeout => write!(f, "TCP path heartbeat timed out"),
            Self::DatagramResponseTimedOut => write!(f, "datagram response timed out"),
            Self::SenderServiceBlocked => {
                write!(f, "sender service has no currently admissible path")
            }
            Self::ReliablePathSessionClosed => write!(f, "reliable path session closed"),
            Self::SessionRetentionTimeout => {
                write!(
                    f,
                    "session retention timeout expired without an available path"
                )
            }
            Self::ProductPolicy(error) => write!(f, "product policy error: {error}"),
            Self::GatewayStatePoisoned => write!(f, "product balancer state lock is poisoned"),
            Self::GatewayUnavailable(error) => write!(f, "product balancer unavailable: {error}"),
            Self::DestinationDenied(error) => write!(f, "destination denied: {error}"),
            Self::RouteRejected => write!(f, "destination rejected by routing policy"),
            Self::RouteDropped => write!(f, "destination silently dropped by routing policy"),
            Self::RemoteReset(reason) => write!(f, "remote reset stream: {reason:?}"),
            Self::RemoteClosed(reason) => write!(f, "remote closed session: {reason:?}"),
            Self::AuthenticationRejected(message) => {
                write!(f, "authentication rejected: {message}")
            }
            Self::CredentialAdmission(error) => {
                write!(f, "credential admission rejected: {error}")
            }
            Self::ProductAdmission(error) => {
                write!(f, "Product resource admission rejected: {error}")
            }
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Encrypted(err) => Some(err),
            Self::QuicCarrier(err) => Some(err),
            Self::Auth(err) => Some(err),
            Self::Random(_) => None,
            Self::Socks5(err) => Some(err),
            Self::HttpConnect(err) => Some(err),
            Self::Outbound(err) => Some(err),
            Self::OutboundConnect(err) => Some(err),
            Self::Stream(err) => Some(err),
            Self::Datagram(err) => Some(err),
            Self::PathSpec(err) => Some(err),
            Self::TunDevice(err) => Some(err),
            Self::TaskJoin(err) => Some(err),
            Self::NoTcpPath
            | Self::NoDatagramPath
            | Self::NoSchedulableReliablePath
            | Self::NoSchedulableTcpPath
            | Self::NoSchedulableUdpPath
            | Self::PathIdOverflow
            | Self::PathOpenTimedOut
            | Self::PathHeartbeatTimeout
            | Self::DatagramResponseTimedOut
            | Self::SenderServiceBlocked
            | Self::ReliablePathSessionClosed
            | Self::SessionRetentionTimeout
            | Self::ProductPolicy(_)
            | Self::GatewayStatePoisoned
            | Self::GatewayUnavailable(_)
            | Self::DestinationDenied(_)
            | Self::RouteRejected
            | Self::RouteDropped
            | Self::RemoteReset(_)
            | Self::RemoteClosed(_)
            | Self::AuthenticationRejected(_)
            | Self::CredentialAdmission(_)
            | Self::ProductAdmission(_)
            | Self::Protocol(_) => None,
        }
    }
}
