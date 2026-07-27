mod connect;
mod destination;
pub mod http_connect;
pub mod socks5;

pub use connect::{
    HttpsProxyConfig, OutboundConfig, OutboundConnectError, OutboundError, OutboundTcpStream,
    OutboundUdpSocket, ProxyConfig, ProxyCredentials, Socks5UdpAssociation, TargetProtocol,
    connect_tcp, connect_tcp_with_configurator, connect_udp, connect_udp_with_configurator,
    validate_target,
};
pub(crate) use connect::{
    connect_tcp_authorized_with_configurator, connect_tcp_literal_target_with_configurator,
    connect_udp_authorized_with_configurator, resolve_authorized_target_before,
};
pub use destination::{
    DestinationAuthorization, DestinationAuthorizationError, DestinationAuthorizer,
    ServerDestinationPolicy,
};
