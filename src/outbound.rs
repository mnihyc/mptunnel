mod connect;
mod destination;
pub mod http_connect;
pub mod socks5;

pub(crate) use connect::{
    ConnectorTarget, connect_tcp_literal_target_with_configurator,
    connect_tcp_target_with_configurator, connect_udp_target_with_configurator,
    resolve_authorization_before, resolve_authorized_domain_before,
};
pub use connect::{
    HttpsProxyConfig, OutboundConfig, OutboundConnectError, OutboundError, OutboundTcpStream,
    OutboundUdpSocket, ProxyConfig, ProxyCredentials, Socks5UdpAssociation, TargetProtocol,
    connect_tcp, connect_tcp_with_configurator, connect_udp, connect_udp_with_configurator,
    validate_target,
};
pub(crate) use destination::protocol_target_addr;
pub use destination::{
    DestinationAuthorization, DestinationAuthorizationError, DestinationAuthorizer,
    ServerDestinationPolicy,
};
