pub mod http_connect;
pub mod socks5;

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfig {
    Socks5 { listen: SocketAddr },
    HttpConnect { listen: SocketAddr },
}
