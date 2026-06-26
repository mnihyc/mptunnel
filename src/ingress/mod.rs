pub mod http_connect;
pub mod socks5;
pub mod tun;

use std::net::SocketAddr;
use tun::TunL4Config;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfig {
    Socks5 { listen: Vec<SocketAddr> },
    HttpConnect { listen: Vec<SocketAddr> },
    TunL4(TunL4Config),
}
