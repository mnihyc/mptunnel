pub mod http_connect;
pub mod socks5;

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfig {
    Socks5 { listen: SocketAddr },
    HttpConnect { listen: SocketAddr },
    TunL4(TunConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunConfig {
    pub name: String,
    pub mtu: u16,
}

impl TunConfig {
    pub fn new(name: impl Into<String>, mtu: u16) -> Result<Self, IngressConfigError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(IngressConfigError::EmptyTunName);
        }
        if mtu < 576 {
            return Err(IngressConfigError::TunMtuTooSmall);
        }
        Ok(Self { name, mtu })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressConfigError {
    EmptyTunName,
    TunMtuTooSmall,
}

impl std::fmt::Display for IngressConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTunName => write!(f, "TUN name must not be empty"),
            Self::TunMtuTooSmall => write!(f, "TUN MTU must be at least 576 bytes"),
        }
    }
}

impl std::error::Error for IngressConfigError {}
