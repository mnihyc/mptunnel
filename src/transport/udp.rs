use crate::protocol::UnderlayProtocol;
use crate::transport::{Endpoint, PathSpec};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::timeout;

// Raw UDP socket setup only. The QUIC carrier consumes UDP below mptunnel,
// while application DatagramData forwarding lives in runtime target workers;
// neither protocol's scheduling or reliability policy belongs here.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConnectOptions {
    pub source_ip: Option<IpAddr>,
    pub timeout: Duration,
}

impl Default for UdpConnectOptions {
    fn default() -> Self {
        Self {
            source_ip: None,
            timeout: Duration::from_secs(10),
        }
    }
}

pub async fn connect_path(
    path: &PathSpec,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    if path.underlay != UnderlayProtocol::Udp {
        return Err(UdpTransportError::WrongUnderlay(path.underlay));
    }
    connect_endpoint(&path.endpoint, options).await
}

pub async fn connect_endpoint(
    endpoint: &Endpoint,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    let addrs = resolve_endpoint(endpoint).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = options.source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match connect_addr(addr, options).await {
            Ok(socket) => return Ok(socket),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(UdpTransportError::NoCompatibleAddress))
}

pub async fn bind_socket(path: &PathSpec) -> Result<UdpSocket, UdpTransportError> {
    if path.underlay != UnderlayProtocol::Udp {
        return Err(UdpTransportError::WrongUnderlay(path.underlay));
    }
    Ok(UdpSocket::bind(path.endpoint.authority()).await?)
}

async fn resolve_endpoint(endpoint: &Endpoint) -> Result<Vec<SocketAddr>, UdpTransportError> {
    let addrs = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(UdpTransportError::ResolutionEmpty(endpoint.authority()))
    } else {
        Ok(addrs)
    }
}

pub async fn connect_addr(
    addr: SocketAddr,
    options: UdpConnectOptions,
) -> Result<UdpSocket, UdpTransportError> {
    let local_addr = match options.source_ip {
        Some(source_ip) => SocketAddr::new(source_ip, 0),
        None if addr.is_ipv4() => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        None => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let socket = UdpSocket::bind(local_addr).await?;
    timeout(options.timeout, socket.connect(addr))
        .await
        .map_err(|_| UdpTransportError::ConnectTimedOut(addr))??;
    Ok(socket)
}

#[derive(Debug)]
pub enum UdpTransportError {
    WrongUnderlay(UnderlayProtocol),
    ResolutionEmpty(String),
    NoCompatibleAddress,
    ConnectTimedOut(SocketAddr),
    Io(std::io::Error),
}

impl From<std::io::Error> for UdpTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for UdpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongUnderlay(underlay) => {
                write!(f, "UDP transport cannot use {underlay:?} path")
            }
            Self::ResolutionEmpty(authority) => {
                write!(f, "no socket addresses resolved for {authority}")
            }
            Self::NoCompatibleAddress => {
                write!(f, "no resolved address is compatible with source binding")
            }
            Self::ConnectTimedOut(addr) => write!(f, "UDP connect to {addr} timed out"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UdpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn udp_path_connects_to_bound_socket_and_datagrams_work() {
        let bind_path = reserve_udp_path().await;
        let socket = bind_socket(&bind_path).await.expect("bind socket");
        let local_addr = socket.local_addr().expect("local addr");
        let client_path = format!("udp://{local_addr}")
            .parse::<PathSpec>()
            .expect("client path");

        let server = tokio::spawn(async move {
            let mut buf = [0u8; 64];
            let (len, peer) = socket.recv_from(&mut buf).await.expect("recv");
            assert_eq!(&buf[..len], b"ping");
            socket.send_to(b"pong", peer).await.expect("send");
        });

        let socket = connect_path(
            &client_path,
            UdpConnectOptions {
                timeout: Duration::from_secs(1),
                ..UdpConnectOptions::default()
            },
        )
        .await
        .expect("connect");
        socket.send(b"ping").await.expect("send");
        let mut buf = [0u8; 64];
        let len = timeout(Duration::from_secs(1), socket.recv(&mut buf))
            .await
            .expect("recv timeout")
            .expect("recv");
        assert_eq!(&buf[..len], b"pong");

        server.await.expect("join");
    }

    #[tokio::test]
    async fn udp_path_rejects_tcp_underlay() {
        let path = "tcp://127.0.0.1:1234".parse::<PathSpec>().expect("path");
        let err = connect_path(&path, UdpConnectOptions::default())
            .await
            .expect_err("wrong underlay");

        assert!(matches!(
            err,
            UdpTransportError::WrongUnderlay(UnderlayProtocol::Tcp)
        ));
    }

    async fn reserve_udp_path() -> PathSpec {
        let probe = UdpSocket::bind("127.0.0.1:0").await.expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("udp://127.0.0.1:{port}")
            .parse()
            .expect("bind path")
    }
}
