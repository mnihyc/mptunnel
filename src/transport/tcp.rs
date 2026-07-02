use crate::protocol::UnderlayProtocol;
use crate::transport::{Endpoint, PathSpec};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream, lookup_host};
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectOptions {
    pub source_ip: Option<IpAddr>,
    pub timeout: Duration,
    pub nodelay: bool,
}

impl Default for TcpConnectOptions {
    fn default() -> Self {
        Self {
            source_ip: None,
            timeout: Duration::from_secs(10),
            nodelay: true,
        }
    }
}

pub async fn connect_path(
    path: &PathSpec,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    if path.underlay != UnderlayProtocol::Tcp {
        return Err(TcpTransportError::WrongUnderlay(path.underlay));
    }
    connect_endpoint(&path.endpoint, options).await
}

pub async fn connect_endpoint(
    endpoint: &Endpoint,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    let addrs = resolve_endpoint(endpoint).await?;
    let mut last_error = None;
    for addr in addrs {
        if let Some(source_ip) = options.source_ip
            && source_ip.is_ipv4() != addr.is_ipv4()
        {
            continue;
        }
        match connect_addr(addr, options).await {
            Ok(stream) => {
                stream.set_nodelay(options.nodelay)?;
                return Ok(stream);
            }
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or(TcpTransportError::NoCompatibleAddress))
}

pub async fn bind_listener(path: &PathSpec) -> Result<TcpListener, TcpTransportError> {
    if path.underlay != UnderlayProtocol::Tcp {
        return Err(TcpTransportError::WrongUnderlay(path.underlay));
    }
    let listener = TcpListener::bind(path.endpoint.authority()).await?;
    Ok(listener)
}

async fn resolve_endpoint(endpoint: &Endpoint) -> Result<Vec<SocketAddr>, TcpTransportError> {
    let addrs = lookup_host((endpoint.host.as_str(), endpoint.port))
        .await?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        Err(TcpTransportError::ResolutionEmpty(endpoint.authority()))
    } else {
        Ok(addrs)
    }
}

pub async fn connect_addr(
    addr: SocketAddr,
    options: TcpConnectOptions,
) -> Result<TcpStream, TcpTransportError> {
    let connect = async {
        match options.source_ip {
            Some(source_ip) => {
                let socket = if addr.is_ipv4() {
                    TcpSocket::new_v4()?
                } else {
                    TcpSocket::new_v6()?
                };
                socket.bind(SocketAddr::new(source_ip, 0))?;
                socket.connect(addr).await
            }
            None => TcpStream::connect(addr).await,
        }
    };
    let stream = timeout(options.timeout, connect)
        .await
        .map_err(|_| TcpTransportError::ConnectTimedOut(addr))?
        .map_err(TcpTransportError::Io)?;
    stream.set_nodelay(options.nodelay)?;
    Ok(stream)
}

#[derive(Debug)]
pub enum TcpTransportError {
    WrongUnderlay(UnderlayProtocol),
    ResolutionEmpty(String),
    NoCompatibleAddress,
    ConnectTimedOut(SocketAddr),
    Io(std::io::Error),
}

impl From<std::io::Error> for TcpTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::fmt::Display for TcpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongUnderlay(underlay) => {
                write!(f, "TCP transport cannot use {underlay:?} path")
            }
            Self::ResolutionEmpty(authority) => {
                write!(f, "no socket addresses resolved for {authority}")
            }
            Self::NoCompatibleAddress => {
                write!(f, "no resolved address is compatible with source binding")
            }
            Self::ConnectTimedOut(addr) => write!(f, "TCP connect to {addr} timed out"),
            Self::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for TcpTransportError {
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
    use crate::protocol::codec::CodecLimits;
    use crate::protocol::{Frame, SessionId};
    use crate::transport::framed::FramedStream;
    use std::time::Duration;

    #[tokio::test]
    async fn tcp_path_connects_to_bound_listener_and_frames_work() {
        let bind_path = reserve_tcp_path().await;
        let listener = bind_listener(&bind_path).await.expect("bind listener");
        let local_addr = listener.local_addr().expect("local addr");
        let client_path = format!("tcp://{local_addr}")
            .parse::<PathSpec>()
            .expect("client path");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut framed = FramedStream::new(stream, CodecLimits::default());
            framed.read_frame().await.expect("read")
        });

        let stream = connect_path(
            &client_path,
            TcpConnectOptions {
                timeout: Duration::from_secs(1),
                ..TcpConnectOptions::default()
            },
        )
        .await
        .expect("connect");
        let mut framed = FramedStream::new(stream, CodecLimits::default());
        let frame = Frame::SessionHello {
            session_id: SessionId(7),
        };
        framed.write_frame(&frame).await.expect("write");
        framed.flush().await.expect("flush");

        assert_eq!(server.await.expect("join"), frame);
    }

    #[tokio::test]
    async fn tcp_path_rejects_udp_underlay() {
        let path = "udp://127.0.0.1:1234".parse::<PathSpec>().expect("path");
        let err = connect_path(&path, TcpConnectOptions::default())
            .await
            .expect_err("wrong underlay");

        assert!(matches!(
            err,
            TcpTransportError::WrongUnderlay(UnderlayProtocol::Udp)
        ));
    }

    async fn reserve_tcp_path() -> PathSpec {
        let probe = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve port");
        let port = probe.local_addr().expect("reserved addr").port();
        drop(probe);
        format!("tcp://127.0.0.1:{port}")
            .parse()
            .expect("bind path")
    }
}
