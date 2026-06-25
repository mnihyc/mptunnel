pub mod http_connect;
pub mod socks5;

use crate::protocol::TargetAddr;
use crate::transport::Endpoint;
use crate::transport::tcp::{self, TcpConnectOptions, TcpTransportError};
use crate::transport::udp::{self, UdpConnectOptions, UdpTransportError};
use std::net::IpAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;

const MAX_HTTP_CONNECT_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundConfig {
    Direct,
    BindSourceIp(IpAddr),
    Socks5 { proxy: Endpoint },
    HttpConnect { proxy: Endpoint },
    ConnectUdp { proxy: Endpoint },
}

impl OutboundConfig {
    pub fn supports_tcp_targets(&self) -> bool {
        !matches!(self, Self::ConnectUdp { .. })
    }

    pub fn supports_udp_targets(&self) -> bool {
        matches!(
            self,
            Self::Direct | Self::BindSourceIp(_) | Self::Socks5 { .. } | Self::ConnectUdp { .. }
        )
    }

    pub fn ensure_supports(&self, target_protocol: TargetProtocol) -> Result<(), OutboundError> {
        match target_protocol {
            TargetProtocol::Tcp if self.supports_tcp_targets() => Ok(()),
            TargetProtocol::Udp if self.supports_udp_targets() => Ok(()),
            TargetProtocol::Tcp => Err(OutboundError::TcpNotSupported),
            TargetProtocol::Udp => Err(OutboundError::UdpNotSupported),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundError {
    TcpNotSupported,
    UdpNotSupported,
    DomainTooLong,
    InvalidTargetPort,
    InvalidProxyPort,
}

impl std::fmt::Display for OutboundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TcpNotSupported => write!(f, "outbound policy does not support TCP targets"),
            Self::UdpNotSupported => write!(f, "outbound policy does not support UDP targets"),
            Self::DomainTooLong => write!(f, "target domain is too long"),
            Self::InvalidTargetPort => write!(f, "target port must be greater than zero"),
            Self::InvalidProxyPort => write!(f, "proxy port must be greater than zero"),
        }
    }
}

impl std::error::Error for OutboundError {}

pub fn validate_target(target: &TargetAddr) -> Result<(), OutboundError> {
    if target.port() == 0 {
        return Err(OutboundError::InvalidTargetPort);
    }
    if let TargetAddr::Domain { host, .. } = target
        && host.len() > u8::MAX as usize
    {
        return Err(OutboundError::DomainTooLong);
    }
    Ok(())
}

pub async fn connect_tcp(
    config: &OutboundConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Tcp)?;
    validate_target(target)?;
    match config {
        OutboundConfig::Direct => connect_direct_tcp(target, None, timeout).await,
        OutboundConfig::BindSourceIp(ip) => connect_direct_tcp(target, Some(*ip), timeout).await,
        OutboundConfig::Socks5 { proxy } => connect_socks5_tcp(proxy, target, timeout).await,
        OutboundConfig::HttpConnect { proxy } => {
            connect_http_connect_tcp(proxy, target, timeout).await
        }
        OutboundConfig::ConnectUdp { .. } => Err(OutboundError::TcpNotSupported.into()),
    }
}

pub async fn connect_udp(
    config: &OutboundConfig,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<UdpSocket, OutboundConnectError> {
    config.ensure_supports(TargetProtocol::Udp)?;
    validate_target(target)?;
    match config {
        OutboundConfig::Direct => connect_direct_udp(target, None, timeout).await,
        OutboundConfig::BindSourceIp(ip) => connect_direct_udp(target, Some(*ip), timeout).await,
        OutboundConfig::Socks5 { .. } => Err(OutboundConnectError::UdpProxyNotImplemented(
            "SOCKS5 UDP ASSOCIATE",
        )),
        OutboundConfig::HttpConnect { .. } => Err(OutboundError::UdpNotSupported.into()),
        OutboundConfig::ConnectUdp { .. } => {
            Err(OutboundConnectError::UdpProxyNotImplemented("CONNECT-UDP"))
        }
    }
}

async fn connect_direct_tcp(
    target: &TargetAddr,
    source_ip: Option<IpAddr>,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let endpoint = endpoint_from_target(target)?;
    Ok(tcp::connect_endpoint(
        &endpoint,
        TcpConnectOptions {
            source_ip,
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?)
}

async fn connect_direct_udp(
    target: &TargetAddr,
    source_ip: Option<IpAddr>,
    timeout: Duration,
) -> Result<UdpSocket, OutboundConnectError> {
    let endpoint = endpoint_from_target(target)?;
    Ok(udp::connect_endpoint(&endpoint, UdpConnectOptions { source_ip, timeout }).await?)
}

async fn connect_socks5_tcp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let mut stream = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    stream.write_all(&socks5::no_auth_greeting()).await?;
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await?;
    let method = socks5::parse_method_selection(&method)?;
    if method.method != 0x00 {
        return Err(OutboundConnectError::ProxyAuthRejected(method.method));
    }
    let request = socks5::connect_request(target)?;
    stream.write_all(&request).await?;
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await?;
    if prefix[0] != 0x05 {
        return Err(OutboundConnectError::InvalidProxyResponse);
    }
    if prefix[1] != 0x00 {
        return Err(OutboundConnectError::ProxyRejected(prefix[1] as u16));
    }
    if prefix[2] != 0x00 {
        return Err(OutboundConnectError::InvalidProxyResponse);
    }
    let rest_len = match prefix[3] {
        0x01 => 4 + 2,
        0x04 => 16 + 2,
        0x03 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            len[0] as usize + 2
        }
        _ => return Err(OutboundConnectError::InvalidProxyResponse),
    };
    let mut rest = vec![0u8; rest_len];
    stream.read_exact(&mut rest).await?;
    Ok(stream)
}

async fn connect_http_connect_tcp(
    proxy: &Endpoint,
    target: &TargetAddr,
    timeout: Duration,
) -> Result<TcpStream, OutboundConnectError> {
    let mut stream = tcp::connect_endpoint(
        proxy,
        TcpConnectOptions {
            timeout,
            ..TcpConnectOptions::default()
        },
    )
    .await?;
    let request = http_connect::connect_request(target, None)?;
    stream.write_all(&request).await?;
    let mut response = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if response.len() >= MAX_HTTP_CONNECT_RESPONSE_BYTES {
            return Err(OutboundConnectError::InvalidProxyResponse);
        }
        stream.read_exact(&mut byte).await?;
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let response = http_connect::parse_connect_response(&response)?;
    if response.status != 200 {
        return Err(OutboundConnectError::ProxyRejected(response.status));
    }
    Ok(stream)
}

fn endpoint_from_target(target: &TargetAddr) -> Result<Endpoint, OutboundConnectError> {
    match target {
        TargetAddr::Domain { host, port } => Ok(Endpoint::new(host.clone(), *port)?),
        TargetAddr::Ip(addr) => Ok(Endpoint::new(addr.ip().to_string(), addr.port())?),
    }
}

#[derive(Debug)]
pub enum OutboundConnectError {
    Policy(OutboundError),
    Endpoint(crate::transport::EndpointParseError),
    Tcp(TcpTransportError),
    Udp(UdpTransportError),
    Io(std::io::Error),
    Socks5Client(socks5::Socks5ClientError),
    HttpConnectClient(http_connect::HttpConnectClientError),
    ProxyAuthRejected(u8),
    ProxyRejected(u16),
    UdpProxyNotImplemented(&'static str),
    InvalidProxyResponse,
}

impl From<OutboundError> for OutboundConnectError {
    fn from(value: OutboundError) -> Self {
        Self::Policy(value)
    }
}

impl From<crate::transport::EndpointParseError> for OutboundConnectError {
    fn from(value: crate::transport::EndpointParseError) -> Self {
        Self::Endpoint(value)
    }
}

impl From<TcpTransportError> for OutboundConnectError {
    fn from(value: TcpTransportError) -> Self {
        Self::Tcp(value)
    }
}

impl From<UdpTransportError> for OutboundConnectError {
    fn from(value: UdpTransportError) -> Self {
        Self::Udp(value)
    }
}

impl From<std::io::Error> for OutboundConnectError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<socks5::Socks5ClientError> for OutboundConnectError {
    fn from(value: socks5::Socks5ClientError) -> Self {
        Self::Socks5Client(value)
    }
}

impl From<http_connect::HttpConnectClientError> for OutboundConnectError {
    fn from(value: http_connect::HttpConnectClientError) -> Self {
        Self::HttpConnectClient(value)
    }
}

impl std::fmt::Display for OutboundConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(err) => write!(f, "{err}"),
            Self::Endpoint(err) => write!(f, "{err}"),
            Self::Tcp(err) => write!(f, "{err}"),
            Self::Udp(err) => write!(f, "{err}"),
            Self::Io(err) => write!(f, "{err}"),
            Self::Socks5Client(err) => write!(f, "{err}"),
            Self::HttpConnectClient(err) => write!(f, "{err}"),
            Self::ProxyAuthRejected(method) => {
                write!(f, "SOCKS5 proxy selected unsupported auth method {method}")
            }
            Self::ProxyRejected(status) => {
                write!(f, "upstream proxy rejected CONNECT with {status}")
            }
            Self::UdpProxyNotImplemented(mode) => {
                write!(f, "{mode} outbound UDP runtime is not implemented yet")
            }
            Self::InvalidProxyResponse => write!(f, "invalid upstream proxy response"),
        }
    }
}

impl std::error::Error for OutboundConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(err) => Some(err),
            Self::Endpoint(err) => Some(err),
            Self::Tcp(err) => Some(err),
            Self::Udp(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Socks5Client(err) => Some(err),
            Self::HttpConnectClient(err) => Some(err),
            Self::ProxyAuthRejected(_)
            | Self::ProxyRejected(_)
            | Self::UdpProxyNotImplemented(_)
            | Self::InvalidProxyResponse => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UdpSocket};

    #[test]
    fn outbound_support_matrix_matches_protocol_semantics() {
        assert!(
            OutboundConfig::Direct
                .ensure_supports(TargetProtocol::Tcp)
                .is_ok()
        );
        assert!(
            OutboundConfig::Direct
                .ensure_supports(TargetProtocol::Udp)
                .is_ok()
        );
        assert!(
            OutboundConfig::HttpConnect {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Tcp)
            .is_ok()
        );
        assert_eq!(
            OutboundConfig::HttpConnect {
                proxy: "127.0.0.1:8080".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Udp),
            Err(OutboundError::UdpNotSupported)
        );
        assert_eq!(
            OutboundConfig::ConnectUdp {
                proxy: "127.0.0.1:8443".parse().expect("proxy")
            }
            .ensure_supports(TargetProtocol::Tcp),
            Err(OutboundError::TcpNotSupported)
        );
    }

    #[tokio::test]
    async fn direct_tcp_outbound_connects_to_target() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::Direct,
            &TargetAddr::Ip(addr),
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn direct_udp_outbound_connects_to_target() {
        let target = UdpSocket::bind("127.0.0.1:0").await.expect("target");
        let target_addr = target.local_addr().expect("target addr");
        let server = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let (len, peer) = target.recv_from(&mut buf).await.expect("recv");
            assert_eq!(&buf[..len], b"ping");
            target.send_to(b"pong", peer).await.expect("send");
        });

        let socket = connect_udp(
            &OutboundConfig::Direct,
            &TargetAddr::Ip(target_addr),
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        socket.send(b"ping").await.expect("send");
        let mut buf = [0u8; 16];
        let len = socket.recv(&mut buf).await.expect("recv");

        assert_eq!(&buf[..len], b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn udp_proxy_outbounds_fail_explicitly_until_runtime_is_added() {
        let target = TargetAddr::Ip("127.0.0.1:53".parse().expect("target"));

        assert!(matches!(
            connect_udp(
                &OutboundConfig::Socks5 {
                    proxy: "127.0.0.1:1080".parse().expect("proxy"),
                },
                &target,
                Duration::from_secs(1),
            )
            .await,
            Err(OutboundConnectError::UdpProxyNotImplemented(
                "SOCKS5 UDP ASSOCIATE"
            ))
        ));
        assert!(matches!(
            connect_udp(
                &OutboundConfig::ConnectUdp {
                    proxy: "127.0.0.1:8443".parse().expect("proxy"),
                },
                &target,
                Duration::from_secs(1),
            )
            .await,
            Err(OutboundConnectError::UdpProxyNotImplemented("CONNECT-UDP"))
        ));
    }

    #[tokio::test]
    async fn socks5_tcp_outbound_builds_connect_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut greeting = [0u8; 3];
            stream.read_exact(&mut greeting).await.expect("greeting");
            assert_eq!(greeting, socks5::no_auth_greeting());
            stream.write_all(&[0x05, 0x00]).await.expect("method");

            let mut request = vec![0u8; 5 + 11 + 2];
            stream.read_exact(&mut request).await.expect("request");
            assert_eq!(
                request,
                socks5::connect_request(&TargetAddr::Domain {
                    host: "example.com".to_string(),
                    port: 443,
                })
                .expect("expected request")
            );
            stream
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .expect("reply");

            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("payload read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("payload write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::Socks5 { proxy },
            &TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("payload write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }

    #[tokio::test]
    async fn http_connect_tcp_outbound_builds_connect_tunnel() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let proxy: Endpoint = listener
            .local_addr()
            .expect("addr")
            .to_string()
            .parse()
            .expect("proxy");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                stream.read_exact(&mut byte).await.expect("request byte");
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert_eq!(
                request,
                http_connect::connect_request(
                    &TargetAddr::Domain {
                        host: "example.com".to_string(),
                        port: 443,
                    },
                    None,
                )
                .expect("expected request")
            );
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .expect("reply");

            let mut buf = [0u8; 4];
            stream.read_exact(&mut buf).await.expect("payload read");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").await.expect("payload write");
        });

        let mut stream = connect_tcp(
            &OutboundConfig::HttpConnect { proxy },
            &TargetAddr::Domain {
                host: "example.com".to_string(),
                port: 443,
            },
            Duration::from_secs(1),
        )
        .await
        .expect("connect");
        stream.write_all(b"ping").await.expect("payload write");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.expect("payload read");

        assert_eq!(&buf, b"pong");
        server.await.expect("server");
    }
}
