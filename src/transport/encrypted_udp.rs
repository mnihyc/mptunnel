use crate::protocol::Frame;
use crate::protocol::codec::{CodecError, CodecLimits, decode_frame, encode_frame};
use crate::transport::encrypted::PeerRole;
use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce, Tag};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

const MAGIC: &[u8; 4] = b"MPTU";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 18;
const TAG_LEN: usize = 16;
const DIR_CLIENT_TO_SERVER: u8 = 1;
const DIR_SERVER_TO_CLIENT: u8 = 2;
const REPLAY_WINDOW_PACKETS: u64 = 1024;

pub struct EncryptedUdpSocket {
    socket: Arc<UdpSocket>,
    cipher: ChaCha20Poly1305,
    limits: CodecLimits,
    send_direction: u8,
    recv_direction: u8,
    send_counter: u64,
    replay: ReplayWindow,
}

impl std::fmt::Debug for EncryptedUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedUdpSocket")
            .field("socket", &self.socket)
            .field("limits", &self.limits)
            .field("send_direction", &self.send_direction)
            .field("recv_direction", &self.recv_direction)
            .field("send_counter", &self.send_counter)
            .finish_non_exhaustive()
    }
}

impl EncryptedUdpSocket {
    pub fn new(socket: UdpSocket, secret: &[u8], role: PeerRole, limits: CodecLimits) -> Self {
        Self::from_shared(Arc::new(socket), secret, role, limits)
    }

    pub fn from_shared(
        socket: Arc<UdpSocket>,
        secret: &[u8],
        role: PeerRole,
        limits: CodecLimits,
    ) -> Self {
        let key = derive_key(secret);
        Self {
            socket,
            cipher: ChaCha20Poly1305::new(Key::from_slice(&key)),
            limits,
            send_direction: send_direction(role),
            recv_direction: recv_direction(role),
            send_counter: 0,
            replay: ReplayWindow::new(REPLAY_WINDOW_PACKETS),
        }
    }

    pub fn max_datagram_bytes(&self) -> Result<usize, EncryptedUdpTransportError> {
        max_datagram_bytes(self.limits)
    }

    pub fn into_inner(self) -> UdpSocket {
        match Arc::try_unwrap(self.socket) {
            Ok(socket) => socket,
            Err(_) => panic!("encrypted UDP socket is still shared"),
        }
    }

    pub async fn send_frame(&mut self, frame: &Frame) -> Result<usize, EncryptedUdpTransportError> {
        let datagram = self.seal_frame(frame)?;
        Ok(self.socket.send(&datagram).await?)
    }

    pub async fn send_frame_to(
        &mut self,
        frame: &Frame,
        peer: SocketAddr,
    ) -> Result<usize, EncryptedUdpTransportError> {
        let datagram = self.seal_frame(frame)?;
        Ok(self.socket.send_to(&datagram, peer).await?)
    }

    pub async fn recv_frame(
        &mut self,
        buffer: &mut [u8],
    ) -> Result<Frame, EncryptedUdpTransportError> {
        self.ensure_buffer_capacity(buffer)?;
        let len = self.socket.recv(buffer).await?;
        self.open_datagram(&buffer[..len])
    }

    pub async fn recv_frame_from(
        &mut self,
        buffer: &mut [u8],
    ) -> Result<(Frame, SocketAddr), EncryptedUdpTransportError> {
        self.ensure_buffer_capacity(buffer)?;
        let (len, peer) = self.socket.recv_from(buffer).await?;
        let frame = self.open_datagram(&buffer[..len])?;
        Ok((frame, peer))
    }

    pub(crate) fn open_frame_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<Frame, EncryptedUdpTransportError> {
        self.open_datagram(datagram)
    }

    fn seal_frame(&mut self, frame: &Frame) -> Result<Vec<u8>, EncryptedUdpTransportError> {
        let mut payload = encode_frame(frame, self.limits)?;
        let ciphertext_len = payload
            .len()
            .checked_add(TAG_LEN)
            .ok_or(EncryptedUdpTransportError::LengthOverflow)?;
        validate_ciphertext_len(ciphertext_len, self.limits)?;
        let header = encode_header(self.send_direction, self.send_counter, ciphertext_len)?;
        let nonce = build_nonce(self.send_direction, self.send_counter);
        let tag = self
            .cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), &header, &mut payload)
            .map_err(|_| EncryptedUdpTransportError::Crypto)?;
        let mut datagram = Vec::with_capacity(HEADER_LEN + ciphertext_len);
        datagram.extend_from_slice(&header);
        datagram.extend_from_slice(&payload);
        datagram.extend_from_slice(&tag);
        self.send_counter = self
            .send_counter
            .checked_add(1)
            .ok_or(EncryptedUdpTransportError::CounterOverflow)?;
        Ok(datagram)
    }

    fn open_datagram(&mut self, datagram: &[u8]) -> Result<Frame, EncryptedUdpTransportError> {
        if datagram.len() < HEADER_LEN {
            return Err(EncryptedUdpTransportError::UnexpectedEof);
        }
        let header: [u8; HEADER_LEN] = datagram[..HEADER_LEN]
            .try_into()
            .expect("header slice length");
        let Header {
            direction,
            counter,
            ciphertext_len,
        } = decode_header(&header, self.limits)?;
        if direction != self.recv_direction {
            return Err(EncryptedUdpTransportError::WrongDirection {
                expected: self.recv_direction,
                actual: direction,
            });
        }
        let expected_len = HEADER_LEN
            .checked_add(ciphertext_len)
            .ok_or(EncryptedUdpTransportError::LengthOverflow)?;
        if datagram.len() < expected_len {
            return Err(EncryptedUdpTransportError::UnexpectedEof);
        }
        if datagram.len() > expected_len {
            return Err(EncryptedUdpTransportError::TrailingBytes);
        }
        self.replay.check(counter)?;

        let encrypted = &datagram[HEADER_LEN..];
        let tag_start = encrypted
            .len()
            .checked_sub(TAG_LEN)
            .ok_or(EncryptedUdpTransportError::Crypto)?;
        let tag_bytes: [u8; TAG_LEN] = encrypted[tag_start..].try_into().expect("tag slice length");
        let mut payload = encrypted[..tag_start].to_vec();
        let nonce = build_nonce(direction, counter);
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                &header,
                &mut payload,
                Tag::from_slice(&tag_bytes),
            )
            .map_err(|_| EncryptedUdpTransportError::Crypto)?;
        self.replay.insert(counter);
        Ok(decode_frame(&payload, self.limits)?)
    }

    fn ensure_buffer_capacity(&self, buffer: &[u8]) -> Result<(), EncryptedUdpTransportError> {
        let minimum = self.max_datagram_bytes()?;
        if buffer.len() < minimum {
            return Err(EncryptedUdpTransportError::BufferTooSmall {
                actual: buffer.len(),
                minimum,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header {
    direction: u8,
    counter: u64,
    ciphertext_len: usize,
}

fn encode_header(
    direction: u8,
    counter: u64,
    ciphertext_len: usize,
) -> Result<[u8; HEADER_LEN], EncryptedUdpTransportError> {
    if ciphertext_len > u32::MAX as usize {
        return Err(EncryptedUdpTransportError::LengthOverflow);
    }
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[5] = direction;
    header[6..14].copy_from_slice(&counter.to_be_bytes());
    header[14..18].copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
    Ok(header)
}

fn decode_header(
    header: &[u8; HEADER_LEN],
    limits: CodecLimits,
) -> Result<Header, EncryptedUdpTransportError> {
    if &header[0..4] != MAGIC {
        return Err(EncryptedUdpTransportError::InvalidMagic);
    }
    if header[4] != VERSION {
        return Err(EncryptedUdpTransportError::UnsupportedVersion(header[4]));
    }
    let direction = header[5];
    if !matches!(direction, DIR_CLIENT_TO_SERVER | DIR_SERVER_TO_CLIENT) {
        return Err(EncryptedUdpTransportError::InvalidDirection(direction));
    }
    let counter = u64::from_be_bytes(header[6..14].try_into().expect("counter slice"));
    let ciphertext_len =
        u32::from_be_bytes(header[14..18].try_into().expect("length slice")) as usize;
    validate_ciphertext_len(ciphertext_len, limits)?;
    Ok(Header {
        direction,
        counter,
        ciphertext_len,
    })
}

fn validate_ciphertext_len(
    ciphertext_len: usize,
    limits: CodecLimits,
) -> Result<(), EncryptedUdpTransportError> {
    if ciphertext_len < TAG_LEN {
        return Err(EncryptedUdpTransportError::Crypto);
    }
    let max_ciphertext_len = limits
        .max_frame_bytes
        .checked_add(TAG_LEN)
        .ok_or(EncryptedUdpTransportError::LengthOverflow)?;
    if ciphertext_len > max_ciphertext_len {
        return Err(EncryptedUdpTransportError::FrameTooLarge {
            actual: ciphertext_len,
            limit: max_ciphertext_len,
        });
    }
    Ok(())
}

fn max_datagram_bytes(limits: CodecLimits) -> Result<usize, EncryptedUdpTransportError> {
    HEADER_LEN
        .checked_add(limits.max_frame_bytes)
        .and_then(|len| len.checked_add(TAG_LEN))
        .ok_or(EncryptedUdpTransportError::LengthOverflow)
}

fn derive_key(secret: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel encrypted udp datagram v1");
    hasher.update(secret);
    hasher.finalize().into()
}

fn build_nonce(direction: u8, counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0] = direction;
    nonce[4..12].copy_from_slice(&counter.to_be_bytes());
    nonce
}

fn send_direction(role: PeerRole) -> u8 {
    match role {
        PeerRole::Client => DIR_CLIENT_TO_SERVER,
        PeerRole::Server => DIR_SERVER_TO_CLIENT,
    }
}

fn recv_direction(role: PeerRole) -> u8 {
    match role {
        PeerRole::Client => DIR_SERVER_TO_CLIENT,
        PeerRole::Server => DIR_CLIENT_TO_SERVER,
    }
}

#[derive(Debug, Default)]
struct ReplayWindow {
    largest: Option<u64>,
    seen: BTreeSet<u64>,
    window: u64,
}

impl ReplayWindow {
    fn new(window: u64) -> Self {
        Self {
            largest: None,
            seen: BTreeSet::new(),
            window,
        }
    }

    fn check(&self, counter: u64) -> Result<(), EncryptedUdpTransportError> {
        if self.seen.contains(&counter) {
            return Err(EncryptedUdpTransportError::Replay);
        }
        if let Some(largest) = self.largest
            && counter.saturating_add(self.window) <= largest
        {
            return Err(EncryptedUdpTransportError::Replay);
        }
        Ok(())
    }

    fn insert(&mut self, counter: u64) {
        if self.largest.is_none_or(|largest| counter > largest) {
            self.largest = Some(counter);
        }
        self.seen.insert(counter);
        let floor = self.largest.unwrap_or(counter).saturating_sub(self.window);
        self.seen.retain(|seen| *seen >= floor);
    }
}

#[derive(Debug)]
pub enum EncryptedUdpTransportError {
    Io(std::io::Error),
    Codec(CodecError),
    Crypto,
    Replay,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidDirection(u8),
    WrongDirection { expected: u8, actual: u8 },
    CounterOverflow,
    LengthOverflow,
    FrameTooLarge { actual: usize, limit: usize },
    BufferTooSmall { actual: usize, minimum: usize },
    UnexpectedEof,
    TrailingBytes,
}

impl From<std::io::Error> for EncryptedUdpTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for EncryptedUdpTransportError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl std::fmt::Display for EncryptedUdpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Codec(err) => write!(f, "{err}"),
            Self::Crypto => write!(f, "encrypted UDP frame authentication failed"),
            Self::Replay => write!(f, "encrypted UDP frame counter replay detected"),
            Self::InvalidMagic => write!(f, "invalid encrypted UDP frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported encrypted UDP frame version {version}")
            }
            Self::InvalidDirection(direction) => {
                write!(f, "invalid encrypted UDP frame direction {direction}")
            }
            Self::WrongDirection { expected, actual } => {
                write!(
                    f,
                    "encrypted UDP frame direction {actual} does not match expected {expected}"
                )
            }
            Self::CounterOverflow => write!(f, "encrypted UDP frame counter overflow"),
            Self::LengthOverflow => write!(f, "encrypted UDP frame length overflow"),
            Self::FrameTooLarge { actual, limit } => {
                write!(f, "encrypted UDP frame is {actual} bytes, limit is {limit}")
            }
            Self::BufferTooSmall { actual, minimum } => {
                write!(
                    f,
                    "encrypted UDP receive buffer is {actual} bytes, minimum is {minimum}"
                )
            }
            Self::UnexpectedEof => write!(f, "unexpected end of encrypted UDP frame"),
            Self::TrailingBytes => write!(f, "encrypted UDP datagram has trailing bytes"),
        }
    }
}

impl std::error::Error for EncryptedUdpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Codec(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DatagramFlowId, DatagramId, Frame};
    use bytes::Bytes;

    #[tokio::test]
    async fn encrypted_udp_round_trips_datagram_frames() {
        let (mut client, mut server) = connected_pair(
            b"0123456789abcdef",
            b"0123456789abcdef",
            PeerRole::Client,
            PeerRole::Server,
            CodecLimits::default(),
        )
        .await;
        let frame = Frame::DatagramData {
            flow_id: DatagramFlowId(7),
            datagram_id: DatagramId(11),
            ttl_ms: 250,
            payload: Bytes::from_static(b"dns"),
        };

        client.send_frame(&frame).await.expect("send");
        let mut buffer = vec![0u8; server.max_datagram_bytes().expect("max")];

        assert_eq!(server.recv_frame(&mut buffer).await.expect("recv"), frame);
    }

    #[tokio::test]
    async fn encrypted_udp_rejects_wrong_secret() {
        let (mut client, mut server) = connected_pair(
            b"0123456789abcdef",
            b"fedcba9876543210",
            PeerRole::Client,
            PeerRole::Server,
            CodecLimits::default(),
        )
        .await;

        client
            .send_frame(&Frame::Ping { nonce: 1 })
            .await
            .expect("send");
        let mut buffer = vec![0u8; server.max_datagram_bytes().expect("max")];

        assert!(matches!(
            server.recv_frame(&mut buffer).await,
            Err(EncryptedUdpTransportError::Crypto)
        ));
    }

    #[tokio::test]
    async fn encrypted_udp_rejects_wrong_direction() {
        let (mut client, mut wrong_role_server) = connected_pair(
            b"0123456789abcdef",
            b"0123456789abcdef",
            PeerRole::Client,
            PeerRole::Client,
            CodecLimits::default(),
        )
        .await;

        client
            .send_frame(&Frame::Ping { nonce: 1 })
            .await
            .expect("send");
        let mut buffer = vec![0u8; wrong_role_server.max_datagram_bytes().expect("max")];

        assert!(matches!(
            wrong_role_server.recv_frame(&mut buffer).await,
            Err(EncryptedUdpTransportError::WrongDirection { .. })
        ));
    }

    #[tokio::test]
    async fn encrypted_udp_rejects_oversize_before_decoding_payload() {
        let raw_sender = UdpSocket::bind("127.0.0.1:0").await.expect("sender");
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("receiver");
        let receiver_addr = receiver.local_addr().expect("receiver addr");
        let limits = CodecLimits {
            max_frame_bytes: 32,
            ..CodecLimits::default()
        };
        let mut encrypted =
            EncryptedUdpSocket::new(receiver, b"0123456789abcdef", PeerRole::Server, limits);
        let header = encode_header(
            DIR_CLIENT_TO_SERVER,
            0,
            limits.max_frame_bytes + TAG_LEN + 1,
        )
        .expect("header");
        let mut datagram = Vec::from(header);
        datagram.resize(HEADER_LEN + limits.max_frame_bytes + TAG_LEN + 1, 0);

        raw_sender
            .send_to(&datagram, receiver_addr)
            .await
            .expect("send");
        let mut buffer = vec![0u8; encrypted.max_datagram_bytes().expect("max") + 1];

        assert!(matches!(
            encrypted.recv_frame_from(&mut buffer).await,
            Err(EncryptedUdpTransportError::FrameTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn encrypted_udp_rejects_replayed_counters() {
        let raw_sender = UdpSocket::bind("127.0.0.1:0").await.expect("sender");
        let receiver = UdpSocket::bind("127.0.0.1:0").await.expect("receiver");
        let sender_addr = raw_sender.local_addr().expect("sender addr");
        let receiver_addr = receiver.local_addr().expect("receiver addr");
        raw_sender
            .connect(receiver_addr)
            .await
            .expect("connect sender");
        receiver
            .connect(sender_addr)
            .await
            .expect("connect receiver");
        let mut sender = EncryptedUdpSocket::new(
            raw_sender,
            b"0123456789abcdef",
            PeerRole::Client,
            CodecLimits::default(),
        );
        let mut receiver = EncryptedUdpSocket::new(
            receiver,
            b"0123456789abcdef",
            PeerRole::Server,
            CodecLimits::default(),
        );
        let datagram = sender
            .seal_frame(&Frame::Ping { nonce: 9 })
            .expect("sealed datagram");
        let sender = sender.into_inner();
        sender.send(&datagram).await.expect("first send");
        sender.send(&datagram).await.expect("replay send");
        let mut buffer = vec![0u8; receiver.max_datagram_bytes().expect("max")];

        assert_eq!(
            receiver.recv_frame(&mut buffer).await.expect("first recv"),
            Frame::Ping { nonce: 9 }
        );
        assert!(matches!(
            receiver.recv_frame(&mut buffer).await,
            Err(EncryptedUdpTransportError::Replay)
        ));
    }

    async fn connected_pair(
        client_secret: &[u8],
        server_secret: &[u8],
        client_role: PeerRole,
        server_role: PeerRole,
        limits: CodecLimits,
    ) -> (EncryptedUdpSocket, EncryptedUdpSocket) {
        let client = UdpSocket::bind("127.0.0.1:0").await.expect("client bind");
        let server = UdpSocket::bind("127.0.0.1:0").await.expect("server bind");
        let client_addr = client.local_addr().expect("client addr");
        let server_addr = server.local_addr().expect("server addr");
        client.connect(server_addr).await.expect("client connect");
        server.connect(client_addr).await.expect("server connect");
        (
            EncryptedUdpSocket::new(client, client_secret, client_role, limits),
            EncryptedUdpSocket::new(server, server_secret, server_role, limits),
        )
    }
}
