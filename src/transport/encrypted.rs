use crate::config::CipherSuite;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
use crate::protocol::codec::{CodecError, CodecLimits, decode_frame, encode_frame};
use crate::transport::aead::{AEAD_TAG_LEN, TransportAead};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

const MAGIC: &[u8; 4] = b"MPTE";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 18;
const TAG_LEN: usize = AEAD_TAG_LEN;
const DIR_CLIENT_TO_SERVER: u8 = 1;
const DIR_SERVER_TO_CLIENT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerRole {
    Client,
    Server,
}

impl PeerRole {
    fn send_direction(self) -> u8 {
        match self {
            Self::Client => DIR_CLIENT_TO_SERVER,
            Self::Server => DIR_SERVER_TO_CLIENT,
        }
    }

    fn recv_direction(self) -> u8 {
        match self {
            Self::Client => DIR_SERVER_TO_CLIENT,
            Self::Server => DIR_CLIENT_TO_SERVER,
        }
    }
}

pub struct EncryptedFramedStream<S> {
    stream: S,
    cipher: TransportAead,
    limits: CodecLimits,
    send_direction: u8,
    recv_direction: u8,
    send_counter: u64,
    recv_counter: u64,
}

pub struct EncryptedFramedReader<R> {
    stream: R,
    cipher: TransportAead,
    limits: CodecLimits,
    recv_direction: u8,
    recv_counter: u64,
}

pub struct EncryptedFramedWriter<W> {
    stream: W,
    cipher: TransportAead,
    limits: CodecLimits,
    send_direction: u8,
    send_counter: u64,
}

impl<S: std::fmt::Debug> std::fmt::Debug for EncryptedFramedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedFramedStream")
            .field("stream", &self.stream)
            .field("limits", &self.limits)
            .field("send_direction", &self.send_direction)
            .field("recv_direction", &self.recv_direction)
            .field("send_counter", &self.send_counter)
            .field("recv_counter", &self.recv_counter)
            .finish_non_exhaustive()
    }
}

impl<S> EncryptedFramedStream<S> {
    pub fn new(stream: S, secret: &[u8], role: PeerRole, limits: CodecLimits) -> Self {
        Self::with_cipher_suite(stream, secret, role, limits, CipherSuite::default())
    }

    pub fn with_cipher_suite(
        stream: S,
        secret: &[u8],
        role: PeerRole,
        limits: CodecLimits,
        cipher_suite: CipherSuite,
    ) -> Self {
        let key = derive_key(secret, cipher_suite);
        Self {
            stream,
            cipher: TransportAead::new(cipher_suite, &key),
            limits,
            send_direction: role.send_direction(),
            recv_direction: role.recv_direction(),
            send_counter: 0,
            recv_counter: 0,
        }
    }

    pub fn limits(&self) -> CodecLimits {
        self.limits
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    pub fn split(
        self,
    ) -> (
        EncryptedFramedReader<ReadHalf<S>>,
        EncryptedFramedWriter<WriteHalf<S>>,
    )
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Self {
            stream,
            cipher,
            limits,
            send_direction,
            recv_direction,
            send_counter,
            recv_counter,
        } = self;
        let (read_half, write_half) = tokio::io::split(stream);
        (
            EncryptedFramedReader {
                stream: read_half,
                cipher: cipher.clone(),
                limits,
                recv_direction,
                recv_counter,
            },
            EncryptedFramedWriter {
                stream: write_half,
                cipher,
                limits,
                send_direction,
                send_counter,
            },
        )
    }
}

impl<S> EncryptedFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_frame_from(
            &mut self.stream,
            &self.cipher,
            self.limits,
            self.recv_direction,
            &mut self.recv_counter,
        )
        .await
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frame_to(
            &mut self.stream,
            &self.cipher,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            frame,
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        #[cfg(feature = "lab-diagnostics")]
        let started = std::time::Instant::now();
        self.stream.flush().await?;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("transport.tcp.flush_wait", started.elapsed(), 0);
        Ok(())
    }
}

impl<R> EncryptedFramedReader<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        read_frame_from(
            &mut self.stream,
            &self.cipher,
            self.limits,
            self.recv_direction,
            &mut self.recv_counter,
        )
        .await
    }
}

impl<W> EncryptedFramedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frame_to(
            &mut self.stream,
            &self.cipher,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            frame,
        )
        .await
    }

    pub async fn flush(&mut self) -> Result<(), EncryptedFramedTransportError> {
        #[cfg(feature = "lab-diagnostics")]
        let started = std::time::Instant::now();
        self.stream.flush().await?;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record("transport.tcp.flush_wait", started.elapsed(), 0);
        Ok(())
    }
}

async fn read_frame_from<R>(
    stream: &mut R,
    cipher: &TransportAead,
    limits: CodecLimits,
    recv_direction: u8,
    recv_counter: &mut u64,
) -> Result<Frame, EncryptedFramedTransportError>
where
    R: AsyncRead + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    let mut header = [0u8; HEADER_LEN];
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    stream.read_exact(&mut header).await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.read_header_wait",
        stage_started.elapsed(),
        HEADER_LEN,
    );
    let Header {
        direction,
        counter,
        ciphertext_len,
    } = decode_header(&header, limits)?;
    if direction != recv_direction {
        return Err(EncryptedFramedTransportError::WrongDirection {
            expected: recv_direction,
            actual: direction,
        });
    }
    if counter != *recv_counter {
        return Err(EncryptedFramedTransportError::UnexpectedCounter {
            expected: *recv_counter,
            actual: counter,
        });
    }

    let mut encrypted = vec![0u8; ciphertext_len];
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    stream.read_exact(&mut encrypted).await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.read_payload_wait",
        stage_started.elapsed(),
        ciphertext_len,
    );
    let tag_start = encrypted
        .len()
        .checked_sub(TAG_LEN)
        .ok_or(EncryptedFramedTransportError::Crypto)?;
    let tag_bytes: [u8; TAG_LEN] = encrypted[tag_start..].try_into().expect("tag slice length");
    encrypted.truncate(tag_start);
    let nonce = build_nonce(direction, counter);
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    cipher
        .decrypt_in_place_detached(&nonce, &header, &mut encrypted, &tag_bytes)
        .map_err(|_| EncryptedFramedTransportError::Crypto)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.decrypt",
        stage_started.elapsed(),
        encrypted.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let frame = decode_frame(&encrypted, limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.decode_frame",
        stage_started.elapsed(),
        encrypted.len(),
    );
    *recv_counter = recv_counter
        .checked_add(1)
        .ok_or(EncryptedFramedTransportError::CounterOverflow)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.read_frame_total",
        total_started.elapsed(),
        HEADER_LEN + ciphertext_len,
    );
    Ok(frame)
}

async fn write_frame_to<W>(
    stream: &mut W,
    cipher: &TransportAead,
    limits: CodecLimits,
    send_direction: u8,
    send_counter: &mut u64,
    frame: &Frame,
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let mut payload = encode_frame(frame, limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.encode_frame",
        stage_started.elapsed(),
        payload.len(),
    );
    let ciphertext_len = payload
        .len()
        .checked_add(TAG_LEN)
        .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
    validate_encrypted_len(ciphertext_len, limits)?;
    let header = encode_header(send_direction, *send_counter, ciphertext_len)?;
    let nonce = build_nonce(send_direction, *send_counter);
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let tag = cipher
        .encrypt_in_place_detached(&nonce, &header, &mut payload)
        .map_err(|_| EncryptedFramedTransportError::Crypto)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.encrypt",
        stage_started.elapsed(),
        payload.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    let written_bytes = HEADER_LEN + payload.len() + tag.len();
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let mut encrypted_frame = Vec::with_capacity(HEADER_LEN + payload.len() + tag.len());
    encrypted_frame.extend_from_slice(&header);
    encrypted_frame.extend_from_slice(&payload);
    encrypted_frame.extend_from_slice(&tag);
    stream.write_all(&encrypted_frame).await?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_socket_wait",
        stage_started.elapsed(),
        written_bytes,
    );
    *send_counter = send_counter
        .checked_add(1)
        .ok_or(EncryptedFramedTransportError::CounterOverflow)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_frame_total",
        total_started.elapsed(),
        written_bytes,
    );
    Ok(())
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
) -> Result<[u8; HEADER_LEN], EncryptedFramedTransportError> {
    if ciphertext_len > u32::MAX as usize {
        return Err(EncryptedFramedTransportError::LengthOverflow);
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
) -> Result<Header, EncryptedFramedTransportError> {
    if &header[0..4] != MAGIC {
        return Err(EncryptedFramedTransportError::InvalidMagic);
    }
    if header[4] != VERSION {
        return Err(EncryptedFramedTransportError::UnsupportedVersion(header[4]));
    }
    let direction = header[5];
    if !matches!(direction, DIR_CLIENT_TO_SERVER | DIR_SERVER_TO_CLIENT) {
        return Err(EncryptedFramedTransportError::InvalidDirection(direction));
    }
    let counter = u64::from_be_bytes(header[6..14].try_into().expect("counter slice"));
    let ciphertext_len =
        u32::from_be_bytes(header[14..18].try_into().expect("length slice")) as usize;
    validate_encrypted_len(ciphertext_len, limits)?;
    Ok(Header {
        direction,
        counter,
        ciphertext_len,
    })
}

fn validate_encrypted_len(
    ciphertext_len: usize,
    limits: CodecLimits,
) -> Result<(), EncryptedFramedTransportError> {
    if ciphertext_len < TAG_LEN {
        return Err(EncryptedFramedTransportError::Crypto);
    }
    let max_encrypted_len = limits
        .max_frame_bytes
        .checked_add(TAG_LEN)
        .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
    if ciphertext_len > max_encrypted_len {
        return Err(EncryptedFramedTransportError::FrameTooLarge {
            actual: ciphertext_len,
            limit: max_encrypted_len,
        });
    }
    Ok(())
}

fn derive_key(secret: &[u8], cipher_suite: CipherSuite) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel encrypted framed v1");
    hasher.update(cipher_suite.key_context());
    hasher.update(secret);
    hasher.finalize().into()
}

fn build_nonce(direction: u8, counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0] = direction;
    nonce[4..12].copy_from_slice(&counter.to_be_bytes());
    nonce
}

#[derive(Debug)]
pub enum EncryptedFramedTransportError {
    Io(std::io::Error),
    Codec(CodecError),
    Crypto,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidDirection(u8),
    WrongDirection { expected: u8, actual: u8 },
    UnexpectedCounter { expected: u64, actual: u64 },
    CounterOverflow,
    LengthOverflow,
    FrameTooLarge { actual: usize, limit: usize },
}

impl From<std::io::Error> for EncryptedFramedTransportError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CodecError> for EncryptedFramedTransportError {
    fn from(value: CodecError) -> Self {
        Self::Codec(value)
    }
}

impl std::fmt::Display for EncryptedFramedTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Codec(err) => write!(f, "{err}"),
            Self::Crypto => write!(f, "encrypted frame authentication failed"),
            Self::InvalidMagic => write!(f, "invalid encrypted frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported encrypted frame version {version}")
            }
            Self::InvalidDirection(direction) => {
                write!(f, "invalid encrypted frame direction {direction}")
            }
            Self::WrongDirection { expected, actual } => {
                write!(
                    f,
                    "encrypted frame direction {actual} does not match expected {expected}"
                )
            }
            Self::UnexpectedCounter { expected, actual } => {
                write!(
                    f,
                    "encrypted frame counter {actual} does not match expected {expected}"
                )
            }
            Self::CounterOverflow => write!(f, "encrypted frame counter overflow"),
            Self::LengthOverflow => write!(f, "encrypted frame length overflow"),
            Self::FrameTooLarge { actual, limit } => {
                write!(f, "encrypted frame is {actual} bytes, limit is {limit}")
            }
        }
    }
}

impl std::error::Error for EncryptedFramedTransportError {
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
    use crate::protocol::{Frame, SessionId};
    use tokio::io::{AsyncWriteExt, duplex};

    #[tokio::test]
    async fn encrypted_stream_round_trips_frames_and_hides_plaintext() {
        let (client, server) = duplex(2048);
        let limits = CodecLimits::default();
        let mut client =
            EncryptedFramedStream::new(client, b"0123456789abcdef", PeerRole::Client, limits);
        let mut server =
            EncryptedFramedStream::new(server, b"0123456789abcdef", PeerRole::Server, limits);
        let frame = Frame::SessionHello {
            session_id: SessionId(42),
        };

        client.write_frame(&frame).await.expect("write");
        client.flush().await.expect("flush");

        assert_eq!(server.read_frame().await.expect("read"), frame);
    }

    #[tokio::test]
    async fn encrypted_stream_supports_explicit_chacha20_poly1305() {
        let (client, server) = duplex(2048);
        let limits = CodecLimits::default();
        let mut client = EncryptedFramedStream::with_cipher_suite(
            client,
            b"0123456789abcdef",
            PeerRole::Client,
            limits,
            CipherSuite::Chacha20Poly1305,
        );
        let mut server = EncryptedFramedStream::with_cipher_suite(
            server,
            b"0123456789abcdef",
            PeerRole::Server,
            limits,
            CipherSuite::Chacha20Poly1305,
        );
        let frame = Frame::SessionHello {
            session_id: SessionId(43),
        };

        client.write_frame(&frame).await.expect("write");
        client.flush().await.expect("flush");

        assert_eq!(server.read_frame().await.expect("read"), frame);
    }

    #[tokio::test]
    async fn encrypted_stream_rejects_mismatched_cipher_suite() {
        let (client, server) = duplex(2048);
        let limits = CodecLimits::default();
        let mut client = EncryptedFramedStream::with_cipher_suite(
            client,
            b"0123456789abcdef",
            PeerRole::Client,
            limits,
            CipherSuite::Aes256Gcm,
        );
        let mut server = EncryptedFramedStream::with_cipher_suite(
            server,
            b"0123456789abcdef",
            PeerRole::Server,
            limits,
            CipherSuite::Chacha20Poly1305,
        );

        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(44),
            })
            .await
            .expect("write");

        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::Crypto)
        ));
    }

    #[tokio::test]
    async fn encrypted_stream_rejects_wrong_secret() {
        let (client, server) = duplex(2048);
        let limits = CodecLimits::default();
        let mut client =
            EncryptedFramedStream::new(client, b"0123456789abcdef", PeerRole::Client, limits);
        let mut server =
            EncryptedFramedStream::new(server, b"fedcba9876543210", PeerRole::Server, limits);

        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(7),
            })
            .await
            .expect("write");

        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::Crypto)
        ));
    }

    #[tokio::test]
    async fn encrypted_stream_rejects_oversize_before_ciphertext_allocation() {
        let (mut writer, reader) = duplex(1024);
        let limits = CodecLimits {
            max_frame_bytes: 32,
            ..CodecLimits::default()
        };
        let mut reader =
            EncryptedFramedStream::new(reader, b"0123456789abcdef", PeerRole::Server, limits);
        let header = encode_header(
            DIR_CLIENT_TO_SERVER,
            0,
            limits.max_frame_bytes + TAG_LEN + 1,
        )
        .expect("header");

        writer.write_all(&header).await.expect("write header");

        assert!(matches!(
            reader.read_frame().await,
            Err(EncryptedFramedTransportError::FrameTooLarge { .. })
        ));
    }
}
