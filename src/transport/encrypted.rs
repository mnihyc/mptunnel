use crate::config::CipherSuite;
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::Frame;
use crate::protocol::codec::{CodecError, CodecLimits, decode_frame_bytes, encode_frame_into};
use crate::transport::aead::{AEAD_TAG_LEN, TransportAead};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::{self, IoSlice};
use std::sync::{Arc, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

const MAGIC: &[u8; 4] = b"MPTE";
const VERSION: u8 = 2;
const CONNECTION_SALT_LEN: usize = 16;
const HEADER_LEN: usize = 34;
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
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    send_crypto: Option<SendCrypto>,
    recv_crypto: Option<RecvCrypto>,
    limits: CodecLimits,
    send_direction: u8,
    recv_direction: u8,
    send_counter: u64,
    recv_counter: u64,
    write_poisoned: bool,
    encode_buffer: Vec<u8>,
    pending_frames: std::collections::VecDeque<Frame>,
}

pub struct EncryptedFramedReader<R> {
    stream: R,
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    recv_crypto: Option<RecvCrypto>,
    limits: CodecLimits,
    recv_direction: u8,
    recv_counter: u64,
    pending_frames: std::collections::VecDeque<Frame>,
}

pub struct EncryptedFramedWriter<W> {
    stream: W,
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    send_crypto: Option<SendCrypto>,
    limits: CodecLimits,
    send_direction: u8,
    send_counter: u64,
    write_poisoned: bool,
    wire_bytes_written: u64,
    encode_buffer: Vec<u8>,
}

pub type EncryptedFramedSplit<S> = (
    EncryptedFramedReader<ReadHalf<S>>,
    EncryptedFramedWriter<WriteHalf<S>>,
);

#[derive(Clone)]
struct SendCrypto {
    connection_salt: [u8; CONNECTION_SALT_LEN],
    cipher: TransportAead,
}

#[derive(Clone)]
struct RecvCrypto {
    connection_salt: [u8; CONNECTION_SALT_LEN],
    cipher: TransportAead,
}

struct WritePoisonGuard<'a> {
    poisoned: &'a mut bool,
    armed: bool,
}

impl<'a> WritePoisonGuard<'a> {
    fn new(poisoned: &'a mut bool) -> Result<Self, EncryptedFramedTransportError> {
        if *poisoned {
            return Err(EncryptedFramedTransportError::WriteStatePoisoned);
        }
        Ok(Self {
            poisoned,
            armed: true,
        })
    }

    fn commit(mut self) {
        self.armed = false;
    }
}

impl Drop for WritePoisonGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.poisoned = true;
        }
    }
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
    pub fn new(
        stream: S,
        secret: &[u8],
        role: PeerRole,
        limits: CodecLimits,
    ) -> Result<Self, EncryptedFramedTransportError> {
        Self::with_cipher_suite(stream, secret, role, limits, CipherSuite::default())
    }

    pub fn with_cipher_suite(
        stream: S,
        secret: &[u8],
        role: PeerRole,
        limits: CodecLimits,
        cipher_suite: CipherSuite,
    ) -> Result<Self, EncryptedFramedTransportError> {
        let key_material = derive_key_material(secret, cipher_suite);
        let client_connection_salt = Arc::new(OnceLock::new());
        let send_crypto = if role == PeerRole::Client {
            let connection_salt = random_connection_salt()?;
            client_connection_salt
                .set(connection_salt)
                .expect("new connection salt cell is empty");
            let key = derive_connection_key(
                &key_material,
                cipher_suite,
                role.send_direction(),
                &connection_salt,
                None,
            );
            Some(SendCrypto {
                connection_salt,
                cipher: TransportAead::new(cipher_suite, &key),
            })
        } else {
            None
        };
        Ok(Self {
            stream,
            key_material,
            cipher_suite,
            client_connection_salt,
            send_crypto,
            recv_crypto: None,
            limits,
            send_direction: role.send_direction(),
            recv_direction: role.recv_direction(),
            send_counter: 0,
            recv_counter: 0,
            write_poisoned: false,
            encode_buffer: Vec::new(),
            pending_frames: std::collections::VecDeque::new(),
        })
    }

    pub fn limits(&self) -> CodecLimits {
        self.limits
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    pub fn split(self) -> Result<EncryptedFramedSplit<S>, EncryptedFramedTransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let Self {
            stream,
            key_material,
            cipher_suite,
            client_connection_salt,
            send_crypto,
            recv_crypto,
            limits,
            send_direction,
            recv_direction,
            send_counter,
            recv_counter,
            write_poisoned,
            encode_buffer: _,
            pending_frames,
        } = self;
        if client_connection_salt.get().is_none() || send_crypto.is_none() || recv_crypto.is_none()
        {
            return Err(EncryptedFramedTransportError::KeyExchangeIncomplete);
        }
        let (read_half, write_half) = tokio::io::split(stream);
        Ok((
            EncryptedFramedReader {
                stream: read_half,
                key_material,
                cipher_suite,
                client_connection_salt: Arc::clone(&client_connection_salt),
                recv_crypto,
                limits,
                recv_direction,
                recv_counter,
                pending_frames,
            },
            EncryptedFramedWriter {
                stream: write_half,
                key_material,
                cipher_suite,
                client_connection_salt,
                send_crypto,
                limits,
                send_direction,
                send_counter,
                write_poisoned,
                wire_bytes_written: 0,
                encode_buffer: Vec::new(),
            },
        ))
    }
}

impl<S> EncryptedFramedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<Frame, EncryptedFramedTransportError> {
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(frame);
        }
        let mut frames = read_frames_from(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.recv_crypto,
            self.limits,
            self.recv_direction,
            &mut self.recv_counter,
        )
        .await?;
        if frames.is_empty() {
            return Err(EncryptedFramedTransportError::Codec(
                CodecError::UnexpectedEof,
            ));
        }
        for frame in frames.drain(1..) {
            self.pending_frames.push_back(frame);
        }
        Ok(frames.remove(0))
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frame_to(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.send_crypto,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            &mut self.write_poisoned,
            None,
            frame,
            &mut self.encode_buffer,
        )
        .await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frames_to(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.send_crypto,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            &mut self.write_poisoned,
            None,
            frames,
            &mut self.encode_buffer,
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
        if let Some(frame) = self.pending_frames.pop_front() {
            return Ok(frame);
        }
        let mut frames = read_frames_from(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.recv_crypto,
            self.limits,
            self.recv_direction,
            &mut self.recv_counter,
        )
        .await?;
        if frames.is_empty() {
            return Err(EncryptedFramedTransportError::Codec(
                CodecError::UnexpectedEof,
            ));
        }
        for frame in frames.drain(1..) {
            self.pending_frames.push_back(frame);
        }
        Ok(frames.remove(0))
    }

    pub async fn read_frames(&mut self) -> Result<Vec<Frame>, EncryptedFramedTransportError> {
        if !self.pending_frames.is_empty() {
            return Ok(self.pending_frames.drain(..).collect());
        }
        read_frames_from(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.recv_crypto,
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
    /// Encoded record bytes accepted by the underlying writer since this writer was split.
    pub fn wire_bytes_written(&self) -> u64 {
        self.wire_bytes_written
    }

    pub async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frame_to(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.send_crypto,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            &mut self.write_poisoned,
            Some(&mut self.wire_bytes_written),
            frame,
            &mut self.encode_buffer,
        )
        .await
    }

    pub async fn write_frames(
        &mut self,
        frames: &[Frame],
    ) -> Result<(), EncryptedFramedTransportError> {
        write_frames_to(
            &mut self.stream,
            self.key_material,
            self.cipher_suite,
            &self.client_connection_salt,
            &mut self.send_crypto,
            self.limits,
            self.send_direction,
            &mut self.send_counter,
            &mut self.write_poisoned,
            Some(&mut self.wire_bytes_written),
            frames,
            &mut self.encode_buffer,
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

#[allow(clippy::too_many_arguments)]
async fn read_frames_from<R>(
    stream: &mut R,
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: &Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    recv_crypto: &mut Option<RecvCrypto>,
    limits: CodecLimits,
    recv_direction: u8,
    recv_counter: &mut u64,
) -> Result<Vec<Frame>, EncryptedFramedTransportError>
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
        connection_salt,
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
    let pending_crypto = match recv_crypto {
        Some(crypto) if crypto.connection_salt != connection_salt => {
            return Err(EncryptedFramedTransportError::ConnectionSaltChanged);
        }
        Some(_) => None,
        None => {
            let client_salt = match direction {
                DIR_CLIENT_TO_SERVER => connection_salt,
                DIR_SERVER_TO_CLIENT => *client_connection_salt
                    .get()
                    .ok_or(EncryptedFramedTransportError::MissingClientConnectionSalt)?,
                _ => unreachable!("header direction validated"),
            };
            let server_salt = (direction == DIR_SERVER_TO_CLIENT).then_some(&connection_salt);
            let key = derive_connection_key(
                &key_material,
                cipher_suite,
                direction,
                &client_salt,
                server_salt,
            );
            Some(RecvCrypto {
                connection_salt,
                cipher: TransportAead::new(cipher_suite, &key),
            })
        }
    };

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
    recv_crypto
        .as_ref()
        .or(pending_crypto.as_ref())
        .expect("existing or pending receive crypto is available")
        .cipher
        .decrypt_in_place_detached(&nonce, &header, &mut encrypted, &tag_bytes)
        .map_err(|_| EncryptedFramedTransportError::Crypto)?;
    if let Some(crypto) = pending_crypto {
        if direction == DIR_CLIENT_TO_SERVER {
            client_connection_salt
                .set(connection_salt)
                .map_err(|_| EncryptedFramedTransportError::ConnectionSaltChanged)?;
        }
        *recv_crypto = Some(crypto);
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.decrypt",
        stage_started.elapsed(),
        encrypted.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let decrypted_len = encrypted.len();
    let frame = decode_frame_bytes(Bytes::from(encrypted), limits)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.decode_frame",
        stage_started.elapsed(),
        decrypted_len,
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
    Ok(vec![frame])
}

#[allow(clippy::too_many_arguments)]
async fn write_frame_to<W>(
    stream: &mut W,
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: &Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    send_crypto: &mut Option<SendCrypto>,
    limits: CodecLimits,
    send_direction: u8,
    send_counter: &mut u64,
    write_poisoned: &mut bool,
    wire_bytes_written: Option<&mut u64>,
    frame: &Frame,
    payload: &mut Vec<u8>,
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    if *write_poisoned {
        return Err(EncryptedFramedTransportError::WriteStatePoisoned);
    }
    let next_counter = send_counter
        .checked_add(1)
        .ok_or(EncryptedFramedTransportError::CounterOverflow)?;
    initialize_send_crypto(
        &key_material,
        cipher_suite,
        send_direction,
        client_connection_salt,
        send_crypto,
    )?;
    let crypto = send_crypto
        .as_ref()
        .expect("send crypto initialized before record encoding");
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    payload.clear();
    encode_frame_into(frame, limits, payload)?;
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
    let header = encode_header(
        send_direction,
        crypto.connection_salt,
        *send_counter,
        ciphertext_len,
    )?;
    let nonce = build_nonce(send_direction, *send_counter);
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let tag = crypto
        .cipher
        .encrypt_in_place_detached(&nonce, &header, payload)
        .map_err(|_| EncryptedFramedTransportError::Crypto)?;
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.encrypt",
        stage_started.elapsed(),
        payload.len(),
    );
    let written_bytes = HEADER_LEN
        .checked_add(payload.len())
        .and_then(|len| len.checked_add(tag.len()))
        .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
    let next_wire_bytes_written = match wire_bytes_written.as_deref() {
        Some(current) => Some(
            current
                .checked_add(
                    u64::try_from(written_bytes)
                        .map_err(|_| EncryptedFramedTransportError::LengthOverflow)?,
                )
                .ok_or(EncryptedFramedTransportError::LengthOverflow)?,
        ),
        None => None,
    };
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let write_guard = WritePoisonGuard::new(write_poisoned)?;
    write_all_vectored_parts(stream, [&header, payload.as_slice(), tag.as_slice()]).await?;
    *send_counter = next_counter;
    if let Some(next_wire_bytes_written) = next_wire_bytes_written {
        *wire_bytes_written.expect("wire byte counter exists when next value was calculated") =
            next_wire_bytes_written;
    }
    write_guard.commit();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_socket_wait",
        stage_started.elapsed(),
        written_bytes,
    );
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_frame_total",
        total_started.elapsed(),
        written_bytes,
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_frames_to<W>(
    stream: &mut W,
    key_material: [u8; 32],
    cipher_suite: CipherSuite,
    client_connection_salt: &Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    send_crypto: &mut Option<SendCrypto>,
    limits: CodecLimits,
    send_direction: u8,
    send_counter: &mut u64,
    write_poisoned: &mut bool,
    wire_bytes_written: Option<&mut u64>,
    frames: &[Frame],
    payload: &mut Vec<u8>,
) -> Result<(), EncryptedFramedTransportError>
where
    W: AsyncWrite + Unpin,
{
    if frames.is_empty() {
        return Ok(());
    }
    if *write_poisoned {
        return Err(EncryptedFramedTransportError::WriteStatePoisoned);
    }
    if frames.len() == 1 {
        return write_frame_to(
            stream,
            key_material,
            cipher_suite,
            client_connection_salt,
            send_crypto,
            limits,
            send_direction,
            send_counter,
            write_poisoned,
            wire_bytes_written,
            &frames[0],
            payload,
        )
        .await;
    }
    let frame_count =
        u64::try_from(frames.len()).map_err(|_| EncryptedFramedTransportError::CounterOverflow)?;
    let final_counter = send_counter
        .checked_add(frame_count)
        .ok_or(EncryptedFramedTransportError::CounterOverflow)?;
    initialize_send_crypto(
        &key_material,
        cipher_suite,
        send_direction,
        client_connection_salt,
        send_crypto,
    )?;
    let crypto = send_crypto
        .as_ref()
        .expect("send crypto initialized before record encoding");
    #[cfg(feature = "lab-diagnostics")]
    let total_started = std::time::Instant::now();
    payload.clear();
    let mut next_counter = *send_counter;
    for frame in frames {
        let record_start = payload.len();
        payload.resize(record_start + HEADER_LEN, 0);
        let plaintext_start = payload.len();
        #[cfg(feature = "lab-diagnostics")]
        let stage_started = std::time::Instant::now();
        encode_frame_into(frame, limits, payload)?;
        let plaintext_len = payload.len().saturating_sub(plaintext_start);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "transport.tcp.encode_frame",
            stage_started.elapsed(),
            plaintext_len,
        );
        let ciphertext_len = plaintext_len
            .checked_add(TAG_LEN)
            .ok_or(EncryptedFramedTransportError::LengthOverflow)?;
        validate_encrypted_len(ciphertext_len, limits)?;
        let header = encode_header(
            send_direction,
            crypto.connection_salt,
            next_counter,
            ciphertext_len,
        )?;
        payload[record_start..plaintext_start].copy_from_slice(&header);
        let nonce = build_nonce(send_direction, next_counter);
        #[cfg(feature = "lab-diagnostics")]
        let stage_started = std::time::Instant::now();
        let tag = crypto
            .cipher
            .encrypt_in_place_detached(&nonce, &header, &mut payload[plaintext_start..])
            .map_err(|_| EncryptedFramedTransportError::Crypto)?;
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "transport.tcp.encrypt",
            stage_started.elapsed(),
            plaintext_len,
        );
        payload.extend_from_slice(&tag);
        next_counter += 1;
    }
    let next_wire_bytes_written = match wire_bytes_written.as_deref() {
        Some(current) => Some(
            current
                .checked_add(
                    u64::try_from(payload.len())
                        .map_err(|_| EncryptedFramedTransportError::LengthOverflow)?,
                )
                .ok_or(EncryptedFramedTransportError::LengthOverflow)?,
        ),
        None => None,
    };
    #[cfg(feature = "lab-diagnostics")]
    let stage_started = std::time::Instant::now();
    let write_guard = WritePoisonGuard::new(write_poisoned)?;
    stream.write_all(payload).await?;
    debug_assert_eq!(next_counter, final_counter);
    *send_counter = final_counter;
    if let Some(next_wire_bytes_written) = next_wire_bytes_written {
        *wire_bytes_written.expect("wire byte counter exists when next value was calculated") =
            next_wire_bytes_written;
    }
    write_guard.commit();
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_socket_wait",
        stage_started.elapsed(),
        payload.len(),
    );
    #[cfg(feature = "lab-diagnostics")]
    lab_perf_record(
        "transport.tcp.write_frame_total",
        total_started.elapsed(),
        payload.len(),
    );
    Ok(())
}

async fn write_all_vectored_parts<W>(stream: &mut W, parts: [&[u8]; 3]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut part_index = 0usize;
    let mut part_offset = 0usize;
    while part_index < parts.len() {
        while part_index < parts.len() && part_offset == parts[part_index].len() {
            part_index += 1;
            part_offset = 0;
        }
        if part_index >= parts.len() {
            return Ok(());
        }

        let first = &parts[part_index][part_offset..];
        let second = parts.get(part_index + 1).copied().unwrap_or(&[]);
        let third = parts.get(part_index + 2).copied().unwrap_or(&[]);
        let bufs = [
            IoSlice::new(first),
            IoSlice::new(second),
            IoSlice::new(third),
        ];
        let mut written = stream.write_vectored(&bufs).await?;
        if written == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        while written > 0 && part_index < parts.len() {
            let available = parts[part_index].len().saturating_sub(part_offset);
            if written < available {
                part_offset += written;
                written = 0;
            } else {
                written -= available;
                part_index += 1;
                part_offset = 0;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Header {
    direction: u8,
    connection_salt: [u8; CONNECTION_SALT_LEN],
    counter: u64,
    ciphertext_len: usize,
}

fn encode_header(
    direction: u8,
    connection_salt: [u8; CONNECTION_SALT_LEN],
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
    header[6..22].copy_from_slice(&connection_salt);
    header[22..30].copy_from_slice(&counter.to_be_bytes());
    header[30..34].copy_from_slice(&(ciphertext_len as u32).to_be_bytes());
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
    let connection_salt = header[6..22].try_into().expect("connection salt slice");
    let counter = u64::from_be_bytes(header[22..30].try_into().expect("counter slice"));
    let ciphertext_len =
        u32::from_be_bytes(header[30..34].try_into().expect("length slice")) as usize;
    validate_encrypted_len(ciphertext_len, limits)?;
    Ok(Header {
        direction,
        connection_salt,
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

fn derive_key_material(secret: &[u8], cipher_suite: CipherSuite) -> [u8; 32] {
    hkdf_extract(
        b"mptunnel encrypted framed v2",
        &[cipher_suite.key_context(), secret],
    )
}

fn derive_connection_key(
    key_material: &[u8; 32],
    cipher_suite: CipherSuite,
    direction: u8,
    client_connection_salt: &[u8; CONNECTION_SALT_LEN],
    server_connection_salt: Option<&[u8; CONNECTION_SALT_LEN]>,
) -> [u8; 32] {
    let prk = hkdf_extract(client_connection_salt, &[key_material]);
    let mut mac = Hmac::<Sha256>::new_from_slice(&prk).expect("HMAC accepts a 32-byte key");
    mac.update(b"mptunnel encrypted framed v2 traffic key");
    mac.update(cipher_suite.key_context());
    mac.update(&[direction]);
    if let Some(server_connection_salt) = server_connection_salt {
        mac.update(server_connection_salt);
    }
    mac.update(&[1]);
    mac.finalize().into_bytes().into()
}

fn hkdf_extract(salt: &[u8], ikm_parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(salt).expect("HMAC accepts any salt length");
    for part in ikm_parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn initialize_send_crypto(
    key_material: &[u8; 32],
    cipher_suite: CipherSuite,
    send_direction: u8,
    client_connection_salt: &Arc<OnceLock<[u8; CONNECTION_SALT_LEN]>>,
    send_crypto: &mut Option<SendCrypto>,
) -> Result<(), EncryptedFramedTransportError> {
    if send_crypto.is_some() {
        return Ok(());
    }
    let client_salt = *client_connection_salt
        .get()
        .ok_or(EncryptedFramedTransportError::MissingClientConnectionSalt)?;
    let connection_salt = match send_direction {
        DIR_CLIENT_TO_SERVER => client_salt,
        DIR_SERVER_TO_CLIENT => random_connection_salt()?,
        _ => {
            return Err(EncryptedFramedTransportError::InvalidDirection(
                send_direction,
            ));
        }
    };
    let server_salt = (send_direction == DIR_SERVER_TO_CLIENT).then_some(&connection_salt);
    let key = derive_connection_key(
        key_material,
        cipher_suite,
        send_direction,
        &client_salt,
        server_salt,
    );
    *send_crypto = Some(SendCrypto {
        connection_salt,
        cipher: TransportAead::new(cipher_suite, &key),
    });
    Ok(())
}

fn random_connection_salt() -> Result<[u8; CONNECTION_SALT_LEN], EncryptedFramedTransportError> {
    let mut connection_salt = [0u8; CONNECTION_SALT_LEN];
    getrandom::getrandom(&mut connection_salt).map_err(EncryptedFramedTransportError::Random)?;
    Ok(connection_salt)
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
    Random(getrandom::Error),
    Crypto,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidDirection(u8),
    WrongDirection { expected: u8, actual: u8 },
    ConnectionSaltChanged,
    MissingClientConnectionSalt,
    KeyExchangeIncomplete,
    WriteStatePoisoned,
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
            Self::Random(err) => write!(f, "failed to generate TCP connection salt: {err}"),
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
            Self::ConnectionSaltChanged => {
                write!(
                    f,
                    "encrypted frame connection salt changed within one direction"
                )
            }
            Self::MissingClientConnectionSalt => {
                write!(
                    f,
                    "TCP encryption requires an authenticated client connection salt"
                )
            }
            Self::KeyExchangeIncomplete => {
                write!(f, "TCP encryption key exchange is incomplete")
            }
            Self::WriteStatePoisoned => {
                write!(
                    f,
                    "encrypted TCP write state is unusable after an incomplete write"
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
            Self::Random(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::codec::{encode_frame, encode_frames};
    use crate::protocol::{Frame, SessionId};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    const TEST_SECRET: &[u8] = b"0123456789abcdef";

    fn test_stream<S>(stream: S, role: PeerRole, limits: CodecLimits) -> EncryptedFramedStream<S> {
        EncryptedFramedStream::new(stream, TEST_SECRET, role, limits)
            .expect("initialize encrypted stream")
    }

    fn encrypt_test_record(
        direction: u8,
        client_salt: [u8; CONNECTION_SALT_LEN],
        server_salt: Option<[u8; CONNECTION_SALT_LEN]>,
        counter: u64,
        mut plaintext: Vec<u8>,
    ) -> Vec<u8> {
        let connection_salt = if direction == DIR_CLIENT_TO_SERVER {
            client_salt
        } else {
            server_salt.expect("server salt")
        };
        let header = encode_header(
            direction,
            connection_salt,
            counter,
            plaintext.len() + TAG_LEN,
        )
        .expect("encode header");
        let key_material = derive_key_material(TEST_SECRET, CipherSuite::default());
        let key = derive_connection_key(
            &key_material,
            CipherSuite::default(),
            direction,
            &client_salt,
            server_salt.as_ref(),
        );
        let cipher = TransportAead::new(CipherSuite::default(), &key);
        let tag = cipher
            .encrypt_in_place_detached(&build_nonce(direction, counter), &header, &mut plaintext)
            .expect("encrypt test record");
        let mut record = Vec::with_capacity(header.len() + plaintext.len() + tag.len());
        record.extend_from_slice(&header);
        record.extend_from_slice(&plaintext);
        record.extend_from_slice(&tag);
        record
    }

    async fn read_test_record(
        wire: &mut tokio::io::DuplexStream,
        limits: CodecLimits,
    ) -> (Header, Vec<u8>) {
        let mut header_bytes = [0u8; HEADER_LEN];
        wire.read_exact(&mut header_bytes)
            .await
            .expect("read header");
        let header = decode_header(&header_bytes, limits).expect("decode header");
        let mut encrypted = vec![0u8; header.ciphertext_len];
        wire.read_exact(&mut encrypted)
            .await
            .expect("read ciphertext");
        (header, encrypted)
    }

    #[tokio::test]
    async fn encrypted_stream_round_trips_frames_and_hides_plaintext() {
        let (client, server) = duplex(2048);
        let limits = CodecLimits::default();
        let mut client = test_stream(client, PeerRole::Client, limits);
        let mut server = test_stream(server, PeerRole::Server, limits);
        let frame = Frame::SessionHello {
            session_id: SessionId(42),
        };

        client.write_frame(&frame).await.expect("write");
        client.flush().await.expect("flush");

        assert_eq!(server.read_frame().await.expect("read"), frame);
        server
            .write_frame(&Frame::Pong { nonce: 9 })
            .await
            .expect("write response");
        assert_eq!(
            client.read_frame().await.expect("read response"),
            Frame::Pong { nonce: 9 }
        );
        client.split().expect("client key exchange complete");
        server.split().expect("server key exchange complete");
    }

    #[tokio::test]
    async fn encrypted_writer_keeps_one_protocol_frame_per_record() {
        let (client, mut wire) = duplex(4096);
        let limits = CodecLimits::default();
        let mut client = test_stream(client, PeerRole::Client, limits);
        let frames = vec![
            Frame::SessionHello {
                session_id: SessionId(42),
            },
            Frame::Ping { nonce: 7 },
            Frame::Pong { nonce: 7 },
        ];

        client.write_frames(&frames).await.expect("write batch");
        client.flush().await.expect("flush");

        let mut connection_salt = None;
        for (counter, frame) in frames.iter().enumerate() {
            let mut header = [0u8; HEADER_LEN];
            wire.read_exact(&mut header).await.expect("read header");
            let header = decode_header(&header, limits).expect("decode header");
            assert_eq!(header.counter, counter as u64);
            assert_eq!(
                *connection_salt.get_or_insert(header.connection_salt),
                header.connection_salt
            );
            assert_eq!(
                header.ciphertext_len,
                crate::protocol::codec::encode_frame(frame, limits)
                    .expect("encode expected frame")
                    .len()
                    + TAG_LEN
            );
            let mut ciphertext = vec![0u8; header.ciphertext_len];
            wire.read_exact(&mut ciphertext)
                .await
                .expect("read ciphertext");
        }
    }

    #[tokio::test]
    async fn split_writer_accumulates_exact_encoded_wire_bytes() {
        let (client, server) = duplex(4096);
        let limits = CodecLimits::default();
        let mut client = test_stream(client, PeerRole::Client, limits);
        let mut server = test_stream(server, PeerRole::Server, limits);
        let hello = Frame::SessionHello {
            session_id: SessionId(42),
        };
        client.write_frame(&hello).await.expect("write hello");
        assert_eq!(server.read_frame().await.expect("read hello"), hello);
        server
            .write_frame(&Frame::Pong { nonce: 42 })
            .await
            .expect("write response");
        assert_eq!(
            client.read_frame().await.expect("read response"),
            Frame::Pong { nonce: 42 }
        );
        let (_reader, mut writer) = client.split().expect("split confirmed client");
        let first = Frame::Ping { nonce: 7 };
        let batch = [Frame::Pong { nonce: 7 }, Frame::Ping { nonce: 8 }];
        let record_wire_len = |frame: &Frame| {
            u64::try_from(
                HEADER_LEN
                    + encode_frame(frame, limits)
                        .expect("encode expected frame")
                        .len()
                    + TAG_LEN,
            )
            .expect("test record length fits u64")
        };

        assert_eq!(writer.wire_bytes_written(), 0);
        writer.write_frame(&first).await.expect("write first frame");
        assert_eq!(writer.wire_bytes_written(), record_wire_len(&first));
        writer
            .write_frames(&batch)
            .await
            .expect("write frame batch");
        assert_eq!(
            writer.wire_bytes_written(),
            record_wire_len(&first) + batch.iter().map(record_wire_len).sum::<u64>()
        );
    }

    #[tokio::test]
    async fn split_writer_does_not_publish_partial_record_bytes() {
        let (client, server) = duplex(256);
        let limits = CodecLimits::default();
        let mut client = test_stream(client, PeerRole::Client, limits);
        let mut server = test_stream(server, PeerRole::Server, limits);
        client
            .write_frame(&Frame::SessionHello {
                session_id: SessionId(42),
            })
            .await
            .expect("write hello");
        server.read_frame().await.expect("read hello");
        server
            .write_frame(&Frame::Pong { nonce: 42 })
            .await
            .expect("write response");
        client.read_frame().await.expect("read response");
        let (_reader, mut writer) = client.split().expect("split confirmed client");
        let frame = Frame::PathCapacityData {
            path_id: crate::protocol::PathId(1),
            calibration_id: 7,
            payload: Bytes::from(vec![0; 4096]),
        };

        assert!(
            tokio::time::timeout(Duration::from_millis(10), writer.write_frame(&frame))
                .await
                .is_err()
        );
        assert_eq!(writer.wire_bytes_written(), 0);
        assert!(matches!(
            writer.write_frame(&Frame::Ping { nonce: 9 }).await,
            Err(EncryptedFramedTransportError::WriteStatePoisoned)
        ));
        assert_eq!(writer.wire_bytes_written(), 0);
    }

    #[tokio::test]
    async fn encrypted_reader_rejects_multi_frame_record() {
        let (mut wire, server) = duplex(4096);
        let limits = CodecLimits::default();
        let mut server = test_stream(server, PeerRole::Server, limits);
        let plaintext = encode_frames(
            &[Frame::Ping { nonce: 7 }, Frame::Pong { nonce: 7 }],
            limits,
        )
        .expect("encode multi-frame plaintext");
        let record = encrypt_test_record(DIR_CLIENT_TO_SERVER, [7; 16], None, 0, plaintext);
        wire.write_all(&record).await.expect("write record");

        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::Codec(
                CodecError::TrailingBytes
            ))
        ));
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
        )
        .expect("initialize client");
        let mut server = EncryptedFramedStream::with_cipher_suite(
            server,
            b"0123456789abcdef",
            PeerRole::Server,
            limits,
            CipherSuite::Chacha20Poly1305,
        )
        .expect("initialize server");
        let frame = Frame::SessionHello {
            session_id: SessionId(43),
        };

        client.write_frame(&frame).await.expect("write");
        client.flush().await.expect("flush");

        assert_eq!(server.read_frame().await.expect("read"), frame);
        server
            .write_frame(&Frame::Pong { nonce: 43 })
            .await
            .expect("write response");
        assert_eq!(
            client.read_frame().await.expect("read response"),
            Frame::Pong { nonce: 43 }
        );
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
        )
        .expect("initialize client");
        let mut server = EncryptedFramedStream::with_cipher_suite(
            server,
            b"0123456789abcdef",
            PeerRole::Server,
            limits,
            CipherSuite::Chacha20Poly1305,
        )
        .expect("initialize server");

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
        let mut client = test_stream(client, PeerRole::Client, limits);
        let mut server =
            EncryptedFramedStream::new(server, b"fedcba9876543210", PeerRole::Server, limits)
                .expect("initialize server");

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
        let mut reader = test_stream(reader, PeerRole::Server, limits);
        let header = encode_header(
            DIR_CLIENT_TO_SERVER,
            [1; CONNECTION_SALT_LEN],
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

    #[test]
    fn encrypted_connection_keys_are_domain_separated() {
        let key_material = derive_key_material(TEST_SECRET, CipherSuite::Aes256Gcm);
        let client_salt = [3; CONNECTION_SALT_LEN];
        let other_client_salt = [4; CONNECTION_SALT_LEN];
        let server_salt = [5; CONNECTION_SALT_LEN];
        let other_server_salt = [6; CONNECTION_SALT_LEN];
        let client_key = derive_connection_key(
            &key_material,
            CipherSuite::Aes256Gcm,
            DIR_CLIENT_TO_SERVER,
            &client_salt,
            None,
        );
        let server_key = derive_connection_key(
            &key_material,
            CipherSuite::Aes256Gcm,
            DIR_SERVER_TO_CLIENT,
            &client_salt,
            Some(&server_salt),
        );

        assert_ne!(client_key, server_key);
        assert_ne!(
            client_key,
            derive_connection_key(
                &key_material,
                CipherSuite::Aes256Gcm,
                DIR_CLIENT_TO_SERVER,
                &other_client_salt,
                None,
            )
        );
        assert_ne!(
            server_key,
            derive_connection_key(
                &key_material,
                CipherSuite::Aes256Gcm,
                DIR_SERVER_TO_CLIENT,
                &client_salt,
                Some(&other_server_salt),
            )
        );
    }

    #[tokio::test]
    async fn identical_first_frames_use_distinct_connection_domains() {
        let limits = CodecLimits::default();
        let (client_one, mut wire_one) = duplex(2048);
        let (client_two, mut wire_two) = duplex(2048);
        let mut client_one = test_stream(client_one, PeerRole::Client, limits);
        let mut client_two = test_stream(client_two, PeerRole::Client, limits);
        let frame = Frame::Ping { nonce: 77 };

        client_one.write_frame(&frame).await.expect("write one");
        client_two.write_frame(&frame).await.expect("write two");
        let (header_one, ciphertext_one) = read_test_record(&mut wire_one, limits).await;
        let (header_two, ciphertext_two) = read_test_record(&mut wire_two, limits).await;

        assert_eq!(header_one.counter, 0);
        assert_eq!(header_two.counter, 0);
        assert_ne!(header_one.connection_salt, header_two.connection_salt);
        assert_ne!(ciphertext_one, ciphertext_two);
    }

    #[tokio::test]
    async fn failed_first_tag_does_not_bind_receive_salt() {
        let limits = CodecLimits::default();
        let (mut wire, server) = duplex(4096);
        let mut server = test_stream(server, PeerRole::Server, limits);
        let plaintext = encode_frame(&Frame::Ping { nonce: 1 }, limits).expect("encode");
        let mut invalid = encrypt_test_record(
            DIR_CLIENT_TO_SERVER,
            [8; CONNECTION_SALT_LEN],
            None,
            0,
            plaintext,
        );
        *invalid.last_mut().expect("tag byte") ^= 1;
        wire.write_all(&invalid)
            .await
            .expect("write invalid record");
        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::Crypto)
        ));

        let plaintext = encode_frame(&Frame::Ping { nonce: 2 }, limits).expect("encode");
        let valid = encrypt_test_record(
            DIR_CLIENT_TO_SERVER,
            [9; CONNECTION_SALT_LEN],
            None,
            0,
            plaintext,
        );
        wire.write_all(&valid).await.expect("write valid record");
        assert_eq!(
            server.read_frame().await.expect("read valid record"),
            Frame::Ping { nonce: 2 }
        );
    }

    #[tokio::test]
    async fn encrypted_reader_rejects_midstream_connection_salt_change() {
        let limits = CodecLimits::default();
        let (mut wire, server) = duplex(4096);
        let mut server = test_stream(server, PeerRole::Server, limits);
        let first = encrypt_test_record(
            DIR_CLIENT_TO_SERVER,
            [10; CONNECTION_SALT_LEN],
            None,
            0,
            encode_frame(&Frame::Ping { nonce: 1 }, limits).expect("encode first"),
        );
        wire.write_all(&first).await.expect("write first");
        assert_eq!(
            server.read_frame().await.expect("read first"),
            Frame::Ping { nonce: 1 }
        );

        let changed = encrypt_test_record(
            DIR_CLIENT_TO_SERVER,
            [11; CONNECTION_SALT_LEN],
            None,
            1,
            encode_frame(&Frame::Ping { nonce: 2 }, limits).expect("encode second"),
        );
        wire.write_all(&changed).await.expect("write changed salt");
        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::ConnectionSaltChanged)
        ));
    }

    #[tokio::test]
    async fn old_server_record_fails_against_fresh_client_connection() {
        let limits = CodecLimits::default();
        let old_client_salt = [12; CONNECTION_SALT_LEN];
        let (mut old_wire, old_server) = duplex(4096);
        let mut old_server = test_stream(old_server, PeerRole::Server, limits);
        let request = encrypt_test_record(
            DIR_CLIENT_TO_SERVER,
            old_client_salt,
            None,
            0,
            encode_frame(&Frame::Ping { nonce: 3 }, limits).expect("encode request"),
        );
        old_wire
            .write_all(&request)
            .await
            .expect("write old request");
        old_server.read_frame().await.expect("read old request");
        old_server
            .write_frame(&Frame::Pong { nonce: 3 })
            .await
            .expect("write old response");
        let (old_header, old_ciphertext) = read_test_record(&mut old_wire, limits).await;
        let mut old_record = encode_header(
            old_header.direction,
            old_header.connection_salt,
            old_header.counter,
            old_header.ciphertext_len,
        )
        .expect("re-encode old header")
        .to_vec();
        old_record.extend_from_slice(&old_ciphertext);

        let (mut fresh_wire, fresh_client) = duplex(4096);
        let mut fresh_client = test_stream(fresh_client, PeerRole::Client, limits);
        assert_ne!(
            fresh_client.client_connection_salt.get(),
            Some(&old_client_salt)
        );
        fresh_wire
            .write_all(&old_record)
            .await
            .expect("replay old response");
        assert!(matches!(
            fresh_client.read_frame().await,
            Err(EncryptedFramedTransportError::Crypto)
        ));
    }

    #[tokio::test]
    async fn encrypted_reader_rejects_mptev1_without_fallback() {
        let limits = CodecLimits::default();
        let (mut wire, server) = duplex(1024);
        let mut server = test_stream(server, PeerRole::Server, limits);
        let mut old_header = [0u8; HEADER_LEN];
        old_header[0..4].copy_from_slice(MAGIC);
        old_header[4] = 1;
        old_header[5] = DIR_CLIENT_TO_SERVER;
        wire.write_all(&old_header).await.expect("write v1 header");

        assert!(matches!(
            server.read_frame().await,
            Err(EncryptedFramedTransportError::UnsupportedVersion(1))
        ));
    }

    #[tokio::test]
    async fn encrypted_stream_rejects_split_before_bidirectional_key_confirmation() {
        let (client, _wire) = duplex(1024);
        let client = test_stream(client, PeerRole::Client, CodecLimits::default());

        assert!(matches!(
            client.split(),
            Err(EncryptedFramedTransportError::KeyExchangeIncomplete)
        ));
    }

    #[tokio::test]
    async fn server_cannot_emit_before_authenticating_client_connection_salt() {
        let (server, _wire) = duplex(1024);
        let mut server = test_stream(server, PeerRole::Server, CodecLimits::default());

        assert!(matches!(
            server.write_frame(&Frame::Pong { nonce: 1 }).await,
            Err(EncryptedFramedTransportError::MissingClientConnectionSalt)
        ));
        assert_eq!(server.send_counter, 0);
    }

    #[tokio::test]
    async fn encrypted_writer_rejects_counter_overflow_before_emission() {
        let (client, mut wire) = duplex(1024);
        let mut client = test_stream(client, PeerRole::Client, CodecLimits::default());
        client.send_counter = u64::MAX;

        assert!(matches!(
            client.write_frame(&Frame::Ping { nonce: 1 }).await,
            Err(EncryptedFramedTransportError::CounterOverflow)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), wire.read_u8())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cancelled_partial_write_permanently_poisons_writer() {
        let (client, _wire) = duplex(1);
        let mut client = test_stream(client, PeerRole::Client, CodecLimits::default());

        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                client.write_frame(&Frame::Ping { nonce: 1 }),
            )
            .await
            .is_err()
        );
        assert_eq!(client.send_counter, 0);
        assert!(matches!(
            client.write_frame(&Frame::Ping { nonce: 2 }).await,
            Err(EncryptedFramedTransportError::WriteStatePoisoned)
        ));
    }
}
