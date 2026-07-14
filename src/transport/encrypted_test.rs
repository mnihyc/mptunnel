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
