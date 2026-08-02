use super::*;
use hex_literal::hex;
use std::io;

fn check_pn(typed: PacketNumber, encoded: &[u8]) {
    let mut buf = Vec::new();
    typed.encode(&mut buf);
    assert_eq!(&buf[..], encoded);
    let decoded = PacketNumber::decode(typed.len(), &mut io::Cursor::new(&buf)).unwrap();
    assert_eq!(typed, decoded);
}

#[test]
fn roundtrip_packet_numbers() {
    check_pn(PacketNumber::U8(0x7f), &hex!("7f"));
    check_pn(PacketNumber::U16(0x80), &hex!("0080"));
    check_pn(PacketNumber::U16(0x3fff), &hex!("3fff"));
    check_pn(PacketNumber::U32(0x0000_4000), &hex!("0000 4000"));
    check_pn(PacketNumber::U32(0xffff_ffff), &hex!("ffff ffff"));
}

#[test]
fn pn_encode() {
    check_pn(PacketNumber::new(0x10, 0), &hex!("10"));
    check_pn(PacketNumber::new(0x100, 0), &hex!("0100"));
    check_pn(PacketNumber::new(0x10000, 0), &hex!("010000"));
}

#[test]
fn pn_expand_roundtrip() {
    for expected in 0..1024 {
        for actual in expected..1024 {
            assert_eq!(actual, PacketNumber::new(actual, expected).expand(expected));
        }
    }
}

#[cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]
#[test]
fn header_encoding() {
    use crate::Side;
    use crate::crypto::rustls::{initial_keys, initial_suite_from_provider};
    #[cfg(all(feature = "rustls-aws-lc-rs", not(feature = "rustls-ring")))]
    use rustls::crypto::aws_lc_rs::default_provider;
    #[cfg(feature = "rustls-ring")]
    use rustls::crypto::ring::default_provider;
    use rustls::quic::Version;

    let dcid = ConnectionId::new(&hex!("06b858ec6f80452b"));
    let provider = default_provider();

    let suite = initial_suite_from_provider(&std::sync::Arc::new(provider)).unwrap();
    let client = initial_keys(Version::V1, dcid, Side::Client, &suite);
    let mut buf = Vec::new();
    let header = Header::Initial(InitialHeader {
        number: PacketNumber::U8(0),
        src_cid: ConnectionId::new(&[]),
        dst_cid: dcid,
        token: Bytes::new(),
        version: crate::DEFAULT_SUPPORTED_VERSIONS[0],
    });
    let encode = header.encode(&mut buf);
    let header_len = buf.len();
    buf.resize(header_len + 16 + client.packet.local.tag_len(), 0);
    encode.finish(
        &mut buf,
        &*client.header.local,
        Some((0, &*client.packet.local)),
    );

    for byte in &buf {
        print!("{byte:02x}");
    }
    println!();
    assert_eq!(
        buf[..],
        hex!(
            "c8000000010806b858ec6f80452b00004021be
             3ef50807b84191a196f760a6dad1e9d1c430c48952cba0148250c21c0a6a70e1"
        )[..]
    );

    let server = initial_keys(Version::V1, dcid, Side::Server, &suite);
    let supported_versions = crate::DEFAULT_SUPPORTED_VERSIONS.to_vec();
    let decode = PartialDecode::new(
        buf.as_slice().into(),
        &FixedLengthConnectionIdParser::new(0),
        &supported_versions,
        false,
    )
    .unwrap()
    .0;
    let mut packet = decode.finish(Some(&*server.header.remote)).unwrap();
    assert_eq!(
        packet.header_data[..],
        hex!("c0000000010806b858ec6f80452b0000402100")[..]
    );
    server
        .packet
        .remote
        .decrypt(0, &packet.header_data, &mut packet.payload)
        .unwrap();
    assert_eq!(packet.payload[..], [0; 16]);
    match packet.header {
        Header::Initial(InitialHeader {
            number: PacketNumber::U8(0),
            ..
        }) => {}
        _ => {
            panic!("unexpected header {:?}", packet.header);
        }
    }
}
