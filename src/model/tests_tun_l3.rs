use super::*;

#[test]
fn parses_ipv4_identity_without_changing_packet() {
    let packet = [
        0x45, 0, 0, 24, 0x12, 0x34, 0, 0, 64, 17, 0, 0, 10, 88, 0, 2, 10, 88, 0, 1, 0x12, 0x34, 0,
        53,
    ];
    let parsed = parse_ip_packet(&packet).expect("IPv4 packet");
    assert_eq!(
        parsed.source,
        "10.88.0.2".parse::<IpAddr>().expect("source")
    );
    assert_eq!(
        parsed.destination,
        "10.88.0.1".parse::<IpAddr>().expect("destination")
    );
    assert_eq!(parsed.flow_key.next_header, 17);
}

#[test]
fn ipv4_fragments_share_one_flow_key() {
    let packet = |fragment: u16| {
        let mut bytes = vec![
            0x45, 0, 0, 20, 0x12, 0x34, 0, 0, 64, 17, 0, 0, 10, 88, 0, 2, 10, 88, 0, 1,
        ];
        bytes[6..8].copy_from_slice(&fragment.to_be_bytes());
        bytes
    };
    let first = parse_ip_packet(&packet(0x2000)).expect("first fragment");
    let next = parse_ip_packet(&packet(1)).expect("next fragment");
    assert_eq!(first.flow_key, next.flow_key);
}

#[test]
fn parses_ipv6_transport_after_extension_header() {
    let mut packet = vec![0_u8; 52];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&12_u16.to_be_bytes());
    packet[6] = 0;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&"fd88::2".parse::<Ipv6Addr>().expect("source").octets());
    packet[24..40].copy_from_slice(&"fd88::1".parse::<Ipv6Addr>().expect("destination").octets());
    packet[40] = 17;
    packet[41] = 0;
    packet[48..52].copy_from_slice(&[0x12, 0x34, 0, 53]);
    let parsed = parse_ip_packet(&packet).expect("IPv6 packet");
    assert_eq!(parsed.flow_key.next_header, 17);
}

#[test]
fn rejects_ambiguous_lengths_and_jumbograms() {
    let mut ipv4 = vec![0_u8; 20];
    ipv4[0] = 0x45;
    ipv4[2..4].copy_from_slice(&21_u16.to_be_bytes());
    assert_eq!(parse_ip_packet(&ipv4), Err(IpPacketError::InvalidLength));

    let mut ipv6 = vec![0_u8; 41];
    ipv6[0] = 0x60;
    assert_eq!(
        parse_ip_packet(&ipv6),
        Err(IpPacketError::UnsupportedJumbogram)
    );
}
