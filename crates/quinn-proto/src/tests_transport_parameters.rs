use std::convert::Infallible;

use rand::TryRng;

use super::*;

#[test]
fn coding() {
    let mut buf = Vec::new();
    let params = TransportParameters {
        initial_src_cid: Some(ConnectionId::new(&[])),
        original_dst_cid: Some(ConnectionId::new(&[])),
        initial_max_streams_bidi: 16u32.into(),
        initial_max_streams_uni: 16u32.into(),
        ack_delay_exponent: 2u32.into(),
        max_udp_payload_size: 1200u32.into(),
        preferred_address: Some(PreferredAddress {
            address_v4: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 42)),
            address_v6: Some(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 24, 0, 0)),
            connection_id: ConnectionId::new(&[0x42]),
            stateless_reset_token: [0xab; RESET_TOKEN_SIZE].into(),
        }),
        grease_quic_bit: true,
        min_ack_delay: Some(2_000u32.into()),
        ..TransportParameters::default()
    };
    params.write(&mut buf);
    assert_eq!(
        TransportParameters::read(Side::Client, &mut buf.as_slice()).unwrap(),
        params
    );
}

#[test]
fn reserved_transport_parameter_generate_reserved_id() {
    let mut rngs = [
        StepRng(0),
        StepRng(1),
        StepRng(27),
        StepRng(31),
        StepRng(u32::MAX as u64),
        StepRng(u32::MAX as u64 - 1),
        StepRng(u32::MAX as u64 + 1),
        StepRng(u32::MAX as u64 - 27),
        StepRng(u32::MAX as u64 + 27),
        StepRng(u32::MAX as u64 - 31),
        StepRng(u32::MAX as u64 + 31),
        StepRng(u64::MAX),
        StepRng(u64::MAX - 1),
        StepRng(u64::MAX - 27),
        StepRng(u64::MAX - 31),
        StepRng(1 << 62),
        StepRng((1 << 62) - 1),
        StepRng((1 << 62) + 1),
        StepRng((1 << 62) - 27),
        StepRng((1 << 62) + 27),
        StepRng((1 << 62) - 31),
        StepRng((1 << 62) + 31),
    ];
    for rng in &mut rngs {
        let id = ReservedTransportParameter::generate_reserved_id(rng);
        assert!(id.0 % 31 == 27)
    }
}

struct StepRng(u64);

impl TryRng for StepRng {
    type Error = Infallible;

    #[inline]
    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        Ok(self.next_u64() as u32)
    }

    #[inline]
    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        let res = self.0;
        self.0 = self.0.wrapping_add(1);
        Ok(res)
    }

    fn try_fill_bytes(&mut self, dst: &mut [u8]) -> Result<(), Self::Error> {
        let mut left = dst;
        while left.len() >= 8 {
            let (l, r) = left.split_at_mut(8);
            left = r;
            l.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        let n = left.len();
        if n > 0 {
            left.copy_from_slice(&self.next_u32().to_le_bytes()[..n]);
        }

        Ok(())
    }
}

#[test]
fn reserved_transport_parameter_ignored_when_read() {
    let mut buf = Vec::new();
    let reserved_parameter = ReservedTransportParameter::random(&mut rand::rng());
    assert!(reserved_parameter.payload_len < ReservedTransportParameter::MAX_PAYLOAD_LEN);
    assert!(reserved_parameter.id.0 % 31 == 27);

    reserved_parameter.write(&mut buf);
    assert!(!buf.is_empty());
    let read_params = TransportParameters::read(Side::Server, &mut buf.as_slice()).unwrap();
    assert_eq!(read_params, TransportParameters::default());
}

#[test]
fn read_semantic_validation() {
    #[allow(clippy::type_complexity)]
    let illegal_params_builders: Vec<Box<dyn FnMut(&mut TransportParameters)>> = vec![
        Box::new(|t| {
            // This min_ack_delay is bigger than max_ack_delay!
            let min_ack_delay = t.max_ack_delay.0 * 1_000 + 1;
            t.min_ack_delay = Some(VarInt::from_u64(min_ack_delay).unwrap())
        }),
        Box::new(|t| {
            // Preferred address can only be sent by senders (and we are reading the transport
            // params as a client)
            t.preferred_address = Some(PreferredAddress {
                address_v4: Some(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 42)),
                address_v6: None,
                connection_id: ConnectionId::new(&[]),
                stateless_reset_token: [0xab; RESET_TOKEN_SIZE].into(),
            })
        }),
    ];

    for mut builder in illegal_params_builders {
        let mut buf = Vec::new();
        let mut params = TransportParameters::default();
        builder(&mut params);
        params.write(&mut buf);

        assert_eq!(
            TransportParameters::read(Side::Server, &mut buf.as_slice()),
            Err(Error::IllegalValue)
        );
    }
}

#[test]
fn resumption_params_validation() {
    let high_limit = TransportParameters {
        initial_max_streams_uni: 32u32.into(),
        ..TransportParameters::default()
    };
    let low_limit = TransportParameters {
        initial_max_streams_uni: 16u32.into(),
        ..TransportParameters::default()
    };
    high_limit.validate_resumption_from(&low_limit).unwrap();
    low_limit.validate_resumption_from(&high_limit).unwrap_err();
}
