use super::*;
#[cfg(all(feature = "aws-lc-rs", not(feature = "ring")))]
use aws_lc_rs::hkdf;
#[cfg(feature = "ring")]
use ring::hkdf;

fn token_round_trip(payload: TokenPayload) -> TokenPayload {
    let rng = &mut rand::rng();
    let token = Token::new(payload, rng);
    let mut master_key = [0; 64];
    rng.fill_bytes(&mut master_key);
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(&master_key);
    let encoded = token.encode(&prk);
    let decoded = Token::decode(&prk, &encoded).expect("token didn't decrypt / decode");
    assert_eq!(token.nonce, decoded.nonce);
    decoded.payload
}

#[test]
fn retry_token_sanity() {
    use crate::MAX_CID_SIZE;
    use crate::cid_generator::{ConnectionIdGenerator, RandomConnectionIdGenerator};
    use crate::{Duration, UNIX_EPOCH};

    use std::net::Ipv6Addr;

    let address_1 = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 4433);
    let orig_dst_cid_1 = RandomConnectionIdGenerator::new(MAX_CID_SIZE).generate_cid();
    let issued_1 = UNIX_EPOCH + Duration::from_secs(42); // Fractional seconds would be lost
    let payload_1 = TokenPayload::Retry {
        address: address_1,
        orig_dst_cid: orig_dst_cid_1,
        issued: issued_1,
    };
    let TokenPayload::Retry {
        address: address_2,
        orig_dst_cid: orig_dst_cid_2,
        issued: issued_2,
    } = token_round_trip(payload_1)
    else {
        panic!("token decoded as wrong variant");
    };

    assert_eq!(address_1, address_2);
    assert_eq!(orig_dst_cid_1, orig_dst_cid_2);
    assert_eq!(issued_1, issued_2);
}

#[test]
fn validation_token_sanity() {
    use crate::{Duration, UNIX_EPOCH};

    use std::net::Ipv6Addr;

    let ip_1 = Ipv6Addr::LOCALHOST.into();
    let issued_1 = UNIX_EPOCH + Duration::from_secs(42); // Fractional seconds would be lost

    let payload_1 = TokenPayload::Validation {
        ip: ip_1,
        issued: issued_1,
    };
    let TokenPayload::Validation {
        ip: ip_2,
        issued: issued_2,
    } = token_round_trip(payload_1)
    else {
        panic!("token decoded as wrong variant");
    };

    assert_eq!(ip_1, ip_2);
    assert_eq!(issued_1, issued_2);
}

#[test]
fn invalid_token_returns_err() {
    let rng = &mut rand::rng();

    let mut master_key = [0; 64];
    rng.fill_bytes(&mut master_key);

    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(&master_key);

    let mut invalid_token = Vec::new();

    let mut random_data = [0; 32];
    rand::rng().fill_bytes(&mut random_data);
    invalid_token.put_slice(&random_data);

    // Assert: garbage sealed data returns err
    assert!(Token::decode(&prk, &invalid_token).is_none());
}
