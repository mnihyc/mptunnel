use super::error::UdpCarrierTransportError;
use crate::config::CipherSuite;
use crate::transport::aead::TransportAead;
use sha2::{Digest, Sha256};

pub(super) const DIR_CLIENT_TO_SERVER: u8 = 1;
pub(super) const DIR_SERVER_TO_CLIENT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarrierRole {
    Client,
    Server,
}

impl CarrierRole {
    pub(super) fn send_direction(self) -> u8 {
        match self {
            Self::Client => DIR_CLIENT_TO_SERVER,
            Self::Server => DIR_SERVER_TO_CLIENT,
        }
    }

    pub(super) fn recv_direction(self) -> u8 {
        match self {
            Self::Client => DIR_SERVER_TO_CLIENT,
            Self::Server => DIR_CLIENT_TO_SERVER,
        }
    }
}

#[derive(Clone)]
pub(super) struct PacketCipher {
    cipher: TransportAead,
}

impl std::fmt::Debug for PacketCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketCipher").finish_non_exhaustive()
    }
}

impl PacketCipher {
    pub(super) fn new(
        secret: &[u8],
        cipher_suite: CipherSuite,
        connection_id: u64,
    ) -> Result<Self, UdpCarrierTransportError> {
        if secret.is_empty() {
            return Err(UdpCarrierTransportError::EmptySecret);
        }
        let key = derive_packet_key(secret, cipher_suite, connection_id);
        Ok(Self {
            cipher: TransportAead::new(cipher_suite, &key),
        })
    }

    pub(super) fn encrypt(
        &self,
        direction: u8,
        packet_number: u64,
        aad: &[u8],
        payload: &mut [u8],
    ) -> Result<[u8; crate::transport::aead::AEAD_TAG_LEN], ()> {
        self.cipher
            .encrypt_in_place_detached(&packet_nonce(direction, packet_number), aad, payload)
    }

    pub(super) fn decrypt(
        &self,
        direction: u8,
        packet_number: u64,
        aad: &[u8],
        payload: &mut [u8],
        tag: &[u8; crate::transport::aead::AEAD_TAG_LEN],
    ) -> Result<(), ()> {
        self.cipher.decrypt_in_place_detached(
            &packet_nonce(direction, packet_number),
            aad,
            payload,
            tag,
        )
    }
}

fn derive_packet_key(secret: &[u8], cipher_suite: CipherSuite, connection_id: u64) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"mptunnel udp carrier packet key v1");
    hasher.update(cipher_suite.key_context());
    hasher.update(connection_id.to_be_bytes());
    hasher.update(secret);
    hasher.finalize().into()
}

fn packet_nonce(direction: u8, packet_number: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0] = direction;
    nonce[4..12].copy_from_slice(&packet_number.to_be_bytes());
    nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_key_depends_on_connection_id_and_cipher() {
        let secret = b"mptunnel integration test secret with enough entropy";
        let first = derive_packet_key(secret, CipherSuite::Aes256Gcm, 1);
        assert_eq!(first, derive_packet_key(secret, CipherSuite::Aes256Gcm, 1));
        assert_ne!(first, derive_packet_key(secret, CipherSuite::Aes256Gcm, 2));
        assert_ne!(
            first,
            derive_packet_key(secret, CipherSuite::Chacha20Poly1305, 1)
        );
    }
}
