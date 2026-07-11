use crate::config::CipherSuite;
use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{
    ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce, Tag as ChaChaTag,
};
use ring::aead as ring_aead;

pub(crate) const AEAD_TAG_LEN: usize = 16;

#[derive(Clone)]
pub(crate) enum TransportAead {
    Aes256Gcm(Box<ring_aead::LessSafeKey>),
    Chacha20Poly1305(ChaCha20Poly1305),
}

impl TransportAead {
    pub(crate) fn new(suite: CipherSuite, key: &[u8; 32]) -> Self {
        match suite {
            CipherSuite::Aes256Gcm => Self::Aes256Gcm(Box::new(ring_aead::LessSafeKey::new(
                ring_aead::UnboundKey::new(&ring_aead::AES_256_GCM, key)
                    .expect("AES-256-GCM accepts a 32-byte key"),
            ))),
            CipherSuite::Chacha20Poly1305 => {
                Self::Chacha20Poly1305(ChaCha20Poly1305::new(ChaChaKey::from_slice(key)))
            }
        }
    }

    pub(crate) fn encrypt_in_place_detached(
        &self,
        nonce: &[u8; 12],
        aad: &[u8],
        payload: &mut [u8],
    ) -> Result<[u8; AEAD_TAG_LEN], ()> {
        match self {
            Self::Aes256Gcm(cipher) => cipher
                .seal_in_place_separate_tag(
                    ring_aead::Nonce::assume_unique_for_key(*nonce),
                    ring_aead::Aad::from(aad),
                    payload,
                )
                .map(tag_to_array)
                .map_err(|_| ()),
            Self::Chacha20Poly1305(cipher) => cipher
                .encrypt_in_place_detached(ChaChaNonce::from_slice(nonce), aad, payload)
                .map(tag_to_array)
                .map_err(|_| ()),
        }
    }

    pub(crate) fn decrypt_in_place_detached(
        &self,
        nonce: &[u8; 12],
        aad: &[u8],
        payload: &mut [u8],
        tag: &[u8; AEAD_TAG_LEN],
    ) -> Result<(), ()> {
        match self {
            Self::Aes256Gcm(cipher) => cipher
                .open_in_place_separate_tag(
                    ring_aead::Nonce::assume_unique_for_key(*nonce),
                    ring_aead::Aad::from(aad),
                    ring_aead::Tag::from(*tag),
                    payload,
                    0..,
                )
                .map(|_| ())
                .map_err(|_| ()),
            Self::Chacha20Poly1305(cipher) => cipher
                .decrypt_in_place_detached(
                    ChaChaNonce::from_slice(nonce),
                    aad,
                    payload,
                    ChaChaTag::from_slice(tag),
                )
                .map_err(|_| ()),
        }
    }
}

fn tag_to_array<T>(tag: T) -> [u8; AEAD_TAG_LEN]
where
    T: AsRef<[u8]>,
{
    let mut out = [0u8; AEAD_TAG_LEN];
    out.copy_from_slice(tag.as_ref());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes_256_gcm_matches_nist_cavs_aad_vector() {
        let key = [
            0x92, 0xe1, 0x1d, 0xcd, 0xaa, 0x86, 0x6f, 0x5c, 0xe7, 0x90, 0xfd, 0x24, 0x50, 0x1f,
            0x92, 0x50, 0x9a, 0xac, 0xf4, 0xcb, 0x8b, 0x13, 0x39, 0xd5, 0x0c, 0x9c, 0x12, 0x40,
            0x93, 0x5d, 0xd0, 0x8b,
        ];
        let nonce = [
            0xac, 0x93, 0xa1, 0xa6, 0x14, 0x52, 0x99, 0xbd, 0xe9, 0x02, 0xf2, 0x1a,
        ];
        let aad = [
            0x1e, 0x08, 0x89, 0x01, 0x6f, 0x67, 0x60, 0x1c, 0x8e, 0xbe, 0xa4, 0x94, 0x3b, 0xc2,
            0x3a, 0xd6,
        ];
        let mut payload = [
            0x2d, 0x71, 0xbc, 0xfa, 0x91, 0x4e, 0x4a, 0xc0, 0x45, 0xb2, 0xaa, 0x60, 0x95, 0x5f,
            0xad, 0x24,
        ];
        let expected_ciphertext = [
            0x89, 0x95, 0xae, 0x2e, 0x6d, 0xf3, 0xdb, 0xf9, 0x6f, 0xac, 0x7b, 0x71, 0x37, 0xba,
            0xe6, 0x7f,
        ];
        let expected_tag = [
            0xec, 0xa5, 0xaa, 0x77, 0xd5, 0x1d, 0x4a, 0x0a, 0x14, 0xd9, 0xc5, 0x1e, 0x1d, 0xa4,
            0x74, 0xab,
        ];
        let cipher = TransportAead::new(CipherSuite::Aes256Gcm, &key);

        let tag = cipher
            .encrypt_in_place_detached(&nonce, &aad, &mut payload)
            .expect("encrypt NIST vector");
        assert_eq!(payload, expected_ciphertext);
        assert_eq!(tag, expected_tag);
        cipher
            .decrypt_in_place_detached(&nonce, &aad, &mut payload, &tag)
            .expect("decrypt NIST vector");
        assert_eq!(
            payload,
            [
                0x2d, 0x71, 0xbc, 0xfa, 0x91, 0x4e, 0x4a, 0xc0, 0x45, 0xb2, 0xaa, 0x60, 0x95, 0x5f,
                0xad, 0x24,
            ]
        );
    }
}
