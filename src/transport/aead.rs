use crate::config::CipherSuite;
use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Key as AesKey, Nonce as AesNonce, Tag as AesTag};
use chacha20poly1305::{
    ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce, Tag as ChaChaTag,
};

pub(crate) const AEAD_TAG_LEN: usize = 16;

#[derive(Clone)]
pub(crate) enum TransportAead {
    Aes256Gcm(Box<Aes256Gcm>),
    Chacha20Poly1305(ChaCha20Poly1305),
}

impl TransportAead {
    pub(crate) fn new(suite: CipherSuite, key: &[u8; 32]) -> Self {
        match suite {
            CipherSuite::Aes256Gcm => Self::Aes256Gcm(Box::new(Aes256Gcm::new(
                AesKey::<Aes256Gcm>::from_slice(key),
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
                .encrypt_in_place_detached(AesNonce::from_slice(nonce), aad, payload)
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
                .decrypt_in_place_detached(
                    AesNonce::from_slice(nonce),
                    aad,
                    payload,
                    AesTag::from_slice(tag),
                )
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
