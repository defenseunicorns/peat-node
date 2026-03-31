//! Encryption at rest — AES-256-GCM for document content stored in the Automerge CRDT.
//!
//! When an encryption key is configured, JSON document payloads are encrypted before
//! being written to the Automerge store and decrypted on read. The Automerge envelope
//! (CRDT metadata, operation history) remains unencrypted so that sync still works,
//! but the application-level content is protected at rest.
//!
//! Format: `ENC:v1:<base64(nonce ++ ciphertext ++ tag)>`
//! - 12-byte random nonce
//! - variable-length ciphertext
//! - 16-byte GCM authentication tag

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use anyhow::{bail, Context};
use base64::Engine;

const PREFIX: &str = "ENC:v1:";

/// Handles encryption/decryption of document content at rest.
#[derive(Clone)]
pub struct StoreCipher {
    cipher: Aes256Gcm,
}

impl StoreCipher {
    /// Create a new cipher from a base64-encoded 32-byte key.
    pub fn from_base64_key(key_b64: &str) -> anyhow::Result<Self> {
        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(key_b64)
            .context("invalid base64 in encryption key")?;
        if key_bytes.len() != 32 {
            bail!(
                "encryption key must be exactly 32 bytes, got {}",
                key_bytes.len()
            );
        }
        let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
        Ok(Self {
            cipher: Aes256Gcm::new(key),
        })
    }

    /// Encrypt a plaintext JSON string. Returns an opaque `ENC:v1:...` string.
    pub fn encrypt(&self, plaintext: &str) -> anyhow::Result<String> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|e| anyhow::anyhow!("encryption failed: {e}"))?;

        // nonce (12) + ciphertext + tag (appended by aes-gcm)
        let mut blob = Vec::with_capacity(12 + ciphertext.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ciphertext);

        let encoded = base64::engine::general_purpose::STANDARD.encode(&blob);
        Ok(format!("{PREFIX}{encoded}"))
    }

    /// Decrypt an `ENC:v1:...` string back to the original plaintext JSON.
    pub fn decrypt(&self, stored: &str) -> anyhow::Result<String> {
        let encoded = stored
            .strip_prefix(PREFIX)
            .context("not an encrypted value (missing ENC:v1: prefix)")?;

        let blob = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .context("invalid base64 in encrypted value")?;

        if blob.len() < 12 {
            bail!("encrypted blob too short (need at least 12-byte nonce)");
        }

        let (nonce_bytes, ciphertext) = blob.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("decryption failed: {e}"))?;

        String::from_utf8(plaintext).context("decrypted content is not valid UTF-8")
    }
}

/// Returns true if the value looks like an encrypted payload.
pub fn is_encrypted(value: &str) -> bool {
    value.starts_with(PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key_b64() -> String {
        base64::engine::general_purpose::STANDARD.encode([0xABu8; 32])
    }

    #[test]
    fn round_trip() {
        let cipher = StoreCipher::from_base64_key(&test_key_b64()).unwrap();
        let original = r#"{"hello":"world","num":42}"#;
        let encrypted = cipher.encrypt(original).unwrap();

        assert!(encrypted.starts_with(PREFIX));
        assert_ne!(encrypted, original);

        let decrypted = cipher.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn different_nonce_each_time() {
        let cipher = StoreCipher::from_base64_key(&test_key_b64()).unwrap();
        let a = cipher.encrypt("same").unwrap();
        let b = cipher.encrypt("same").unwrap();
        assert_ne!(a, b); // random nonce ensures different ciphertext
    }

    #[test]
    fn wrong_key_fails() {
        let cipher1 = StoreCipher::from_base64_key(&test_key_b64()).unwrap();
        let other_key = base64::engine::general_purpose::STANDARD.encode([0xCDu8; 32]);
        let cipher2 = StoreCipher::from_base64_key(&other_key).unwrap();

        let encrypted = cipher1.encrypt("secret").unwrap();
        assert!(cipher2.decrypt(&encrypted).is_err());
    }

    #[test]
    fn invalid_key_length_rejected() {
        let short = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);
        assert!(StoreCipher::from_base64_key(&short).is_err());
    }

    #[test]
    fn is_encrypted_check() {
        assert!(is_encrypted("ENC:v1:AAAA"));
        assert!(!is_encrypted(r#"{"hello":"world"}"#));
    }
}
