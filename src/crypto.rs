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
use hkdf::Hkdf;
use sha2::Sha256;

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

/// Derive a 32-byte iroh `SecretKey` seed from `(formation_secret, pod_name)`.
///
/// Uses `HKDF-SHA256(ikm=formation_secret, info="iroh:" + pod_name)` — the
/// same derivation as `peat_mesh::peer_connector::PeerConnector::derive_peer_endpoint_id`.
/// All nodes in the same formation that know the formation secret can compute
/// any peer's iroh `EndpointId` from its pod name alone:
///
/// ```ignore
/// let seed = derive_iroh_node_key(formation_secret, pod_name);
/// let endpoint_id = iroh::SecretKey::from_bytes(&seed).public();
/// ```
///
/// This is FIPS-approved: HKDF-SHA256 per ADR-049. The output bytes are passed
/// to `AutomergeBackendConfig::iroh_secret_key` at node startup.
///
/// # Panics (debug builds)
///
/// `formation_secret` is the input keying material and MUST be the
/// full-entropy formation secret (the 32-byte base64-decoded shared key).
/// Deriving an identity from a short/empty IKM yields a "deterministic" key
/// from cryptographically weak input — a silent security downgrade. A
/// `debug_assert!` enforces a 16-byte floor so a future caller that passes a
/// truncated or empty slice trips in tests/dev rather than shipping a weak
/// identity. Release builds skip the check (callers validate upstream), so the
/// contract is documented here rather than returned as an error.
pub fn derive_iroh_node_key(formation_secret: &[u8], pod_name: &str) -> [u8; 32] {
    debug_assert!(
        formation_secret.len() >= 16,
        "derive_iroh_node_key: formation_secret is {} bytes; expected the \
         32-byte formation secret. Deriving from weak IKM is a silent security \
         downgrade.",
        formation_secret.len()
    );
    let hk = Hkdf::<Sha256>::new(None, formation_secret);
    let mut okm = [0u8; 32];
    hk.expand(format!("iroh:{pod_name}").as_bytes(), &mut okm)
        .expect("HKDF-SHA256 32-byte expand never fails");
    okm
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

    #[test]
    fn derive_iroh_node_key_is_deterministic() {
        let secret = b"test-formation-secret-32-bytes!!";
        let k1 = derive_iroh_node_key(secret, "peat-node-0");
        let k2 = derive_iroh_node_key(secret, "peat-node-0");
        assert_eq!(k1, k2);
    }

    #[test]
    fn derive_iroh_node_key_differs_per_pod() {
        let secret = b"test-formation-secret-32-bytes!!";
        let k0 = derive_iroh_node_key(secret, "peat-node-0");
        let k1 = derive_iroh_node_key(secret, "peat-node-1");
        assert_ne!(k0, k1);
    }

    #[test]
    fn derive_iroh_node_key_differs_per_formation() {
        let s1 = b"formation-secret-A-32-bytes-long";
        let s2 = b"formation-secret-B-32-bytes-long";
        let k1 = derive_iroh_node_key(s1, "peat-node-0");
        let k2 = derive_iroh_node_key(s2, "peat-node-0");
        assert_ne!(k1, k2);
    }

    /// Pins the exact derivation recipe so it cannot silently drift from
    /// peat-mesh's `PeerConnector` (which uses the identical
    /// `HKDF-SHA256(salt=None, ikm=secret, info="iroh:"+pod)`). If anyone
    /// changes the salt, the `"iroh:"` info prefix, or the hash here, this
    /// fails — surfacing the drift that would otherwise break every K8s peer
    /// connection with no obvious symptom (the IDIOM finding on #151).
    #[test]
    fn derive_iroh_node_key_matches_documented_recipe() {
        let secret = b"test-formation-secret-32-bytes!!";
        let hk = Hkdf::<Sha256>::new(None, secret);
        let mut expected = [0u8; 32];
        hk.expand(b"iroh:peat-node-0", &mut expected).unwrap();
        assert_eq!(derive_iroh_node_key(secret, "peat-node-0"), expected);
    }

    #[test]
    #[should_panic(expected = "formation_secret")]
    fn derive_iroh_node_key_rejects_weak_ikm_in_debug() {
        // A 4-byte IKM is far below the 16-byte floor; debug builds must trip.
        let _ = derive_iroh_node_key(b"weak", "peat-node-0");
    }
}
