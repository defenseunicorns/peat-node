//! Deterministic iroh endpoint identity (peat-node#63 gap-4d).
//!
//! By default iroh mints a fresh random keypair on every `Endpoint::bind()`,
//! so a node's `EndpointId` changes on every restart. That makes static
//! `PEAT_NODE_PEERS` peering unusable across restarts and impossible to
//! pre-configure: you can't know a peer's id until it has booted.
//!
//! Instead we seed the iroh keypair deterministically from
//! `HKDF-SHA256(formation_secret, "iroh:" + node_id)`, where `formation_secret`
//! is the base64-decoded `PEAT_NODE_SHARED_KEY`. Consequences:
//!
//! - A node's `EndpointId` is **stable across restarts** (given a stable
//!   `node_id`).
//! - Any party holding the shared key can **compute any node's `EndpointId`
//!   offline** from its `node_id` alone — no booting, no access to the remote
//!   machine. This is what the `peat-node derive-id` subcommand exposes.
//!
//! ## Invariant
//!
//! This derivation MUST stay byte-for-byte identical to peat-mesh's
//! `peat_mesh::peer_connector::PeerConnector::derive_peer_endpoint_id`
//! (and the reference binary's own-key derivation): `salt = None`,
//! `IKM = base64-STANDARD-decode(shared_key)`, `info = "iroh:" + node_id`,
//! 32-byte output → `iroh::SecretKey::from_bytes`. If these drift, a peer that
//! derives this node's id will not match the identity it actually presents and
//! the authenticated QUIC handshake fails. The `derives_same_id_as_documented_recipe`
//! test pins the recipe.

use anyhow::{Context, Result};
use base64::Engine as _;

use crate::crypto::derive_iroh_node_key;

/// Derive the 32-byte iroh secret-key seed for `node_id` under
/// `base64_shared_key`.
///
/// This is the base64-aware front door for the **static-peering** path; it
/// decodes the shared key and delegates the actual HKDF to
/// [`crate::crypto::derive_iroh_node_key`] — the single derivation shared with
/// the Kubernetes-discovery path, so the two cannot drift (the recipe is pinned
/// by `crypto`'s known-answer test).
///
/// Returns `Ok(None)` when `base64_shared_key` is empty: with no formation
/// secret there is nothing to key the identity from, so the caller falls back
/// to iroh's random per-process identity (pre-feature behaviour). Returns an
/// error when the shared key is non-empty but not valid base64.
pub fn derive_iroh_secret_seed(base64_shared_key: &str, node_id: &str) -> Result<Option<[u8; 32]>> {
    let trimmed = base64_shared_key.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    // IKM is the raw, base64-decoded shared key — matching peat-mesh's
    // `PeerConnector` `formation_secret` ("raw bytes, already base64-decoded").
    let secret = base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .context("PEAT_NODE_SHARED_KEY is not valid base64")?;
    Ok(Some(derive_iroh_node_key(&secret, node_id)))
}

/// Derive the iroh `EndpointId` string for `node_id` under `base64_shared_key`,
/// without binding a network endpoint.
///
/// Powers the offline `peat-node derive-id` subcommand: an operator can compute
/// a peer's `endpoint_id` from `(shared_key, node_id)` on any machine and paste
/// it into `PEAT_NODE_PEERS` as `endpoint_id@host:port`, with no access to the
/// peer. Errors if the shared key is empty (a deterministic identity requires a
/// formation secret) or not valid base64.
pub fn derive_endpoint_id(base64_shared_key: &str, node_id: &str) -> Result<String> {
    let seed = derive_iroh_secret_seed(base64_shared_key, node_id)?.context(
        "a non-empty PEAT_NODE_SHARED_KEY is required to derive a deterministic identity",
    )?;
    let secret = iroh::SecretKey::from_bytes(&seed);
    Ok(secret.public().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A valid base64-encoded 32-byte key (all 0x2a), matching the style of the
    // compose examples' shared key.
    const TEST_KEY: &str = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";

    #[test]
    fn empty_shared_key_yields_no_seed() {
        assert_eq!(derive_iroh_secret_seed("", "node-a").unwrap(), None);
        assert_eq!(derive_iroh_secret_seed("   ", "node-a").unwrap(), None);
    }

    #[test]
    fn derivation_is_deterministic() {
        let a = derive_iroh_secret_seed(TEST_KEY, "node-a")
            .unwrap()
            .unwrap();
        let b = derive_iroh_secret_seed(TEST_KEY, "node-a")
            .unwrap()
            .unwrap();
        assert_eq!(a, b, "same (shared_key, node_id) must give the same seed");
    }

    #[test]
    fn distinct_node_ids_give_distinct_seeds() {
        let a = derive_iroh_secret_seed(TEST_KEY, "node-a")
            .unwrap()
            .unwrap();
        let b = derive_iroh_secret_seed(TEST_KEY, "node-b")
            .unwrap()
            .unwrap();
        assert_ne!(a, b, "different node_id must give a different identity");
    }

    #[test]
    fn distinct_shared_keys_give_distinct_seeds() {
        let other = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let a = derive_iroh_secret_seed(TEST_KEY, "node-a")
            .unwrap()
            .unwrap();
        let b = derive_iroh_secret_seed(other, "node-a").unwrap().unwrap();
        assert_ne!(a, b, "different shared key must give a different identity");
    }

    #[test]
    fn invalid_base64_shared_key_errors() {
        assert!(derive_iroh_secret_seed("not!valid!base64!", "node-a").is_err());
    }

    /// Verifies the static front door delegates to the single shared
    /// derivation (`crypto::derive_iroh_node_key`) over the base64-decoded key,
    /// rather than carrying its own HKDF. The recipe itself (salt / `"iroh:"`
    /// context / IKM) is pinned by `crypto`'s `derive_iroh_node_key_matches_documented_recipe`
    /// known-answer test, so the two paths cannot drift.
    #[test]
    fn delegates_to_shared_crypto_derivation() {
        let secret = base64::engine::general_purpose::STANDARD
            .decode(TEST_KEY)
            .unwrap();
        let expected = crate::crypto::derive_iroh_node_key(&secret, "node-a");

        let got = derive_iroh_secret_seed(TEST_KEY, "node-a")
            .unwrap()
            .unwrap();
        assert_eq!(got, expected);

        // And the public id matches SecretKey::from_bytes(seed).public().
        let expected_id = iroh::SecretKey::from_bytes(&expected).public().to_string();
        assert_eq!(derive_endpoint_id(TEST_KEY, "node-a").unwrap(), expected_id);
    }

    #[test]
    fn derive_endpoint_id_requires_shared_key() {
        assert!(derive_endpoint_id("", "node-a").is_err());
    }
}
