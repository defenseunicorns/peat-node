//! Bounded, byte-exact Core NATS egress for remote Peat bridge documents.
//!
//! Eligibility is deliberately independent of connection state: a remote
//! store event must first prove the durable envelope kind/version, its exact
//! configured route, and non-local provenance. The payload string is then
//! moved directly into [`Bytes`] without parsing or serialization.

use std::collections::HashMap;

use async_nats::Subject;
use buffa::bytes::Bytes;

use crate::nats_bridge::config::SubjectMapping;
use crate::nats_bridge::envelope::{BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION};
use crate::node::BridgeChangeEvent;

/// Stable private marker added to bridge-owned Core NATS publications.
pub(crate) const BRIDGE_ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";

/// Fixed, payload-safe reason that a remote Peat upsert is ineligible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EgressSkipKind {
    MalformedEnvelope,
    UnsupportedKind,
    UnsupportedVersion,
    UnmappedCollection,
    RouteMismatch,
    ReturnedLocal,
}

/// One byte-exact publish request after all envelope and route gates pass.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct EgressItem {
    pub subject: Subject,
    pub payload: Bytes,
}

/// Finite collection-to-subject table derived only from validated startup config.
pub(crate) struct EgressClassifier {
    routes: HashMap<String, Subject>,
    local_node_id: String,
}

impl EgressClassifier {
    pub fn new(mappings: &[SubjectMapping], local_node_id: &str) -> Self {
        let routes = mappings
            .iter()
            .map(|mapping| (mapping.collection().to_owned(), mapping.subject().clone()))
            .collect();
        Self {
            routes,
            local_node_id: local_node_id.to_owned(),
        }
    }

    /// Classify one private remote-only node event without retaining event data.
    pub fn classify(&self, event: BridgeChangeEvent) -> Result<EgressItem, EgressSkipKind> {
        let envelope: BridgeEnvelope = serde_json::from_str(&event.json_data)
            .map_err(|_| EgressSkipKind::MalformedEnvelope)?;
        if envelope.kind != BRIDGE_ENVELOPE_KIND {
            return Err(EgressSkipKind::UnsupportedKind);
        }
        if envelope.version != BRIDGE_ENVELOPE_VERSION {
            return Err(EgressSkipKind::UnsupportedVersion);
        }
        let subject = self
            .routes
            .get(&event.collection)
            .ok_or(EgressSkipKind::UnmappedCollection)?;
        if envelope.subject != subject.as_str() {
            return Err(EgressSkipKind::RouteMismatch);
        }
        if envelope.source_node_id == self.local_node_id {
            return Err(EgressSkipKind::ReturnedLocal);
        }

        Ok(EgressItem {
            subject: subject.clone(),
            payload: Bytes::from(envelope.payload),
        })
    }

    #[cfg(test)]
    fn route_count(&self) -> usize {
        self.routes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_bridge::config::BridgeConfig;

    fn mappings() -> Vec<SubjectMapping> {
        let raw = vec![
            "Vision.Summary=Frame_Store-1".to_owned(),
            "telemetry.reading=telemetry".to_owned(),
        ];
        let BridgeConfig::Enabled(config) =
            BridgeConfig::from_raw(Some("nats://127.0.0.1:4222"), &raw).expect("valid mappings")
        else {
            panic!("mappings must enable bridge");
        };
        config.mappings().to_vec()
    }

    fn classifier() -> EgressClassifier {
        EgressClassifier::new(&mappings(), "local-node")
    }

    fn envelope(subject: &str, source_node_id: &str, payload: &str) -> BridgeEnvelope {
        BridgeEnvelope {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: subject.to_owned(),
            source_node_id: source_node_id.to_owned(),
            payload: payload.to_owned(),
        }
    }

    fn event(collection: &str, envelope: &BridgeEnvelope) -> BridgeChangeEvent {
        BridgeChangeEvent {
            collection: collection.to_owned(),
            doc_id: "untrusted-document-id".to_owned(),
            remote_peer_id: "untrusted-immediate-peer".to_owned(),
            json_data: serde_json::to_string(envelope).expect("serialize envelope"),
        }
    }

    #[test]
    fn classifier_accepts_only_exact_supported_envelope_and_route() {
        let classifier = classifier();
        let valid = envelope("Vision.Summary", "remote-node", r#"{"ok":true}"#);
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &valid)),
            Ok(EgressItem {
                subject: Subject::from("Vision.Summary"),
                payload: Bytes::from_static(br#"{"ok":true}"#),
            })
        );

        let mut unsupported_kind = valid.clone();
        unsupported_kind.kind = "peat.nats-bridge.other".to_owned();
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &unsupported_kind)),
            Err(EgressSkipKind::UnsupportedKind)
        );
        let mut unsupported_version = valid.clone();
        unsupported_version.version += 1;
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &unsupported_version)),
            Err(EgressSkipKind::UnsupportedVersion)
        );
    }

    #[test]
    fn classifier_has_fixed_outcomes_for_malformed_ordinary_unmapped_and_mismatch() {
        let classifier = classifier();
        for json_data in ["not-json", r#"{"ordinary":true}"#] {
            let malformed = BridgeChangeEvent {
                collection: "Frame_Store-1".to_owned(),
                doc_id: "id".to_owned(),
                remote_peer_id: "peer".to_owned(),
                json_data: json_data.to_owned(),
            };
            assert_eq!(
                classifier.classify(malformed),
                Err(EgressSkipKind::MalformedEnvelope)
            );
        }

        let valid = envelope("Vision.Summary", "remote-node", "1");
        assert_eq!(
            classifier.classify(event("unmapped", &valid)),
            Err(EgressSkipKind::UnmappedCollection)
        );
        for subject in ["vision.summary", "Vision.Summary ", "telemetry.reading"] {
            let mismatch = envelope(subject, "remote-node", "1");
            assert_eq!(
                classifier.classify(event("Frame_Store-1", &mismatch)),
                Err(EgressSkipKind::RouteMismatch)
            );
        }
    }

    #[test]
    fn classifier_suppresses_returned_local_using_durable_provenance_only() {
        let classifier = classifier();
        let returned = envelope("Vision.Summary", "local-node", "true");
        let mut remote_event = event("Frame_Store-1", &returned);
        remote_event.remote_peer_id = "definitely-not-local-node".to_owned();
        assert_eq!(
            classifier.classify(remote_event),
            Err(EgressSkipKind::ReturnedLocal)
        );
    }

    #[test]
    fn classifier_preserves_every_payload_byte_and_leaks_no_envelope_metadata() {
        let classifier = classifier();
        for payload in [
            r#"  { "alpha": 1, "beta": 2 }  "#,
            r#"{"beta":2,"alpha":1}"#,
            r#"{"value":1.0}"#,
            r#"{"label":"\u03bb"}"#,
            r#"{"label":"λ"}"#,
            "{\"ok\":true}\n\t ",
        ] {
            let expected = payload.as_bytes().to_vec();
            let valid = envelope("Vision.Summary", "remote-node", payload);
            let item = classifier
                .classify(event("Frame_Store-1", &valid))
                .expect("eligible envelope");
            assert_eq!(item.payload.as_ref(), expected);
            assert!(!item
                .payload
                .windows(BRIDGE_ENVELOPE_KIND.len())
                .any(|part| { part == BRIDGE_ENVELOPE_KIND.as_bytes() }));
            assert!(!item
                .payload
                .windows("remote-node".len())
                .any(|part| { part == b"remote-node" }));
        }
    }

    #[test]
    fn classifier_route_table_is_fixed_by_validated_startup_mappings() {
        let classifier = classifier();
        assert_eq!(classifier.route_count(), 2);
        for sequence in 0..100 {
            let valid = envelope("dynamic", "remote-node", "null");
            assert_eq!(
                classifier.classify(event(&format!("attacker-{sequence}"), &valid)),
                Err(EgressSkipKind::UnmappedCollection)
            );
        }
        assert_eq!(classifier.route_count(), 2);
    }

    #[test]
    fn classifier_header_name_is_stable_and_valid() {
        assert_eq!(BRIDGE_ORIGIN_HEADER, "Peat-Nats-Bridge-Origin");
        let _: async_nats::HeaderName = BRIDGE_ORIGIN_HEADER.parse().expect("valid header name");
    }
}
