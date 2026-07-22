//! Durable, byte-preserving envelope for messages ingested from Core NATS.
//!
//! JSON parsing at this boundary is validation-only. The original UTF-8 text
//! is copied directly into the envelope so later egress can publish the exact
//! bytes received from NATS. Acceptance is intentionally the bounded
//! `serde_json::Value` subset: default recursion protection permits at most 127
//! nested arrays/objects, and numbers must fit the enabled `Number`
//! representations (including finite `f64` fallback). This is narrower than
//! every grammar-valid JSON number.

use serde::{Deserialize, Serialize};

/// Fixed marker used to distinguish bridge documents from application data.
pub const BRIDGE_ENVELOPE_KIND: &str = "peat.nats-bridge";

/// Current durable bridge envelope schema version.
pub const BRIDGE_ENVELOPE_VERSION: u32 = 1;

/// Durable v1 representation of one message accepted from Core NATS.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeEnvelope {
    /// Fixed bridge document marker.
    pub kind: String,
    /// Numeric durable schema version.
    pub version: u32,
    /// Literal NATS subject from which the message was received.
    pub subject: String,
    /// Effective operator-visible peat-node identifier.
    pub source_node_id: String,
    /// Exact validated UTF-8 JSON text received from NATS.
    pub payload: String,
}

impl BridgeEnvelope {
    /// Validate raw NATS bytes and construct a v1 envelope without rewriting them.
    pub fn from_payload(
        subject: &str,
        source_node_id: &str,
        bytes: &[u8],
    ) -> Result<Self, IngressValidationError> {
        let payload =
            std::str::from_utf8(bytes).map_err(|_| IngressValidationError::InvalidUtf8)?;
        serde_json::from_str::<serde_json::Value>(payload)
            .map_err(|_| IngressValidationError::InvalidJson)?;

        Ok(Self {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: subject.to_owned(),
            source_node_id: source_node_id.to_owned(),
            payload: payload.to_owned(),
        })
    }
}

/// Fixed, payload-safe validation classifications for ingress callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressValidationError {
    /// The NATS payload is not valid UTF-8.
    InvalidUtf8,
    /// The UTF-8 payload is outside the bounded `serde_json::Value` subset.
    InvalidJson,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn round_trip(original: &str) -> BridgeEnvelope {
        let envelope =
            BridgeEnvelope::from_payload("Vision.Summary", "jetson-orin-nano", original.as_bytes())
                .expect("payload should be accepted");
        let encoded = serde_json::to_vec(&envelope).expect("envelope should serialize");
        let decoded: BridgeEnvelope =
            serde_json::from_slice(&encoded).expect("envelope should deserialize");
        assert_eq!(decoded.payload.as_bytes(), original.as_bytes());
        decoded
    }

    fn nested_arrays(depth: usize) -> String {
        format!("{}0{}", "[".repeat(depth), "]".repeat(depth))
    }

    fn nested_objects(depth: usize) -> String {
        format!("{}0{}", r#"{"value":"#.repeat(depth), "}".repeat(depth))
    }

    fn assert_invalid_json_without_source(payload: &str) {
        let error = BridgeEnvelope::from_payload("subject", "node", payload.as_bytes())
            .expect_err("payload must be outside the accepted JSON subset");
        assert_eq!(error, IngressValidationError::InvalidJson);
        let rendered = format!("{error:?}");
        assert_eq!(rendered, "InvalidJson");
        assert!(!rendered.contains(payload));
        assert!(!rendered.contains("number out of range"));
        assert!(!rendered.contains("recursion limit exceeded"));
    }

    #[test]
    fn valid_payloads_preserve_every_original_byte() {
        let cases = [
            r#"  { "alpha": 1, "beta": 2 }  "#,
            r#"{"beta":2,"alpha":1}"#,
            r#"{"value":1.0}"#,
            r#"{"value":1}"#,
            r#"{"label":"\u03bb"}"#,
            r#"{"label":"λ"}"#,
            "{\"ok\":true}\n\t ",
        ];

        for original in cases {
            let decoded = round_trip(original);
            assert_eq!(decoded.kind, BRIDGE_ENVELOPE_KIND);
            assert_eq!(decoded.version, BRIDGE_ENVELOPE_VERSION);
            assert_eq!(decoded.subject, "Vision.Summary");
            assert_eq!(decoded.source_node_id, "jetson-orin-nano");
        }
    }

    #[test]
    fn rejects_invalid_utf8_with_fixed_classification() {
        let error = BridgeEnvelope::from_payload("subject", "node", &[0xff, 0xfe])
            .expect_err("invalid UTF-8 must be rejected");
        assert_eq!(error, IngressValidationError::InvalidUtf8);
        assert_eq!(format!("{error:?}"), "InvalidUtf8");
    }

    #[test]
    fn rejects_malformed_and_trailing_token_json() {
        for payload in [b"{\"broken\":".as_slice(), b"{\"ok\":true} false"] {
            let error = BridgeEnvelope::from_payload("subject", "node", payload)
                .expect_err("malformed JSON must be rejected");
            assert_eq!(error, IngressValidationError::InvalidJson);
            assert_eq!(format!("{error:?}"), "InvalidJson");
        }
    }

    #[test]
    fn accepts_127_nested_arrays_and_objects_without_rewriting_bytes() {
        for original in [nested_arrays(127), nested_objects(127)] {
            let decoded = round_trip(&original);
            assert_eq!(decoded.payload.as_bytes(), original.as_bytes());
        }
    }

    #[test]
    fn rejects_the_128th_nested_array_and_object_with_fixed_classification() {
        for payload in [nested_arrays(128), nested_objects(128)] {
            assert_invalid_json_without_source(&payload);
        }
    }

    #[test]
    fn accepted_number_modes_preserve_original_spelling() {
        for original in [
            i64::MIN.to_string(),
            u64::MAX.to_string(),
            "1e308".to_owned(),
            "1.0".to_owned(),
        ] {
            let decoded = round_trip(&original);
            assert_eq!(decoded.payload.as_bytes(), original.as_bytes());
        }
    }

    #[test]
    fn out_of_range_numbers_use_fixed_invalid_json_classification() {
        for payload in ["1e309", "-12345678901234567890.123456789e9999"] {
            assert_invalid_json_without_source(payload);
        }
    }

    #[test]
    fn serialized_schema_has_exactly_five_fields_and_numeric_version() {
        let envelope = round_trip(r#"{"frame":1}"#);
        let value = serde_json::to_value(envelope).expect("envelope should serialize");
        let object = value.as_object().expect("envelope should be an object");
        let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let expected = ["kind", "payload", "source_node_id", "subject", "version"]
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);
        assert!(object["version"].is_u64());
        assert_eq!(object["version"], BRIDGE_ENVELOPE_VERSION);
    }
}
