//! Subscription query matcher.
//!
//! Translates a wire-level `pb::SubscriptionQuery` (from `SubscribeRequest`)
//! into an owned, validated `Matcher` that can be evaluated against each
//! document's JSON payload as it flows through the broadcast channel.
//!
//! Wire shape and semantics are documented on the proto in `proto/sidecar.proto`.
//! Recap of the rules that show up in tests:
//!
//! - `value` fields are JSON-encoded scalars: a string is `"\"vehicle\""`,
//!   a number is `"42"`, a bool is `"true"`. This matches the existing
//!   `string json_data` convention and keeps the wire trivially debuggable.
//! - Field paths are flat top-level keys (e.g. `"node_type"`). Dotted
//!   paths into nested JSON are deliberately not supported yet — when the
//!   need lands, extending here without a proto change is straightforward.
//! - `Lt` / `Gt` compare numbers numerically and strings lexicographically.
//!   Mixed types (e.g. `Lt { value: 5 }` against `"five"`) never match;
//!   bools and null never participate in ordering.
//! - DELETE events have no payload to evaluate, so they pass through the
//!   matcher unconditionally. A query filters *which upserts* a subscriber
//!   sees; tombstones for documents that were previously in their view
//!   would otherwise silently disappear and leave the client with stale state.

use crate::pb;
use crate::pb::subscription_query::Q;

/// Maximum recursion depth for nested `And` / `Or` / `Not` combinators.
///
/// Defensive guard against stack overflow from a hostile or buggy client
/// query. Lower than buffa's default decoder limit (100) on purpose —
/// realistic predicates nest a handful of levels, not dozens.
const MAX_DEPTH: u8 = 16;

/// Owned, validated query — built once per subscription.
#[derive(Debug, Clone)]
pub enum Matcher {
    All,
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Lt {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: serde_json::Value,
    },
    And(Vec<Matcher>),
    Or(Vec<Matcher>),
    Not(Box<Matcher>),
}

/// Errors building a `Matcher` from a proto request.
///
/// Surfaced to the client as `InvalidArgument` by the service layer so
/// malformed predicates fail fast rather than silently match nothing.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("query node has no variant set (oneof q is empty)")]
    EmptyNode,
    #[error("invalid JSON in query value `{0}`: {1}")]
    InvalidJsonValue(String, String),
    #[error("query field path is empty")]
    EmptyField,
    #[error("query nesting exceeds {0} levels")]
    TooDeep(u8),
}

impl Matcher {
    /// Build an owned matcher from a wire-level query. Validation runs
    /// once per subscription so a malformed predicate fails fast as
    /// `InvalidArgument` rather than producing a silently-empty stream.
    /// Callers that may have no query at all should branch on the
    /// optional field before calling this — see `event_passes` for the
    /// "no matcher" path.
    pub fn from_proto(q: &pb::SubscriptionQuery) -> Result<Self, QueryError> {
        Self::build(q, MAX_DEPTH)
    }

    fn build(q: &pb::SubscriptionQuery, depth: u8) -> Result<Self, QueryError> {
        if depth == 0 {
            return Err(QueryError::TooDeep(MAX_DEPTH));
        }
        let inner = q.q.as_ref().ok_or(QueryError::EmptyNode)?;
        match inner {
            Q::All(_) => Ok(Matcher::All),
            Q::Eq(eq) => Ok(Matcher::Eq {
                field: validate_field(&eq.field)?,
                value: parse_value(&eq.value)?,
            }),
            Q::Lt(cmp) => Ok(Matcher::Lt {
                field: validate_field(&cmp.field)?,
                value: parse_value(&cmp.value)?,
            }),
            Q::Gt(cmp) => Ok(Matcher::Gt {
                field: validate_field(&cmp.field)?,
                value: parse_value(&cmp.value)?,
            }),
            Q::And(and) => Ok(Matcher::And(
                and.clauses
                    .iter()
                    .map(|c| Self::build(c, depth - 1))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Q::Or(or) => Ok(Matcher::Or(
                or.clauses
                    .iter()
                    .map(|c| Self::build(c, depth - 1))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Q::Not(inner) => Ok(Matcher::Not(Box::new(Self::build(inner, depth - 1)?))),
        }
    }

    /// Evaluate against an upsert's JSON payload. Returns `false` for malformed JSON.
    pub fn matches_upsert(&self, json_data: &str) -> bool {
        let doc: serde_json::Value = match serde_json::from_str(json_data) {
            Ok(v) => v,
            // Malformed JSON shouldn't propagate to subscribers expecting a
            // filter — drop it. A producer that stored invalid JSON is the
            // bug, surfaced via writer-side validation in node.rs.
            Err(_) => return false,
        };
        self.eval(&doc)
    }

    fn eval(&self, doc: &serde_json::Value) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Eq { field, value } => doc.get(field).is_some_and(|v| json_eq(v, value)),
            Matcher::Lt { field, value } => doc
                .get(field)
                .and_then(|v| compare(v, value))
                .is_some_and(|o| o == std::cmp::Ordering::Less),
            Matcher::Gt { field, value } => doc
                .get(field)
                .and_then(|v| compare(v, value))
                .is_some_and(|o| o == std::cmp::Ordering::Greater),
            Matcher::And(c) => c.iter().all(|m| m.eval(doc)),
            Matcher::Or(c) => c.iter().any(|m| m.eval(doc)),
            Matcher::Not(inner) => !inner.eval(doc),
        }
    }
}

fn validate_field(field: &str) -> Result<String, QueryError> {
    if field.is_empty() {
        return Err(QueryError::EmptyField);
    }
    Ok(field.to_string())
}

fn parse_value(raw: &str) -> Result<serde_json::Value, QueryError> {
    serde_json::from_str(raw)
        .map_err(|e| QueryError::InvalidJsonValue(raw.to_string(), e.to_string()))
}

fn json_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    // serde_json::Value::PartialEq handles all scalar types correctly,
    // including integer/float equivalence (e.g. 1 == 1.0).
    a == b
}

/// Order two JSON scalars. Returns `None` when the values aren't comparable
/// (different types, or non-orderable types like bool/null/object/array).
fn compare(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    use serde_json::Value::*;
    match (a, b) {
        (String(x), String(y)) => Some(x.cmp(y)),
        (Number(x), Number(y)) => {
            let x = x.as_f64()?;
            let y = y.as_f64()?;
            x.partial_cmp(&y)
        }
        _ => None,
    }
}

/// Combine a list-of-collections filter and an optional `Matcher` into a
/// single per-event decision function. Used by the subscribe handler;
/// kept here so the matching policy lives in one place (and is
/// independently unit-tested).
///
/// Semantics:
/// - Empty `collections` means "all collections".
/// - DELETE events are always passed through (when the collection filter
///   matches) — the query filters *upserts*. See module-level docs.
pub fn event_passes(
    collections: &[String],
    matcher: Option<&Matcher>,
    collection: &str,
    json_data: Option<&str>,
) -> bool {
    if !collections.is_empty() && !collections.iter().any(|c| c == collection) {
        return false;
    }
    match (matcher, json_data) {
        (None, _) => true,
        (Some(_), None) => true, // DELETE — pass through
        (Some(m), Some(data)) => m.matches_upsert(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::subscription_query::Q;
    use crate::pb::{QueryAll, QueryAnd, QueryCmp, QueryEq, QueryOr, SubscriptionQuery};

    fn pq(q: Q) -> SubscriptionQuery {
        SubscriptionQuery {
            q: Some(q),
            ..Default::default()
        }
    }

    fn pq_all() -> SubscriptionQuery {
        pq(Q::All(Box::<QueryAll>::default()))
    }

    fn pq_eq(field: &str, value: &str) -> SubscriptionQuery {
        pq(Q::Eq(Box::new(QueryEq {
            field: field.into(),
            value: value.into(),
            ..Default::default()
        })))
    }

    fn pq_lt(field: &str, value: &str) -> SubscriptionQuery {
        pq(Q::Lt(Box::new(QueryCmp {
            field: field.into(),
            value: value.into(),
            ..Default::default()
        })))
    }

    fn pq_gt(field: &str, value: &str) -> SubscriptionQuery {
        pq(Q::Gt(Box::new(QueryCmp {
            field: field.into(),
            value: value.into(),
            ..Default::default()
        })))
    }

    fn pq_and(clauses: Vec<SubscriptionQuery>) -> SubscriptionQuery {
        pq(Q::And(Box::new(QueryAnd {
            clauses,
            ..Default::default()
        })))
    }

    fn pq_or(clauses: Vec<SubscriptionQuery>) -> SubscriptionQuery {
        pq(Q::Or(Box::new(QueryOr {
            clauses,
            ..Default::default()
        })))
    }

    fn pq_not(inner: SubscriptionQuery) -> SubscriptionQuery {
        pq(Q::Not(Box::new(inner)))
    }

    fn build(pq: SubscriptionQuery) -> Matcher {
        Matcher::from_proto(&pq).expect("build matcher")
    }

    // --- Parse / validation errors ---

    #[test]
    fn empty_oneof_node_rejected() {
        let pq = SubscriptionQuery {
            q: None,
            ..Default::default()
        };
        let err = Matcher::from_proto(&pq).unwrap_err();
        assert!(matches!(err, QueryError::EmptyNode), "got {err:?}");
    }

    #[test]
    fn empty_field_rejected() {
        let err = Matcher::from_proto(&pq_eq("", "\"x\"")).unwrap_err();
        assert!(matches!(err, QueryError::EmptyField), "got {err:?}");
    }

    #[test]
    fn invalid_json_value_rejected() {
        let err = Matcher::from_proto(&pq_eq("k", "not-json")).unwrap_err();
        assert!(
            matches!(err, QueryError::InvalidJsonValue(ref raw, _) if raw == "not-json"),
            "got {err:?}"
        );
    }

    #[test]
    fn excessive_nesting_rejected() {
        // Build MAX_DEPTH + 1 nested Not(...) wrappers — should trip the guard.
        let mut q = pq_all();
        for _ in 0..(MAX_DEPTH as usize + 1) {
            q = pq_not(q);
        }
        let err = Matcher::from_proto(&q).unwrap_err();
        assert!(matches!(err, QueryError::TooDeep(_)), "got {err:?}");
    }

    // --- All / Eq ---

    #[test]
    fn all_matches_anything() {
        let m = build(pq_all());
        assert!(m.matches_upsert(r#"{"any":"thing"}"#));
        assert!(m.matches_upsert(r#"{}"#));
    }

    #[test]
    fn all_rejects_malformed_json() {
        // Even Matcher::All shouldn't match invalid JSON — the input is
        // structurally undefined and forwarding garbage to filtered
        // subscribers is worse than silently dropping it.
        let m = build(pq_all());
        assert!(!m.matches_upsert("not json"));
    }

    #[test]
    fn eq_string_match() {
        let m = build(pq_eq("node_type", "\"vehicle\""));
        assert!(m.matches_upsert(r#"{"node_type":"vehicle"}"#));
        assert!(!m.matches_upsert(r#"{"node_type":"aircraft"}"#));
    }

    #[test]
    fn eq_number_match() {
        let m = build(pq_eq("readiness", "0.9"));
        assert!(m.matches_upsert(r#"{"readiness":0.9}"#));
        assert!(!m.matches_upsert(r#"{"readiness":0.8}"#));
    }

    #[test]
    fn eq_bool_match() {
        let m = build(pq_eq("active", "true"));
        assert!(m.matches_upsert(r#"{"active":true}"#));
        assert!(!m.matches_upsert(r#"{"active":false}"#));
    }

    #[test]
    fn eq_missing_field_does_not_match() {
        let m = build(pq_eq("ghost", "\"x\""));
        assert!(!m.matches_upsert(r#"{"present":"yes"}"#));
    }

    #[test]
    fn eq_type_mismatch_does_not_match() {
        let m = build(pq_eq("n", "\"42\""));
        // Doc has number 42; query asked for string "42" — different types,
        // do not match. JSON value equality is type-strict.
        assert!(!m.matches_upsert(r#"{"n":42}"#));
    }

    // --- Lt / Gt ---

    #[test]
    fn lt_numeric() {
        let m = build(pq_lt("readiness", "0.5"));
        assert!(m.matches_upsert(r#"{"readiness":0.1}"#));
        assert!(!m.matches_upsert(r#"{"readiness":0.5}"#));
        assert!(!m.matches_upsert(r#"{"readiness":0.9}"#));
    }

    #[test]
    fn gt_numeric() {
        let m = build(pq_gt("readiness", "0.5"));
        assert!(m.matches_upsert(r#"{"readiness":0.9}"#));
        assert!(!m.matches_upsert(r#"{"readiness":0.5}"#));
        assert!(!m.matches_upsert(r#"{"readiness":0.1}"#));
    }

    #[test]
    fn lt_string_lexicographic() {
        let m = build(pq_lt("name", "\"m\""));
        assert!(m.matches_upsert(r#"{"name":"alice"}"#));
        assert!(!m.matches_upsert(r#"{"name":"zoe"}"#));
    }

    #[test]
    fn lt_type_mismatch_rejects() {
        // Query says number; doc has string — not comparable.
        let m = build(pq_lt("v", "10"));
        assert!(!m.matches_upsert(r#"{"v":"five"}"#));
    }

    #[test]
    fn lt_bool_never_matches() {
        // Bools are not orderable in this matcher.
        let m = build(pq_lt("flag", "true"));
        assert!(!m.matches_upsert(r#"{"flag":false}"#));
        assert!(!m.matches_upsert(r#"{"flag":true}"#));
    }

    #[test]
    fn gt_integer_float_mixed() {
        // Lt/Gt should treat 5 (int) and 5.0 (float) as equal — neither
        // less nor greater. Catches a regression if someone reaches for
        // an integer-only path.
        let m_gt = build(pq_gt("x", "5"));
        let m_lt = build(pq_lt("x", "5"));
        assert!(!m_gt.matches_upsert(r#"{"x":5.0}"#));
        assert!(!m_lt.matches_upsert(r#"{"x":5.0}"#));
    }

    // --- And / Or / Not ---

    #[test]
    fn and_requires_all_clauses() {
        let m = build(pq_and(vec![
            pq_eq("node_type", "\"vehicle\""),
            pq_gt("readiness", "0.5"),
        ]));
        assert!(m.matches_upsert(r#"{"node_type":"vehicle","readiness":0.8}"#));
        assert!(!m.matches_upsert(r#"{"node_type":"vehicle","readiness":0.2}"#));
        assert!(!m.matches_upsert(r#"{"node_type":"aircraft","readiness":0.8}"#));
    }

    #[test]
    fn empty_and_matches_everything() {
        // Vacuous truth — consistent with iter().all() on an empty list.
        let m = build(pq_and(vec![]));
        assert!(m.matches_upsert(r#"{"anything":1}"#));
    }

    #[test]
    fn or_requires_any_clause() {
        let m = build(pq_or(vec![
            pq_eq("category", "\"a\""),
            pq_eq("category", "\"b\""),
        ]));
        assert!(m.matches_upsert(r#"{"category":"a"}"#));
        assert!(m.matches_upsert(r#"{"category":"b"}"#));
        assert!(!m.matches_upsert(r#"{"category":"c"}"#));
    }

    #[test]
    fn empty_or_matches_nothing() {
        // Vacuous falsity — consistent with iter().any() on an empty list.
        let m = build(pq_or(vec![]));
        assert!(!m.matches_upsert(r#"{"anything":1}"#));
    }

    #[test]
    fn not_inverts_inner() {
        let m = build(pq_not(pq_eq("status", "\"offline\"")));
        assert!(m.matches_upsert(r#"{"status":"ready"}"#));
        assert!(!m.matches_upsert(r#"{"status":"offline"}"#));
        // Missing field: inner is false, Not is true.
        assert!(m.matches_upsert(r#"{}"#));
    }

    #[test]
    fn nested_and_or_not() {
        // (node_type == "vehicle") AND NOT (status == "offline")
        let m = build(pq_and(vec![
            pq_eq("node_type", "\"vehicle\""),
            pq_not(pq_eq("status", "\"offline\"")),
        ]));
        assert!(m.matches_upsert(r#"{"node_type":"vehicle","status":"ready"}"#));
        assert!(!m.matches_upsert(r#"{"node_type":"vehicle","status":"offline"}"#));
        assert!(!m.matches_upsert(r#"{"node_type":"aircraft","status":"ready"}"#));
    }

    // --- event_passes integration ---

    #[test]
    fn event_passes_no_filter_no_query() {
        assert!(event_passes(&[], None, "anything", Some(r#"{}"#)));
        assert!(event_passes(&[], None, "anything", None));
    }

    #[test]
    fn event_passes_collection_filter() {
        let cols = vec!["alpha".to_string()];
        assert!(event_passes(&cols, None, "alpha", Some(r#"{}"#)));
        assert!(!event_passes(&cols, None, "bravo", Some(r#"{}"#)));
    }

    #[test]
    fn event_passes_query_filters_upserts_but_not_deletes() {
        let m = build(pq_eq("node_type", "\"vehicle\""));
        // Upsert that matches → pass
        assert!(event_passes(
            &[],
            Some(&m),
            "nodes",
            Some(r#"{"node_type":"vehicle"}"#)
        ));
        // Upsert that doesn't match → drop
        assert!(!event_passes(
            &[],
            Some(&m),
            "nodes",
            Some(r#"{"node_type":"aircraft"}"#)
        ));
        // Delete → pass (no payload to evaluate; subscriber needs to learn
        // the doc is gone or they keep stale state).
        assert!(event_passes(&[], Some(&m), "nodes", None));
    }

    #[test]
    fn event_passes_collection_and_query_combine() {
        let m = build(pq_eq("status", "\"ready\""));
        let cols = vec!["nodes".to_string()];
        assert!(event_passes(
            &cols,
            Some(&m),
            "nodes",
            Some(r#"{"status":"ready"}"#)
        ));
        // Wrong collection → drop, even with matching payload.
        assert!(!event_passes(
            &cols,
            Some(&m),
            "tracks",
            Some(r#"{"status":"ready"}"#)
        ));
    }
}
