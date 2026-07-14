//! Typed configuration boundary for the native Core NATS bridge.
//!
//! Raw operator input is converted here into connection-ready values. Raw NATS
//! URLs and parser errors are deliberately discarded so credentials cannot
//! escape through ordinary formatting or validation diagnostics.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use async_nats::{ServerAddr, Subject};

const DEFAULT_NATS_PORT: u16 = 4222;

/// Validated bridge configuration.
pub enum BridgeConfig {
    /// No subject mappings were configured, so no bridge runtime may start.
    Disabled,
    /// Connection and routing inputs are complete and validated.
    Enabled(EnabledBridgeConfig),
}

impl BridgeConfig {
    /// Validate raw URL and repeatable `subject=collection` mapping values.
    pub fn from_raw(
        nats_url: Option<&str>,
        raw_mappings: &[String],
    ) -> Result<Self, BridgeConfigErrors> {
        let mut issues = Vec::new();
        let parsed_endpoint = match nats_url {
            Some(raw) => parse_endpoint(raw)
                .map_err(|kind| issues.push(BridgeConfigIssue::new(kind)))
                .ok(),
            None => {
                if !raw_mappings.is_empty() {
                    issues.push(BridgeConfigIssue::new(BridgeConfigIssueKind::MissingUrl));
                }
                None
            }
        };

        let mappings = parse_mappings(raw_mappings, &mut issues);

        if !issues.is_empty() {
            return Err(BridgeConfigErrors { issues });
        }

        if raw_mappings.is_empty() {
            return Ok(Self::Disabled);
        }

        let (server_addr, endpoint) = parsed_endpoint.expect("validated enabled config has a URL");
        Ok(Self::Enabled(EnabledBridgeConfig {
            server_addr,
            endpoint,
            mappings,
        }))
    }
}

impl fmt::Debug for BridgeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Enabled(config) => f.debug_tuple("Enabled").field(config).finish(),
        }
    }
}

/// Validated values required to run the bridge.
pub struct EnabledBridgeConfig {
    server_addr: ServerAddr,
    endpoint: SanitizedEndpoint,
    mappings: Vec<SubjectMapping>,
}

impl EnabledBridgeConfig {
    /// Address passed directly to the NATS client. Do not format this value.
    pub fn server_addr(&self) -> &ServerAddr {
        &self.server_addr
    }

    /// Credential-free endpoint suitable for logs and status output.
    pub fn endpoint(&self) -> &SanitizedEndpoint {
        &self.endpoint
    }

    /// Validated literal routes in operator-specified order.
    pub fn mappings(&self) -> &[SubjectMapping] {
        &self.mappings
    }
}

impl fmt::Debug for EnabledBridgeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnabledBridgeConfig")
            .field("endpoint", &self.endpoint)
            .field("mappings", &self.mappings)
            .finish()
    }
}

/// One validated literal NATS subject to Peat collection route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectMapping {
    subject: Subject,
    collection: String,
}

impl SubjectMapping {
    /// Exact, case-preserving subject after outer whitespace trimming.
    pub fn subject(&self) -> &Subject {
        &self.subject
    }

    /// Exact, case-preserving collection after outer whitespace trimming.
    pub fn collection(&self) -> &str {
        &self.collection
    }
}

/// A credential-free representation of a validated NATS endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct SanitizedEndpoint {
    scheme: EndpointScheme,
    host: String,
    port: u16,
    authenticated: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EndpointScheme {
    Nats,
    Tls,
}

impl EndpointScheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Nats => "nats",
            Self::Tls => "tls",
        }
    }
}

impl fmt::Display for SanitizedEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if self.authenticated {
            write!(
                f,
                "{}://<redacted>@{}:{}",
                self.scheme.as_str(),
                host,
                self.port
            )
        } else {
            write!(f, "{}://{}:{}", self.scheme.as_str(), host, self.port)
        }
    }
}

impl fmt::Debug for SanitizedEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// All validation issues found in one configuration attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeConfigErrors {
    issues: Vec<BridgeConfigIssue>,
}

impl BridgeConfigErrors {
    /// Issues in deterministic validation order.
    pub fn issues(&self) -> &[BridgeConfigIssue] {
        &self.issues
    }
}

impl fmt::Display for BridgeConfigErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid NATS bridge configuration")?;
        for issue in &self.issues {
            write!(f, "; {issue}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BridgeConfigErrors {}

/// One credential-safe, typed configuration issue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeConfigIssue {
    /// One-based mapping position, when the issue belongs to a mapping.
    pub mapping_index: Option<usize>,
    /// Machine-matchable validation category.
    pub kind: BridgeConfigIssueKind,
}

impl BridgeConfigIssue {
    fn new(kind: BridgeConfigIssueKind) -> Self {
        Self {
            mapping_index: None,
            kind,
        }
    }

    fn mapping(mapping_index: usize, kind: BridgeConfigIssueKind) -> Self {
        Self {
            mapping_index: Some(mapping_index),
            kind,
        }
    }
}

impl fmt::Display for BridgeConfigIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(index) = self.mapping_index {
            write!(f, "mapping {index}: ")?;
        }
        f.write_str(self.kind.message())
    }
}

/// Typed categories used by callers and tests without retaining unsafe input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BridgeConfigIssueKind {
    MissingUrl,
    UnsupportedUrlScheme,
    InvalidUrl,
    MissingUrlHost,
    UrlPathNotAllowed,
    UrlQueryNotAllowed,
    UrlFragmentNotAllowed,
    InvalidMappingDelimiter,
    EmptySubject,
    EmptyCollection,
    SubjectContainsWhitespace,
    CollectionContainsWhitespace,
    InvalidSubject,
    SubjectContainsWildcard,
    ReservedSubject,
    InvalidCollection,
    DuplicateSubject { original_index: usize },
    DuplicateCollection { original_index: usize },
}

impl BridgeConfigIssueKind {
    fn message(&self) -> &'static str {
        match self {
            Self::MissingUrl => "a NATS URL is required when mappings are configured",
            Self::UnsupportedUrlScheme => "URL scheme must be explicitly nats:// or tls://",
            Self::InvalidUrl => "NATS URL is malformed",
            Self::MissingUrlHost => "NATS URL requires a host",
            Self::UrlPathNotAllowed => "NATS URL must not contain a path",
            Self::UrlQueryNotAllowed => "NATS URL must not contain a query",
            Self::UrlFragmentNotAllowed => "NATS URL must not contain a fragment",
            Self::InvalidMappingDelimiter => "mapping must contain exactly one '=' delimiter",
            Self::EmptySubject => "mapping subject must not be empty",
            Self::EmptyCollection => "mapping collection must not be empty",
            Self::SubjectContainsWhitespace => "mapping subject must not contain whitespace",
            Self::CollectionContainsWhitespace => "mapping collection must not contain whitespace",
            Self::InvalidSubject => "mapping subject is invalid",
            Self::SubjectContainsWildcard => "mapping subject must be literal",
            Self::ReservedSubject => "mapping subject uses a reserved prefix",
            Self::InvalidCollection => "mapping collection has invalid characters",
            Self::DuplicateSubject { .. } => "mapping subject duplicates an earlier mapping",
            Self::DuplicateCollection { .. } => "mapping collection duplicates an earlier mapping",
        }
    }
}

fn parse_endpoint(raw: &str) -> Result<(ServerAddr, SanitizedEndpoint), BridgeConfigIssueKind> {
    let scheme = if raw.starts_with("nats://") {
        EndpointScheme::Nats
    } else if raw.starts_with("tls://") {
        EndpointScheme::Tls
    } else {
        return Err(BridgeConfigIssueKind::UnsupportedUrlScheme);
    };

    // Discard the client parser error: it can quote credential-bearing input.
    let server_addr = ServerAddr::from_str(raw).map_err(|_| BridgeConfigIssueKind::InvalidUrl)?;
    let url = server_addr.clone().into_inner();
    if url.host_str().is_none() || server_addr.host().is_empty() {
        return Err(BridgeConfigIssueKind::MissingUrlHost);
    }
    if url.path() != "" && url.path() != "/" {
        return Err(BridgeConfigIssueKind::UrlPathNotAllowed);
    }
    if url.query().is_some() {
        return Err(BridgeConfigIssueKind::UrlQueryNotAllowed);
    }
    if url.fragment().is_some() {
        return Err(BridgeConfigIssueKind::UrlFragmentNotAllowed);
    }

    let endpoint = SanitizedEndpoint {
        scheme,
        host: server_addr.host().to_owned(),
        port: url.port().unwrap_or(DEFAULT_NATS_PORT),
        authenticated: !url.username().is_empty() || url.password().is_some(),
    };
    Ok((server_addr, endpoint))
}

fn parse_mappings(
    raw_mappings: &[String],
    issues: &mut Vec<BridgeConfigIssue>,
) -> Vec<SubjectMapping> {
    let mut mappings = Vec::new();
    let mut subjects = HashMap::<String, usize>::new();
    let mut collections = HashMap::<String, usize>::new();
    for (offset, raw) in raw_mappings.iter().enumerate() {
        let index = offset + 1;
        if raw.matches('=').count() != 1 {
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::InvalidMappingDelimiter,
            ));
            continue;
        }
        let (subject, collection) = raw.split_once('=').expect("one delimiter was counted");
        let subject = subject.trim();
        let collection = collection.trim();
        let mut subject_value = None;
        let mut collection_valid = true;

        if subject.is_empty() {
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::EmptySubject,
            ));
        } else if subject.chars().any(char::is_whitespace) {
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::SubjectContainsWhitespace,
            ));
        } else {
            match Subject::validated(subject) {
                Ok(parsed) => {
                    if subject.contains(['*', '>']) {
                        issues.push(BridgeConfigIssue::mapping(
                            index,
                            BridgeConfigIssueKind::SubjectContainsWildcard,
                        ));
                    } else {
                        let first_token = subject.split('.').next().unwrap_or_default();
                        if first_token.starts_with('$') || first_token == "_INBOX" {
                            issues.push(BridgeConfigIssue::mapping(
                                index,
                                BridgeConfigIssueKind::ReservedSubject,
                            ));
                        } else {
                            subject_value = Some(parsed);
                        }
                    }
                }
                Err(_) => issues.push(BridgeConfigIssue::mapping(
                    index,
                    BridgeConfigIssueKind::InvalidSubject,
                )),
            }
        }

        if collection.is_empty() {
            collection_valid = false;
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::EmptyCollection,
            ));
        } else if collection.chars().any(char::is_whitespace) {
            collection_valid = false;
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::CollectionContainsWhitespace,
            ));
        } else if !valid_collection(collection) {
            collection_valid = false;
            issues.push(BridgeConfigIssue::mapping(
                index,
                BridgeConfigIssueKind::InvalidCollection,
            ));
        }

        let mut unique = true;
        if let Some(subject_value) = &subject_value {
            if let Some(original_index) = subjects.get(subject_value.as_str()) {
                unique = false;
                issues.push(BridgeConfigIssue::mapping(
                    index,
                    BridgeConfigIssueKind::DuplicateSubject {
                        original_index: *original_index,
                    },
                ));
            }
        }
        if collection_valid {
            if let Some(original_index) = collections.get(collection) {
                unique = false;
                issues.push(BridgeConfigIssue::mapping(
                    index,
                    BridgeConfigIssueKind::DuplicateCollection {
                        original_index: *original_index,
                    },
                ));
            }
        }

        if let (Some(subject), true, true) = (subject_value, collection_valid, unique) {
            subjects.insert(subject.as_str().to_owned(), index);
            collections.insert(collection.to_owned(), index);
            mappings.push(SubjectMapping {
                subject,
                collection: collection.to_owned(),
            });
        }
    }
    mappings
}

fn valid_collection(collection: &str) -> bool {
    let mut bytes = collection.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphanumeric())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping() -> Vec<String> {
        vec!["vision.summary=frames".to_owned()]
    }

    fn enabled(url: &str) -> EnabledBridgeConfig {
        match BridgeConfig::from_raw(Some(url), &mapping()).expect("configuration should be valid")
        {
            BridgeConfig::Enabled(config) => config,
            BridgeConfig::Disabled => panic!("mapping should enable configuration"),
        }
    }

    #[test]
    fn empty_mappings_are_structurally_disabled() {
        assert!(matches!(
            BridgeConfig::from_raw(None, &[]).expect("empty config is valid"),
            BridgeConfig::Disabled
        ));
        assert!(matches!(
            BridgeConfig::from_raw(Some("nats://broker.example"), &[])
                .expect("unused valid URL is valid"),
            BridgeConfig::Disabled
        ));
    }

    #[test]
    fn mappings_require_a_url() {
        let error = BridgeConfig::from_raw(None, &mapping()).expect_err("URL must be required");
        assert_eq!(error.issues()[0].kind, BridgeConfigIssueKind::MissingUrl);
    }

    #[test]
    fn accepts_dns_ipv4_ipv6_and_tls_with_effective_port() {
        assert_eq!(
            enabled("nats://broker.example").endpoint().to_string(),
            "nats://broker.example:4222"
        );
        assert_eq!(
            enabled("nats://127.0.0.1:4333").endpoint().to_string(),
            "nats://127.0.0.1:4333"
        );
        assert_eq!(
            enabled("tls://[2001:db8::1]").endpoint().to_string(),
            "tls://[2001:db8::1]:4222"
        );
    }

    #[test]
    fn authenticated_endpoints_are_always_redacted() {
        let user_pass = enabled("nats://alice:s3cr%65t@broker.example");
        let token = enabled("nats://token-value@broker.example:4333");
        assert_eq!(
            user_pass.endpoint().to_string(),
            "nats://<redacted>@broker.example:4222"
        );
        assert_eq!(
            token.endpoint().to_string(),
            "nats://<redacted>@broker.example:4333"
        );

        let rendered = format!(
            "{user_pass:?} {:?} {token:?} {:?}",
            user_pass.endpoint(),
            token.endpoint()
        );
        for secret in ["alice", "s3cr%65t", "s3cret", "token-value"] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn rejects_unsupported_or_ambiguous_url_shapes() {
        let cases = [
            "broker.example:4222",
            "http://broker.example",
            "ws://broker.example",
            "wss://broker.example",
            "nats:///missing-host",
            "nats://broker.example/path",
            "nats://broker.example?mode=x",
            "nats://broker.example#fragment",
            "nats://broker.example:99999",
        ];
        for url in cases {
            assert!(BridgeConfig::from_raw(Some(url), &mapping()).is_err());
        }
    }

    #[test]
    fn malformed_credentials_never_appear_in_errors() {
        let url = "nats://raw-user:raw-pass%65ncoded@broker.example:99999";
        let error = BridgeConfig::from_raw(Some(url), &mapping()).expect_err("URL is malformed");
        let rendered = format!("{error} {error:?}");
        for secret in ["raw-user", "raw-pass%65ncoded", "raw-passencoded"] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn trims_outer_whitespace_and_preserves_case() {
        let mappings = vec!["  Vision.Summary = Frame_Store-1  ".to_owned()];
        let config = BridgeConfig::from_raw(Some("nats://broker.example"), &mappings)
            .expect("trimmed mapping should be valid");
        let BridgeConfig::Enabled(config) = config else {
            panic!("mapping should enable configuration");
        };
        assert_eq!(config.mappings()[0].subject().as_str(), "Vision.Summary");
        assert_eq!(config.mappings()[0].collection(), "Frame_Store-1");
    }

    #[test]
    fn rejects_delimiter_blank_and_whitespace_errors() {
        let cases = [
            ("subject", BridgeConfigIssueKind::InvalidMappingDelimiter),
            ("a=b=c", BridgeConfigIssueKind::InvalidMappingDelimiter),
            ("=collection", BridgeConfigIssueKind::EmptySubject),
            ("subject=", BridgeConfigIssueKind::EmptyCollection),
            (
                "vision summary=frames",
                BridgeConfigIssueKind::SubjectContainsWhitespace,
            ),
            (
                "vision.summary=frame\u{2003}store",
                BridgeConfigIssueKind::CollectionContainsWhitespace,
            ),
        ];
        for (raw, expected) in cases {
            let error = BridgeConfig::from_raw(Some("nats://broker.example"), &[raw.to_owned()])
                .expect_err("mapping should be rejected");
            assert!(error.issues().iter().any(|issue| issue.kind == expected));
        }
    }

    #[test]
    fn rejects_invalid_literal_and_reserved_subjects() {
        let cases = [
            (".a=x", BridgeConfigIssueKind::InvalidSubject),
            ("a.=x", BridgeConfigIssueKind::InvalidSubject),
            ("a..b=x", BridgeConfigIssueKind::InvalidSubject),
            ("a.*=x", BridgeConfigIssueKind::SubjectContainsWildcard),
            ("a>b=x", BridgeConfigIssueKind::SubjectContainsWildcard),
            ("$SYS.events=x", BridgeConfigIssueKind::ReservedSubject),
            ("_INBOX=x", BridgeConfigIssueKind::ReservedSubject),
            ("_INBOX.reply=x", BridgeConfigIssueKind::ReservedSubject),
        ];
        for (raw, expected) in cases {
            let error = BridgeConfig::from_raw(Some("nats://broker.example"), &[raw.to_owned()])
                .expect_err("subject should be rejected");
            assert!(error.issues().iter().any(|issue| issue.kind == expected));
        }

        assert!(BridgeConfig::from_raw(
            Some("nats://broker.example"),
            &["_INBOXED.reply=x".to_owned()]
        )
        .is_ok());
    }

    #[test]
    fn collection_grammar_matches_the_operating_contract() {
        for collection in ["A", "frames.v1", "frame_store", "frame-store", "9frames"] {
            let raw = format!("vision.summary={collection}");
            assert!(BridgeConfig::from_raw(Some("nats://broker.example"), &[raw]).is_ok());
        }
        for collection in ["_frames", "-frames", ".frames", "frame:store", "fråmes"] {
            let raw = format!("vision.summary={collection}");
            let error = BridgeConfig::from_raw(Some("nats://broker.example"), &[raw])
                .expect_err("collection should be rejected");
            assert_eq!(
                error.issues()[0].kind,
                BridgeConfigIssueKind::InvalidCollection
            );
        }
    }

    #[test]
    fn duplicate_subjects_and_collections_are_exact_and_indexed() {
        let mappings = vec![
            "Vision.Summary=Frames".to_owned(),
            "Vision.Summary=Other".to_owned(),
            "vision.summary=Frames".to_owned(),
        ];
        let error = BridgeConfig::from_raw(Some("nats://broker.example"), &mappings)
            .expect_err("exact duplicates should be rejected");
        assert_eq!(
            error.issues(),
            &[
                BridgeConfigIssue::mapping(
                    2,
                    BridgeConfigIssueKind::DuplicateSubject { original_index: 1 }
                ),
                BridgeConfigIssue::mapping(
                    3,
                    BridgeConfigIssueKind::DuplicateCollection { original_index: 1 }
                ),
            ]
        );

        assert!(BridgeConfig::from_raw(
            Some("nats://broker.example"),
            &[
                "Vision.Summary=Frames".to_owned(),
                "vision.summary=frames".to_owned(),
            ]
        )
        .is_ok());
    }

    #[test]
    fn aggregates_mapping_issues_in_stable_input_order() {
        let mappings = vec![
            "bad".to_owned(),
            "=x".to_owned(),
            "a=".to_owned(),
            "a=x".to_owned(),
            "a=y".to_owned(),
            "b=x".to_owned(),
        ];
        let error = BridgeConfig::from_raw(Some("nats://broker.example"), &mappings)
            .expect_err("all issues should be returned");
        let positions: Vec<_> = error
            .issues()
            .iter()
            .map(|issue| issue.mapping_index)
            .collect();
        assert_eq!(positions, vec![Some(1), Some(2), Some(3), Some(5), Some(6)]);
        assert_eq!(
            error.issues()[3].kind,
            BridgeConfigIssueKind::DuplicateSubject { original_index: 4 }
        );
        assert_eq!(
            error.issues()[4].kind,
            BridgeConfigIssueKind::DuplicateCollection { original_index: 4 }
        );
    }
}
