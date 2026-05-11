//! Agent Watcher — polls a co-located UDS Remote Agent and syncs state to the CRDT mesh.
//!
//! Connects to the agent using the same Connect RPC / HTTP/2 protocol as the CLI and UI.
//! Uses JSON encoding (Connect RPC supports it natively) to avoid vendoring the agent's
//! proto definitions into Rust.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::node::SidecarNode;

/// TLS configuration for mutual TLS to the agent.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to PEM-encoded client certificate.
    pub cert: Option<PathBuf>,
    /// Path to PEM-encoded client private key.
    pub key: Option<PathBuf>,
    /// Path to PEM-encoded CA certificate for server verification.
    pub ca_cert: Option<PathBuf>,
}

impl TlsConfig {
    /// Returns true if at least cert and key are provided.
    pub fn is_enabled(&self) -> bool {
        self.cert.is_some() && self.key.is_some()
    }
}

/// Configuration for the agent watcher.
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Agent address, e.g. "http://localhost:8080" or "https://localhost:8080"
    pub agent_addr: String,
    /// Poll interval.
    pub poll_interval: Duration,
    /// Node ID used as the agent identifier in CRDT collections.
    pub node_id: String,
    /// Optional mTLS configuration for agent communication.
    pub tls: TlsConfig,
}

/// Build the HTTP client, optionally with mTLS.
///
/// Returns an explicit error rather than panicking on configuration
/// problems (partial TLS config, missing files, malformed PEM). The
/// previous panic-on-bad-PEM behavior masked a worse footgun: a
/// CA-only or cert-only / key-only configuration would silently fall
/// through to insecure h2c. That class of partial config is now an
/// explicit error.
pub(crate) fn build_client(tls: &TlsConfig) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(5));

    let has_cert = tls.cert.is_some();
    let has_key = tls.key.is_some();
    let has_ca = tls.ca_cert.is_some();

    match (has_cert, has_key, has_ca) {
        // Coherent: nothing set → insecure h2c.
        (false, false, false) => {
            builder = builder.http2_prior_knowledge();
        }
        // Coherent: cert + key (CA optional) → mTLS.
        (true, true, _) => {
            let cert_path = tls.cert.as_ref().unwrap();
            let key_path = tls.key.as_ref().unwrap();
            let cert_pem = std::fs::read(cert_path)
                .map_err(|e| anyhow::anyhow!("read TLS cert {}: {e}", cert_path.display()))?;
            let key_pem = std::fs::read(key_path)
                .map_err(|e| anyhow::anyhow!("read TLS key {}: {e}", key_path.display()))?;
            let mut identity_pem = cert_pem;
            identity_pem.extend_from_slice(&key_pem);
            let identity = reqwest::Identity::from_pem(&identity_pem)
                .map_err(|e| anyhow::anyhow!("parse TLS identity from cert+key PEM: {e}"))?;
            builder = builder.identity(identity);

            if let Some(ca_path) = &tls.ca_cert {
                let ca_pem = std::fs::read(ca_path)
                    .map_err(|e| anyhow::anyhow!("read TLS CA {}: {e}", ca_path.display()))?;
                let ca = reqwest::Certificate::from_pem(&ca_pem)
                    .map_err(|e| anyhow::anyhow!("parse CA cert PEM: {e}"))?;
                builder = builder.add_root_certificate(ca);
            }

            info!("agent watcher using mTLS");
        }
        // Incoherent: any other combination. Used to silently fall
        // through to insecure; now an explicit error.
        _ => {
            anyhow::bail!(
                "incoherent watcher TLS configuration — mTLS requires BOTH \
                 PEAT_NODE_AGENT_TLS_CERT and PEAT_NODE_AGENT_TLS_KEY. \
                 Setting only one of them, or setting PEAT_NODE_AGENT_TLS_CA \
                 without the cert+key pair, is an error (cert={has_cert}, \
                 key={has_key}, ca={has_ca})"
            );
        }
    }

    builder
        .build()
        .map_err(|e| anyhow::anyhow!("build reqwest client: {e}"))
}

/// Run the agent watcher loop. Polls the local UDS Remote Agent and writes
/// state to the sidecar node's CRDT store.
///
/// Returns early (logging an error) if the watcher TLS configuration is
/// incoherent — partial mTLS settings used to silently fall through to
/// insecure h2c, which was a real footgun. Now caught at startup.
pub async fn run(config: WatcherConfig, node: Arc<SidecarNode>) {
    let client = match build_client(&config.tls) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("agent watcher disabled: {e:#}");
            return;
        }
    };

    let mut interval = tokio::time::interval(config.poll_interval);
    let agent_id = config.node_id.clone();

    info!(
        agent_addr = %config.agent_addr,
        poll_interval = ?config.poll_interval,
        "agent watcher started"
    );

    loop {
        interval.tick().await;

        // Poll agent status
        if let Err(e) = poll_status(&client, &config.agent_addr, &agent_id, &node).await {
            warn!("poll /status failed: {e}");
        }

        // Poll deployed packages via Connect RPC (JSON encoding)
        if let Err(e) = poll_packages(&client, &config.agent_addr, &agent_id, &node).await {
            warn!("poll ListPackages failed: {e}");
        }

        // Poll pulled packages
        if let Err(e) = poll_pulled_packages(&client, &config.agent_addr, &agent_id, &node).await {
            debug!("poll ListPulledPackages failed: {e}");
        }
    }
}

// --- Agent Status ---

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentStatus {
    version: Option<String>,
    architecture: Option<String>,
    classification: Option<String>,
    k8s_version: Option<String>,
    k8s_node_status: Option<String>,
    zarf_version: Option<String>,
    run_mode: Option<String>,
    #[allow(dead_code)]
    system_time: Option<i64>,
}

async fn poll_status(
    client: &reqwest::Client,
    agent_addr: &str,
    agent_id: &str,
    node: &SidecarNode,
) -> anyhow::Result<()> {
    let url = format!("{agent_addr}/status");
    let resp = client.get(&url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("agent /status returned {}", resp.status());
    }

    let status: AgentStatus = resp.json().await?;

    let platform_json = serde_json::json!({
        "agent_id": agent_id,
        "platform_type": "uds-remote-agent",
        "version": status.version,
        "architecture": status.architecture,
        "classification": status.classification,
        "k8s_version": status.k8s_version,
        "k8s_node_status": status.k8s_node_status,
        "zarf_version": status.zarf_version,
        "run_mode": status.run_mode,
        "last_seen": chrono::Utc::now().timestamp(),
    });

    node.put_document("platforms", agent_id, &platform_json.to_string())
        .await?;

    debug!(agent_id, "synced agent status to mesh");
    Ok(())
}

// --- Deployed Packages (via Connect RPC JSON) ---

/// Connect RPC JSON response wrapper.
#[derive(Debug, Deserialize)]
struct ListPackagesResponse {
    packages: Option<Vec<ZarfPackage>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ZarfPackage {
    name: Option<String>,
    version: Option<String>,
    status: Option<i32>,
    flavor: Option<String>,
    namespace_override: Option<String>,
    #[serde(default)]
    annotations: serde_json::Map<String, serde_json::Value>,
}

async fn poll_packages(
    client: &reqwest::Client,
    agent_addr: &str,
    agent_id: &str,
    node: &SidecarNode,
) -> anyhow::Result<()> {
    // Connect RPC JSON encoding: POST with content-type application/json
    let url = format!("{agent_addr}/zarfapi.v1.ZarfAPIService/ListPackages");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("ListPackages returned {}", resp.status());
    }

    let body: ListPackagesResponse = resp.json().await?;
    let packages = body.packages.unwrap_or_default();

    for pkg in &packages {
        let pkg_name = pkg.name.as_deref().unwrap_or("unknown");
        let doc_key = format!("{agent_id}:{pkg_name}");
        let doc = serde_json::json!({
            "agent_id": agent_id,
            "package": pkg_name,
            "version": pkg.version,
            "status": pkg.status,
            "flavor": pkg.flavor,
            "namespace_override": pkg.namespace_override,
            "annotations": pkg.annotations,
            "last_seen": chrono::Utc::now().timestamp(),
        });
        node.put_document("deployments", &doc_key, &doc.to_string())
            .await?;
    }

    debug!(agent_id, count = packages.len(), "synced packages to mesh");
    Ok(())
}

// --- Pulled Packages ---

#[derive(Debug, Deserialize)]
struct ListPulledPackagesResponse {
    registries: Option<Vec<RegistryInfo>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RegistryInfo {
    name: Option<String>,
    host: Option<String>,
    packages: Option<Vec<RegistryPackage>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RegistryPackage {
    repo_name: Option<String>,
    package_name: Option<String>,
    tags: Option<Vec<PackageTag>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PackageTag {
    reference: Option<String>,
    arch: Option<String>,
    version: Option<String>,
    total_bytes: Option<i64>,
    pull_info: Option<PullInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PullInfo {
    downloaded_bytes: Option<i64>,
    status: Option<i32>,
}

async fn poll_pulled_packages(
    client: &reqwest::Client,
    agent_addr: &str,
    agent_id: &str,
    node: &SidecarNode,
) -> anyhow::Result<()> {
    let url = format!("{agent_addr}/registryapi.v1.RegistryService/ListPulledPackages");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("ListPulledPackages returned {}", resp.status());
    }

    let body: ListPulledPackagesResponse = resp.json().await?;
    let registries = body.registries.unwrap_or_default();

    let mut count = 0usize;
    for registry in &registries {
        for pkg in registry.packages.as_deref().unwrap_or_default() {
            for tag in pkg.tags.as_deref().unwrap_or_default() {
                let reference = tag.reference.as_deref().unwrap_or("unknown");
                let arch = tag.arch.as_deref().unwrap_or("unknown");
                let doc_key = format!("{agent_id}:{reference}-{arch}");

                let pull_status = tag.pull_info.as_ref().and_then(|p| p.status).unwrap_or(0);

                let doc = serde_json::json!({
                    "agent_id": agent_id,
                    "registry": registry.host,
                    "package": pkg.package_name,
                    "reference": reference,
                    "arch": arch,
                    "version": tag.version,
                    "total_bytes": tag.total_bytes,
                    "pull_status": pull_status,
                    "last_seen": chrono::Utc::now().timestamp(),
                });
                node.put_document("packages", &doc_key, &doc.to_string())
                    .await?;
                count += 1;
            }
        }
    }

    debug!(agent_id, count, "synced pulled packages to mesh");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pem_files_in_tempdir() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (dir, cert_path, key_path)
    }

    #[test]
    fn no_tls_is_coherent_and_builds_insecure() {
        let tls = TlsConfig::default();
        let client = build_client(&tls).expect("no-TLS config must build");
        // reqwest::Client doesn't expose its config; the assertion is
        // simply that the builder succeeded.
        drop(client);
    }

    #[test]
    fn cert_only_is_rejected() {
        let (_dir, cert_path, _key_path) = pem_files_in_tempdir();
        let tls = TlsConfig {
            cert: Some(cert_path),
            key: None,
            ca_cert: None,
        };
        let err = build_client(&tls).unwrap_err().to_string();
        assert!(
            err.contains("incoherent watcher TLS configuration"),
            "expected partial-config error, got: {err}"
        );
    }

    #[test]
    fn key_only_is_rejected() {
        let (_dir, _cert_path, key_path) = pem_files_in_tempdir();
        let tls = TlsConfig {
            cert: None,
            key: Some(key_path),
            ca_cert: None,
        };
        let err = build_client(&tls).unwrap_err().to_string();
        assert!(
            err.contains("incoherent watcher TLS configuration"),
            "expected partial-config error, got: {err}"
        );
    }

    #[test]
    fn ca_only_is_rejected() {
        let (_dir, cert_path, _key_path) = pem_files_in_tempdir();
        let tls = TlsConfig {
            cert: None,
            key: None,
            ca_cert: Some(cert_path),
        };
        let err = build_client(&tls).unwrap_err().to_string();
        assert!(
            err.contains("incoherent watcher TLS configuration"),
            "expected partial-config error, got: {err}"
        );
    }

    #[test]
    fn cert_plus_key_builds_mtls_client() {
        let (_dir, cert_path, key_path) = pem_files_in_tempdir();
        let tls = TlsConfig {
            cert: Some(cert_path),
            key: Some(key_path),
            ca_cert: None,
        };
        let _client = build_client(&tls).expect("cert+key must build an mTLS client");
    }

    #[test]
    fn cert_plus_key_plus_ca_builds_mtls_client() {
        let (_dir, cert_path, key_path) = pem_files_in_tempdir();
        // Reuse the same self-signed cert as the CA — for the purposes
        // of this test we just need a parseable PEM in the CA slot.
        let ca_path = cert_path.clone();
        let tls = TlsConfig {
            cert: Some(cert_path),
            key: Some(key_path),
            ca_cert: Some(ca_path),
        };
        let _client = build_client(&tls).expect("cert+key+ca must build an mTLS client");
    }

    #[test]
    fn malformed_pem_returns_error_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, b"NOT A PEM").unwrap();
        std::fs::write(&key_path, b"NOT A PEM EITHER").unwrap();
        let tls = TlsConfig {
            cert: Some(cert_path),
            key: Some(key_path),
            ca_cert: None,
        };
        let err = build_client(&tls).unwrap_err();
        assert!(
            err.to_string().contains("TLS identity"),
            "expected identity-parse error, got: {err}"
        );
    }
}
