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

/// A minimal, testable reference to a package reported by RA.
///
/// Built from the private ZarfPackage inside poll_all so the SYNC-01 helper
/// can be invoked directly from integration tests without spinning up a fake
/// RA HTTP server. Intentionally narrow: only the fields SYNC-01 uses.
#[derive(Debug, Clone)]
pub struct DeployedPackageRef {
    pub name: String,
    pub version: String,
    pub status_is_deployed: bool,
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
fn build_client(tls: &TlsConfig) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(5));

    if tls.is_enabled() {
        // Read client identity (cert + key)
        let cert_path = tls.cert.as_ref().unwrap();
        let key_path = tls.key.as_ref().unwrap();

        let cert_pem = std::fs::read(cert_path)
            .unwrap_or_else(|e| panic!("failed to read TLS cert {}: {e}", cert_path.display()));
        let key_pem = std::fs::read(key_path)
            .unwrap_or_else(|e| panic!("failed to read TLS key {}: {e}", key_path.display()));

        let mut identity_pem = cert_pem;
        identity_pem.extend_from_slice(&key_pem);
        let identity = reqwest::Identity::from_pem(&identity_pem)
            .expect("failed to parse TLS identity from cert+key PEM");
        builder = builder.identity(identity);

        // Add custom CA if provided
        if let Some(ca_path) = &tls.ca_cert {
            let ca_pem = std::fs::read(ca_path)
                .unwrap_or_else(|e| panic!("failed to read TLS CA {}: {e}", ca_path.display()));
            let ca = reqwest::Certificate::from_pem(&ca_pem)
                .expect("failed to parse CA certificate PEM");
            builder = builder.add_root_certificate(ca);
        }

        info!("agent watcher using mTLS");
    } else {
        // h2c: HTTP/2 without TLS (same as agent's insecure mode)
        builder = builder.http2_prior_knowledge();
    }

    builder.build().expect("failed to create HTTP client")
}

/// Run the agent watcher loop. Polls the local UDS Remote Agent and writes
/// state to the sidecar node's CRDT store.
pub async fn run(config: WatcherConfig, node: Arc<SidecarNode>) {
    let client = build_client(&config.tls);

    let mut interval = tokio::time::interval(config.poll_interval);
    let agent_id = config.node_id.clone();

    info!(
        agent_addr = %config.agent_addr,
        poll_interval = ?config.poll_interval,
        "agent watcher started"
    );

    loop {
        interval.tick().await;

        // Poll all data and write a single combined document per agent.
        // This ensures one CRDT sync operation transfers all health data.
        if let Err(e) = poll_all(&client, &config.agent_addr, &agent_id, &node).await {
            warn!("poll cycle failed: {e}");
        }
    }
}

/// Poll all agent endpoints and write a single combined document to platforms/{agent_id}.
/// This ensures the entire health snapshot syncs in one CRDT operation.
async fn poll_all(
    client: &reqwest::Client,
    agent_addr: &str,
    agent_id: &str,
    node: &SidecarNode,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now().timestamp();

    // 1. Agent status
    let status = poll_status_data(client, agent_addr).await.ok();

    // 2. Deployed packages
    let packages = poll_packages_data(client, agent_addr).await.unwrap_or_default();

    // 3. Pod health
    let pod_health = poll_pods_data(client, agent_addr).await.ok();

    // 4. CV metrics — forwarded from the cv_metrics collection into the platforms document.
    //    The iOS peat FFI only reliably surfaces documents written internally by the watcher,
    //    so we read what the cv-inference pod PUT and forward it here.
    let cv_metrics_value: Option<serde_json::Value> = node
        .get_document("cv_metrics", agent_id)
        .await
        .ok()
        .flatten()
        .and_then(|json| serde_json::from_str(&json).ok());

    // Build combined document
    let mut pkg_array = Vec::new();
    for pkg in &packages {
        pkg_array.push(serde_json::json!({
            "package": pkg.name,
            "version": pkg.version,
            "status": pkg.status,
            "flavor": pkg.flavor,
            "namespace_override": pkg.namespace_override,
        }));
    }

    let platform_json = serde_json::json!({
        "agent_id": agent_id,
        "platform_type": "uds-remote-agent",
        "version": status.as_ref().and_then(|s| s.version.as_deref()),
        "architecture": status.as_ref().and_then(|s| s.architecture.as_deref()),
        "classification": status.as_ref().and_then(|s| s.classification.as_deref()),
        "k8s_version": status.as_ref().and_then(|s| s.k8s_version.as_deref()),
        "k8s_node_status": status.as_ref().and_then(|s| s.k8s_node_status.as_deref()),
        "zarf_version": status.as_ref().and_then(|s| s.zarf_version.as_deref()),
        "run_mode": status.as_ref().and_then(|s| s.run_mode.as_deref()),
        "packages": pkg_array,
        "total_pods": pod_health.as_ref().map(|p| p.total),
        "ready_pods": pod_health.as_ref().map(|p| p.ready),
        "error_pods": pod_health.as_ref().map(|p| p.errors),
        "error_pod_names": pod_health.as_ref().map(|p| &p.error_names),
        "namespaces": pod_health.as_ref().map(|p| &p.namespace_counts),
        "cv_metrics": cv_metrics_value,
        "last_seen": now,
    });

    node.put_document("platforms", agent_id, &platform_json.to_string())
        .await?;

    // --- SYNC-01: auto-publish deployed packages to available_packages ---
    let arch = status
        .as_ref()
        .and_then(|s| s.architecture.as_deref())
        .unwrap_or("unknown");
    let refs: Vec<DeployedPackageRef> = packages
        .iter()
        .filter_map(|p| {
            Some(DeployedPackageRef {
                name: p.name.as_deref()?.to_string(),
                version: p.version.as_deref()?.to_string(),
                status_is_deployed: p.status == Some(2),
            })
        })
        .collect();
    sync_available_packages(node, &refs, arch, now).await;

    debug!(
        agent_id,
        packages = packages.len(),
        pods = pod_health.as_ref().map(|p| p.total).unwrap_or(0),
        "synced all agent data to mesh"
    );
    Ok(())
}

// --- Data fetchers (return structured data, don't write to store) ---

async fn poll_status_data(client: &reqwest::Client, agent_addr: &str) -> anyhow::Result<AgentStatus> {
    let url = format!("{agent_addr}/status");
    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("agent /status returned {}", resp.status());
    }
    Ok(resp.json().await?)
}

async fn poll_packages_data(client: &reqwest::Client, agent_addr: &str) -> anyhow::Result<Vec<ZarfPackage>> {
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
    Ok(body.packages.unwrap_or_default())
}

struct PodHealthData {
    total: usize,
    ready: usize,
    errors: usize,
    error_names: Vec<String>,
    namespace_counts: std::collections::HashMap<String, usize>,
}

async fn poll_pods_data(client: &reqwest::Client, agent_addr: &str) -> anyhow::Result<PodHealthData> {
    let url = format!("{agent_addr}/zarfapi.v1.ZarfAPIService/ListPods");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("ListPods returned {}", resp.status());
    }
    let body: ListPodsResponse = resp.json().await?;
    let pods = body.pods.unwrap_or_default();

    let total = pods.len();
    let ready = pods.iter().filter(|p| p.ready == Some(true)).count();
    let error_pods: Vec<&StatusPod> = pods
        .iter()
        .filter(|p| matches!(p.status, Some(9) | Some(10) | Some(11) | Some(12)))
        .collect();

    let mut namespace_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for pod in &pods {
        let ns = pod.pod.as_ref()
            .and_then(|p| p.metadata.as_ref())
            .and_then(|m| m.namespace.as_deref())
            .unwrap_or("unknown");
        *namespace_counts.entry(ns.to_string()).or_default() += 1;
    }

    Ok(PodHealthData {
        total,
        ready,
        errors: error_pods.len(),
        error_names: error_pods.iter().filter_map(|p| p.name.clone()).collect(),
        namespace_counts,
    })
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
    /// Status can be either an int32 (proto numeric) or a string (Connect RPC JSON enum name)
    #[serde(default, deserialize_with = "deserialize_status")]
    status: Option<i32>,
    flavor: Option<String>,
    namespace_override: Option<String>,
    #[serde(default)]
    annotations: serde_json::Map<String, serde_json::Value>,
    readiness: Option<serde_json::Value>,
}

/// Deserialize proto enum status from either int or string representation
fn deserialize_status<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        Some(serde_json::Value::Number(n)) => Ok(n.as_i64().map(|v| v as i32)),
        Some(serde_json::Value::String(s)) => Ok(Some(match s.as_str() {
            "DEPLOY_PACKAGE_STATUS_DEPLOYING" => 1,
            "DEPLOY_PACKAGE_STATUS_DEPLOYED" => 2,
            "DEPLOY_PACKAGE_STATUS_DEPLOY_ERROR" => 3,
            "DEPLOY_PACKAGE_STATUS_CANCELLED" => 4,
            "DEPLOY_PACKAGE_STATUS_RECEIVING" => 5,
            "DEPLOY_PACKAGE_STATUS_REMOVING" => 6,
            "DEPLOY_PACKAGE_STATUS_REMOVED" => 7,
            "DEPLOY_PACKAGE_STATUS_REMOVE_ERROR" => 8,
            _ => 0,
        })),
        _ => Ok(None),
    }
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
    #[serde(default, deserialize_with = "deserialize_pull_status")]
    status: Option<i32>,
}

fn deserialize_pull_status<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        Some(serde_json::Value::Number(n)) => Ok(n.as_i64().map(|v| v as i32)),
        Some(serde_json::Value::String(s)) => Ok(Some(match s.as_str() {
            "PULL_STATUS_PENDING" => 1,
            "PULL_STATUS_PULLING" => 2,
            "PULL_STATUS_PULLED" => 3,
            "PULL_STATUS_CANCELLED" => 4,
            "PULL_STATUS_ERROR" => 5,
            _ => 0,
        })),
        _ => Ok(None),
    }
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

// --- Pods (cluster health via ListPods RPC) ---

#[derive(Debug, Deserialize)]
struct ListPodsResponse {
    pods: Option<Vec<StatusPod>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusPod {
    name: Option<String>,
    /// Pod status as string enum name (e.g. "POD_STATUS_RUNNING") or int
    #[serde(default, deserialize_with = "deserialize_pod_status")]
    status: Option<i32>,
    message: Option<String>,
    ready: Option<bool>,
    pod: Option<PodInfo>,
}

fn deserialize_pod_status<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        Some(serde_json::Value::Number(n)) => Ok(n.as_i64().map(|v| v as i32)),
        Some(serde_json::Value::String(s)) => Ok(Some(match s.as_str() {
            "POD_STATUS_RUNNING" => 3,
            "POD_STATUS_COMPLETED" => 5,
            "POD_STATUS_CRASH_LOOP_BACKOFF" => 9,
            "POD_STATUS_ERROR" => 10,
            "POD_STATUS_IMAGE_PULL_BACKOFF" => 11,
            "POD_STATUS_OOM_KILLED" => 12,
            "POD_STATUS_PENDING" => 13,
            "POD_STATUS_TERMINATING" => 1,
            "POD_STATUS_NOT_READY" => 4,
            "POD_STATUS_CONTAINER_CREATING" => 6,
            "POD_STATUS_POD_INITIALIZING" => 7,
            _ => 0,
        })),
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PodInfo {
    metadata: Option<PodMetadata>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PodMetadata {
    name: Option<String>,
    namespace: Option<String>,
    #[serde(default)]
    labels: serde_json::Map<String, serde_json::Value>,
}

async fn poll_pods(
    client: &reqwest::Client,
    agent_addr: &str,
    agent_id: &str,
    node: &SidecarNode,
) -> anyhow::Result<()> {
    let url = format!("{agent_addr}/zarfapi.v1.ZarfAPIService/ListPods");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("ListPods returned {}", resp.status());
    }

    let body: ListPodsResponse = resp.json().await?;
    let pods = body.pods.unwrap_or_default();

    // Aggregate pod health into a summary document
    let total = pods.len();
    let ready = pods.iter().filter(|p| p.ready == Some(true)).count();
    let error_pods: Vec<&StatusPod> = pods
        .iter()
        .filter(|p| {
            // Pod statuses: 9=CrashLoopBackOff, 10=Error, 11=ImagePullBackOff, 12=OOMKilled
            matches!(p.status, Some(9) | Some(10) | Some(11) | Some(12))
        })
        .collect();

    // Count pods by namespace
    let mut namespace_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for pod in &pods {
        let ns = pod
            .pod
            .as_ref()
            .and_then(|p| p.metadata.as_ref())
            .and_then(|m| m.namespace.as_deref())
            .unwrap_or("unknown");
        *namespace_counts.entry(ns.to_string()).or_default() += 1;
    }

    let error_names: Vec<String> = error_pods
        .iter()
        .filter_map(|p| p.name.clone())
        .collect();

    let doc = serde_json::json!({
        "agent_id": agent_id,
        "total_pods": total,
        "ready_pods": ready,
        "error_pods": error_pods.len(),
        "error_pod_names": error_names,
        "namespaces": namespace_counts,
        "last_seen": chrono::Utc::now().timestamp(),
    });

    node.put_document("pods", agent_id, &doc.to_string())
        .await?;

    debug!(agent_id, total, ready, errors = error_pods.len(), "synced pod health to mesh");
    Ok(())
}

/// SYNC-01: auto-publish deployed packages into the available_packages CRDT collection.
///
/// For each DeployedPackageRef where status_is_deployed is true:
///   1. Compute pkg_ref = "{name}-{version}-{arch}".
///   2. Skip if available_packages/{pkg_ref} already exists (SYNC-02 immutability).
///   3. Look up a matching local blob via node.list_local_blobs() — the blob MUST have
///      already been published locally (via publish_blob) for the package to be advertised.
///      This is Recommendation A from 02-RESEARCH.md §Open Questions #1.
///   4. Serialize an AvailablePackage struct and write it to available_packages/{pkg_ref}.
///
/// Per-package errors are logged at warn! and skipped; the caller's poll cycle continues.
/// BlobMetadata accessor: t.metadata.name is a public field (Option<String>), accessed
/// via .as_deref() (field form, not method call).
pub async fn sync_available_packages(
    node: &crate::node::SidecarNode,
    packages: &[DeployedPackageRef],
    arch: &str,
    now: i64,
) {
    // Snapshot local blobs once per call (cheap — scans .meta.json sidecars synchronously).
    let local_blobs = node.list_local_blobs();

    for pkg in packages {
        if !pkg.status_is_deployed {
            continue;
        }
        if pkg.name.is_empty() || pkg.version.is_empty() {
            continue;
        }
        let pkg_ref = format!("{}-{}-{}", pkg.name, pkg.version, arch);

        // Idempotency guard — SYNC-02 immutability.
        match node.get_document("available_packages", &pkg_ref).await {
            Ok(Some(_)) => continue,
            Ok(None) => {}
            Err(e) => {
                warn!(pkg_ref = %pkg_ref, "SYNC-01: failed to check available_packages: {e}");
                continue;
            }
        }

        // Match a local blob by metadata name. publish_blob callers typically use
        // filenames like "{name}-{version}-{arch}.zarf.tar.zst"; we do a tolerant
        // substring match on name AND version to absorb minor filename variations.
        // BlobMetadata.name is a public field (Option<String>), accessed via .as_deref().
        let matched = local_blobs.iter().find(|t| {
            let blob_name = t.metadata.name.as_deref().unwrap_or("");
            blob_name.contains(&pkg.name) && blob_name.contains(&pkg.version)
        });
        let token = match matched {
            Some(t) => t,
            None => {
                debug!(
                    pkg = %pkg.name,
                    version = %pkg.version,
                    arch,
                    "SYNC-01: no matching local blob; skipping advertisement (Recommendation A)"
                );
                continue;
            }
        };

        let avail = crate::types::AvailablePackage {
            name: pkg.name.clone(),
            version: pkg.version.clone(),
            architecture: arch.to_string(),
            iroh_blob_hash: token.hash.as_hex().to_string(),
            sender_endpoint_id: node.endpoint_addr(),
            published_at: now,
        };

        let json = match serde_json::to_string(&avail) {
            Ok(j) => j,
            Err(e) => {
                warn!(pkg_ref = %pkg_ref, "SYNC-01: failed to serialize AvailablePackage: {e}");
                continue;
            }
        };

        if let Err(e) = node.put_document("available_packages", &pkg_ref, &json).await {
            warn!(pkg_ref = %pkg_ref, "SYNC-01: failed to write available_packages doc: {e}");
        } else {
            info!(
                pkg_ref = %pkg_ref,
                iroh_blob_hash = %token.hash.as_hex(),
                "SYNC-01: advertised package to available_packages"
            );
        }
    }
}
