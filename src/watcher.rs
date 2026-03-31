//! Agent Watcher — polls a co-located UDS Remote Agent and syncs state to the CRDT mesh.
//!
//! Connects to the agent using the same Connect RPC / HTTP/2 protocol as the CLI and UI.
//! Uses JSON encoding (Connect RPC supports it natively) to avoid vendoring the agent's
//! proto definitions into Rust.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::node::SidecarNode;

/// Configuration for the agent watcher.
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    /// Agent address, e.g. "http://localhost:8080"
    pub agent_addr: String,
    /// Poll interval.
    pub poll_interval: Duration,
    /// Node ID used as the agent identifier in CRDT collections.
    pub node_id: String,
    // TODO: mTLS cert/key/ca for production
}

/// Run the agent watcher loop. Polls the local UDS Remote Agent and writes
/// state to the sidecar node's CRDT store.
pub async fn run(config: WatcherConfig, node: Arc<SidecarNode>) {
    let client = reqwest::Client::builder()
        .http2_prior_knowledge() // h2c: HTTP/2 without TLS (same as agent's insecure mode)
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to create HTTP client");

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
