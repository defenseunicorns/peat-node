//! Send-side outbox watcher (PRD-006 v1.1, peat-node attachment auto-sync).
//!
//! The symmetric counterpart to the receive-side inbox watcher: poll the
//! configured `--attachment-root` directories and, when a file is *stable*
//! (unchanged across a poll) and hasn't been sent yet, auto-distribute it by
//! synthesising the same `SendAttachments` request an application would send —
//! so dropping a file in the outbox lands it in every peer's inbox with no gRPC
//! call. Reuses `handlers::send_attachments` end-to-end (validate → ingest →
//! content-hash verify → distribute → registry); this module is just the
//! filesystem poll + dedup in front of it.
//!
//! Off by default (`PEAT_NODE_ATTACHMENT_OUTBOX_WATCH`); the explicit RPC stays
//! the safe default. Polling (not inotify) mirrors the inbox watcher and is
//! reliable across container bind mounts where inotify/FSEvents are not.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::node::SidecarNode;

/// What to do with an outbox file this poll, given its current (size, mtime)
/// and the watcher's prior state. Pure so the stability/dedup policy is
/// unit-testable without touching the filesystem.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OutboxDecision {
    /// Stable (unchanged since last poll) and not yet sent at this version.
    Send,
    /// New or changed since last poll — wait one interval to confirm it's not
    /// mid-write before distributing.
    Wait,
    /// Already distributed at this exact (size, mtime).
    Skip,
}

pub(crate) fn outbox_decision(
    current: (u64, SystemTime),
    last_seen: Option<(u64, SystemTime)>,
    last_sent: Option<(u64, SystemTime)>,
) -> OutboxDecision {
    if last_sent == Some(current) {
        OutboxDecision::Skip
    } else if last_seen == Some(current) {
        OutboxDecision::Send
    } else {
        OutboxDecision::Wait
    }
}

/// Drop tracking entries whose path was not observed this poll, so the
/// watcher's state maps don't grow unbounded as files leave the outbox.
fn prune_to_observed(
    map: &mut HashMap<PathBuf, (u64, SystemTime)>,
    observed: &std::collections::HashSet<PathBuf>,
) {
    map.retain(|p, _| observed.contains(p));
}

/// A regular file discovered under a root: `relative_path` (forward-slashed,
/// relative to the root) plus its absolute path and current size/mtime.
struct Found {
    relative_path: String,
    full_path: PathBuf,
    size: u64,
    mtime: SystemTime,
}

/// Recursively list regular files under `root`, skipping dotfiles (the inbox's
/// in-flight markers start with `.`) and anything unreadable.
fn walk_files(root: &Path) -> Vec<Found> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                let Ok(meta) = entry.metadata() else { continue };
                let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                if let Ok(rel) = path.strip_prefix(root) {
                    out.push(Found {
                        relative_path: rel.to_string_lossy().replace('\\', "/"),
                        full_path: path.clone(),
                        size: meta.len(),
                        mtime,
                    });
                }
            }
        }
    }
    out
}

/// Stream-hash a file's sha256 without loading it fully into memory.
async fn sha256_file(path: PathBuf) -> std::io::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut f = std::fs::File::open(&path)?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 65536];
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finalize().to_vec())
    })
    .await
    .map_err(|e| std::io::Error::other(e.to_string()))?
}

fn build_request(
    root_name: &str,
    relative_path: &str,
    size: u64,
    sha256: Vec<u8>,
) -> crate::pb::SendAttachmentsRequest {
    crate::pb::SendAttachmentsRequest {
        files: vec![crate::pb::FileSpec {
            root_name: root_name.to_string(),
            relative_path: relative_path.to_string(),
            size_bytes: size,
            sha256,
            ..Default::default()
        }],
        // AllNodes: the synced-folder model — every peer that knows the sender
        // receives it. Priority left unset -> handlers maps UNSPECIFIED to the
        // configured `default_priority`.
        scope: buffa::MessageField::some(crate::pb::DistributionScopeSpec {
            scope: Some(crate::pb::distribution_scope_spec::Scope::AllNodes(
                Box::default(),
            )),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Run the outbox watcher until the node shuts down. Polls every `poll`
/// interval; auto-distributes each stable, not-yet-sent file in `roots`.
pub async fn run(node: Arc<SidecarNode>, roots: HashMap<String, PathBuf>, poll: Duration) {
    info!(
        roots = ?roots.keys().collect::<Vec<_>>(),
        poll_secs = poll.as_secs(),
        "outbox watcher started — files dropped in a root auto-distribute (AllNodes)"
    );
    // Per absolute-path state: the (size, mtime) seen last poll, and the
    // (size, mtime) last successfully handed to `send_attachments`.
    let mut last_seen: HashMap<PathBuf, (u64, SystemTime)> = HashMap::new();
    let mut last_sent: HashMap<PathBuf, (u64, SystemTime)> = HashMap::new();

    loop {
        // Paths observed this cycle, so we can drop state for files that have
        // since left the outbox (drop→distribute→delete is the common synced-
        // folder pattern). Without this, `last_seen`/`last_sent` grow unbounded.
        let mut observed: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        for (root_name, root_path) in &roots {
            for f in walk_files(root_path) {
                observed.insert(f.full_path.clone());
                let current = (f.size, f.mtime);
                let decision = outbox_decision(
                    current,
                    last_seen.get(&f.full_path).copied(),
                    last_sent.get(&f.full_path).copied(),
                );
                last_seen.insert(f.full_path.clone(), current);
                if decision != OutboxDecision::Send {
                    continue;
                }
                let sha256 = match sha256_file(f.full_path.clone()).await {
                    Ok(h) => h,
                    Err(e) => {
                        warn!(file = %f.relative_path, "outbox: hash failed: {e}");
                        continue;
                    }
                };
                let req = build_request(root_name, &f.relative_path, f.size, sha256);
                match super::handlers::send_attachments(&node, req).await {
                    Ok(resp) => {
                        let dist = resp
                            .handles
                            .first()
                            .map(|h| h.distribution_id.as_str())
                            .unwrap_or("");
                        info!(file = %f.relative_path, distribution_id = %dist, "outbox: auto-distributed");
                    }
                    Err(e) => {
                        // Mark sent regardless so we don't re-attempt the same
                        // version every poll (a content change bumps mtime and
                        // re-triggers). AlreadyExists after a restart lands here
                        // too — expected, not an error worth retrying.
                        warn!(file = %f.relative_path, "outbox: distribute returned: {e}");
                    }
                }
                // Record this version as handled (success or terminal error).
                last_sent.insert(f.full_path.clone(), current);
            }
        }
        // Prune state for files no longer present, bounding map growth.
        prune_to_observed(&mut last_seen, &observed);
        prune_to_observed(&mut last_sent, &observed);
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn new_file_waits_one_poll() {
        // First sighting -> Wait (could still be mid-write).
        assert_eq!(
            outbox_decision((10, t(1)), None, None),
            OutboxDecision::Wait
        );
    }

    #[test]
    fn stable_file_sends() {
        // Seen last poll with identical (size, mtime), never sent -> Send.
        assert_eq!(
            outbox_decision((10, t(1)), Some((10, t(1))), None),
            OutboxDecision::Send
        );
    }

    #[test]
    fn already_sent_version_skips() {
        assert_eq!(
            outbox_decision((10, t(1)), Some((10, t(1))), Some((10, t(1)))),
            OutboxDecision::Skip
        );
    }

    #[test]
    fn changed_after_send_waits_then_resends() {
        // Content changed (new mtime) after a prior send of the old version:
        // not stable yet -> Wait.
        assert_eq!(
            outbox_decision((20, t(2)), Some((10, t(1))), Some((10, t(1)))),
            OutboxDecision::Wait
        );
        // Stable at the new version, old version was the last sent -> Send.
        assert_eq!(
            outbox_decision((20, t(2)), Some((20, t(2))), Some((10, t(1)))),
            OutboxDecision::Send
        );
    }

    #[test]
    fn prune_drops_vanished_files() {
        let mut map: HashMap<PathBuf, (u64, SystemTime)> = HashMap::new();
        map.insert(PathBuf::from("/o/keep.bin"), (1, t(1)));
        map.insert(PathBuf::from("/o/gone.bin"), (2, t(2)));
        let mut observed = std::collections::HashSet::new();
        observed.insert(PathBuf::from("/o/keep.bin"));
        prune_to_observed(&mut map, &observed);
        assert!(map.contains_key(&PathBuf::from("/o/keep.bin")));
        assert!(!map.contains_key(&PathBuf::from("/o/gone.bin")));
    }

    #[test]
    fn walk_skips_dotfiles_and_recurses() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.bin"), b"a").unwrap();
        std::fs::write(dir.path().join(".inflight"), b"x").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/b.bin"), b"bb").unwrap();

        let mut rels: Vec<String> = walk_files(dir.path())
            .into_iter()
            .map(|f| f.relative_path)
            .collect();
        rels.sort();
        assert_eq!(rels, vec!["a.bin".to_string(), "sub/b.bin".to_string()]);
    }
}
