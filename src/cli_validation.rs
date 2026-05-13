// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! CLI argument validation helpers for peat-node.
//!
//! Extracted from `main.rs` so that rules like the /tmp guard (D-06) can be
//! unit-tested without spawning a process.

use std::path::Path;

/// Reject `--blob-work-dir` paths that start with `/tmp` or `/var/tmp`.
///
/// These directories are frequently tmpfs (memory-backed) inside Kubernetes
/// pods. Blobs written there would silently disappear on pod restart, making
/// BLOB-02 startup re-import impossible. Per D-06 we fail loudly at startup
/// rather than falling back silently.
///
/// Returns `Ok(())` for any other path.
pub fn reject_tmp_blob_dir(path: &Path) -> anyhow::Result<()> {
    // Normalize to absolute-ish string for prefix matching. We do NOT canonicalize
    // here because the path may not exist yet (it will be created by main()).
    let s = path.to_string_lossy();

    // Strict prefix match: the path must START with "/tmp" OR "/var/tmp" as the
    // first path component. Checking `starts_with("/tmp")` plus a boundary on the
    // next character ('/' or end-of-string) avoids matching e.g. "/tmpdata/...".
    let is_tmp = s == "/tmp"
        || s == "/var/tmp"
        || s.starts_with("/tmp/")
        || s.starts_with("/var/tmp/");

    if is_tmp {
        anyhow::bail!(
            "--blob-work-dir '{}' is under /tmp or /var/tmp which is often memory-backed in K8s pods; \
             blobs written there disappear on restart and BLOB-02 re-import cannot recover them. \
             Use a persistent path (e.g. under --data-dir).",
            path.display()
        );
    }

    Ok(())
}
