// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Tests for peat-node CLI argument validation and default values.
//!
//! Covers OPS-01, OPS-02 (/tmp guard D-06), OPS-03.

use std::path::Path;

use peat_node::cli_validation::reject_tmp_blob_dir;

#[test]
fn test_reject_tmp_blob_dir_accepts_non_tmp_paths() {
    assert!(reject_tmp_blob_dir(Path::new("/data/blobs")).is_ok());
    assert!(reject_tmp_blob_dir(Path::new("/var/lib/peat/blobs")).is_ok());
    assert!(reject_tmp_blob_dir(Path::new("/home/user/blobs")).is_ok());
}

#[test]
fn test_reject_tmp_blob_dir_rejects_tmp() {
    let err = reject_tmp_blob_dir(Path::new("/tmp/blobs")).unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("/tmp"), "error must cite /tmp; got: {msg}");
    assert!(
        msg.contains("memory-backed") || msg.contains("--blob-work-dir"),
        "error must explain the risk or the flag; got: {msg}"
    );
}

#[test]
fn test_reject_tmp_blob_dir_rejects_var_tmp() {
    let err = reject_tmp_blob_dir(Path::new("/var/tmp/foo")).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("/var/tmp") || msg.contains("/tmp"),
        "error must cite /var/tmp; got: {msg}"
    );
}

#[test]
fn test_reject_tmp_blob_dir_rejects_exact_tmp() {
    assert!(reject_tmp_blob_dir(Path::new("/tmp")).is_err());
    assert!(reject_tmp_blob_dir(Path::new("/var/tmp")).is_err());
}

#[test]
fn test_reject_tmp_blob_dir_accepts_tmp_like_names() {
    // Strict prefix — "/tmpdata" and "/opt/tmp/..." must NOT be rejected.
    assert!(reject_tmp_blob_dir(Path::new("/tmpdata/blobs")).is_ok());
    assert!(reject_tmp_blob_dir(Path::new("/opt/tmp-data/blobs")).is_ok());
}
