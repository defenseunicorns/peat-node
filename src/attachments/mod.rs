//! Attachment distribution surface (PRD-006).
//!
//! Exposes the four attachment RPCs from `proto/sidecar.proto`:
//! `SendAttachments`, `GetAttachmentDistribution`, `SubscribeAttachmentBundle`,
//! `CancelAttachmentDistribution`.
//!
//! Layout (filled in across PRD-006 implementation steps):
//! - [`config`] — operator-facing configuration. Built from CLI/env in
//!   `main.rs`, plumbed into [`crate::node::SidecarConfig`]. Owns the
//!   allowlisted root map (`name → canonicalised PathBuf`) and all caps.
//! - `validate` (Step 3) — validates `SendAttachmentsRequest` against the
//!   12 rules in PRD §Validation Rules.
//! - `ingest` (Step 4) — single-pass hash + blob-store ingest with
//!   content-address rollback safety.
//! - `registry` (Step 5) — bundle handle table with retention + LRU eviction.
//!
//! Safety default: if [`config::AttachmentConfig::has_roots`] is false (no
//! `--attachment-root` configured), all four RPCs return `Unimplemented`.
//! The service-layer stubs in `crate::service` enforce this until the real
//! handlers land in Step 7.

pub mod config;
pub mod handlers;
pub mod ingest;
pub mod registry;
pub mod validate;
