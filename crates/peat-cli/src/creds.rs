//! Credential loading per peat-node ADR-001 + ADR-006.
//!
//! Resolution order: `--creds` argument → `PEAT_CREDS` env → platform config dir.
//! Phase 1 skeleton: real loader lands in Phase 2 alongside the join prelude.
