# Coding Conventions

**Analysis Date:** 2026-07-08

## Naming Patterns

**Files:**
- `snake_case.rs` for all Rust source files
- Integration test files: `{feature}_test.rs` under `tests/`
- Module directories use `mod.rs` barrel pattern (e.g., `src/attachments/mod.rs`)

**Functions:**
- `snake_case` for all functions and methods
- Constructor pattern: `fn new(config: Config) -> Self` or `fn from_*(...)` (e.g., `StoreCipher::from_base64_key` in `src/crypto.rs`)
- Builder/factory helpers in tests: short names like `fn cfg_with(...)`, `fn one_root(...)`, `fn peer(...)` (see `src/attachments/validate.rs:332`, `src/node.rs:1676`)

**Variables:**
- `snake_case` throughout
- Constants: `UPPER_SNAKE_CASE` (e.g., `PREFIX` in `src/crypto.rs:20`, `DEFAULT_MAX_FILE_BYTES` in `src/attachments/config.rs`)

**Types:**
- `PascalCase` for structs, enums, traits
- Enum variants: `PascalCase` (e.g., `FanoutKind::AllPeers`, `FanoutKind::ExcludeSource` in `src/fanout.rs:36`)
- Proto-generated types accessed via `pb::` alias (re-exported in `src/lib.rs:16` as `pub use proto::peat::sidecar::v1 as pb`)

**Modules:**
- Public modules declared in `src/lib.rs` — flat list of `pub mod` statements
- Sub-modules within `src/attachments/` use the barrel `mod.rs` with `pub mod` re-exports

## Code Style

**Formatting:**
- `rustfmt` (standard) — no `.rustfmt.toml` override detected; uses Rust 2021 edition defaults
- Enforced via pre-commit hook at `.githooks/pre-commit` and CI gate: `cargo fmt --check`

**Linting:**
- `clippy` with `-D warnings` (all warnings are errors)
- Selective `#[allow(clippy::...)]` with inline justification comments (e.g., `#![allow(clippy::result_large_err)]` in `src/attachments/validate.rs:29` with a rationale comment explaining why `ConnectError` is large)
- CI gate: `cargo clippy --workspace --all-targets -- -D warnings`

## Import Organization

**Order:**
1. `std::*` imports
2. External crate imports (alphabetical by crate name)
3. Internal crate imports via `crate::` paths

**Path Aliases:**
- `use crate::pb` — proto-generated types (defined in `src/lib.rs:16`)
- No `@`-style path aliases; Rust's module system with `crate::` and `super::` only

**Style:**
- Grouped `use` blocks separated by blank lines between std/external/internal groups
- Nested imports with braces: `use peat_mesh::storage::{AutomergeStore, ChangeOrigin, DocChange, SyncTransport, TtlConfig};` (`src/node.rs:26`)

## Error Handling

**Two-tier strategy:**

1. **`anyhow::Result` for fallible operations** — used throughout `src/node.rs`, `src/watcher.rs`, `src/crypto.rs`, `src/identity.rs` for internal logic where structured error variants are not needed. Chain context with `.context("description")` (`src/crypto.rs:33`).

2. **`thiserror::Error` for domain errors** — used when callers need to match variants. Example: `QueryError` enum in `src/query.rs:59-67` with `#[error("...")]` messages.

3. **RPC boundary conversion** — `fn internal(e: anyhow::Error) -> ConnectError` in `src/service.rs:33` logs the full error chain via `tracing::error!` then returns `ConnectError::internal(e.to_string())`. All service methods use `.map_err(internal)?` to convert.

**Pattern:** Internal code uses `anyhow`, RPC boundary converts to `ConnectError`. Never expose internal error details beyond the string message.

## Logging

**Framework:** `tracing` with `tracing-subscriber` (env-filter feature)

**Patterns:**
- Use `tracing::{debug, info, warn, error}` — imported individually per module
- `error!` at RPC boundary for failed operations (in `fn internal()` at `src/service.rs:34`)
- `info!` for lifecycle events (node start, peer connect)
- `warn!` for degraded but recoverable states
- `debug!` for operational detail

## Comments

**When to Comment:**
- Module-level `//!` doc comments on every file explaining purpose and design rationale (e.g., `src/fanout.rs:1-22`, `src/attachments/validate.rs:1-23`)
- Inline comments for non-obvious design decisions, especially `clippy::allow` justifications
- Dependency comments in `Cargo.toml` are extensive — multi-paragraph changelogs for each version pin explaining *why* the version is pinned

**Doc Comments:**
- `///` on public structs, enums, and their fields
- Fields on config structs document env-var names and defaults (e.g., `src/node.rs:46-58`)

## Function Design

**Size:** Most functions are short (< 50 lines). Large files like `src/node.rs` (1759 lines) concentrate complexity in a few lifecycle methods; most are small helpers.

**Parameters:** Use config structs for multi-field initialization (`SidecarConfig`, `AttachmentConfig`, `AutomergeBackendConfig`). Avoid long parameter lists.

**Return Values:** `Result<T, E>` for fallible operations. `anyhow::Result<T>` internally, `Result<(Response, Context), ConnectError>` at the RPC boundary.

**Async:** All network/IO functions are `async`. Runtime is Tokio multi-thread. Tests use `#[tokio::test]` or `#[tokio::test(flavor = "multi_thread")]`.

## Module Design

**Exports:** `src/lib.rs` exports all public modules flat. The `pb` type alias provides convenient access to generated proto types.

**Barrel Files:** `src/attachments/mod.rs` re-exports sub-modules with `pub mod`. Each sub-module is self-contained with its own `#[cfg(test)] mod tests`.

**Visibility:** Default to `pub(crate)` for internal-only items. Use `pub` only for items consumed by integration tests or external crates.

## Config Pattern

**CLI args use `clap` derive** in `src/main.rs:25` with `#[arg(long, env = "PEAT_NODE_*")]` for every flag, providing both CLI and environment variable configuration. Config structs (`SidecarConfig`, `AttachmentConfig`) are plain `#[derive(Debug, Clone)]` structs — no builder pattern.

**Default implementations** via `Default` derive or manual `impl Default` with constants for caps/limits (see `src/attachments/config.rs` constants like `DEFAULT_MAX_FILE_BYTES`).

---

*Convention analysis: 2026-07-08*
