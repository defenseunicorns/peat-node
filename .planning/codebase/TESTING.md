# Testing Patterns

**Analysis Date:** 2026-07-08

## Test Framework

**Runner:**
- Rust built-in test framework (`#[test]`, `#[tokio::test]`)
- Config: None (standard `cargo test`)

**Assertion Library:**
- Standard `assert!`, `assert_eq!`, `assert_ne!` macros
- Pattern matching with `match` + `panic!` for enum variant assertions (e.g., `src/node.rs:1693`)

**Serialization for integration tests:**
- `serial_test` crate for tests that spin up real iroh endpoints — `#[serial_test::serial(iroh_two_node)]` group

**Run Commands:**
```bash
cargo test --workspace          # Run all tests (unit + integration)
cargo test -p peat-node         # Root crate only
cargo test --test grpc_test     # Single integration test file
```

## Test File Organization

**Location:**
- **Unit tests:** Co-located in each source file as `#[cfg(test)] mod tests { ... }` at the bottom
- **Integration tests:** Separate files in `tests/` directory, one per feature area

**Naming:**
- Integration tests: `{feature}_test.rs` (e.g., `grpc_test.rs`, `sync_test.rs`, `attachments_smoke_test.rs`)
- Unit test modules: `mod tests` (standard) or descriptive like `mod k8s_discovery_tests` (`src/node.rs:1665`)

**Structure:**
```
src/
├── node.rs              # contains #[cfg(test)] mod k8s_discovery_tests
├── crypto.rs            # contains #[cfg(test)] mod tests
├── query.rs             # contains #[cfg(test)] mod tests
├── fanout.rs            # contains #[cfg(test)] mod tests
├── identity.rs          # contains #[cfg(test)] mod tests
├── watcher.rs           # contains #[cfg(test)] mod tests
├── main.rs              # contains #[cfg(test)] mod tests
└── attachments/
    ├── validate.rs      # contains #[cfg(test)] mod tests
    ├── ingest.rs        # contains #[cfg(test)] mod tests
    ├── registry.rs      # contains #[cfg(test)] mod tests
    ├── runtime.rs       # contains #[cfg(test)] mod tests
    ├── config.rs        # contains #[cfg(test)] mod tests
    ├── outbox.rs        # contains #[cfg(test)] mod tests
    └── inbox.rs         # contains #[cfg(test)] mod tests
tests/
├── grpc_test.rs                    # Connect HTTP+JSON CRUD (570 lines)
├── sync_test.rs                    # Two-node CRDT sync (223 lines)
├── subscribe_test.rs               # Document Subscribe streaming
├── subscribe_query_test.rs         # Subscribe with query filters (500 lines)
├── node_test.rs                    # In-process SidecarNode tests (394 lines)
├── attachments_acceptance_test.rs  # PRD-006 acceptance (712 lines)
├── attachments_smoke_test.rs       # PRD-006 smoke (410 lines)
├── attachments_e2e_test.rs         # PRD-006 end-to-end (783 lines)
├── attachments_deferred_test.rs    # PRD-006 deferred delivery (839 lines)
├── attachments_subscribe_test.rs   # PRD-006 subscribe (339 lines)
├── attachments_multi_peer_test.rs  # PRD-006 multi-peer (276 lines)
├── cross_peer_encryption_test.rs   # Encrypted sync (154 lines)
├── formation_isolation_test.rs     # Formation isolation (178 lines)
├── partition_test.rs               # Network partition (199 lines)
├── typed_collections_test.rs       # Typed collection CRUD (190 lines)
├── collection_config_test.rs       # Collection config (219 lines)
├── sync_control_test.rs            # Start/stop sync (129 lines)
├── sync_subprocess_test.rs         # Subprocess sync (341 lines)
├── uds_test.rs                     # Unix domain socket (104 lines)
├── mdns_test.rs                    # mDNS discovery (146 lines)
└── auto_reconnect_test.rs          # Auto-reconnect (133 lines)
```

## Test Structure

**Unit test pattern:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    // additional test-only imports

    // Helper functions for test data
    fn cfg_with(...) -> Config { ... }
    fn one_root(name: &str) -> (TempDir, Config) { ... }

    #[test]
    fn descriptive_snake_case_name() {
        // Arrange
        let (dir, cfg) = one_root("data");
        // Act
        let result = validate_request(&req, &cfg);
        // Assert
        assert!(result.is_ok());
    }
}
```

**Integration test pattern (Connect RPC):**
```rust
// Shared boot_server() helper at top of file
async fn boot_server(encryption_key: Option<String>) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(SidecarNode::new(SidecarConfig { ... }).await.unwrap());
    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    tokio::spawn(async move { bound.serve(router).await.unwrap(); });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (client, format!("http://127.0.0.1:{port}"))
}

// Shared call() helper for Connect protocol
async fn call(client: &reqwest::Client, base: &str, method: &str, body: Value) -> Value {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    let resp = client.post(&url).header("content-type", "application/json")
        .json(&body).send().await.unwrap();
    assert!(resp.status().is_success(), "{method} returned {}", resp.status());
    resp.json().await.unwrap()
}

#[tokio::test]
async fn connect_protocol_full_crud_plaintext() {
    let (client, base) = boot_server(None).await;
    // Exercise RPCs via HTTP+JSON
}
```

**Patterns:**
- Each integration test file defines its own `boot_server()` and `call()` helpers (not shared across files)
- Tests use `tempfile::tempdir()` for isolated data directories — call `.keep()` to prevent cleanup during async test
- Async tests use `#[tokio::test]` (single-thread) or `#[tokio::test(flavor = "multi_thread")]` for iroh tests

## Mocking

**Framework:** No mocking framework. Tests use real implementations.

**Patterns:**
- **Real servers:** Integration tests boot actual `SidecarNode` + Connect RPC server on ephemeral ports
- **Real networking:** Two-node sync tests (`sync_test.rs`) use real iroh UDP endpoints on localhost
- **No mocks for peat-mesh:** Tests exercise the full stack through the RPC surface
- **Filesystem isolation:** `tempfile::TempDir` for all file-system-dependent tests

**What to Mock:**
- Nothing currently mocked — the codebase favors full-stack integration tests over mocked unit tests

**What NOT to Mock:**
- `SidecarNode` — always instantiate a real one
- Network transport — use real iroh endpoints
- Automerge store — use real in-memory or file-backed store

## Fixtures and Factories

**Test Data:**
```rust
// Config factory (src/attachments/validate.rs:332)
fn cfg_with(roots: HashMap<String, PathBuf>) -> AttachmentConfig {
    AttachmentConfig {
        roots,
        max_file_bytes: DEFAULT_MAX_FILE_BYTES,
        // ... all fields with defaults
    }
}

// Convenience one-liner (src/attachments/validate.rs:352)
fn one_root(name: &str) -> (TempDir, AttachmentConfig) { ... }

// File fixture (src/attachments/validate.rs:360)
fn write_file(root: &Path, rel: &str, bytes: &[u8]) -> [u8; 32] { ... }

// Proto message builder (src/attachments/validate.rs:379)
fn file_spec(root_name: &str, relative_path: &str, size_bytes: u64, sha256: Vec<u8>) -> pb::FileSpec { ... }
```

**Location:**
- Factory functions are local to each test module — not shared across files
- No centralized `fixtures/` or `testdata/` directory
- Constants for test keys: `const KEY_B64: &str = "..."` (`src/node.rs:1670`)

## Coverage

**Requirements:** No coverage threshold enforced. No coverage tooling configured in repo.

**View Coverage:**
```bash
cargo install cargo-tarpaulin    # if not installed
cargo tarpaulin --workspace      # not configured but available
```

## Test Types

**Unit Tests:**
- Co-located `#[cfg(test)]` modules in 14 source files
- Focus on pure logic: crypto round-trip, query parsing, config validation, K8s discovery decisions
- Synchronous `#[test]` where possible; `#[tokio::test]` only when async is needed

**Integration Tests:**
- 21 files in `tests/` directory (~7,000 lines total)
- Boot real Connect RPC servers and exercise via HTTP+JSON
- Cover: CRUD, sync, subscriptions, attachments, encryption, partitions, formation isolation, mDNS, UDS, auto-reconnect
- Multi-node tests spin up 2+ `SidecarNode` instances in-process

**E2E / Functional Tests:**
- `test/cross-cluster-sync.sh` — shell-based cross-cluster validation (run manually or in CI for sync-path changes)
- Required by SKILL.md when sync-path code or chart changes

## Common Patterns

**Async Testing:**
```rust
#[tokio::test]
async fn test_name() {
    let (client, base) = boot_server(None).await;
    let result = call(&client, &base, "GetStatus", serde_json::json!({})).await;
    assert!(result["nodeId"].as_str().unwrap().starts_with("test-"));
}
```

**Serial Test Execution (iroh two-node tests):**
```rust
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(iroh_two_node)]
async fn end_to_end_attachment_delivery_two_nodes() {
    // Prevents CPU contention from parallel iroh endpoint startup
}
```

**Error Testing:**
```rust
#[test]
fn reject_path_traversal() {
    let result = validate_request(&req, &cfg);
    match result {
        Err(e) => assert!(e.message().contains("traversal")),
        Ok(_) => panic!("expected rejection"),
    }
}
```

**Polling for Async Convergence (sync tests):**
```rust
for _ in 0..60 {
    let result = call(&client, &base, "GetDocument", json!({...})).await;
    if result.get("data").is_some() {
        break;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
}
```

## CI Gates

Per SKILL.md, a task is not done until all pass:
1. `cargo fmt --check` — formatting
2. `cargo clippy --workspace --all-targets -- -D warnings` — linting
3. `cargo test --workspace` — all unit + integration tests
4. `./test/cross-cluster-sync.sh` — for sync-path changes only

---

*Testing analysis: 2026-07-08*
