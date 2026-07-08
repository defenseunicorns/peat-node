# Codebase Structure

**Analysis Date:** 2026-07-08

## Directory Layout

```
peat-node/
├── proto/                  # Protobuf service definition
│   └── sidecar.proto       # Wire contract (Connect RPC)
├── src/                    # Main binary + library source
│   ├── main.rs             # CLI entry point (clap), server bootstrap
│   ├── lib.rs              # Crate root: module declarations + proto re-export
│   ├── node.rs             # SidecarNode: mesh lifecycle, peer mgmt, doc CRUD
│   ├── service.rs          # Connect RPC trait implementation
│   ├── crypto.rs           # AES-256-GCM encryption at rest
│   ├── fanout.rs           # QoS-priority relay fanout queue
│   ├── identity.rs         # Deterministic iroh keypair derivation (HKDF)
│   ├── query.rs            # Subscription query matcher
│   ├── watcher.rs          # Agent watcher (polls UDS Remote Agent)
│   └── attachments/        # PRD-006 file distribution subsystem
│       ├── mod.rs           # Submodule declarations
│       ├── config.rs        # AttachmentConfig (CLI/env -> validated config)
│       ├── validate.rs      # Request validation (12 rules)
│       ├── ingest.rs        # Hash + blob-store ingest
│       ├── registry.rs      # Bundle handle table (TTL + LRU)
│       ├── runtime.rs       # Per-bundle runtime state (progress channels)
│       ├── handlers.rs      # RPC handler implementations
│       ├── inbox.rs         # Receive-side watcher (blob pull + file write)
│       └── outbox.rs        # Send-side watcher (auto-distribute new files)
├── crates/                 # Workspace member crates
│   └── peat-cli/           # CLI client for interacting with peat-node
│       ├── src/
│       │   ├── main.rs      # CLI entry point
│       │   ├── lib.rs       # Crate root
│       │   ├── creds.rs     # Credential management
│       │   ├── join.rs      # Mesh session join logic
│       │   └── cli/         # Subcommand implementations
│       │       ├── mod.rs
│       │       ├── create.rs
│       │       ├── update.rs
│       │       ├── delete.rs
│       │       ├── query.rs
│       │       ├── observe.rs
│       │       ├── attach.rs
│       │       ├── schema.rs
│       │       ├── writes.rs
│       │       └── output.rs
│       └── tests/           # CLI tests
│           ├── cli_parser.rs
│           └── e2e/         # End-to-end scenario tests
├── tests/                  # Integration tests (cargo test)
├── build.rs                # Proto compilation + dep version extraction
├── chart/                  # Helm chart
│   └── peat-node/
│       └── templates/
├── bundle/                 # Zarf bundle assets
├── examples/               # Docker Compose examples
│   └── compose/
│       └── attachments/    # Attachment demo directories
├── docs/                   # Documentation
├── test/                   # Test support files
├── Cargo.toml              # Workspace + main crate manifest
├── Cargo.lock              # Locked dependency versions
├── Dockerfile              # Container image build
├── Cross.toml              # Cross-compilation config
├── zarf.yaml               # Zarf package manifest
├── SKILL.md                # Repo-specific workflow + verification checklist
└── CLAUDE.md               # AI assistant instructions
```

## Directory Purposes

**`src/`:**
- Purpose: Main binary and library code for the peat-node sidecar
- Contains: Rust source files organized as flat modules + one submodule (`attachments/`)
- Key files: `main.rs` (entry), `node.rs` (core logic), `service.rs` (RPC impl)

**`src/attachments/`:**
- Purpose: PRD-006 file distribution subsystem
- Contains: Config, validation, blob ingest, bundle registry, inbox/outbox watchers, RPC handlers
- Key files: `handlers.rs` (RPC dispatch), `validate.rs` (12 validation rules), `inbox.rs` (receive-side)

**`crates/peat-cli/`:**
- Purpose: Standalone CLI client that connects to a running peat-node over Connect RPC
- Contains: Subcommand implementations for create/update/delete/query/observe/attach/schema
- Key files: `src/main.rs`, `src/cli/mod.rs`

**`proto/`:**
- Purpose: Wire contract definition
- Contains: Single `sidecar.proto` file defining the `PeatSidecar` service
- Key files: `sidecar.proto` -- compiled by `build.rs` via `connectrpc_build`

**`tests/`:**
- Purpose: Integration tests that spin up real `SidecarNode` instances with iroh endpoints
- Contains: Multi-peer sync tests, attachment tests, gRPC tests, partition tests, reconnect tests
- Key files: 20 test files covering sync, attachments, encryption, discovery, subscriptions

**`chart/peat-node/`:**
- Purpose: Helm chart for Kubernetes deployment
- Contains: Chart templates, values

**`bundle/`:**
- Purpose: Zarf package bundle assets for airgapped deployment

**`examples/compose/`:**
- Purpose: Docker Compose examples for local multi-node development
- Contains: Attachment demo with inbox/outbox directories

## Key File Locations

**Entry Points:**
- `src/main.rs`: Binary entry point -- CLI parsing, server bootstrap, listener loop
- `crates/peat-cli/src/main.rs`: CLI client entry point
- `build.rs`: Build-time proto compilation + version extraction

**Configuration:**
- `Cargo.toml`: Workspace manifest, dependencies with version commentary
- `proto/sidecar.proto`: Wire contract (the source of truth for the RPC surface)
- `chart/peat-node/`: Helm values and templates
- `zarf.yaml`: Zarf package manifest
- `Cross.toml`: Cross-compilation settings

**Core Logic:**
- `src/node.rs`: `SidecarNode` -- the central struct (1760 lines), owns all mesh state
- `src/service.rs`: RPC trait implementation dispatching to `SidecarNode`
- `src/fanout.rs`: QoS-priority change propagation
- `src/attachments/handlers.rs`: Attachment RPC handler implementations

**Testing:**
- `tests/`: 20 integration test files at the crate root
- `crates/peat-cli/tests/`: CLI parser tests + e2e scenarios

## Naming Conventions

**Files:**
- Snake_case: `auto_reconnect_test.rs`, `collection_config_test.rs`
- Test files suffixed with `_test.rs`: `tests/sync_test.rs`, `tests/grpc_test.rs`
- Module files: `mod.rs` for directory modules

**Modules:**
- Flat layout in `src/`: one file per module (`node.rs`, `service.rs`, `crypto.rs`)
- Single subdirectory module: `attachments/` with `mod.rs`

**Types:**
- PascalCase structs: `SidecarNode`, `SidecarConfig`, `PeatSidecarService`, `StoreCipher`
- PascalCase enums: `ChangeType`, `FanoutKind`, `StoredDeletionPolicy`
- Constants: SCREAMING_SNAKE_CASE (`RECONNECT_WATCHDOG_INTERVAL`, `CONNECT_RETRY_ATTEMPTS`)

**Proto:**
- Package: `peat.sidecar.v1`
- RPC methods: PascalCase (`PutDocument`, `GetStatus`, `SendAttachments`)
- Generated Rust: re-exported as `pub use proto::peat::sidecar::v1 as pb` in `src/lib.rs`

## Where to Add New Code

**New RPC endpoint:**
1. Add the RPC definition to `proto/sidecar.proto`
2. Implement the business logic as a method on `SidecarNode` in `src/node.rs`
3. Implement the service trait method in `src/service.rs`
4. Add integration test in `tests/`

**New standalone module (e.g., a new cross-cutting concern):**
1. Create `src/{module_name}.rs`
2. Add `pub mod {module_name};` to `src/lib.rs`
3. Import and use from `src/node.rs` or `src/service.rs`

**New attachment feature:**
1. Add implementation in `src/attachments/{feature}.rs`
2. Add `pub mod {feature};` to `src/attachments/mod.rs`
3. Wire into handlers in `src/attachments/handlers.rs`

**New peat-cli subcommand:**
1. Create `crates/peat-cli/src/cli/{command}.rs`
2. Register in `crates/peat-cli/src/cli/mod.rs`
3. Add e2e test in `crates/peat-cli/tests/e2e/scenarios.rs`

**New integration test:**
1. Create `tests/{feature}_test.rs`
2. Use `#[tokio::test(flavor = "multi_thread")]`
3. For iroh two-node tests, add `#[serial_test::serial(iroh_two_node)]`

## Special Directories

**`target/`:**
- Purpose: Cargo build output (includes generated proto code in `OUT_DIR`)
- Generated: Yes
- Committed: No (gitignored)

**`proto/`:**
- Purpose: Source-of-truth protobuf definitions
- Generated: No (hand-maintained)
- Committed: Yes

**`chart/`:**
- Purpose: Helm chart for Kubernetes deployment
- Generated: No
- Committed: Yes

**`bundle/`:**
- Purpose: Zarf bundle assets for airgapped deployment
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-07-08*
