# peat-node-ADR-001: `peat` — Operator CLI as a Peat Node

**Status:** Proposed
**Date:** 2026-05-27
**Authors:** Kit Plummer, Claude
**Relates To:**
- ADR-005: Data Sync Abstraction
- ADR-006: Security, Authentication, and Authorization
- ADR-007: Automerge-Based Sync Engine
- ADR-019: Quality of Service and Data Prioritization (sync modes)
- ADR-021: Document-Oriented Architecture
- ADR-032: Pluggable Transport Abstraction
- ADR-034: Record Deletion and Tombstone Management
- ADR-035: Peat-Lite Embedded Nodes

---

## Executive Summary

This ADR proposes adding `peat`, an operator-facing command-line utility, as a new crate (`peat-cli`) inside the `peat-node` repository. `peat` is itself a Peat node — it joins the mesh using the same protocol, schema, transports, security, and sync engine as any other participant — but in a CLI form factor optimized for interactive operator use and scripted automation.

The CLI exposes commands for both inspecting and modifying mesh state:

**Read commands:**
- **`query`** — connect, fetch, exit. Joins the mesh, retrieves the current state of the target, prints, closes. One-shot. Like `psql` returning the rows for a `SELECT`: results are the materialized state, not the operation log that produced it.
- **`observe`** — connect, subscribe, stream. Joins the mesh and emits live updates until interrupted. The `full-history` mode is the surface for change-stream / CDC-style consumption.

**Write commands:**
- **`create`** — create a new document in a collection.
- **`update`** — apply a delta to an existing document (full-document replacement or surgical field updates).
- **`delete`** — tombstone a document per ADR-034.

All commands share a single join / auth / sync prelude and credentials path. Read commands differ in subscription posture; write commands differ in operation type. Read sync modes map directly to those defined in ADR-019.

The crate ships as an independent binary, built and tested as part of the `peat-node` workspace, and is included in the `peat-node` container image so that `kubectl exec` (or equivalent) gives operators an in-cluster debug surface with no additional sidecar.

---

## Context

### The problem

Operators, integrators, and developers working against a Peat deployment currently lack a low-friction way to inspect mesh state. Today, the practical options are:

1. Stand up a custom client application against the peat crates for one-off questions.
2. Add ad-hoc logging or REST endpoints to `peat-node` and parse the output.
3. Read the on-disk Automerge document store directly — fragile, storage-coupled, and stale.

None of these scale. Operators need a `kubectl`-shaped tool: predictable verbs, scriptable output, sensible defaults, and the same security posture as the system being observed.

### Why a node, not a control client

An earlier framing considered building `peat` as a control client talking to a long-running `peat-node` over a local socket or gRPC interface. That option was rejected because:

- It requires growing and versioning a parallel admin API alongside the protocol.
- It cannot operate without a co-located `peat-node` daemon.
- It diverges from Peat's design ethos: the protocol is the interface.

By making `peat` a node, the CLI dogfoods the protocol, requires no additional API surface, and works in any context where a node can join — including from an operator workstation adjacent to a formation, not just from inside a node's container.

### Why this lives in peat-node

This is a **packaging decision**, not a charter change. `peat-node`'s charter — be the in-cluster sidecar that exposes `peat-protocol` over Connect/gRPC — is unchanged. `peat-cli` lives here for two concrete distribution reasons:

1. **Container inclusion.** Shipping `peat` inside the existing `peat-node` container image gives operators a `kubectl exec`-reachable debug surface with no additional sidecar, no extra image to pull, and no separate supply-chain story.
2. **Release artifacts.** `peat-node`'s release pipeline already produces cross-arch / cross-OS binaries via the existing GitHub release workflow. Adding `peat` to the same workflow yields Linux (x86_64, aarch64), macOS (aarch64, x86_64), and Windows (x86_64) binaries from one tagged release, without standing up a parallel release pipeline in a separate repo.

The CLI also happens to consume the same upstream crates (`peat-protocol`, `peat-mesh`, plus `peat-schema` for rendering) that `peat-node` already depends on, so colocation avoids cross-repo version pinning between two consumers of the same protocol surface. That's a convenience, not the rationale.

**Scope discipline.** This colocation is not a precedent that operator tooling generically belongs in `peat-node`. Future operator surfaces (UIs, scripted runners, non-Rust admin clients) get evaluated on their own merits and against ADR-043; the answer may be `peat-gateway`, a dedicated repo, or here. `peat-cli` lands here because it is a Rust binary consuming the same upstream crates and shipping in the same container and release stream — not because `peat-node` is the home for operator tooling.

---

## Decision Drivers

### Requirements

1. **Real Peat participant.** `peat` joins the mesh using the same protocol stack as any other node. No bespoke admin path.
2. **Read and write commands from one tool.** `query` and `observe` for reads; `create`, `update`, and `delete` for writes. All share the prelude, credentials, and identity model.
3. **Lifecycle correctness on writes.** `create` and `update` are distinct operations, per ADR-021: documents are created once and evolve through deltas; recreation is an error. `delete` issues a tombstone per ADR-034.
4. **Schema-validated writes for known types.** When a target collection or document corresponds to a known `peat-schema` type, write input is validated against the schema before submission. Application-defined types are accepted structurally without validation.
5. **Handle both peat-schema documents and arbitrary CRDT documents.** The CLI must render docs whose types are defined in `peat-schema` with type-aware formatting, and must also render documents from applications that define their own types — falling back to a generic CRDT structure renderer rather than failing or producing useless output.
6. **Pipe-safe.** Output must be cleanly consumable by downstream files and commands: data goes to stdout, all other text to stderr, formatting respects TTY detection, streaming output is line-buffered, and SIGPIPE does not produce errors.
7. **Authenticated and authorized.** All actions gated by application credentials per ADR-006. Write operations require explicit write scopes; reads and writes do not share authorization.
8. **Scriptable.** Default human-readable output; opt-in machine-readable formats (`json`, `ndjson`).
9. **Cross-platform operator targets.** Build for Linux (x86_64, aarch64), macOS (aarch64, x86_64), Windows (x86_64).
10. **Container-resident.** Binary included in the `peat-node` container image for in-cluster debug.
11. **Test coverage required from first commit.** No "we'll add tests later" path.
12. **Functional end-to-end sync coverage.** Tests must exercise real CRDT sync between live nodes, not just unit-level isolation.

### Constraints

1. **No new admin API.** Anything `peat` does, an embedded application could also do via the upstream crates.
2. **Presence discipline.** A short-lived CLI node must not pollute mesh topology, capability advertisement, or QoS routing. See "Ephemeral node posture" below.
3. **Binary size budget.** `peat` must not balloon the `peat-node` container image. Dependency scope is kept narrow; FFI surfaces (Android JNI, iOS UniFFI, BlueZ) used by other peat-node targets are not pulled into the CLI crate.
4. **Single canonical CLI parser.** `clap` (derive macros) for all argument handling. Manual parsing or alternative crates are out of scope.

---

## Decision

Add a `peat-cli` crate within the `peat-node` repository. The crate produces an independent binary named `peat`, joins the mesh as a real Peat node, and exposes operator commands via `clap`. The binary is included in the `peat-node` container image.

---

## Detailed Design

### Crate layout

```
peat-node/
├── crates/
│   └── peat-cli/
│       ├── Cargo.toml              # [[bin]] name = "peat"
│       ├── src/
│       │   ├── main.rs             # entrypoint, clap parser
│       │   ├── cli/
│       │   │   ├── mod.rs
│       │   │   ├── query.rs        # `query` subcommand
│       │   │   ├── observe.rs      # `observe` subcommand
│       │   │   └── output.rs       # text / json / ndjson formatters
│       │   ├── join.rs             # shared join / auth / sync prelude
│       │   ├── creds.rs            # credential loading
│       │   └── lib.rs              # exposed for integration tests
│       └── tests/
│           ├── unit/               # unit tests
│           └── e2e/                # multi-node functional tests
```

`peat-cli` depends on the upstream peat crates:

- **`peat-protocol`** — protocol participation: identity, presence, subscription semantics, sync mode selection.
- **`peat-schema`** — document and collection definitions needed to deserialize, render, and validate data the CLI reports on.
- **`peat-mesh`** — discovery, transport, and session establishment with peers.

These are the same upstream dependencies `peat-node` itself consumes. `peat-cli` does *not* pull in FFI crates intended for embedded or mobile targets (Android JNI, iOS UniFFI, BlueZ).

### Command surface (initial)

```
peat <COMMAND> [OPTIONS]

Read commands:
  query    Fetch current state of a target and exit
  observe  Subscribe and stream updates until interrupted

Write commands:
  create   Create a new document in a collection
  update   Apply a delta to an existing document
  delete   Tombstone a document

Other:
  help     Print this message or the help of the given subcommand

Common options:
  --creds <PATH>            Path to credentials file (or PEAT_CREDS env)
  --as <ID>                 Identity this CLI joins as (default: ephemeral)
  --target <ID>             Optional target peer to bias view toward
  --transport <NAME>        Transport hint (quic, btle, …); default auto
  --timeout <DURATION>      Join / sync timeout (default 10s)
  --output <FMT>            text | json | ndjson (default: text)
  -v, --verbose...          Increase log verbosity
```

**Read commands**

```
peat query <COLLECTION>[/<DOC_ID>] [OPTIONS]
  --limit <N>               Cap the number of records emitted
```

`query` returns the materialized current state of the target — a collection of documents if `<COLLECTION>` alone is given, a single document if `<COLLECTION>/<DOC_ID>` is given. There is no operation-log mode and no pagination cursor; if more selectivity is needed, narrow the target or use `--limit`. This mirrors how `psql` handles a `SELECT`: the result is the rows, not the WAL that produced them.

For change-stream / CDC-style consumption, see `observe --mode full-history` below.

```
peat observe <COLLECTION>[/<DOC_ID>] [OPTIONS]
  --mode <SYNC_MODE>        latest-only | windowed | full-history
                            (default: latest-only)
```

`observe --since <TIMESTAMP>` is deferred; it requires sync mode replay semantics still being finalized in ADR-019. Captured as future work.

**Write commands**

```
peat create <COLLECTION> [OPTIONS]
  --id <DOC_ID>             Explicit document id (default: generated)
  --from <PATH | ->         Read document content from file or stdin (- for stdin)
  --set <PATH=VALUE>...     Build document from path=value pairs (repeatable)
  --dry-run                 Validate and prepare the operation; do not submit
  --wait-for-sync           Block until at least one peer has acknowledged
  --no-validate             Skip schema validation (emits warning to stderr)
```

`--from` and `--set` are mutually exclusive. At least one is required.

```
peat update <COLLECTION>/<DOC_ID> [OPTIONS]
  --from <PATH | ->         Read full document content (delta computed from current)
  --set <PATH=VALUE>...     Surgical field updates (repeatable)
  --dry-run                 Validate and prepare the operation; do not submit
  --wait-for-sync           Block until at least one peer has acknowledged
  --no-validate             Skip schema validation (emits warning to stderr)
```

`--from` and `--set` are mutually exclusive. At least one is required.

```
peat delete <COLLECTION>/<DOC_ID> [OPTIONS]
  --wait-for-sync           Block until at least one peer has acknowledged
```

`delete` does not prompt; the explicit `<COLLECTION>/<DOC_ID>` requirement is the safeguard. Tombstone semantics per ADR-034.

### Lifecycle

All commands share a single prelude:

1. Load credentials (file path, env var, or platform keystore depending on build).
2. Construct a node identity (ephemeral by default; explicit `--as <id>` overrides).
3. Initialize the local sync backend (per ADR-005).
4. Start peer discovery (per ADR-032 transport configuration).
5. Wait for at least one peer connection or timeout.
6. Open a subscription on `<collection>` (and optionally `<doc_id>`) with the sync mode appropriate for the command and flags. Writes use a `LatestOnly` subscription on the target collection so that `update` and `delete` operate against current state.

After the prelude:

- **`query`** drains the subscription, renders the materialized state to stdout, and exits.
- **`observe`** streams each subsequent update as a record on stdout until SIGINT or pipe close.
- **`create`** validates input, constructs the document, submits a creation operation, and exits. With `--wait-for-sync`, blocks until at least one peer acknowledges before exiting.
- **`update`** validates input. If the target document exists, computes the delta against current state and submits it. If the target document does not exist, submits a creation operation with the provided content. With `--wait-for-sync`, blocks until at least one peer acknowledges.
- **`delete`** submits a tombstone for the target document and exits. With `--wait-for-sync`, blocks until at least one peer acknowledges.

All commands exit with the codes documented under "Shell integration discipline."

### Sync mode mapping (per ADR-019)

| Command                       | Sync mode used     | Purpose                                  |
|-------------------------------|--------------------|------------------------------------------|
| `query`                       | `LatestOnly`       | Materialized current-state snapshot      |
| `observe --mode latest-only`  | `LatestOnly`       | Stream current-state updates only        |
| `observe --mode windowed`     | `WindowedHistory`  | Tail recent history then live updates    |
| `observe --mode full-history` | `FullHistory`      | Every delta; forensics, debugging, CDC   |
| `create` / `update` / `delete`| `LatestOnly`       | Read current state for delta computation |

### Write semantics

Write commands are not RPCs against a server; they are local CRDT operations whose effect propagates through normal Peat sync. The CLI's job is to construct the right operation and submit it; consistency, conflict resolution, and propagation are the protocol's job.

**Authorship.** Every write is authored by the CLI's joined identity (`--as <id>`, or the credential-derived identity if unspecified). The author identity is recorded in the operation's metadata and visible in operation logs. There is no "anonymous write" path.

**Authorization.** Read scopes and write scopes are distinct per ADR-006. Credentials lacking write scope on the target collection cause `create` / `update` / `delete` to fail fast with exit 3, before any document content is parsed.

**Idempotency on `create`.** Creating a document with an `--id` that already exists fails with a clear error (exit 4). `create` is the strict-create path; if you want upsert semantics, use `update`.

**`update` semantics.** `update` is upsert-shaped: if the target document does not exist, `update` creates it; if it exists, `update` applies a delta against current state. This preserves ADR-021's "create once, evolve through deltas" invariant — the initial `update` against a missing document is *initial creation*, not recreation. Subsequent updates apply deltas. No document is ever recreated.

Together, `create` and `update` cover the operational space without needing a separate `apply` command:

- Use `create` when you want a fail-fast guarantee that a doc is genuinely new (setup scripts, first-time provisioning).
- Use `update` when you want create-or-modify semantics (general operator use, round-trip edits).

**Delta computation on `update`.** When invoked with `--from`, the CLI reads the full proposed document content, fetches current state via the `LatestOnly` subscription, and computes a minimal CRDT delta between them. When invoked with `--set <path>=<value>`, the CLI constructs the delta directly from the path expressions. Either way, what propagates is a delta, not a re-creation — preserving ADR-021's "create once, evolve through deltas" invariant.

**Validation.** When the target collection / document type is known to `peat-schema`, input is validated before submission. Validation failure returns exit 4 with a stderr explanation. `--no-validate` skips this check and emits a warning to stderr; intended for incident response and never for routine use.

**`--dry-run`.** Performs validation, delta computation, and authorization checks, then prints the operation that *would* be submitted (in the selected output format) and exits 0 without submitting. Useful for scripting and review.

**`--wait-for-sync`.** Without this flag, write commands exit when the local sync engine has accepted the operation. With it, the CLI subscribes for the operation's acknowledgement from at least one peer and blocks until acknowledgement or `--timeout`. Acknowledgement does not imply global consistency, only first-peer observation. The trade-off:

- Without `--wait-for-sync`: fast, suitable for high-frequency scripting and fire-and-forget contexts.
- With `--wait-for-sync`: slower, suitable for "I need to know it landed" workflows like setup scripts and audit trails.

**Output of write commands.**

- On success, stdout receives the operation result:
  - `create`: the document id (one line) or full operation record (`--output json`).
  - `update`: the delta / change id (one line) or full operation record (`--output json`).
  - `delete`: the tombstone id (one line) or full operation record (`--output json`).
- On failure, nothing is written to stdout; the error explanation goes to stderr; exit code reflects the failure class.

**Round-trip editing.** The `--from -` input mode is designed to compose with `query --output json`:

```
peat query contacts/c-1234 --output json \
  | jq '.position.lat = 40.7128' \
  | peat update contacts/c-1234 --from -
```

This pattern requires that `query` output is a valid input to `update` for the same document type. The schemas of `query --output json` and `update --from` inputs are defined to be compatible.

### Node posture per command

`peat` is a real `peat-protocol` participant, but its posture varies by command rather than being fixed at session start. The CLI is fundamentally CRUD-shaped, and each command has a different relationship with mesh state:

- **`query` and `observe`** — passive observer. No application capabilities published. Short TTL on presence records (default 60s, refreshed while running). Marked with a role hint other peers can filter from routing and QoS planning.
- **`create`, `update`, `delete`** — active participant for the duration of the operation. The CLI authors operations stamped with its identity. Presence is still short-TTL and observer-flagged; the distinction is operation submission, not capability advertisement.

In both cases, the node is short-lived and identity-stamped; the mesh does not plan around it as a durable participant.

### Output formats

- **`text`** (default): human-readable. Columnar for collections; pretty-printed for single docs. Layout adapts to whether the document type is known (see "Document rendering" below).
- **`json`**: single canonical JSON value for `query`; ignored for `observe`.
- **`ndjson`**: one JSON record per line. Natural for `observe | jq` and log shipping pipelines.

### Document rendering

The CLI must operate against documents whose types are defined in `peat-schema` *and* against documents whose types are defined by applications and unknown to `peat-schema`. Peat documents are CRDT documents and therefore schemaless at the protocol level; `peat-schema` is a convention layer providing type definitions and metadata for common document shapes.

Rendering follows a dispatch model:

1. **Type identification via metadata.** `peat-schema` documents carry metadata that identifies their type (the type marker / kind field that schema-defined docs include by convention). The CLI reads this metadata to dispatch the renderer.
2. **Typed renderer.** If the type metadata resolves to a renderer the CLI knows about, a type-specific renderer formats the document — field names, units, coordinate formatting, timestamps in operator-readable form, BlobRef metadata expanded with size and content hint.
3. **Renderer-not-found warning.** If the document carries type metadata but the CLI has no renderer for that type (a CLI older than the schema set being used), the CLI emits a warning to stderr identifying the unrecognized type and falls back to the generic renderer for the actual output.
4. **Generic renderer.** If no type metadata is present (an application-defined document), or the renderer-not-found path triggered, the CLI walks the CRDT document tree and emits its structure faithfully. Maps render as key-value blocks, sequences as ordered lists, text as text, counters as integers, scalars verbatim.

`json` and `ndjson` output formats use the generic structural representation regardless of whether the type is known. This keeps machine-readable output stable across schema additions and keeps consumers off the per-type human-readable layout, which can evolve.

**Binary content** receives explicit handling:

- Inline byte sequences (rare in well-modeled docs, common in ad-hoc ones) render as `<base64:LEN=N>` in `text`; as base64-encoded strings in `json` / `ndjson`.
- `BlobRef` values render as the reference and metadata only — never the blob contents. Retrieval of blob contents is a future concern (likely a separate `peat blob` subcommand) and is explicitly out of scope for this ADR.

`peat-schema` is a hard dependency of the `peat-cli` crate (not optional, not feature-gated). The generic-fallback path is always present.

### Shell integration discipline

`peat` is expected to be piped into files and downstream commands. The following are not nice-to-haves; they are baseline correctness:

- **Streams.** All document data goes to **stdout**. All logs, status messages, progress indicators, summary lines, and errors go to **stderr**. A consumer redirecting stdout to a file gets only data.
- **TTY detection.** Color, table borders, and other terminal-only embellishments are emitted only when stdout is a TTY. Piped or redirected stdout is plain.
- **Buffering.** `query` uses default buffering and flushes on exit. `observe` is line-buffered so that `peat observe ... | jq ...` works without per-tool flush hacks.
- **SIGPIPE.** When a downstream consumer closes its end of the pipe, `peat` exits silently with status 0 (or its current status, whichever is non-zero-preserving). It does not print a broken-pipe error.
- **SIGINT.** Clean shutdown: drop the subscription, allow presence record cleanup, exit with status 130 per convention.
- **UTF-8.** Output is UTF-8. Non-UTF-8 bytes in document content are escaped, never written raw.
- **Exit codes.** Single source of truth:
  - `0` — success
  - `1` — timeout / no peers / generic failure
  - `2` — authentication failure
  - `3` — permission denied
  - `4` — malformed request (bad collection / doc id / flag combination)
  - `130` — SIGINT

### Credentials

Application credentials per ADR-006. Resolution order:

1. `--creds <PATH>` argument (path to a YAML file).
2. `PEAT_CREDS` environment variable. May contain credential material directly, or a path to a YAML credentials file.
3. `$XDG_CONFIG_HOME/peat/credentials.yaml` (or platform equivalent).

The on-disk format is YAML. The file structure carries the application credential bundle defined by ADR-006; the CLI does not invent a new credential schema.

Failure to resolve credentials is a fatal error; `peat` does not silently fall back to anonymous join.

---

## Testing Strategy

Coverage is a release blocker, not a follow-up.

### Unit tests (in-crate)

- Clap parser construction: every subcommand, every flag, every conflict case (notably `--from` vs `--set`).
- Output formatters: text / json / ndjson against fixture documents and fixture operation logs.
- **Typed renderers**: each known `peat-schema` document type has at least one rendering fixture (text and json).
- **Generic renderer**: synthetic CRDT documents with no schema match render structurally and stably (golden-file fixtures).
- **Binary content rendering**: inline bytes and BlobRefs render per the specified rules in both text and json.
- **Write input parsing**: `--from <file>`, `--from -` (stdin), and `--set path=value` repeatable parsing; type coercion against known schemas.
- **Delta computation**: given current state and proposed state, the computed delta is minimal and applying it reproduces the proposed state.
- **Write validation**: schema-valid inputs pass; schema-invalid inputs return exit 4 with explanation; `--no-validate` bypasses with stderr warning.
- **Dry-run**: `--dry-run` produces the would-be operation in the selected format and exits 0 without submitting.
- **Shell integration**: TTY-vs-pipe detection produces different `text` output; SIGPIPE handling exits cleanly; stderr/stdout separation is enforced; exit codes match the documented table.
- Credential resolution: each source path, including precedence and failure modes.
- Lifecycle state machine: timeout handling, signal handling, exit code mapping.

Target: **≥ 90% line coverage** for the `peat-cli` crate, enforced in CI (`cargo llvm-cov` or `tarpaulin`). Raising existing `peat-node` crate coverage to the same bar is desirable but out of scope for this ADR and tracked separately.

### Integration tests (in-crate, mock backend)

- Subscription invocation against a mocked peat-protocol / peat-mesh backend.
- Sync-mode-to-subscription-API mapping is exercised for every command/flag combination.
- Auth failure and permission-denied paths return correct exit codes.

### End-to-end functional sync tests (in-crate)

A `tests/e2e/` suite inside the `peat-cli` crate that stands up real `peat-node` instances and the `peat` binary, then asserts on observed behavior across the mesh. The harness lives with the crate so that CLI-only changes can iterate without touching workspace-level test infrastructure.

Test harness requirements:

- **Multi-process topology.** At minimum: two `peat-node` instances + one `peat` invocation. Tests run nodes as child processes (not threads), so the network stack is exercised end-to-end.
- **Deterministic transport selection.** Tests pin transport (e.g., loopback QUIC) to remove environmental flakiness.
- **Time-bounded.** Every assertion has an explicit timeout; no unbounded waits.
- **Containerized variant.** A subset runs inside docker-compose to validate the in-container debug story.

Functional scenarios to cover from day one:

1. **`query` against a static document** — write a doc on node A; `peat query` returns the materialized state.
2. **`query` after N deltas** — apply N deltas on node A; CLI returns the latest state (one render).
3. **`observe` receives a delta** — start `observe` on a doc, apply a delta on node A, assert the CLI emits the new state within timeout.
4. **`observe --mode full-history`** — apply N deltas; CLI emits N records.
5. **`observe --mode latest-only`** — apply N deltas; CLI emits ≤ N records and the final state is current.
6. **Auth failure** — invalid credentials produce exit 2 and a clear error.
7. **Permission denied** — valid credentials lacking read on the collection produce exit 3.
8. **Partition tolerance** — kill node A mid-observe; CLI continues to report state from node B.
9. **Presence cleanup** — CLI exits; other nodes observe its presence record expire within TTL.
10. **Output schema stability** — `ndjson` records validate against a versioned JSON schema fixture.
11. **Schema-typed document rendering** — write a known `peat-schema` document type on node A; `peat query --output text` renders with type-aware formatting; `--output json` produces canonical structural JSON.
12. **Schema-less document rendering** — write a document of an application-defined type unknown to `peat-schema` on node A; `peat query --output text` renders structurally without error; `--output json` produces valid JSON.
13. **Pipe integration** — `peat query ... --output json > snapshot.json` produces a clean JSON file with no stderr noise written to it; subsequent `jq` consumption succeeds.
14. **Streaming pipe integration** — `peat observe ... --output ndjson | head -n 5` exits cleanly on SIGPIPE after consuming exactly 5 records, no broken-pipe error printed.
15. **TTY vs non-TTY output** — invoking `peat query` with stdout redirected to a file produces plain-text output with no ANSI escape sequences.
16. **Renderer-not-found** — write a document whose type metadata identifies a `peat-schema` type the running CLI doesn't have a renderer for; `peat query` emits a stderr warning naming the unrecognized type and produces correct generic-renderer output on stdout. Exit code is 0.
17. **`--limit`** — write 50 docs in a collection; `peat query <collection> --limit 10` emits exactly 10 records.
18. **`create` with schema-typed document** — `peat create <collection> --from doc.json` on a `peat-schema` type succeeds; the document appears on node B via `peat query`.
19. **`create` with schema-less document** — `peat create <collection> --from doc.json` on an application-defined type succeeds and is observable on node B.
20. **`create` rejects duplicate id** — second `create` with same `--id` fails with exit 4; original document unchanged.
21. **`update --from`** — apply a full-document update; node B sees the new state; the operation history shows a delta, not a recreation.
22. **`update --set`** — surgical update of a single field; node B sees the change; other fields are preserved.
23. **`update` against missing doc** — `peat update <collection>/<missing-id> --from doc.json` creates the document; subsequent reads on node B observe it. Operation history shows initial creation, not "recreation."
24. **`update` schema validation** — submitting input that violates a known type's schema fails with exit 4 and a clear error.
25. **`update --no-validate`** — submitting type-violating input with `--no-validate` succeeds but emits a stderr warning.
26. **`delete`** — `peat delete <collection>/<doc_id>` tombstones the document; node B observes the tombstone per ADR-034.
27. **`--dry-run`** — `create`, `update`, and `delete` with `--dry-run` produce the would-be operation on stdout and do not modify any node's state.
28. **`--wait-for-sync`** — write commands block until at least one peer acknowledges; without the flag, they return as soon as the local engine accepts the operation.
29. **Write authorization** — credentials lacking write scope produce exit 3 for `create`, `update`, and `delete`.
30. **Round-trip edit** — `peat query <doc> --output json | jq '...' | peat update <doc> --from -` produces the expected delta.

### CI gates

- Unit + integration tests on every PR.
- Coverage threshold enforced.
- E2E suite runs on every PR against a Linux runner; nightly run additionally on macOS.
- Container build + container-internal `peat --help` smoke test on every PR.

---

## Implementation Plan

### Phase 1 — Skeleton (Week 1)

- Add `peat-cli` crate to the workspace with `[[bin]] name = "peat"`.
- Stub `clap` parser for `query`, `observe`, `create`, `update`, and `delete` with all flags.
- `--help` text complete and reviewed.
- CI wiring: crate builds, unit test scaffold runs.

### Phase 2 — Join prelude and read commands (Week 2)

- Credential loader.
- Shared join / auth / sync prelude.
- `query` against `LatestOnly` subscription, returning materialized state.
- `--limit` support.
- `text` and `json` formatters; typed and generic renderers.
- Unit + mock-backend integration tests passing at coverage target.

### Phase 3 — `observe` (Week 3)

- `observe` subcommand for all three sync modes.
- `ndjson` formatter.
- Signal handling, pipe-close handling.
- Mock-backend integration tests for all `observe` modes.

### Phase 4 — Write commands (Week 4)

- `--from` (file and stdin) and `--set` input parsing.
- Schema validation against `peat-schema` for known types; structural acceptance for unknown types.
- Delta computation for `update --from`.
- `create`, `update`, `delete` against mock backend.
- `--dry-run` and `--wait-for-sync` semantics.
- Write authorization paths (exit 3).
- Unit + mock-backend integration tests at coverage target.

### Phase 5 — End-to-end suite (Week 5)

- In-crate `crates/peat-cli/tests/e2e/` harness: multi-process topology runner.
- All thirty functional scenarios passing on Linux.
- Container image updated to include `peat`; in-container smoke test green.

### Phase 6 — Cross-platform and hardening (Week 6)

- macOS and Windows build matrix.
- Cross-arch verification (aarch64 Linux at minimum).
- `peat` binaries attached to the existing `peat-node` GitHub release workflow: Linux (x86_64, aarch64), macOS (aarch64, x86_64), Windows (x86_64).
- Documentation: README, operator quickstart, examples (including round-trip edit pattern).
- Open-source release prep (license headers, contribution notes).

---

## Consequences

### Positive

- Operators get a `kubectl`-shaped tool against any Peat deployment with no parallel admin API.
- The CLI exercises the same protocol path as production nodes; bugs surface here too.
- Container image gains a built-in debug surface; no sidecar needed.
- Cross-repo coupling stays zero; protocol and tooling refactor atomically.
- A reusable join / subscribe / write prelude becomes available for future small utilities.
- `observe --mode full-history` provides a poor-operator's CDC surface for change-stream consumers without standing up a separate change-capture pipeline.
- Operators can author and modify mesh state directly without writing a custom application — useful for setup scripts, incident response, and test harness construction.

### Negative

- One more crate to maintain.
- A short-lived "node" joining the mesh has measurable presence cost; ephemeral posture mitigates but does not eliminate.
- Container image gains a few MB from the additional binary.
- The binary name `peat` is short and convenient but easy to conflate with the project name in conversation and docs; documentation needs to be explicit.
- A general-purpose write tool raises the operational stakes. A misused `peat delete` or `peat update` can introduce real damage. Mitigation: explicit `<COLLECTION>/<DOC_ID>` targeting, `--dry-run`, distinct write authorization scopes, and identity-stamped operation history.

### Risks

- **Presence pollution.** A misbehaving or stuck CLI process could leave dangling presence records. Mitigated by short TTL and explicit cleanup on graceful exit.
- **Credential exfiltration.** A CLI on an operator workstation handles credentials in a different threat environment than a daemon on a hardened device. Mitigated by treating creds as application-scoped and time-bounded.
- **Output schema drift.** Scripts will depend on `json` / `ndjson` output. The schema must be versioned and breaking changes called out per the standard release notes.
- **Write blast radius.** A scripted loop with `peat update` or `peat delete` could damage state quickly. Mitigated by separate write authorization scopes (per ADR-006) and audit logging of authored operations.
- **`--no-validate` misuse.** Operators may reach for it under pressure and introduce schema-violating documents the rest of the mesh cannot handle. Mitigated by stderr warning and reserved usage guidance in docs; consider future telemetry on `--no-validate` invocations.

---

## Open Questions

1. **Credential bundle file format.** Formalised in the ADR-006 amendment landed via [peat#944](https://github.com/defenseunicorns/peat/pull/944) (closes [peat#940](https://github.com/defenseunicorns/peat/issues/940), 2026-05-29). `peat-cli`'s `crates/peat-cli/src/creds.rs` shape (`app_id` + `shared_key` + optional `peers`) is now the canonical operational bundle. File-system custody normative requirements (mode 0600, refuse on world/group-readable) apply.
2. **Authorization model — deferred.** Earlier framing assumed an authorization layer (per-collection write scopes, exit 3 before content parse). That framing implicitly required a client-server architecture; Peat is serverless, and today's access model is "hold the formation key → participate fully." A real authorization model needs ADR-006 Layer 1 (Device Identity) to be implemented first so operations have authenticated authors to bind enforcement against. Tracked deferred at [peat#941](https://github.com/defenseunicorns/peat/issues/941). **Consequence for this ADR:** scenarios 7 and 29 of the test plan ("write authorization" → exit 3) are removed from near-term scope. `CliError::PermissionDenied` and exit code 3 remain in code as scaffolding for when the authorization design exercise resumes.
3. ~~**Automerge delta API for `update --from`.**~~ Resolved via [peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187) → peat-mesh rc.28 (2026-05-29). `AutomergeStore::diff(current, proposed) -> AutomergeDelta` plus `store.apply_delta(key, &delta)` ship the round-trip-edit path; `crates/peat-cli/src/cli/update.rs` reads `--from`, computes the minimal delta against the stored doc, and applies it — preserving ADR-021's "create once, evolve through deltas" invariant. Upsert on missing doc still falls through to `put`.
4. ~~**Lamport-clock source for tombstone authorship.**~~ Resolved via [peat-mesh#192](https://github.com/defenseunicorns/peat-mesh/issues/192) → peat-mesh rc.28 (2026-05-29). `AutomergeBackend::next_lamport()` (claim-and-advance), `current_lamport()` (read), and `observe_lamport()` (wired into the receive-side tombstone path via peat-mesh#196) ship a node-local Lamport source. `crates/peat-cli/src/cli/delete.rs` now stamps tombstones via `session.backend().next_lamport()`; the wall-clock-nanos proxy is gone.
5. ~~**Typed renderer dispatch.**~~ Resolved via [peat#946](https://github.com/defenseunicorns/peat/issues/946) / [peat#947](https://github.com/defenseunicorns/peat/pull/947) (merged 2026-05-29). `peat-schema` ships a runtime `TypeRegistry` with `TypeDescriptor.fields` field metadata; `peat-cli`'s `text` mode of `query` dispatches typed when the collection is known (`for_collection` returns a descriptor) and falls back to the generic CRDT walk otherwise. Write paths (`create` / `update`) run `validate_json` against the registry — known collections enforce field-level constraints, unknown collections accept structurally. `--no-validate` skips with a stderr warning. peat-cli consumes peat-schema via the workspace `>=0.9.0-rc.19, <0.9.1` range as of rc.19.
6. **Output schema versioning mechanism.** `json` and `ndjson` are a stability contract for downstream scripts, and the Risks section flags drift. The mechanism — embedded version field in each record, top-level envelope, `--output-schema-version` flag pinning, release-notes-only with semver discipline, or some combination — is not yet chosen.
7. **`peat observe` does not see remote tombstone arrivals.** `cli/observe.rs` subscribes to `peat_mesh::storage::AutomergeStore::subscribe_to_observer_changes`, which fires only on `put`-path writes. The tombstone-receive path on the destination peer (`apply_tombstone` → `store.delete` → table remove) does not fire `observer_tx`, so an observer-CLI receives no CDC event when another peer authors a `delete`. The `render_observe_deleted` arm of the renderer remains reachable from a locally-observed concurrent put-then-tombstone race, but not from a remote tombstone arrival. Fix shape: either peat-mesh's `store.delete` fires `observer_tx` (matches the CDC contract documented on `subscribe_to_observer_changes` — "fires for ALL document changes"), or peat-mesh exposes a sibling `subscribe_to_tombstones` channel the renderer can multiplex. Tracked: [peat-mesh#202](https://github.com/defenseunicorns/peat-mesh/issues/202).
8. ~~**`--set` partial payloads fail on registered types.**~~ Resolved via [peat-node#112](https://github.com/defenseunicorns/peat-node/issues/112) on this PR. `crates/peat-cli/src/cli/writes.rs::apply_proto3_defaults` underlays proto3 zero-defaults for every field of a known registered collection before validation, then merges the operator's `--set` overlay on top (operator wins per field). All 5 builtin types (`capabilities`, `node-configs`, `node-states`, `cell-configs`, `cell-states`) accept partial `--set` payloads end-to-end; coverage in `tests/e2e/scenarios.rs::lifecycle_*` exercises the full lifecycle through `--set`. Defaults table is hardcoded per-collection (`FieldFormat::JsonString` is ambiguous between real `string` and `optional Message` in the rc.19 registry, so a generic descriptor-driven default would be wrong); the `defaults_pure_pass_prost_deserialize_for_every_registered_type` unit test catches drift if peat-schema adds a required field upstream. Long-term shape (deferred): `peat-schema` exposes `proto3_zero()` per `TypeDescriptor` and peat-cli's table becomes registry-driven.

---

## Resolved During Review

The following design decisions were settled during ADR review and are reflected in the sections above:

- **Command names.** `query` (one-shot, materialized state only), `observe` (streaming), `create` / `update` / `delete` (write).
- **No `query --deltas`.** `query` returns the rows, not the WAL — same model as `psql` on a `SELECT`. Operation-stream consumption lives on `observe --mode full-history`, which is the surface for change-stream / CDC-style use.
- **No `apply` command.** `create` is strict-create (errors if doc exists); `update` is upsert (creates if missing, applies delta if present). ADR-021's "create once, evolve through deltas" invariant holds because initial `update` on a missing doc is initial creation, not recreation.
- **Node posture varies by command.** Reads are passive observers; writes are active participants. Both ephemeral, both identity-stamped, both short-TTL.
- **Credentials.** YAML config file (path via `--creds` argument, `PEAT_CREDS` env var, or platform default config location). Bundle format shipped in `crates/peat-cli/src/creds.rs` as a placeholder pending the ADR-006 amendment tracked in [peat#940](https://github.com/defenseunicorns/peat/issues/940).
- **`observe --since`.** Deferred to future work pending ADR-019 sync mode replay semantics.
- **No pagination.** `--limit <N>` on `query` for capping output; no cursor.
- **Type identification.** Via document metadata emitted by `peat-schema`-defined types.
- **Renderer-not-found.** Emit stderr warning identifying the unrecognized type; fall back to generic renderer; exit 0.
- **`--set` path syntax.** jq / JSON-pointer-style in v1. Expected to evolve as needs emerge.

---

## Validation Checklist

Before this ADR moves from Proposed to Accepted:

- [x] ~~Write authorization scope model confirmed against ADR-006~~ — **deferred**: authorization model needs ADR-006 Layer 1 (Device Identity) first; tracked at [peat#941](https://github.com/defenseunicorns/peat/issues/941). This ADR no longer commits to exit-3-before-content-parse in near-term scope; the surface (CliError variant + exit code) stays as scaffolding.
- [ ] Output schema versioning approach signed off

Before this ADR moves from Accepted to Implemented:

- [ ] E2E scenarios passing in CI (8 representative scenarios landed in Phase 5). Remaining ADR scenarios are scoped as follows: scenarios 7 + 29 (write authorization → exit 3) are **out of near-term scope** per the deferred authorization model in Open Question 2; scenarios that depend on `update --from` or `observe --since` are gated on the upstream issues filed under Open Questions.
- [ ] Coverage target met (`cargo llvm-cov -p peat-cli` ≥ 90 %)
- [x] `peat` binary present in container image (Phase 6 — Dockerfile builds `--workspace` and copies `/usr/local/bin/peat`)
- [ ] Cross-platform builds green
- [ ] Operator quickstart documented (including round-trip edit pattern)

---

## References

- ADR-005: Data Sync Abstraction
- ADR-006: Security, Authentication, and Authorization
- ADR-007: Automerge-Based Sync Engine
- ADR-019: Quality of Service and Data Prioritization
- ADR-021: Document-Oriented Architecture
- ADR-032: Pluggable Transport Abstraction
- ADR-034: Record Deletion and Tombstone Management
- `clap` (Rust CLI parser): https://docs.rs/clap
- `cargo llvm-cov`: https://github.com/taiki-e/cargo-llvm-cov
