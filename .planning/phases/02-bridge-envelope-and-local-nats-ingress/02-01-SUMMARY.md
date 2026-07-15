---
phase: 02-bridge-envelope-and-local-nats-ingress
plan: "01"
subsystem: nats-bridge-storage
tags: [nats, serde, automerge, encryption, immutable-documents]

requires:
  - phase: 01-bridge-configuration-and-operating-contract
    provides: validated bridge configuration and optional runtime lifecycle
provides:
  - byte-exact five-field v1 bridge envelope and typed ingress validation
  - create-only SidecarNode persistence through the canonical encrypted or structured store path
  - collision, encryption, payload-integrity, and single-observer-event test evidence
affects: [02-02-ingress-pipeline, 03-remote-nats-egress, bridge-document-storage]

tech-stack:
  added: []
  patterns:
    - validation-only JSON parsing with original UTF-8 payload retention
    - shared upsert/create-only document write helper with fixed bridge error classifications

key-files:
  created:
    - src/nats_bridge/envelope.rs
  modified:
    - src/nats_bridge/mod.rs
    - src/node.rs
    - tests/node_test.rs

key-decisions:
  - "Bridge payload parsing validates syntax but never reserializes the original payload text."
  - "Create-only bridge writes use the existing store observer and local-origin fanout path without direct bridge event emission."

patterns-established:
  - "Bridge errors cross the ingress boundary only as fixed, source-free classifications."
  - "Create-only conversion uses no Automerge base after an explicit existing-key check."

requirements-completed:
  - ING-03
  - ING-04
  - ING-05
  - EGR-01
  - TEST-01

duration: 12 min
completed: 2026-07-15
---

# Phase 02 Plan 01: Bridge Envelope and Create-Only Persistence Summary

**A five-field v1 bridge envelope preserves exact NATS payload bytes and persists immutably through SidecarNode's existing encryption, observer, and mesh-fanout path.**

## Performance

- **Duration:** 12 min
- **Started:** 2026-07-15T00:20:00Z
- **Completed:** 2026-07-15T00:32:53Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Defined a fixed `peat.nats-bridge` v1 envelope with exactly kind, numeric version, literal subject, operator-visible source node ID, and exact payload text.
- Added validation-only UTF-8/JSON parsing with byte-level tests covering whitespace, key order, numeric spelling, escapes, Unicode, and trailing whitespace.
- Added create-only bridge persistence with collision rejection, optional encryption, fixed safe errors, and exactly one canonical store observer event.

## Task Commits

Each task was committed atomically:

1. **Task 1: Implement the exact v1 bridge envelope and validation boundary** - `e56d3c3` (feat)
2. **Task 2: Add typed create-only persistence on the canonical node write path** - `16a8831` (feat)

## Files Created/Modified

- `src/nats_bridge/envelope.rs` - Defines the durable envelope, validation boundary, and byte-exact schema tests.
- `src/nats_bridge/mod.rs` - Exposes the envelope module to the bridge subsystem.
- `src/node.rs` - Shares document write preparation and adds fixed create-only bridge persistence.
- `tests/node_test.rs` - Proves plain/encrypted round trips, collision preservation, safe errors, and one observer event.

## Decisions Made

- Retained unrestricted source errors only inside the shared write implementation so existing upsert diagnostics remain detailed while bridge-facing errors stay fixed and payload-safe.
- Used the existing `store.put` observer as the sole bridge event and mesh propagation trigger.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- Sandboxed node integration tests could not bind Iroh sockets (`Operation not permitted`); rerunning the same tests with approved local socket access passed.

## User Setup Required

None - no external service configuration required.

## Verification

- `cargo fmt --check` - passed.
- `cargo test -p peat-node nats_bridge::envelope::tests -- --nocapture` - 4 envelope tests passed.
- `cargo test --test node_test -- --nocapture` - all 25 node integration tests passed.
- `cargo clippy -p peat-node --lib --tests -- -D warnings` - passed.
- `git diff --check` - passed.
- `git diff -- proto/sidecar.proto src/service.rs Cargo.toml Cargo.lock` - empty.

## Next Phase Readiness

- The immutable envelope and node writer seam are ready for the bounded serial ingress pipeline in Plan 02-02.
- No blockers or compatibility changes remain.

## Self-Check: PASSED

- `src/nats_bridge/envelope.rs` exists.
- Task commits `e56d3c3` and `16a8831` exist in git history.
- All task acceptance criteria and plan-level verification commands pass.

---
*Phase: 02-bridge-envelope-and-local-nats-ingress*
*Completed: 2026-07-15*
