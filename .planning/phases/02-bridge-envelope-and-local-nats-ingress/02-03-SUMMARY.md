---
phase: 02-bridge-envelope-and-local-nats-ingress
plan: "03"
subsystem: nats-bridge-runtime
tags: [nats, tokio, bounded-queue, readiness, reconnect]

requires:
  - phase: 02-bridge-envelope-and-local-nats-ingress
    plan: "02"
    provides: bounded serial ingress queue, writer, and label-free counters
provides:
  - atomic broker-confirmed readiness for the complete configured subject set
  - one bounded complete subscription generation with reconnect retention and full-set rebuild
  - deterministic lifecycle, backpressure, flush, and slow-consumer seam evidence
affects: [02-04-process-wiring, phase-03-nats-egress, phase-04-observability]

tech-stack:
  added: []
  patterns:
    - one FuturesUnordered reader generation owning every subscriber handle
    - bounded awaited lifecycle signaling independent from ingress backpressure

key-files:
  created: []
  modified:
    - src/nats_bridge/readiness.rs
    - src/nats_bridge/runtime.rs
    - src/main.rs

key-decisions:
  - "Only a successful client flush may atomically copy the complete configured subject set into readiness."
  - "Ordinary disconnect retains subscriber handles, while one ended stream cancels and replaces the complete generation."
  - "The node-backed three-argument BridgeRuntime::spawn contract is exposed now; current main startup temporarily uses an explicit connection-only compatibility constructor until Plan 02-04 wiring."

patterns-established:
  - "Generation ownership: one parent reader future polls all subscribers and dropping it drops every sibling handle."
  - "Lifecycle isolation: a bounded 64-item awaited event channel clears readiness independently of the bounded ingress sender."

requirements-completed:
  - ING-01
  - ING-06
  - TEST-01

duration: 9 min
completed: 2026-07-15
---

# Phase 02 Plan 03: Subscription-Aware NATS Runtime Summary

**A broker-flushed complete subscription generation feeds bounded ingress before readiness, survives ordinary reconnects, and rebuilds atomically after any stream ends.**

## Performance

- **Duration:** 9 min
- **Started:** 2026-07-15T00:51:33Z
- **Completed:** 2026-07-15T01:00:39Z
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Replaced incremental subject readiness with one atomic establish-all transition plus connection-preserving generation invalidation.
- Added subscriber capacity 1, one bounded 64-signal supervisor channel, one shared 256-item ingress queue, one serial writer, and complete generation ownership.
- Preserved live handles over disconnect/reconnect, requiring a fresh flush before readiness, while rebuilding every configured subject after one stream ends.
- Added deterministic evidence for pre-flush ingestion, full-queue disconnect handling, reconnect versus rebuild actions, flush failure, safe diagnostics, and 60-second process-wide slow-consumer warnings.

## Task Commits

Each task was committed atomically:

1. **Task 1: Make readiness establishment and generation invalidation atomic** - `c89b665` (feat)
2. **Task 2: Own one complete subscription generation and feed bounded ingress** - `2949950` (feat)
3. **Task 3: Prove reconnect, generation-end, and slow-consumer semantics at runtime seams** - `a8b3cfe` (test)

Additional blocking interface fix: `87d8976` (fix)

## Files Created/Modified

- `src/nats_bridge/readiness.rs` - Atomically establishes all configured subjects and invalidates a generation without claiming disconnect.
- `src/nats_bridge/runtime.rs` - Owns bounded lifecycle signaling, subscriber generation readers, flush barriers, reconnect/rebuild transitions, ingress stats, and focused tests.
- `src/main.rs` - Temporarily names the Phase 1 connection-only constructor so the node-backed `BridgeRuntime::spawn` API is ready for Plan 02-04 wiring.

## Decisions Made

- Used one `FuturesUnordered` reader generation rather than detached per-subject tasks so the first ended stream drops every sibling handle with the generation task.
- Kept async-nats as the sole reconnect owner and retained live subscription handles across ordinary disconnects.
- Exposed ingress statistics through the runtime handle for bounded integration evidence without adding public RPC or labeled metrics.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Preserved current process compilation while exposing the planned node-backed spawn API**
- **Found during:** Final acceptance audit after Task 3
- **Issue:** Plan 02-04 expects `BridgeRuntime::spawn(config, source_node_id, node)`, but changing the method directly would break current `main.rs` before its dedicated wiring task.
- **Fix:** Reserved `BridgeRuntime::spawn` for the required three-argument contract and moved the temporary Phase 1 path to `spawn_connection_only`, changing one existing main call site without activating ingress early.
- **Files modified:** `src/nats_bridge/runtime.rs`, `src/main.rs`
- **Verification:** Runtime tests and `cargo clippy -p peat-node --lib --bin peat-node -- -D warnings` pass.
- **Committed in:** `87d8976`

---

**Total deviations:** 1 auto-fixed (1 blocking interface issue). **Impact:** The intended architecture is unchanged; Plan 02-04 can now perform its dedicated node/identity wiring without an out-of-plan runtime edit.

## Issues Encountered

- The package-level filtered test command builds every integration-test target; focused `--lib` repetitions were used for faster deterministic final verification after the exact package command had passed.

## User Setup Required

None - no external service configuration required.

## Verification

- `cargo fmt --check` - passed.
- `cargo test -p peat-node --lib nats_bridge::readiness::tests -- --nocapture` - 6 readiness tests passed.
- `cargo test -p peat-node --lib nats_bridge::runtime::tests -- --nocapture` - 13 runtime tests passed.
- `cargo test -p peat-node --lib nats_bridge::ingress::tests -- --nocapture` - 12 ingress tests passed.
- `cargo clippy -p peat-node --lib -- -D warnings` - passed.
- `git diff --check` - passed.
- Prohibited-pattern inspection found no bridge `unbounded_channel` or NATS `publish` call.

## Next Phase Readiness

- The exact three-argument `BridgeRuntime::spawn` contract is ready for Plan 02-04 to pass the effective node ID and `Arc<SidecarNode>` from process startup.
- The scripted NATS peer can now exercise real pre-flush ingress, atomic readiness, retained reconnect, and complete generation rebuild behavior.
- No public proto, service, egress, or dependency-pin changes were introduced.

## Self-Check: PASSED

- Task commits `c89b665`, `2949950`, `a8b3cfe`, and interface fix `87d8976` exist in git history.
- All task acceptance criteria and plan-level verification commands pass.
- Protected user-owned paths remain unstaged and unchanged by this execution.

---
*Phase: 02-bridge-envelope-and-local-nats-ingress*
*Completed: 2026-07-15*
