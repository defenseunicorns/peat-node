---
phase: 02-bridge-envelope-and-local-nats-ingress
plan: "02"
subsystem: nats-bridge-ingress
tags: [nats, tokio, bounded-queue, uuid, tracing]

requires:
  - phase: 02-bridge-envelope-and-local-nats-ingress
    plan: "01"
    provides: byte-exact bridge envelope and create-only SidecarNode persistence
provides:
  - one awaited process-wide 256-item ingress FIFO and one serial Peat writer
  - same-identity transient storage retries with fixed 50 ms and 200 ms delays
  - label-free ingress counters and payload-safe rate-limited diagnostics
affects: [02-03-subscription-runtime, 02-04-ingress-integration, phase-04-metrics]

tech-stack:
  added: []
  patterns:
    - bounded Tokio MPSC with awaited senders and a single receiver-owned writer
    - typed source-free diagnostic actions backed by minimal atomic counters

key-files:
  created:
    - src/nats_bridge/ingress.rs
  modified:
    - src/nats_bridge/mod.rs

key-decisions:
  - "The ingress channel constructor and serial receiver loop remain separate so the exact 256-item boundary is directly testable without weakening production ownership."
  - "Per-subject warning state is pre-seeded only from configured subjects; unexpected subjects are counted and debugged but never allocate rate-limit state."

patterns-established:
  - "Ingress identity pattern: create one UUID v4 and one serialized envelope before any storage attempt, then reuse both for every retry."
  - "Ingress diagnostics pattern: actions contain only route metadata, byte length, generated ID, bounded attempt/delay/count values, and fixed error discriminators."

requirements-completed:
  - ING-02
  - ING-03
  - ING-04
  - ING-06
  - TEST-01

duration: 11 min
completed: 2026-07-15
---

# Phase 02 Plan 02: Bounded Serial NATS Ingress Summary

**A shared 256-item FIFO validates raw NATS messages, assigns fresh UUID v4 identities, performs same-ID bounded create retries, and reports failures through payload-safe counters and rate-limited actions.**

## Performance

- **Duration:** 11 min
- **Started:** 2026-07-15T00:36:43Z
- **Completed:** 2026-07-15T00:47:51Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added one awaited bounded queue and one serial processor preserving global FIFO order across all configured subjects.
- Validated UTF-8/JSON before persistence, generated distinct UUID v4 IDs for identical messages, and reused immutable ID/envelope values across exactly three transient attempts with 50 ms and 200 ms delays.
- Added label-free atomic counters, immediate per-subject invalid-input warnings, 60-second suppressed summaries, debug-only retry actions, and one terminal storage warning.
- Proved formatted actions and captured tracing output exclude representative payload, credential URL, parser excerpt, filesystem/store detail, and source-chain strings.

## Task Commits

Each task was committed atomically:

1. **Task 1: Build the bounded FIFO, serial processor, and same-ID retry contract** - `4d40e08` (feat)
2. **Task 2: Add minimal counters and payload-safe rate-limited diagnostics** - `8b4625a` (feat)

## Files Created/Modified

- `src/nats_bridge/ingress.rs` - Owns the bounded channel, serial processor, writer seam, retry policy, atomic stats, typed actions, rate limiting, and deterministic unit tests.
- `src/nats_bridge/mod.rs` - Exposes the ingress module through the native bridge subsystem.

## Decisions Made

- Used a narrow boxed-future writer trait so `Arc<SidecarNode>` and deterministic fakes share the exact create-only call contract without a new dependency.
- Kept suppression state inside the sole processor and seeded its map from the finite configured subject set, preventing payload-derived or unexpected subjects from growing state.
- Used one generic invalid-input summary discriminator because a subject's suppressed interval may contain both invalid UTF-8 and invalid JSON while the atomic counters retain exact differentiation.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Verification

- `cargo fmt --check` - passed.
- `cargo test -p peat-node nats_bridge::ingress::tests -- --nocapture` - 12 ingress tests passed.
- `cargo test -p peat-node nats_bridge::envelope::tests -- --nocapture` - 4 envelope tests passed.
- `cargo clippy -p peat-node --lib -- -D warnings` - passed.
- `git diff --check` - passed.
- Prohibited-pattern inspection found no ingress violation; the existing runtime supervisor channel is explicitly replaced by Plan 02-03.

## Next Phase Readiness

- The bounded sender, serial processor, writer seam, stats, and slow-consumer counter hook are ready for subscription-generation ownership in Plan 02-03.
- No blockers remain.

## Self-Check: PASSED

- `src/nats_bridge/ingress.rs` exists.
- Task commits `4d40e08` and `8b4625a` exist in git history.
- All task acceptance criteria and plan-level verification commands pass.
- No stubs or unplanned threat surfaces were introduced.

---
*Phase: 02-bridge-envelope-and-local-nats-ingress*
*Completed: 2026-07-15*
