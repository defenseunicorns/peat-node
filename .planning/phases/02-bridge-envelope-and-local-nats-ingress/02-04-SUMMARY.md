---
phase: 02-bridge-envelope-and-local-nats-ingress
plan: "04"
subsystem: nats-bridge
tags: [rust, tokio, core-nats, async-nats, automerge, integration-testing]

requires:
  - phase: 02-bridge-envelope-and-local-nats-ingress
    provides: subscription generations, bounded ingress, immutable bridge envelopes, and create-only Peat storage
provides:
  - enabled-only process wiring with effective node identity and SidecarNode ownership
  - broker-confirmed atomic readiness through a reserved no-responder request barrier
  - scripted Core NATS protocol evidence for exact ingress, reconnect, and timeout behavior
  - operator contract for permissions, memory bounds, privacy, retries, and Core NATS loss
affects: [03-local-nats-egress, 04-reliability-and-observability, deployment-configuration]

tech-stack:
  added: []
  patterns:
    - reserved empty Core NATS request with broker 503 as a bounded establishment barrier
    - bounded scripted TCP peer for protocol-level async-nats integration evidence

key-files:
  created:
    - tests/nats_bridge_ingress_test.rs
  modified:
    - src/main.rs
    - src/nats_bridge/config.rs
    - src/nats_bridge/runtime.rs
    - docs/CONFIGURATION.md

key-decisions:
  - "Use _PEAT.NATS_BRIDGE.READINESS for a two-second empty request barrier and accept either broker NoResponders or a normal response as round-trip confirmation."
  - "Reserve the complete _PEAT first-token namespace from configured application mappings."
  - "Require publish permission for the barrier and subscribe permission for the async-nats _INBOX.> response inbox."

patterns-established:
  - "Atomic readiness: create and start every application subscription reader, confirm one broker round trip, then establish the complete subject set in one transition."
  - "Control isolation: fixed reserved subject, empty payload, no application data, and no route into bridge collections or egress."

requirements-completed: [ING-01, ING-02, ING-03, ING-04, ING-05, ING-06, EGR-01, TEST-01]

duration: 38min
completed: 2026-07-15
---

# Phase 2 Plan 4: Process Integration and Protocol Evidence Summary

**Node-backed Core NATS ingress now starts only when configured, preserves exact JSON bytes in immutable Peat documents, and reports ready only after a bounded broker-confirmed request round trip.**

## Performance

- **Duration:** 38 min
- **Started:** 2026-07-15T01:55:00Z
- **Completed:** 2026-07-15T02:32:55Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Wired the effective operator-visible node ID and constructed `Arc<SidecarNode>` into enabled-only bridge startup without coupling sidecar availability to NATS connectivity.
- Added a bounded scripted Core NATS peer proving complete application `SUB` ordering, pre-readiness ingestion, broker 503 confirmation, exact five-field envelopes, UUID v4 uniqueness, validation counters, reconnect resubscription, and timeout failure behavior.
- Documented exact request permissions, `256 + O(configured mappings)` memory bounds, three-attempt storage behavior, privacy constraints, at-most-once loss, and Phase 3/4 boundaries.

## Task Commits

Each task was committed atomically:

1. **Task 1: Wire effective node identity and SidecarNode into enabled-only runtime startup** - `a72844f` (feat)
2. **Task 2: Add scripted Core NATS protocol evidence for ingress and readiness** - `48c38ec` (test)
3. **Task 3: Document the implemented contract and run full compatibility gates** - `2dbec2b` (docs)

## Files Created/Modified

- `src/main.rs` - Starts the node-backed bridge only for enabled validated configuration and passes the effective node identity.
- `src/nats_bridge/config.rs` - Prevents application mappings from entering the reserved `_PEAT` control namespace.
- `src/nats_bridge/runtime.rs` - Replaces the invalid flush assumption with the bounded reserved request/no-responder barrier.
- `tests/nats_bridge_ingress_test.rs` - Implements the bounded protocol peer and end-to-end ingress/readiness evidence.
- `docs/CONFIGURATION.md` - States the delivered operating, permission, privacy, resource, and loss contract.

## Decisions Made

- The readiness barrier sends no payload to `_PEAT.NATS_BRIDGE.READINESS`, uses a two-second request timeout, treats `RequestErrorKind::NoResponders` as broker confirmation, and also accepts a normal response.
- All configured application subscriptions and their reader generation exist before the barrier request is issued; readiness remains false until the response is received.
- Timeout and client errors are deliberately collapsed to a false barrier result so no sensitive async-nats error or source chain reaches lifecycle diagnostics.

## Deviations from Plan

### Checkpoint-approved architectural correction

**1. Replaced `Client::flush()` / PING-PONG readiness with a broker no-responder request barrier**
- **Found during:** Task 2 protocol integration
- **Issue:** Empirical testing against the pinned async-nats 0.49.1 client showed `Client::flush()` completing without emitting a protocol `PING`; the deliberately failing pre-PONG assertion proved it could not establish broker receipt of the application `SUB` commands.
- **Resolution:** After a decision checkpoint, the user selected a reserved empty request. The scripted peer now proves all application `SUB` frames precede the request and holds readiness false until it sends a server-style `HMSG` carrying `NATS/1.0 503`.
- **Files modified:** `src/nats_bridge/runtime.rs`, `src/nats_bridge/config.rs`, `tests/nats_bridge_ingress_test.rs`, `docs/CONFIGURATION.md`
- **Verification:** The protocol suite proves 503 success and unanswered-request timeout failure; full workspace tests and clippy pass.
- **Committed in:** `48c38ec` and `2dbec2b`

---

**Total deviations:** 1 checkpoint-approved architectural correction.
**Impact on plan:** The replacement provides the broker confirmation the original flush design intended, without dependency patches, public proto changes, or application-payload exposure.

## Issues Encountered

- Localhost integration tests required permission to bind ephemeral loopback ports in the execution sandbox.
- The capacity-1 subscriber correctly reported slow-consumer events when the scripted peer injected a burst without awaiting processing; the test was made deterministic by pacing each injected frame against observed ingress counters.

## User Setup Required

NATS authorization must allow subscriptions to configured application subjects, publish to `_PEAT.NATS_BRIDGE.READINESS`, and subscription to the async-nats request inbox (`_INBOX.>`). No new peat-node environment variable is required.

## Verification

- `cargo fmt --check` - passed
- `cargo test --workspace` - passed
- `cargo test --test nats_bridge_ingress_test -- --nocapture` - passed repeatedly without an external NATS server
- `cargo check --workspace` - passed
- `cargo clippy --workspace --all-targets -- -D warnings` - passed
- `cargo tree -i iroh@1.0.2` - passed; locked `peat-mesh 0.9.0-rc.46` relationship preserved
- `git diff --check` - passed
- Protected proto, service, and dependency files remained unchanged
- No local NATS egress or JetStream implementation was added

## Next Phase Readiness

- Phase 2 ingress and mesh synchronization are complete and ready for Phase 3 remote-origin egress and loop prevention.
- Phase 4 still owns bounded shutdown draining, persisted reconciliation, and the complete metrics surface.

## Self-Check: PASSED

---
*Phase: 02-bridge-envelope-and-local-nats-ingress*
*Completed: 2026-07-15*
