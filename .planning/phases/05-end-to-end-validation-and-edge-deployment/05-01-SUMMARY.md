---
phase: 05-end-to-end-validation-and-edge-deployment
plan: "01"
subsystem: testing
tags: [docker-compose, core-nats, nats-cli, exact-bytes, edge]

requires:
  - phase: 04-reliability-lifecycle-and-observability
    provides: native bridge lifecycle, remote-only egress, and exact-byte reliability contract
provides:
  - isolated two-site Compose graph with Peat as the sole cross-site transport
  - exact-byte vision fixture publisher for one-shot CI and 30-second demonstrations
  - raw NATS translator verifier with fixed match or mismatch records
affects: [05-02-automated-e2e, 05-03-edge-deployment]

tech-stack:
  added: []
  patterns:
    - one base release topology plus a build-only local override
    - direct file-descriptor publication and translator-stdin comparison
    - physical profile preflights reject missing LAN peer coordinates

key-files:
  created:
    - examples/compose/nats-bridge/docker-compose.yml
    - examples/compose/nats-bridge/docker-compose.local.yml
    - examples/compose/nats-bridge/vision-summary.json
    - examples/compose/nats-bridge/publish-vision.sh
    - examples/compose/nats-bridge/verify-vision.sh
  modified: []

key-decisions:
  - "Only peat-a and peat-b join the mesh network; each Core NATS broker remains private to its own internal site network."
  - "The checked-in 327-byte fixture is the publication and comparison oracle, including its terminal newline."
  - "nats-box 0.19.5 requires --no-templates; --templates=false is parsed as positional input and is unsafe for this helper."

patterns-established:
  - "Compose topology changes image provenance through an override without duplicating or changing the service network graph."
  - "Payload bytes travel only from fixture stdin to NATS and from translator stdin to cmp; shell variables and JSON serializers never carry them."

requirements-completed:
  - TEST-03
  - TEST-04

duration: 8 min
completed: 2026-07-16
---

# Phase 05 Plan 01: Isolated Topology and Byte-Safe Helpers Summary

**A reusable two-site Compose package now structurally isolates independent Core NATS brokers while publishing and verifying one stable vision fixture without byte normalization.**

## Performance

- **Duration:** 8 min
- **Started:** 2026-07-16T18:03:43Z
- **Completed:** 2026-07-16T18:11:24Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Defined local, Site A, and Site B profiles with separate internal broker networks, a Peat-only mesh, deterministic identities, fixed Iroh UDP ports, isolated state volumes, bounded health checks, and explicit physical peer preflights.
- Added a valid, secret-free 327-byte JSON fixture with deliberate escaped Unicode, numeric spelling, formatting, and terminal newline sent directly through NATS CLI stdin.
- Added a translator target that compares raw message stdin with `cmp` and records exactly `match` or `mismatch` without exposing body or credential data.
- Probed the pinned official images and exercised a real broker/helper publication: nats-box published 327 bytes to `vision.summary`, translator stdin compared equal, and the record was exactly `match`.

## Task Commits

1. **Task 1: Define the isolated two-site Compose topology and local-build override** - `5751eb7` (feat)
2. **Task 2: Add the stable vision fixture and exact-byte 30-second publisher** - `4c8aa21` (feat)
3. **Task 3: Add raw NATS receipt verification with payload-safe records** - `e183c4e` (feat)

## Files Created/Modified

- `examples/compose/nats-bridge/docker-compose.yml` - Shared local/device topology, pinned helpers, physical preflights, receiver wiring, networks, and volumes.
- `examples/compose/nats-bridge/docker-compose.local.yml` - Current-checkout image build override for both Peat services.
- `examples/compose/nats-bridge/vision-summary.json` - Stable 327-byte exact-content oracle.
- `examples/compose/nats-bridge/publish-vision.sh` - Validated direct-stdin publisher with one-shot and continuous cadence controls.
- `examples/compose/nats-bridge/verify-vision.sh` - Raw translator-stdin comparator and fixed delivery recorder.

## Decisions Made

- Used the deterministic endpoint IDs derived from the fixed demo formation key and stable `edge-a`/`edge-b` node IDs for local service-DNS peering.
- Kept broker monitor port 8222 private and used it only for bounded site-local health/readiness evidence; neither broker publishes a host port or joins mesh.
- Made physical preflight services part of their site profiles so missing LAN peer overrides and mutable/invalid image inputs fail before an operator treats the launch as valid.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Correctness] Replaced an unsafe NATS CLI boolean spelling**
- **Found during:** Task 3 pinned-image translator probe
- **Issue:** With nats-box 0.19.5, the planned `--templates=false` spelling was parsed as positional input; the probe reported a 14-byte publication to subject `false` instead of publishing the fixture to `vision.summary`.
- **Fix:** Used the pinned CLI's documented canonical `--no-templates` flag and retained a source comment explaining why the superficially equivalent spelling is unsafe.
- **Files modified:** `examples/compose/nats-bridge/publish-vision.sh`
- **Verification:** The corrected pinned-image probe reported `Published 327 bytes to "vision.summary"`, translator stdin produced exactly `match`, and the one-shot helper did not sleep.
- **Committed in:** `e183c4e`

---

**Total deviations:** 1 auto-fixed (1 correctness issue). **Impact:** The helper now matches the actual pinned CLI parser while preserving the locked no-template and exact-byte requirements; no topology or delivery semantics changed.

## Issues Encountered

- Docker Desktop was initially stopped and sandboxed daemon access was denied. Docker was started with approval, and all pinned-image probes and temporary-container/network cleanup then completed successfully.

## Verification

- Base and local-override Compose renders for local, Site A, and Site B profiles - passed.
- JSON-rendered service network/environment assertions and release-image missing-variable failure - passed.
- Physical preflight missing-peer rejection and explicit-LAN-peer acceptance - passed.
- `nats:2.14.3-alpine` probe: `/usr/bin/wget`, monitor health endpoint, and `--http_port` support - passed.
- `natsio/nats-box:0.19.5` probe: NATS CLI 0.4.0, `/usr/bin/cmp`, `--translate`, `--count`, and `--wait` - passed.
- Live official-broker publication/translator test: 327-byte publish to `vision.summary` and exact `match` record - passed.
- Fixture JSON, byte count, SHA-256 `9bf89518ff24a4a964e174c9b30c5a54f062277ac2eb779c47fe49f2139e766b`, and terminal newline - passed.
- Fake-client one-shot/no-sleep/default-30-second publisher probes and invalid-input failures - passed.
- Verifier exact input acceptance plus one-byte and missing-newline rejection without body disclosure - passed.
- `git diff --exit-code -- src proto Cargo.toml Cargo.lock` - empty.
- `git diff --check` - passed.

## User Setup Required

None. Plan 05-03 will document the explicit per-device environment and launch procedure.

## Next Phase Readiness

Ready for 05-02 to build the automated lifecycle harness and dedicated CI job on this single topology and its independently proven byte helpers.

## Self-Check: PASSED

- All five key files exist and both shell helpers are executable.
- All three scoped task commits exist and final plan-level verification passes.
- High threats T-05-01 through T-05-03 have static, helper, and pinned-image evidence.
- Production Rust, proto, Cargo manifests, and dependency locks are unchanged.
- Unrelated `.gitignore`, `.planning/PROJECT.md`, `.obsidian/`, and `AGENTS.md` changes remain unstaged and untouched.

---
*Phase: 05-end-to-end-validation-and-edge-deployment*
*Completed: 2026-07-16*
