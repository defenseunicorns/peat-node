---
phase: 05-end-to-end-validation-and-edge-deployment
plan: "02"
subsystem: testing
tags: [docker-compose, core-nats, github-actions, exact-bytes, quiescence]

requires:
  - phase: 05-end-to-end-validation-and-edge-deployment
    plan: "01"
    provides: isolated Compose topology, exact-byte fixture publisher, and raw translator verifier
provides:
  - one-command TEST-03 proof across topology, broker subscriptions, Peat convergence, exact bytes, and quiescence
  - dedicated least-privilege GitHub Actions execution of the same checked-in harness
affects: [05-03-edge-deployment, release-validation]

tech-stack:
  added: []
  patterns:
    - private in-broker monitoring with exact-subject subscription deltas
    - trap-owned unique Compose projects with bounded gates and payload-safe diagnostics
    - dedicated CI wrapper that delegates all proof logic to the local harness

key-files:
  created:
    - test/nats-bridge-e2e.sh
    - .github/workflows/nats-bridge-e2e.yml
  modified: []

key-decisions:
  - "Validate and normalize NATS 2.14 subscription objects to subject strings before exact vision.summary filtering; malformed entries fail closed."
  - "Keep the workflow declarative and invoke the checked-in harness exactly once rather than duplicating topology or assertion logic."

patterns-established:
  - "Publication follows private broker health, exact node identity, Peat peer, bridge subscription, and receiver +1 subscription gates."
  - "Positive and negative Compose runs share automatic down -v cleanup and a combined 120-line diagnostic ceiling."

requirements-completed:
  - TEST-03

duration: 20 min
completed: 2026-07-16
---

# Phase 05 Plan 02: Automated End-to-End Proof and Dedicated CI Summary

**A single bounded command now proves one exact vision message crosses two isolated Core NATS sites only through Peat, remains one same-key document per node, arrives once byte-identically, and stays quiescent; dedicated CI runs that same proof.**

## Performance

- **Duration:** 20 min
- **Started:** 2026-07-16T18:18:00Z
- **Completed:** 2026-07-16T18:38:36Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added a unique-project, fresh-volume Compose harness with bounded startup, broker, process, identity, peer, bridge-subscription, receiver-subscription, document, receipt, and quiescence gates.
- Proved the complete TEST-03 conjunction with two official independent NATS servers and two full current-checkout peat-node containers: one same-key exact envelope on both nodes, one byte-identical remote body, and no continuing delivery or document loop.
- Added a separate pull-request, main-push, and manual GitHub Actions workflow with read-only permissions, ref-scoped cancellation, current-checkout build, and a hard 15-minute timeout.
- Exercised fail-closed behavior with a shortened startup deadline, a stopped Site B broker, and a wrong expected fixture; every fault exited nonzero and removed its stack and volumes.

## Task Commits

Each task was committed atomically:

1. **Task 1: Build the one-command packaged end-to-end proof** - `f6547c6` (test)
2. **Task 2: Run the same proof in a dedicated bounded CI workflow** - `155465f` (ci)

## Files Created/Modified

- `test/nats-bridge-e2e.sh` - Owns clean lifecycle, private readiness evidence, one publication, exact document/body assertions, quiescence, bounded diagnostics, and teardown.
- `.github/workflows/nats-bridge-e2e.yml` - Runs the checked-in harness in a dedicated least-privilege 15-minute job.

## Decisions Made

- The pinned NATS 2.14.3 monitor returns `subscriptions_list` entries as objects, so the harness validates each object and extracts its string `subject` before applying the required exact-string filter. It also accepts an already-string entry while rejecting every other shape.
- `EXPECTED_FIXTURE` is an assertion-only fault-injection hook. Compose publication and remote byte comparison remain pinned to the checked-in fixture, so overriding the oracle can only make a mismatch fail.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Adapted the subscription parser to the pinned broker's actual response shape**

- **Found during:** Task 1 full Docker verification
- **Issue:** NATS 2.14.3 returns `subscriptions_list` as objects containing a string `subject`, while the plan described the entries themselves as strings. The original literal-array parser failed closed even though bridge readiness and broker state were correct.
- **Fix:** Validate the array and every entry, normalize official object entries through `.subject`, then count with the required exact `select(. == "vision.summary")` string predicate.
- **Files modified:** `test/nats-bridge-e2e.sh`
- **Verification:** A payload-safe monitor schema probe showed object entries and exact subject count one; the repaired full harness passed counts 1/1, then two with an exact +1 receiver delta.
- **Committed in:** `f6547c6`

---

**Total deviations:** 1 auto-fixed (1 bug). **Impact on plan:** Required for compatibility with the plan's pinned official NATS image; exact-subject and fail-closed semantics are preserved without weakening any acceptance boundary.

## Issues Encountered

- The host's Ruby 2.6 `YAML.load_file` does not accept the newer `aliases:` keyword used in the planned verification command. The workflow parsed successfully with the same loader without that unsupported keyword; the file contains no YAML aliases.
- The first full run correctly failed at the monitor parser boundary before publication. Its trap emitted bounded diagnostics and cleaned up, enabling the response-shape correction above.

## Verification

- `bash -n test/nats-bridge-e2e.sh` - passed.
- Static exact-subject checks - passed; `subscriptions_list` is validated and normalized, exact `vision.summary` filtering is present, and no assertion requires global `num_subscriptions` to equal one or two.
- `./test/nats-bridge-e2e.sh` - passed against two NATS 2.14.3 brokers and two current-checkout peat-node containers with the exact required final PASS line.
- Fault probes - shortened startup deadline, unavailable Site B broker, and wrong fixture oracle each failed nonzero, emitted bounded diagnostics, and left no matching containers or volumes.
- CI YAML parse and policy checks - passed; harness invocation count one, job timeout count one, read-only permissions, and no artifacts, released image, or `continue-on-error`.
- `git diff --exit-code -- src proto Cargo.toml Cargo.lock` - passed; no runtime, proto, or dependency changes.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 05-03 to document and exercise the same topology as a local walkthrough and two-host Jetson smoke procedure. No implementation blocker remains.

## Self-Check: PASSED

- Both key files exist and are committed in atomic task commits.
- All Task 1 and Task 2 acceptance criteria and plan-level static checks pass.
- The real end-to-end proof and all three planned negative probes completed with automatic cleanup.

---
*Phase: 05-end-to-end-validation-and-edge-deployment*
*Completed: 2026-07-16*
