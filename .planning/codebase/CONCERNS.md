# Codebase Concerns

**Analysis Date:** 2026-07-08

## Tech Debt

**`src/node.rs` is the largest file at 1759 lines with 34 functions:**
- Issue: This file owns too many responsibilities — lifecycle, peer discovery, sync coordination, encryption, collection config, and attachment wiring. It acts as a god object.
- Files: `src/node.rs`
- Impact: Hard to reason about, test in isolation, or modify one concern without risking another.
- Fix approach: Extract peer discovery logic, encryption wiring, and attachment bootstrapping into dedicated modules. The `SidecarNode` struct could delegate to focused sub-structs.

**`src/attachments/handlers.rs` (954 lines) and `src/attachments/config.rs` suppress clippy `too_many_arguments`:**
- Issue: `#[allow(clippy::too_many_arguments)]` at `src/attachments/config.rs:157` and `src/attachments/handlers.rs:543` signal functions with excessive parameter counts.
- Files: `src/attachments/config.rs`, `src/attachments/handlers.rs`
- Impact: Functions are hard to call correctly; easy to swap arguments of the same type.
- Fix approach: Introduce config/context structs to bundle related parameters.

**Dead code annotations:**
- Issue: `#[allow(dead_code)]` at `src/watcher.rs:165` and `src/attachments/ingest.rs:606` indicate unused fields/structs kept around.
- Files: `src/watcher.rs:165`, `src/attachments/ingest.rs:606`
- Impact: Minor — code noise, potential confusion about intended API surface.
- Fix approach: Remove if truly unused, or document why they exist for future use.

## Known Bugs

No explicit bugs found via code markers. The codebase has no `FIXME`, `TODO`, `HACK`, or `XXX` comments in `src/`, which is a positive signal.

## Security Considerations

**Mutex/RwLock poison-on-expect pattern:**
- Risk: Production code uses `std::sync::Mutex` and `std::sync::RwLock` (not `tokio` variants) with `.expect("... poisoned")` throughout the attachments subsystem. If any thread panics while holding a lock, the process aborts on the next access.
- Files: `src/attachments/runtime.rs:126,146,217,228,240`, `src/attachments/registry.rs:255`
- Current mitigation: Comments assert "no panic holding it" — the invariant is maintained by convention, not enforcement.
- Recommendations: This is acceptable for a sidecar that should crash-restart on internal corruption, but document this as an intentional crash-on-corruption strategy. Consider `parking_lot::Mutex` which does not poison.

**TLS certificate unwrap in watcher:**
- Risk: `src/watcher.rs:69-70` calls `.unwrap()` on `tls.cert` and `tls.key` inside a match arm that already checked `has_cert` and `has_key` are true. Logically safe but structurally fragile — if the match guard logic changes, these unwraps could panic.
- Files: `src/watcher.rs:69-70`
- Current mitigation: The match arm `(true, true, _)` guarantees presence.
- Recommendations: Destructure with `if let` or use `let (Some(cert), Some(key)) = ...` pattern for compile-time safety.

## Performance Bottlenecks

**`std::sync::Mutex` used in async context:**
- Problem: `src/attachments/runtime.rs` and `src/attachments/ingest.rs` use `std::sync::Mutex` in code called from async tasks. Holding a `std::sync::Mutex` across an `.await` point would block the tokio runtime thread.
- Files: `src/attachments/runtime.rs:87`, `src/attachments/ingest.rs:150,248`
- Cause: `std::sync::Mutex` is fine if the critical section is short and never crosses an await. The current code appears to follow this pattern (lock, clone/update, drop).
- Improvement path: Audit that no `.await` is called while any `std::sync::Mutex` guard is held. If any are found, switch to `tokio::sync::Mutex`.

**Full `.clone()` of per-distribution progress on every snapshot:**
- Problem: `per_distribution_snapshot()` in `src/attachments/runtime.rs:123-128` clones the entire `Vec<PerDistributionProgress>` under lock on every subscribe call.
- Files: `src/attachments/runtime.rs:123-128`
- Cause: Convenience — clone under lock, release lock, return owned data.
- Improvement path: Low priority unless bundles with many files cause latency spikes on subscribe.

## Fragile Areas

**Attachment handler expect chains:**
- Files: `src/attachments/handlers.rs:138,248,252,332,336,822`
- Why fragile: Multiple `.expect()` calls rely on invariants maintained elsewhere (e.g., "file_distribution must be present when has_roots() is true"). If the invariant chain is broken by a refactor in `node.rs` or `config.rs`, these become runtime panics with no compile-time safety net.
- Safe modification: When changing attachment configuration flow or `SidecarNode` construction, verify that `file_distribution` is always `Some` when attachment RPCs are reachable. Consider returning `ConnectError` instead of panicking.
- Test coverage: Integration tests in `tests/attachments_*.rs` (7 test files) cover the happy path. Edge cases around misconfiguration are not tested.

**Base64 decode expect in Kubernetes discovery:**
- Files: `src/node.rs:751-753`
- Why fragile: `.expect("shared_key base64 validated during backend construction")` relies on validation having occurred earlier in the call chain. A code path that skips validation would panic at runtime.
- Safe modification: This is intentionally loud (see inline comment at lines 746-750). Acceptable if the upstream validation is well-tested.

## Scaling Limits

**Broadcast channel for attachment progress:**
- Current capacity: `broadcast::channel` capacity is set at construction (not inspected here, likely fixed).
- Limit: Slow subscribers will see `RecvError::Lagged` and lose progress frames. The `.expect("broadcast must deliver to live subscriber")` at `src/attachments/runtime.rs:301` would panic if the channel is closed.
- Scaling path: Use `tokio::sync::watch` for latest-state or handle `SendError` gracefully instead of panicking.

## Dependencies at Risk

No obviously unmaintained or problematic dependencies detected. The project uses `iroh 1.0.2` (stable) and tracks `peat-mesh`/`peat-protocol` release candidates closely.

## Missing Critical Features

No critical gaps identified from code analysis. The deferred test file `tests/attachments_deferred_test.rs` documents known feature gaps for the attachment subsystem but these appear tracked.

## Test Coverage Gaps

**mDNS discovery test is `#[ignore]`'d:**
- What's not tested: mDNS peer discovery end-to-end in CI.
- Files: `tests/mdns_test.rs:119`
- Risk: mDNS regressions won't be caught until bare-metal testing.
- Priority: Low — environment-specific, documented with clear rationale.

**Attachment misconfiguration paths:**
- What's not tested: Scenarios where attachment RPCs are called but `file_distribution` is `None` (which currently panics via `.expect()`).
- Files: `src/attachments/handlers.rs:138,252,336`
- Risk: A misconfigured deployment could cause a runtime panic instead of a graceful error.
- Priority: Medium — defensive error handling would be more robust than panic.

---

*Concerns audit: 2026-07-08*
