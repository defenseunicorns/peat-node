//! PRD-006 §Testing Plan tests that need functionality or fault-injection
//! hooks the v1 attachment surface doesn't yet expose. Each test is
//! `#[ignore]`'d with the exact gap noted so the moment the gap closes,
//! dropping the attribute exercises the contract end-to-end.
//!
//! Tests covered by other files:
//!
//! - 20 → `attachments_smoke_test::attachments_disabled_when_no_root`
//! - 21 → `attachments_multi_peer_test` (also #[ignore]'d, different gap)
//! - 26, 27, 30 → `attachments_acceptance_test`
//! - 28 → `attachments_subscribe_test`
//!
//! Tests deferred here: 22, 23, 24, 25, 29.

/// PRD test 22 — `send_node_list_only_delivers_to_listed`.
///
/// Three-node cluster; scope = `NodeList{[node_b]}`. After sender reports
/// COMPLETED, assert node_b has the blob and node_c does not.
///
/// **Gap:** the receive-side pull (PRD test 21 commentary) is required
/// for `blob_exists_locally` to return true on node_b without an explicit
/// manual fetch_blob call. Without it, neither node_b nor node_c sees
/// the blob — the test would pass vacuously on node_c but fail on
/// node_b. Once receive-side observer hooks ship, the NodeList filter
/// in `IrohFileDistribution::resolve_targets` (already implemented)
/// constrains which receivers attempt the pull, and this test asserts
/// the filter behavior end-to-end.
#[tokio::test]
#[ignore = "needs peat-protocol receive-side observer hooks (same gap as test 21)"]
async fn send_node_list_only_delivers_to_listed() {}

/// PRD test 23 — `subscribe_emits_progress_then_terminal`.
///
/// Send a 4 MiB file, subscribe, assert at least one IN_PROGRESS frame
/// and exactly one terminal frame.
///
/// **Gap:** for IN_PROGRESS to fire, a receiver must be actively
/// downloading from the sender, which requires (a) connected peers and
/// (b) the receive-side auto-pull from the distribution document. v1
/// receivers don't auto-pull, so the sender's progress channel never
/// observes a peer-side update. The zero-peer scenario in
/// `attachments_subscribe_test::subscribe_zero_peer_distribution_closes_after_terminal_frame`
/// covers the terminal-frame half of the contract via the watcher's
/// initial-status zero-target short-circuit.
#[tokio::test]
#[ignore = "needs peat-protocol receive-side observer hooks to drive sender-side progress"]
async fn subscribe_emits_progress_then_terminal() {}

/// PRD test 24 — `cancel_in_flight_stops_transfer`.
///
/// Start a large transfer, cancel mid-flight, assert status flips to
/// CANCELLED within 1s.
///
/// **Gap:** "mid-flight" requires a measurable in-flight window —
/// without receivers actively pulling, the watcher idles in
/// subscribe_progress waiting for events that never come, and there is
/// no real "in-flight" state to interrupt. Cancel against a watcher in
/// that idle state does fire on the registry side (the unit-tested
/// `BundleStatus::Cancelled` path), but the proto status surfacing the
/// transition observably requires a real transfer to interrupt.
///
/// Once auto-pull lands, this test sends a 100+ MiB blob to a peer
/// throttled to a few Mbps and verifies Cancel within 1s.
#[tokio::test]
#[ignore = "needs (a) receive-side pull and (b) bandwidth-controlled receiver to create a measurable in-flight window"]
async fn cancel_in_flight_stops_transfer() {}

/// PRD test 25 — `unknown_node_id_marked_failed_after_grace`.
///
/// `NodeList{[nonexistent]}`; assert that after `discovery_grace_secs`,
/// per-node status is FAILED.
///
/// **Gap:** the grace-period mechanism is not yet implemented. v1
/// records `--attachment-discovery-grace-secs` as a config knob but
/// there is no background task that scans pending NodeList targets for
/// unresolved IDs and promotes them to FAILED. Currently a
/// `NodeList{[nonexistent]}` ingest succeeds and the resulting
/// distribution sits idle with empty node_statuses indefinitely.
///
/// Implementation outline for the follow-up: spawn a per-bundle grace
/// timer on send; when it fires, walk `IrohFileDistribution::status`,
/// compute the set of declared-but-unconnected targets, and synthesise
/// FAILED entries into the runtime via `apply_progress`. The watcher's
/// terminal counter then drives `maybe_finalize_bundle` as today.
#[tokio::test]
#[ignore = "needs the --attachment-discovery-grace-secs background task (not yet implemented in v1)"]
async fn unknown_node_id_marked_failed_after_grace() {}

/// PRD test 29 — `subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight`.
///
/// Bundle with one distribution driven to FAILED via fault injection
/// while a second is still IN_PROGRESS; subscribe; assert snapshot
/// frame for the terminal one then live frames for the in-flight one.
///
/// **Gap:** two pieces are missing. (1) The IN_PROGRESS half needs a
/// real transfer to a real peer — same gap as test 23. (2) The FAILED
/// half needs a deterministic way to drive a single distribution to
/// FAILED. peat-protocol's `IrohFileDistribution::distribute` doesn't
/// expose fault injection; a clean v2 mechanism is needed (test-only
/// hook or a deterministic timeout flag on the distribution document).
///
/// `attachments_subscribe_test::subscribe_after_terminal_emits_snapshot_then_eof`
/// covers the "all-terminal at subscribe time" half (with two Completed
/// distributions instead of Completed+Failed). The mixed-state ordering
/// (snapshot before live) is exercised in unit tests on the
/// `StreamCloser` adapter and `BundleRuntime::per_distribution_snapshot`.
#[tokio::test]
#[ignore = "needs (a) real transfer for IN_PROGRESS and (b) fault-injection hook for FAILED"]
async fn subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight() {}
