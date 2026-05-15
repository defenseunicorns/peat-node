//! PRD-006 §Testing Plan tests that need functionality or fault-injection
//! hooks the v1 attachment surface doesn't yet expose. Each test is
//! `#[ignore]`'d with the exact gap noted so the moment the gap closes,
//! dropping the attribute exercises the contract end-to-end.
//!
//! Tests covered by other files:
//!
//! - 20 → `attachments_smoke_test::attachments_disabled_when_no_root`
//! - 21 → `attachments_multi_peer_test::receiver_can_fetch_blob_pushed_by_sender`
//!   covers the substrate; `attachments_e2e_test::end_to_end_attachment_delivery_two_nodes`
//!   covers the full sender→inbox path
//! - 22 → `attachments_e2e_test::node_list_scope_only_delivers_to_listed_nodes`
//! - 26, 27, 30 → `attachments_acceptance_test`
//! - 28 → `attachments_subscribe_test`
//!
//! Tests deferred here: 23, 24, 25, 29.
//!
//! # Upstream gaps
//!
//! Tests 23 and 24 share a single upstream gap: peat-protocol's
//! `IrohFileDistribution` creates the `subscribe_progress` broadcast
//! channel but never publishes to it — `broadcast_progress()` in
//! `peat-protocol/src/storage/file_distribution.rs` is `#[allow(dead_code)]`
//! and has no callers. Tracked at
//! <https://github.com/defenseunicorns/peat/issues/864>. Once that lands,
//! the receive-side already in `attachments::inbox` (peat-node #65)
//! drives real per-peer progress, and both tests un-ignore.

/// PRD test 23 — `subscribe_emits_progress_then_terminal`.
///
/// Send a 4 MiB file, subscribe, assert at least one IN_PROGRESS frame
/// and exactly one terminal frame.
///
/// **Gap:** peat-protocol's `IrohFileDistribution::distribute` creates
/// the `subscribe_progress` broadcast channel but never publishes to
/// it (`broadcast_progress()` is `#[allow(dead_code)]`). Receive-side
/// auto-pull works (peat-node `attachments::inbox`, #65), so receivers
/// do fetch — but their fetch progress is invisible to the sender's
/// subscribe stream. Tracked upstream at
/// <https://github.com/defenseunicorns/peat/issues/864>.
///
/// The zero-peer terminal-frame half of the contract is already
/// covered by
/// `attachments_subscribe_test::subscribe_zero_peer_distribution_closes_after_terminal_frame`
/// via the watcher's initial-status short-circuit.
#[tokio::test]
#[ignore = "blocked on peat#864: IrohFileDistribution::broadcast_progress is dead code, no peer-side updates emit"]
async fn subscribe_emits_progress_then_terminal() {}

/// PRD test 24 — `cancel_in_flight_stops_transfer`.
///
/// Start a large transfer, cancel mid-flight, assert status flips to
/// CANCELLED within 1s.
///
/// **Gap:** auto-pull on receivers landed in peat-node #65, but the
/// sender-observable in-flight state still doesn't exist — same root
/// cause as test 23 (peat#864). Without sender-side progress frames
/// landing on the subscribe stream, the watcher idles in
/// subscribe_progress waiting for events that never come. Cancel on
/// the registry side does work today (unit-tested via the
/// `BundleStatus::Cancelled` path), but the proto status surfacing the
/// transition observably requires real progress frames to interrupt.
///
/// Once peat#864 lands, this test sends a 100+ MiB blob to a peer
/// throttled to a few Mbps and verifies Cancel within 1s.
#[tokio::test]
#[ignore = "blocked on peat#864 (sender-observable in-flight state) + bandwidth-controlled receiver fixture"]
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
/// **Gap:** two pieces are missing. (1) The IN_PROGRESS half needs
/// sender-observable progress frames — same upstream block as test 23
/// (peat#864). (2) The FAILED half needs a deterministic way to drive
/// a single distribution to FAILED. peat-protocol's
/// `IrohFileDistribution::distribute` doesn't expose fault injection;
/// a clean v2 mechanism is needed (test-only hook or a deterministic
/// timeout flag on the distribution document).
///
/// `attachments_subscribe_test::subscribe_after_terminal_emits_snapshot_then_eof`
/// covers the "all-terminal at subscribe time" half (with two Completed
/// distributions instead of Completed+Failed). The mixed-state ordering
/// (snapshot before live) is exercised in unit tests on the
/// `StreamCloser` adapter and `BundleRuntime::per_distribution_snapshot`.
#[tokio::test]
#[ignore = "blocked on peat#864 (IN_PROGRESS frames) + fault-injection hook for FAILED"]
async fn subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight() {}
