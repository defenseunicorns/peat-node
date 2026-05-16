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

/// PRD test 23 — `subscribe_emits_progress_then_terminal`.
///
/// Send a 4 MiB file, subscribe, assert at least one IN_PROGRESS frame
/// and exactly one terminal frame.
///
/// **Status after peat-protocol 0.9.0-rc.7 + peat-node receiver-side
/// node-status writes**: the IN_PROGRESS half is unblocked and lands
/// reliably end-to-end (the sender's progress watcher fires on the
/// receiver's first `Transferring` write into the distribution doc).
/// The exactly-one-terminal-frame half remains blocked on a separate
/// substrate-level race in peat-mesh:
///
/// The inbox watcher writes `Transferring` and (after a sub-second
/// 4 MiB local fetch) `Completed` into the same distribution doc back
/// to back. peat-mesh's automerge sync delivers the first change to
/// the sender's observer reliably, but the second update — when it
/// lands ~60 ms after the first — appears to be coalesced or
/// observer-debounced before the sender's watcher polls the doc
/// again. Direct probe at stall: the receiver's local doc has
/// `node_statuses[receiver] = Completed`, but the sender's
/// `IrohFileDistribution::status()` and the underlying doc on the
/// sender both still read `Transferring`. So the sender's
/// `subscribe_progress` stream stalls one frame short of terminal.
///
/// This race is below the peat-node layer: peat-mesh decides when to
/// fire observer events on inbound document deltas. A clean fix lives
/// upstream (separate-key Automerge map on `node_statuses` so each
/// receiver write produces a distinct change set, observer fan-out
/// guarantees per-change firing, or sender-side polling of the doc
/// independent of observer events). Tracking work continues outside
/// peat-node #75.
///
/// What this PR did still accomplish for this test:
///   1. peat-protocol `0.9.0-rc.7` ships the sender-side watcher that
///      consumes the receiver's writes (closes peat#864 — confirmed
///      via the peat-protocol e2e suite).
///   2. peat-node's `attachments::inbox` now writes Transferring +
///      Completed into the distribution doc on every delivery
///      (confirmed via `RUST_LOG=peat_node::attachments=debug` —
///      both writes complete, sender observes Transferring frame).
#[tokio::test]
#[ignore = "blocked on peat-mesh substrate observer/sync coalescing for back-to-back receiver doc writes (the Transferring → Completed gap)"]
async fn subscribe_emits_progress_then_terminal() {}

/// PRD test 24 — `cancel_in_flight_stops_transfer`.
///
/// Start a large transfer, cancel mid-flight, assert status flips to
/// CANCELLED within 1s.
///
/// **Status after peat-protocol 0.9.0-rc.7**: the sender-side
/// observability piece of this contract works in isolation — `cancel()`
/// flips the doc to "cancelled", `broadcast_progress` emits a terminal
/// CANCELLED frame, and the channel drops. The remaining blocker is a
/// **bandwidth-controlled receiver fixture**: no in-tree way to throttle
/// a single peer's iroh-blob fetch to keep the transfer in-flight long
/// enough to issue Cancel and verify the flip within 1s. A real fixture
/// needs either a mock `NetworkedIrohBlobStore` implementing throttled
/// fetch, OS-level traffic shaping (`tc netem`, brittle in CI), or a
/// tokio sleep injection into the receiver's `BlobStore::fetch_blob` path.
/// All three are non-trivial design decisions deferred from this PR.
#[tokio::test]
#[ignore = "needs a bandwidth-controlled receiver fixture (peat-node-only design decision)"]
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
/// **Status after peat-protocol 0.9.0-rc.7 + this PR**: the
/// IN_PROGRESS half is partially driveable (sender observes at least
/// one IN_PROGRESS frame from the receiver's Transferring write,
/// modulo the back-to-back-write race documented on test 23). The
/// FAILED half still needs a deterministic way to drive a single
/// distribution to FAILED — peat-protocol's
/// `IrohFileDistribution::distribute` does not expose fault injection.
/// A clean v2 mechanism is needed (test-only hook or a deterministic
/// timeout flag on the distribution document).
///
/// `attachments_subscribe_test::subscribe_after_terminal_emits_snapshot_then_eof`
/// covers the "all-terminal at subscribe time" half (with two Completed
/// distributions instead of Completed+Failed). The mixed-state ordering
/// (snapshot before live) is exercised in unit tests on the
/// `StreamCloser` adapter and `BundleRuntime::per_distribution_snapshot`.
#[tokio::test]
#[ignore = "needs a fault-injection hook to drive a single distribution to FAILED"]
async fn subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight() {}
