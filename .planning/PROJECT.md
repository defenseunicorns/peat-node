# Native Core NATS Bridge for peat-node

## What This Is

Add a production-ready, native Core NATS bridge to peat-node. Each sidecar will consume JSON messages from its local NATS instance, store each message as an immutable document in a subject-mapped Peat collection, synchronize that document over the existing Peat mesh, and publish synchronized documents to its local NATS instance.

The first deployment target is a Jetson Orin Nano producing simulated vision-summary frames every 30 seconds and a second machine receiving the identical frames through its own local NATS server. The two NATS servers remain independent; Peat is the only cross-machine transport.

## Core Value

An application can exchange every local Core NATS message with a distant, independent NATS instance through the Peat mesh without changing the message JSON or creating a local publish/consume loop.

## Requirements

### Validated

- ✓ peat-node exposes Connect/gRPC document CRUD and server-streaming subscriptions — existing
- ✓ peat-node persists Automerge-backed JSON documents and synchronizes them peer-to-peer through peat-mesh/Iroh — existing
- ✓ peat-node propagates local and mesh-received store changes through an origin-aware fanout path — existing
- ✓ peat-node provides configuration via CLI flags and `PEAT_NODE_*` environment variables — existing

### Active

- [ ] Configure one or more Core NATS subscription subjects and their corresponding Peat collections.
- [ ] Consume JSON messages from each configured local NATS subject and store every message as a new Peat document with a generated ID.
- [ ] Preserve the received JSON payload exactly when storing and publishing it.
- [ ] Publish documents received from remote Peat peers to the matching local Core NATS subject.
- [ ] Prevent a bridge from re-consuming its own NATS publication while allowing the remote bridge to persist an identical document.
- [ ] Provide operational configuration, lifecycle handling, logging, and failure behavior appropriate for a native sidecar feature.
- [ ] Verify an end-to-end two-node deployment in which the Jetson-originated simulated frame arrives on the remote NATS server every 30 seconds.

### Out of Scope

- JetStream streams, consumers, durable delivery, replay, or acknowledgements — this milestone is intentionally Core NATS pub/sub.
- NATS-to-NATS federation, clustering, or using NATS as the cross-machine transport — Peat remains the only inter-node link.
- Transforming, validating against a vision-specific schema, aggregating, or deduplicating frame payloads — the bridge must relay arbitrary JSON identically.
- Per-subject authorization and tenant policy — current Peat formation-key authorization is the repository-wide boundary.
- Changes to peat-mesh transport or CRDT synchronization semantics — those are upstream responsibilities.

## Context

- peat-node is a Rust/Tokio Peat mesh sidecar. `SidecarNode` composes Automerge storage, peat-mesh/Iroh synchronization, encryption, and origin-aware change fanout; `PeatSidecarService` provides the external RPC boundary.
- Existing document changes are broadcast through node fanout, including mesh-received writes; source-origin exclusion prevents synchronization echo. The NATS bridge must use equivalent provenance to avoid publishing a local message into a subscription that immediately ingests it again.
- Core NATS uses ephemeral pub/sub semantics. Delivery/retry guarantees are therefore bounded by the local NATS connection and must be stated accurately in configuration and operations documentation.
- The desired payload is ordinary JSON, such as a timestamped vision summary with a source `node_id`; message identity is not derived from payload fields—each incoming message receives a fresh ID.
- The deployed topology contains one NATS server co-located with each peat-node instance. The servers do not connect to one another; Peat mesh synchronization carries bridge documents between nodes.

## Constraints

- **Runtime**: Integrate natively in Rust/Tokio with peat-node's lifecycle and configuration conventions — the feature runs in the existing sidecar process.
- **NATS mode**: Core NATS pub/sub only — requested deployment does not require JetStream.
- **Message integrity**: JSON must be stored and republished identically — downstream consumers rely on the original frame content.
- **Identity**: Generate a fresh document ID per NATS input message — messages are unique even when payload fields overlap.
- **Loop prevention**: A node must not ingest NATS publications it produced itself — avoids unbounded local duplicate creation.
- **Topology**: Independent local NATS instances connected only through Peat — validates the intended edge-to-edge architecture.
- **Compatibility**: Preserve the existing proto contract unless a new RPC is demonstrably needed — bridge operation should be configuration-driven.
- **Dependency safety**: Do not disturb the locked peat-mesh/Iroh/Automerge version relationship.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use Core NATS rather than JetStream | The requested transport is a simple pub/sub bridge with 30-second frames; persistence and acknowledgements are not needed for this milestone. | — Pending |
| Map each NATS subject to a Peat collection | Subjects define the bridge routing boundary while collections provide replicated storage. | — Pending |
| Generate a fresh Peat document ID for each NATS input | Every incoming message is unique and must remain individually represented. | — Pending |
| Preserve payload JSON without transformation | The remote NATS consumer must observe the same data as at the source. | — Pending |
| Run one NATS server per Peat node | The Peat mesh, not NATS federation, is the cross-machine transport. | — Pending |
| Suppress local self-reconsumption using bridge provenance | Prevent loops without stopping the distant side from storing and publishing the replicated document. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-07-14 after initialization*
