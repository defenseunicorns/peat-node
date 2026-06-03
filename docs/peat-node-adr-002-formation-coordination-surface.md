# peat-node-ADR-002: Where Formation & Leadership Coordination Runs — Substrate Client vs. Coordination Runtime

**Status:** Proposed
**Date:** 2026-06-03
**Authors:** Kit Plummer, Claude
**Relates To:**
- peat-node-ADR-001: `peat` — Operator CLI as a Peat Node
- ADR-024: Flexible Hierarchy Strategies for Adaptive Mesh Organization
- ADR-043: Consumer Interface Adapters (Compatibility Layer)
- ADR-066: Abstract Hierarchy Vocabulary
- ADR-005 (Data Sync Abstraction), ADR-006 (Security/Auth), ADR-021 (Document-Oriented Architecture)
- Developer guide: `peat/docs/guides/developer/FORMATION_AND_LEADERSHIP.md`

---

## Executive Summary

Two usage patterns have emerged for Peat:

1. **Deep integration.** A Rust application links `peat-protocol` and *drives* the cell
   lifecycle itself — `LeaderElectionManager`, `CellCoordinator`, `RoleScorer`,
   `LeadershipScore` — then publishes the resulting state (members, `leader_id`, roles)
   as CRDT documents. This is what the `FORMATION_AND_LEADERSHIP` developer guide describes.
2. **Operator / integration.** `peat-node` (the sidecar) and `peat` (the CLI) are the
   surface; the system is operated, not coded against.

An investigation of the current code establishes a hard boundary: **`peat-node` is a pure
sync *substrate*.** Its runtime instantiates only `AutomergeBackend` (CRDT document sync)
and `IrohFileDistribution`; it has **zero** references to `peat_protocol::cell::*`. The
gRPC surface is generic document CRUD / observe / sync (the `Cell` typed-collection RPC
stores a `Cell` *document* with `leader_id`/`formation_id` as plain data fields — it does
not run election). `peat-cli` likewise can **join a formation, author, and observe** cell
documents, but cannot *decide*: it has no election, scoring, formation-gating, or failover.

So today, automated formation and leadership are available **only** to deep-integration
(Rust) consumers. For operators there is no turnkey path — only declarative document
authoring, with policy supplied externally.

peat-node-ADR-001 deliberately left this open ("future operator surfaces … get evaluated
on their own merits and against ADR-043; the answer may be `peat-gateway`, a dedicated
repo, or here"). **This ADR makes that call**: it frames the decision between keeping
Peat a *substrate + full-featured droppable client* (policy external) versus providing a
*first-party coordination runtime* that is "more than `peat-cli`", and recommends a posture.

## Context

### What `peat-cli` can and cannot do against the formation/leadership lifecycle

| Lifecycle step | `peat-cli` today |
|---|---|
| Discovery | ✅ mDNS + creds-bundle `peers` |
| Form & authenticate (join) | ✅ creds (`app_id`/`shared_key`) → formation-key handshake |
| Leader **election** | ❌ writing `leader_id` stores a value; it does not run the scoring/announce state machine |
| Role assignment | ⚠️ can *write* role fields; no `RoleScorer`/enforcement |
| Formation completion | ⚠️ can write/observe state; no `CellCoordinator` gating (size/readiness/approval) |
| Failover / re-election | ❌ no liveness/heartbeat/re-election; can *observe* a `leader_id` change, not cause one |

`peat-cli` is excellent at the **substrate** half (join + author + observe). The
**decision** half (who leads, role fit, formation-complete, failover) lives in
`peat-protocol` and runs only inside a Rust process that drives it.

### A constraint that shapes every option

Cell coordination in Peat is **deterministic and CRDT-based by design** (capability score
+ lexicographic tie-break for the leader; LWW `leader_id`; OR-Set membership; no
consensus round-trips — see ADR-024 and the developer guide). It is built so that *every
node computes the same result independently*. This is a direct expression of the ecosystem
invariants: **serverless / peer-equal** and **interoperability-first ("Peat must never
require a counterpart to run Peat software")**. Any "coordination runtime" we add must
therefore run **per-node, co-located** — it must never become a central orchestrator/server
that other nodes depend on. That rules out the obvious-but-wrong "a controller service
decides leaders for the formation" shape.

## Decision Drivers

- **Charter clarity.** peat-node-ADR-001 fixed peat-node's charter as the in-cluster
  substrate sidecar. Growing it into an orchestrator is a charter change, not a feature.
- **Reach without Rust.** Operators and non-Rust integrators currently have no turnkey
  automated formation/leadership. Is that a gap to close, or correct?
- **One canonical implementation.** The deterministic coordination logic should not be
  re-implemented per consumer (divergent leaders across heterogeneous nodes is a
  correctness failure). Reuse of `peat-protocol::cell` is strongly preferred over re-writes.
- **Surface & blast radius.** A coordination runtime is long-running, holds opinions, and
  writes authoritative state — a larger operational and security surface than a CRUD client.
- **Peer-equality.** Whatever we build must stay serverless and co-located (see above).

## Considered Options

### Option A — Substrate + full-featured *droppable client* (policy external)

Formalize the status quo: `peat-node` stays a pure sync substrate; `peat` (CLI) is the
feature-complete **client** for substrate operations (join, CRUD, observe, query, schema).
Automated coordination remains a **deep-integration** concern — the host system either
embeds `peat-protocol` and runs the cell runtime, or implements its own policy and writes
decisions back as documents. The operator experience is **declarative**: author
`cell-states`/`cell-configs`, observe convergence.

- **Pros:** Charter intact. Maximally interoperable and serverless. Smallest surface; no
  new daemon to secure. `peat-cli` stays thin and genuinely droppable.
- **Cons:** No turnkey self-organization for non-Rust shops. The canonical coordination
  logic isn't reused by operators — host systems risk re-implementing election subtly
  differently (divergence). "Drop it in and it forms cells" is not available.

### Option B — First-party coordination runtime *inside* `peat-node` (opt-in mode)

Add an opt-in mode to the sidecar that instantiates `LeaderElectionManager` /
`CellCoordinator` / `RoleScorer` and drives cell coordination automatically (configured via
documents/flags), running per-node alongside the existing sync.

- **Pros:** Turnkey — deploy peat-node and it self-organizes (peer-to-peer, reusing the
  canonical deterministic library, so no divergence). No Rust required by operators.
- **Cons:** **Charter change** — peat-node becomes a coordination runtime, not just a
  substrate. Larger attack/operational surface in the load-bearing sidecar. Couples the
  sidecar to one organizing scheme (ADR-024 strategies would need surfacing as config).
  Scope-creep gravity (routing/QoS policy next?). Cuts against ADR-001's explicit scope
  discipline.

### Option C — Separate, co-located coordination component ("more than `peat-cli`")

A distinct first-party artifact (a `peat`-family coordination runner, or an ADR-043
adapter / `peat-gateway` capability) that runs the coordination runtime **next to** each
node, keeping `peat-node` a pure substrate. Deployed per-node, never centrally.

- **Pros:** Turnkey coordination *without* changing peat-node's charter. Clean separation
  (substrate vs. orchestration); each evolves independently. Natural home for richer
  operator features later (policy, approval UIs). Matches ADR-001's "evaluate against
  ADR-043 — may be `peat-gateway` or a dedicated repo."
- **Cons:** Another artifact to build, secure, release. Two processes where one might do.
  Must be disciplined to stay **per-node and peer-equal** — the failure mode is it quietly
  becomes a central controller.

### Option D — Orchestration subcommands in `peat-cli` (rejected)

e.g. `peat form` / `peat elect` that run the loop while attached. Rejected: the CLI's
charter (ADR-001) is join → act → exit; a coordinator is long-running and would make the
CLI a daemon — which *is* Option B or C wearing a CLI hat. Keep the CLI ephemeral.

## Decision (recommended — open for Kit's call)

**Adopt Option A now; choose Option C (not B) if and when turnkey automated coordination
for non-Rust operators becomes a committed requirement.**

1. **Now:** Formalize `peat-node` as the substrate and `peat-cli` as the full-featured
   droppable substrate client. Ship the operator guide as a **declarative formation-admin**
   how-to (join → observe `cell-states` → author members/designated leader/roles → watch
   convergence), with an explicit "this does not run election/role-scoring/failover — see
   the developer guide / deep integration" boundary. Automated coordination = deep
   integration via `peat-protocol::cell`.
2. **Later (if required):** Provide automated coordination as a **separate, per-node,
   co-located component** (Option C), evaluated under ADR-043, reusing the canonical
   `peat-protocol::cell` runtime — explicitly **not** by loading it into the sidecar
   (Option B), and explicitly **not** as a central service.

Rationale: Option A honors the charter, ships immediately, and stays maximally
interoperable/serverless. Reserving automation for Option C keeps the substrate pure,
keeps the coordination runtime peer-equal and independently evolvable, and reuses one
canonical implementation. Option B is the tempting shortcut whose cost (charter change +
surface growth in the load-bearing sidecar) is the highest and hardest to walk back.

This is a genuine product-direction decision; the recommendation is a proposal, not a
ruling. The binary the question posed — "a full-featured app/service beyond `peat-cli`"
(B/C) vs. "`peat-cli`/`peat-node` as a full-featured droppable client" (A) — resolves to:
**droppable client now, separate co-located runtime later if/when needed.**

## Consequences

- **If accepted as recommended:** The pending operator guide is written against Option A
  (declarative admin). No charter change. A roadmap item is opened for the Option-C
  coordination component, deferred until there is a committed non-Rust automation
  requirement. The deep-integration path remains the supported route to automated
  formation/leadership in the meantime.
- **If B is chosen instead:** peat-node-ADR-001's charter statement is amended; the sidecar
  gains a coordination mode, its security review expands, and ADR-024 strategy selection
  must be exposed as sidecar configuration.
- **Either way:** the deterministic, peer-equal coordination design (ADR-024) is the source
  of truth; no option re-implements election outside `peat-protocol::cell`.

## Open Questions

1. Is turnkey automated formation for non-Rust operators a *committed* requirement, or is
   declarative admin + deep integration sufficient for the foreseeable roadmap?
2. If Option C: dedicated repo/binary vs. an ADR-043 `peat-gateway` capability?
3. How does a co-located coordination component authenticate its authority to write
   authoritative cell state, given ADR-006 Layer-1 identity is still deferred (peat#941)?
4. Does the operator guide ship now under Option A regardless, with a forward pointer to
   this ADR's outcome?

## References

- peat-node-ADR-001 — `peat` operator CLI; charter + scope-discipline statement.
- ADR-024 — flexible hierarchy strategies (the coordination runtime this concerns).
- ADR-043 — consumer interface adapters / compatibility layer (candidate home for Option C).
- ADR-066 — Cell/Cohort/Federation/Coalition vocabulary.
- `peat/docs/guides/developer/FORMATION_AND_LEADERSHIP.md` — the deep-integration flow.
- Investigation (2026-06-03): peat-node runtime instantiates only `AutomergeBackend` +
  `IrohFileDistribution`; no `peat_protocol::cell` usage; gRPC surface is generic
  document/peer/sync/subscribe + typed-collection CRUD.
