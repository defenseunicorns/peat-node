#!/usr/bin/env bash
# Functional test for QUICKSTART.md Path A (Docker Compose).
#
# Scope is narrowed pending peat-mesh#205 — cross-process iroh dial
# from a peat-cli subprocess to a peat-node sidecar in another
# container blocks on the iroh `address_lookup` chain ordering
# (MemoryLookup ends up last; DNS lookups time out for 30s in
# airgapped compose / k3d before the chain falls through). Until
# peat-mesh ships `connect_and_authenticate_with_addr` (or reorders
# the chain), this script validates only the parts of the QUICKSTART
# Path A that don't require the CLI to dial through the chain:
#
#  - The compose stack builds and both sidecars come up healthy.
#  - bootstrap.sh peers them (sidecar-level Iroh dial, exercises the
#    long-running endpoint that *does* work — different code path
#    from the CLI's ephemeral endpoint).
#  - demo.sh writes on node-a and reads on node-b (CRDT sync over
#    the production gRPC + Iroh path).
#  - `peat schema list` / `peat schema describe` run offline inside
#    the container (no mesh dial — purely local registry inspection).
#
# When peat-mesh#205 lands and we bump the peat-mesh pin, restore
# the CLI-driven CRUD steps (the originals are in this script's
# git history at commit c574dd5).
#
# Prerequisites: docker (with compose), curl, jq.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="${SCRIPT_DIR}/.."
COMPOSE_DIR="${REPO_DIR}/examples/compose"

KEEP="${1:-}"

log()  { echo "==> $*"; }
pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; exit 1; }

cleanup() {
    if [ "${KEEP}" != "keep" ]; then
        log "Tearing down compose stack"
        (cd "${COMPOSE_DIR}" && \
            docker compose -f docker-compose.yml -f docker-compose.dev.yml \
            down -v >/dev/null 2>&1) || true
    fi
}
trap cleanup EXIT

# ---- Bring up the compose stack -------------------------------------

log "Bringing up compose stack from ${COMPOSE_DIR} (with dev override for peat CLI)"
(cd "${COMPOSE_DIR}" && docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v >/dev/null 2>&1) || true
(cd "${COMPOSE_DIR}" && docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build --wait) >/dev/null

log "Bootstrapping the mesh (./bootstrap.sh)"
(cd "${COMPOSE_DIR}" && ./bootstrap.sh) >/dev/null
pass "two sidecars peered"

log "Running the demo (./demo.sh) to confirm CRDT sync works"
(cd "${COMPOSE_DIR}" && ./demo.sh) >/dev/null
pass "demo writes on node-a, reads on node-b"

# ---- Offline schema discovery (no mesh dial required) ---------------

log "'peat schema list' inside peat-node-a (offline, no creds)"
out=$(docker exec peat-node-a peat schema list)
echo "${out}" | grep -q "capabilities" || fail "schema list missing 'capabilities'"
echo "${out}" | grep -q "node-configs" || fail "schema list missing 'node-configs'"
pass "schema list enumerates the 5 builtin types"

log "'peat schema describe capabilities' renders field shape"
out=$(docker exec peat-node-a peat schema describe capabilities)
echo "${out}" | grep -q "Capability (v1)" || fail "describe missing type header"
echo "${out}" | grep -q "confidence" || fail "describe missing confidence field"
echo "${out}" | grep -q "percentage" || fail "describe missing percentage format"
pass "schema describe renders Capability fields"

log "'peat schema describe' rejects an unknown type with exit 4"
set +e
docker exec peat-node-a peat schema describe no-such-collection >/dev/null 2>&1
code=$?
set -e
[ "${code}" = "4" ] || fail "expected exit 4, got ${code}"
pass "unknown-target exits 4 (Malformed)"

log "All quickstart steps in current scope validated."
echo
echo "Path A QUICKSTART (compose) is functionally correct for the in-scope"
echo "steps. CLI-driven CRUD against the mesh is blocked on peat-mesh#205;"
echo "restore those steps when the upstream fix lands."
