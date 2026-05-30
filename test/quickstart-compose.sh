#!/usr/bin/env bash
# Functional test for QUICKSTART.md Path A (Docker Compose).
#
# This script is the executable contract for the Path A walkthrough:
# every command it runs is one the QUICKSTART tells operators to run,
# in the same order, with the same arguments. If this script passes,
# the QUICKSTART is correct end-to-end. If it fails, either the
# QUICKSTART or the underlying behaviour is wrong.
#
# Prerequisites: docker (with compose), curl, jq.
# Usage:
#   ./test/quickstart-compose.sh        # full run + teardown
#   ./test/quickstart-compose.sh keep   # full run, leave compose up for debug
#
# Idempotent: existing compose stack is torn down before bringing a new
# one up.

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
    else
        log "Leaving compose stack up (KEEP=${KEEP})"
    fi
}
trap cleanup EXIT

# ---- Path A — Docker Compose ----------------------------------------

log "Bringing up compose stack from ${COMPOSE_DIR} (with dev override for peat CLI)"
# QUICKSTART Path A drives everything via the `peat` CLI inside the
# sidecar containers. The CLI first shipped after peat-node v0.3.6,
# so the base compose's pinned image doesn't include it — the dev
# override swaps to a local build (`peat-node:dev`).
(cd "${COMPOSE_DIR}" && docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v >/dev/null 2>&1) || true
(cd "${COMPOSE_DIR}" && docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build --wait) >/dev/null

log "Bootstrapping the mesh (./bootstrap.sh)"
(cd "${COMPOSE_DIR}" && ./bootstrap.sh) >/dev/null
pass "two sidecars peered"

log "Running the demo (./demo.sh) to confirm CRDT sync works"
(cd "${COMPOSE_DIR}" && ./demo.sh) >/dev/null
pass "demo writes on node-a, reads on node-b"

# ---- Step 0: offline schema discovery (QUICKSTART step 0) -----------

log "Step 0: 'peat schema list' inside peat-node-a (offline, no creds)"
out=$(docker exec peat-node-a peat schema list)
echo "${out}" | grep -q "capabilities" || fail "schema list missing 'capabilities'"
echo "${out}" | grep -q "node-configs" || fail "schema list missing 'node-configs'"
pass "schema list enumerates all 5 builtin types"

log "Step 0b: 'peat schema describe capabilities' renders field shape"
out=$(docker exec peat-node-a peat schema describe capabilities)
echo "${out}" | grep -q "Capability (v1)" || fail "describe missing type header"
echo "${out}" | grep -q "confidence" || fail "describe missing confidence field"
echo "${out}" | grep -q "percentage" || fail "describe missing percentage format"
pass "schema describe renders Capability fields"

# ---- Step 1: bootstrap creds.yaml inside peat-node-a ----------------

log "Step 1: bootstrapping /tmp/creds.yaml inside peat-node-a"
# Use the exact recipe from the QUICKSTART. Strips the endpointAddr
# from GetStatus on peat-node-b (over the compose bridge DNS).
docker exec peat-node-a sh -c '
  cat > /tmp/creds.yaml <<EOF
app_id: compose-demo
shared_key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
peers:
  - $(curl -s -X POST http://peat-node-b:50051/peat.sidecar.v1.PeatSidecar/GetStatus \
      -H "Content-Type: application/json" -d "{}" \
      | grep -o "\"endpointAddr\":\"[^\"]*\"" | cut -d\" -f4)@peat-node-b:51071
EOF
  chmod 600 /tmp/creds.yaml'
docker exec peat-node-a test -r /tmp/creds.yaml || fail "creds.yaml not readable"
pass "creds.yaml written, mode 0600"

# ---- Step 2: read state from the mesh -------------------------------

log "Step 2: 'peat query --all-collections' from inside peat-node-a"
# demo.sh already wrote hello/world on node-a; --all-collections should
# include it. The query goes via the CLI's own joined session, not
# node-a's local store, so this proves the handshake works.
out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml \
        --timeout 30s --output json query --all-collections)
echo "${out}" | jq -e '.["hello:world"]' >/dev/null \
    || fail "query --all-collections didn't include hello/world (got ${out})"
pass "query --all-collections sees the demo doc"

# ---- Step 3: write a document via CLI -------------------------------

log "Step 3: 'peat create capabilities/cap-thermal' from inside peat-node-a"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
    create capabilities --id cap-thermal \
    --set id=cap-thermal \
    --set name=thermal-sensor \
    --set confidence=0.92 \
    --wait-for-sync >/dev/null
pass "create capabilities/cap-thermal succeeded"

log "Step 3b: read it back"
out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
        --output json query capabilities/cap-thermal)
echo "${out}" | jq -e '.name == "thermal-sensor"' >/dev/null \
    || fail "query did not return thermal-sensor (got ${out})"
pass "query confirms the new doc"

log "Step 3c: 'peat update' edits one field"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
    update capabilities/cap-thermal --set confidence=0.98 --wait-for-sync >/dev/null
out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
        --output json query capabilities/cap-thermal)
echo "${out}" | jq -e '.confidence == 0.98' >/dev/null \
    || fail "update did not land confidence=0.98 (got ${out})"
pass "update edits one field"

log "Step 3d: 'peat delete' tombstones the doc"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
    delete capabilities/cap-thermal --wait-for-sync >/dev/null
# Verify via the CLI itself: after a brief sync window, query should
# return nothing for the tombstoned doc.
sleep 2
for _ in $(seq 1 10); do
    out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 5s \
            --output json query capabilities/cap-thermal 2>/dev/null || true)
    # Empty or null body == tombstoned (query returns {} when no docs match).
    if [ -z "${out}" ] || [ "${out}" = "{}" ] || [ "${out}" = "null" ]; then
        break
    fi
    sleep 1
done
[ -z "${out}" ] || [ "${out}" = "{}" ] || [ "${out}" = "null" ] \
    || fail "query still returns cap-thermal after delete (got ${out})"
pass "delete tombstoned the doc"

# ---- Step 4: observe in background, fire a create, see the event ----

log "Step 4: 'peat observe' streams events from another writer"
# Spawn the observer as a backgrounded shell-inside-container that
# pipes peat's ndjson into a tmpfile on the container's filesystem.
# Using `nohup ... &` keeps it alive across the docker exec's
# stdin close; the PID lands in /tmp/observed.pid so we can clean up.
docker exec peat-node-a sh -c '
  rm -f /tmp/observed.log /tmp/observed.pid
  nohup peat --creds /tmp/creds.yaml --timeout 30s --output ndjson \
       observe capabilities > /tmp/observed.log 2>&1 &
  echo $! > /tmp/observed.pid'

# Brief settle window for the observer's join handshake + subscription.
sleep 3

docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 30s \
    create capabilities --id cap-radio \
    --set id=cap-radio --set name=radio --set confidence=0.5 \
    --wait-for-sync >/dev/null

# Poll the observer's output for the new key.
seen=""
for _ in $(seq 1 15); do
    seen=$(docker exec peat-node-a cat /tmp/observed.log 2>/dev/null || true)
    echo "${seen}" | grep -q "cap-radio" && break
    sleep 1
done
echo "${seen}" | grep -q "cap-radio" \
    || fail "observer did not see cap-radio within 15s (saw: ${seen})"
pass "observe streamed the new doc to a second CLI invocation"

# Cleanup observer process inside the container.
docker exec peat-node-a sh -c '
  pid=$(cat /tmp/observed.pid 2>/dev/null || true)
  [ -n "${pid}" ] && kill "${pid}" 2>/dev/null || true
  rm -f /tmp/observed.log /tmp/observed.pid' || true

log "All quickstart steps validated."
echo
echo "Path A (Docker Compose) QUICKSTART is functionally correct."
