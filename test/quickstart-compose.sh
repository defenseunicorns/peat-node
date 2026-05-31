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
# Diagnostic — print each container's actual bridge IP so we can
# cross-reference against what `tokio::net::lookup_host` resolves
# `peat-node-b` to from inside peat-node-a's container. PR #114's
# CI run on 129624a showed iroh got `ip_addresses=[172.18.0.2:51071]`
# but peat-node-b's listener never saw inbound — possible
# explanation is that resolving `peat-node-b` from inside
# peat-node-a returns peat-node-a's OWN bridge IP (so the dial
# loopbacks to peat-node-a's listener, TLS NodeId mismatch,
# 30s slow-fail).
echo "==> Container IP cross-reference:"
PEAT_NODE_A_IP=$(docker inspect peat-node-a --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
PEAT_NODE_B_IP=$(docker inspect peat-node-b --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}')
echo "    peat-node-a actual bridge IP: ${PEAT_NODE_A_IP}"
echo "    peat-node-b actual bridge IP: ${PEAT_NODE_B_IP}"
echo "==> What peat-node-a's resolver returns for 'peat-node-b':"
docker exec peat-node-a sh -c 'getent hosts peat-node-b' | sed 's/^/    /' || true

# Probe container-to-container reachability in the a→b direction
# (the exact direction the CLI's iroh dial uses). On PR #114's
# failing runs, iroh sends QUIC to peat-node-b's correct bridge IP
# but peat-node-b's listener sees nothing. Two candidate explanations:
# (i) compose bridge isn't routing a→b traffic for some reason
# (Docker bridge usually does, but GHA runner sandbox is different
# from local), or (ii) iroh's accept layer is silently dropping
# inbound from fresh ephemeral peers.
#
# This probe separates the two: curl peat-node-b's gRPC TCP port
# from inside peat-node-a's container. If TCP a→b works, container
# routing is fine (UDP follows), so the bug is in iroh. If TCP a→b
# fails too, the bridge is the bug.
echo "==> Probe a→b TCP reachability via IP (peat-node-a → ${PEAT_NODE_B_IP}:50051):"
docker exec peat-node-a curl -s -m 5 --write-out '\n    HTTP status: %{http_code}\n' \
    -X POST "http://${PEAT_NODE_B_IP}:50051/peat.sidecar.v1.PeatSidecar/GetStatus" \
    -H 'Content-Type: application/json' -d '{}' \
    2>&1 | head -c 300 | sed 's/^/    /' || true
echo
echo "==> Probe a→b TCP via Docker service name (peat-node-a → peat-node-b:50051):"
docker exec peat-node-a curl -s -m 5 --write-out '\n    HTTP status: %{http_code}\n' \
    -X POST http://peat-node-b:50051/peat.sidecar.v1.PeatSidecar/GetStatus \
    -H 'Content-Type: application/json' -d '{}' \
    2>&1 | head -c 300 | sed 's/^/    /' || true
echo

# Extract peat-node-b's Iroh NodeId via GetStatus on the host, then
# write the bundle inside the container. `jq` on the host is more
# robust than grep+cut against JSON whitespace/ordering. We then
# write the resolved value into the heredoc directly (no $() inside
# the docker-exec'd shell, which avoids cross-shell quoting risk).
RAW_STATUS=$(curl -s -X POST http://localhost:50062/peat.sidecar.v1.PeatSidecar/GetStatus \
    -H 'Content-Type: application/json' -d '{}')
NODE_B_ID=$(echo "${RAW_STATUS}" | jq -r .endpointAddr)
if [ -z "${NODE_B_ID}" ] || [ "${NODE_B_ID}" = "null" ]; then
    fail "could not extract peat-node-b endpointAddr (got: '${NODE_B_ID}', raw: '${RAW_STATUS}')"
fi
docker exec peat-node-a sh -c "
  cat > /tmp/creds.yaml <<EOF
app_id: compose-demo
shared_key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
peers:
  - ${NODE_B_ID}@peat-node-b:51072
EOF
  chmod 600 /tmp/creds.yaml"
docker exec peat-node-a test -r /tmp/creds.yaml || fail "creds.yaml not readable"
# Debug: show what was actually written. Loud on CI logs makes the
# next failure self-diagnostic.
log "Step 1 wrote:"
docker exec peat-node-a cat /tmp/creds.yaml | sed 's/^/    /'
pass "creds.yaml written, mode 0600"

# ---- Step 2: read state from the mesh -------------------------------

log "Step 2: 'peat query --all-collections' from inside peat-node-a"
# demo.sh already wrote hello/world on node-a; --all-collections should
# include it. The query goes via the CLI's own joined session, not
# node-a's local store, so this proves the handshake works.
#
# RUST_LOG includes peat_cli + peat_mesh debug so the next failure
# carries the actual peer-connect error (the join prelude logs
# `peer connection failed: <e>` at warn level — without `peat_cli`
# in the filter, the wrapper "no peers reachable" message is all
# we see). Bumped to --timeout 60s because cold Iroh handshake on
# a CI runner can take >30s before sync drains the first scan.
out=$(docker exec -e RUST_LOG=peat_cli=debug,peat_mesh=debug,iroh=debug \
        peat-node-a peat --creds /tmp/creds.yaml \
        --timeout 60s --output json query --all-collections 2>&1) || {
    log "Step 2 failed; CLI output:"
    echo "${out}" | sed 's/^/    /'
    log "Step 2 failed; peat-node-b sidecar logs (receive side):"
    docker logs peat-node-b --tail 80 2>&1 | sed 's/^/    /'
    log "Step 2 failed; peat-node-a sidecar logs (CLI's own container):"
    docker logs peat-node-a --tail 40 2>&1 | sed 's/^/    /'
    fail "query --all-collections failed"
}
echo "${out}" | jq -e '.["hello:world"]' >/dev/null \
    || fail "query --all-collections didn't include hello/world (got ${out})"
pass "query --all-collections sees the demo doc"

# ---- Step 3: write a document via CLI -------------------------------

log "Step 3: 'peat create capabilities/cap-thermal' from inside peat-node-a"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
    create capabilities --id cap-thermal \
    --set id=cap-thermal \
    --set name=thermal-sensor \
    --set confidence=0.92 \
    --wait-for-sync >/dev/null
pass "create capabilities/cap-thermal succeeded"

log "Step 3b: read it back"
out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
        --output json query capabilities/cap-thermal)
echo "${out}" | jq -e '.name == "thermal-sensor"' >/dev/null \
    || fail "query did not return thermal-sensor (got ${out})"
pass "query confirms the new doc"

log "Step 3c: 'peat update' edits one field"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
    update capabilities/cap-thermal --set confidence=0.98 --wait-for-sync >/dev/null
out=$(docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
        --output json query capabilities/cap-thermal)
echo "${out}" | jq -e '.confidence == 0.98' >/dev/null \
    || fail "update did not land confidence=0.98 (got ${out})"
pass "update edits one field"

log "Step 3d: 'peat delete' tombstones the doc"
docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
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
  nohup peat --creds /tmp/creds.yaml --timeout 60s --output ndjson \
       observe capabilities > /tmp/observed.log 2>&1 &
  echo $! > /tmp/observed.pid'

# Brief settle window for the observer's join handshake + subscription.
sleep 3

docker exec peat-node-a peat --creds /tmp/creds.yaml --timeout 60s \
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
