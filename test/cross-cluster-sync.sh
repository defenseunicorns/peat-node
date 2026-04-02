#!/usr/bin/env bash
# Cross-cluster CRDT sync test for peat-sidecar.
#
# Creates two k3d clusters, deploys a peat-sidecar to each, connects them
# via Iroh relay, writes data on one side, and verifies it syncs to the other.
#
# Uses kubectl exec + curl (Connect protocol / HTTP+JSON) — no Go toolchain needed.
#
# Prerequisites: k3d, docker, kubectl, python3
#
# IMPORTANT: Cross-cluster sync requires that pods can resolve public DNS
# (Iroh relay at *.relay.iroh.network). Some local k3d/OrbStack configurations
# have broken DNS egress. If peer connection fails, verify with:
#   kubectl exec sidecar -- curl -s https://euw1-1.relay.iroh.network/
# For CI environments with proper DNS, this test works end-to-end.
#
# Usage:
#   ./test/cross-cluster-sync.sh          # full lifecycle
#   ./test/cross-cluster-sync.sh create   # create clusters only
#   ./test/cross-cluster-sync.sh test     # run tests (clusters must exist)
#   ./test/cross-cluster-sync.sh cleanup  # destroy clusters

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="${SCRIPT_DIR}/.."
IMAGE="peat-sidecar:dev"

ALPHA="peat-sync-alpha"
BRAVO="peat-sync-bravo"

# --- Helpers ---

log()  { echo "==> $*"; }
pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; FAILED=true; }

# Execute an RPC via kubectl exec + curl inside the pod (avoids port-forward issues)
rpc_on() {
    local context="$1" method="$2" body="${3:-{}}"
    kubectl --context "${context}" exec sidecar -- \
        curl -s -X POST "http://localhost:50051/peat.sidecar.v1.PeatSidecar/${method}" \
        -H "content-type: application/json" \
        -d "${body}" 2>/dev/null
}

jq_py() {
    python3 -c "import sys,json; $1"
}

# --- Cluster Lifecycle ---

build_image() {
    if docker image inspect "${IMAGE}" &>/dev/null; then
        log "Image ${IMAGE} already exists"
    else
        log "Building ${IMAGE}..."
        docker build -t "${IMAGE}" "${REPO_DIR}"
    fi
}

create_clusters() {
    build_image

    # Both clusters share a Docker network so Iroh QUIC can route between them
    log "Creating k3d cluster: ${ALPHA}"
    k3d cluster create "${ALPHA}" --network peat-mesh-net --wait 2>&1 | tail -1

    log "Creating k3d cluster: ${BRAVO}"
    k3d cluster create "${BRAVO}" --network peat-mesh-net --wait 2>&1 | tail -1

    log "Loading image into clusters..."
    k3d image import "${IMAGE}" -c "${ALPHA}" 2>&1 | tail -1
    k3d image import "${IMAGE}" -c "${BRAVO}" 2>&1 | tail -1

    # Deploy sidecar pods
    for ctx in "k3d-${ALPHA}:alpha-agent" "k3d-${BRAVO}:bravo-agent"; do
        IFS=: read -r context node_id <<< "${ctx}"
        kubectl --context "${context}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: sidecar
  labels:
    app: peat-sidecar
spec:
  containers:
  - name: sidecar
    image: ${IMAGE}
    imagePullPolicy: Never
    args: ["peat-sidecar", "--node-id=${node_id}", "--listen=tcp://0.0.0.0:50051", "--auto-sync"]
    ports:
    - containerPort: 50051
EOF
    done

    log "Waiting for pods..."
    kubectl --context "k3d-${ALPHA}" wait --for=condition=Ready pod/sidecar --timeout=60s
    kubectl --context "k3d-${BRAVO}" wait --for=condition=Ready pod/sidecar --timeout=60s
}

cleanup_clusters() {
    log "Cleaning up..."
    k3d cluster delete "${ALPHA}" 2>/dev/null || true
    k3d cluster delete "${BRAVO}" 2>/dev/null || true
    docker network rm peat-mesh-net 2>/dev/null || true
}

# --- Test ---

run_test() {
    local CTX_A="k3d-${ALPHA}"
    local CTX_B="k3d-${BRAVO}"
    FAILED=false

    # ── Test 1: Both nodes healthy ────────────────────────────────
    log "Test 1: Node health"

    ALPHA_ID=$(rpc_on "${CTX_A}" GetStatus | jq_py "print(json.load(sys.stdin)['nodeId'])")
    [ "${ALPHA_ID}" = "alpha-agent" ] && pass "Alpha node ID: ${ALPHA_ID}" || fail "unexpected Alpha ID: ${ALPHA_ID}"

    BRAVO_ID=$(rpc_on "${CTX_B}" GetStatus | jq_py "print(json.load(sys.stdin)['nodeId'])")
    [ "${BRAVO_ID}" = "bravo-agent" ] && pass "Bravo node ID: ${BRAVO_ID}" || fail "unexpected Bravo ID: ${BRAVO_ID}"

    ALPHA_ENDPOINT=$(rpc_on "${CTX_A}" GetStatus | jq_py "print(json.load(sys.stdin)['endpointAddr'])")

    # ── Test 2: Peer connection (cross-cluster via Iroh relay) ────
    log "Test 2: Cross-cluster peer connection"

    rpc_on "${CTX_B}" ConnectPeer "{\"endpointId\":\"${ALPHA_ENDPOINT}\"}" >/dev/null

    CONNECTED=false
    for i in $(seq 1 30); do
        ALPHA_PEERS=$(rpc_on "${CTX_A}" ListPeers | jq_py "print(len(json.load(sys.stdin).get('peers',[])))" || echo 0)
        if [ "${ALPHA_PEERS}" = "1" ]; then
            pass "Peer connection established in ${i}s"
            CONNECTED=true
            break
        fi
        sleep 2
    done
    ${CONNECTED} || fail "Peer connection not established within 60s"

    BRAVO_PEERS=$(rpc_on "${CTX_B}" ListPeers | jq_py "print(len(json.load(sys.stdin).get('peers',[])))")
    [ "${BRAVO_PEERS}" = "1" ] && pass "Bravo confirms 1 peer" || fail "Bravo sees ${BRAVO_PEERS} peers (expected 1)"

    # ── Test 3: Alpha → Bravo sync ───────────────────────────────
    log "Test 3: Alpha → Bravo CRDT sync"

    rpc_on "${CTX_A}" PutPlatform '{"platform":{"id":"alpha-agent","platformType":"uds-remote-agent","name":"Alpha Edge","status":"PLATFORM_STATUS_READY","latitude":38.89,"longitude":-77.03,"capabilities":["deploy","monitor"]}}' >/dev/null
    pass "Wrote platform on Alpha"

    rpc_on "${CTX_A}" PutDocument '{"collection":"deployments","docId":"alpha-agent:nginx","jsonData":"{\"package\":\"nginx\",\"version\":\"1.25\",\"status\":\"deployed\"}"}' >/dev/null
    pass "Wrote deployment doc on Alpha"

    SYNCED=false
    for i in $(seq 1 30); do
        COUNT=$(rpc_on "${CTX_B}" GetPlatforms | jq_py "print(len(json.load(sys.stdin).get('platforms',[])))" || echo 0)
        if [ "${COUNT}" = "1" ]; then
            pass "Platform synced to Bravo in ${i}s"
            SYNCED=true
            break
        fi
        sleep 1
    done
    ${SYNCED} || fail "Platform did not sync to Bravo within 30s"

    # Verify platform data fidelity
    PLAT_NAME=$(rpc_on "${CTX_B}" GetPlatforms | jq_py "print(json.load(sys.stdin)['platforms'][0]['name'])")
    [ "${PLAT_NAME}" = "Alpha Edge" ] && pass "Platform name correct: ${PLAT_NAME}" || fail "wrong name: ${PLAT_NAME}"

    # Verify deployment doc
    DOC_PKG=$(rpc_on "${CTX_B}" GetDocument '{"collection":"deployments","docId":"alpha-agent:nginx"}' \
        | jq_py "import json as j2; print(j2.loads(json.load(sys.stdin)['jsonData'])['package'])")
    [ "${DOC_PKG}" = "nginx" ] && pass "Deployment doc synced correctly" || fail "deployment doc mismatch: ${DOC_PKG}"

    # ── Test 4: Bravo → Alpha sync (bidirectional) ───────────────
    log "Test 4: Bravo → Alpha bidirectional sync"

    rpc_on "${CTX_B}" PutPlatform '{"platform":{"id":"bravo-agent","platformType":"uds-remote-agent","name":"Bravo Edge","status":"PLATFORM_STATUS_READY","latitude":33.45,"longitude":-112.07,"capabilities":["deploy"]}}' >/dev/null
    pass "Wrote platform on Bravo"

    SYNCED=false
    for i in $(seq 1 30); do
        COUNT=$(rpc_on "${CTX_A}" GetPlatforms | jq_py "print(len(json.load(sys.stdin).get('platforms',[])))" || echo 0)
        if [ "${COUNT}" = "2" ]; then
            pass "Both platforms visible on Alpha in ${i}s"
            SYNCED=true
            break
        fi
        sleep 1
    done
    ${SYNCED} || fail "Bravo platform did not sync to Alpha within 30s"

    # Final fleet-wide state check
    ALPHA_COUNT=$(rpc_on "${CTX_A}" GetPlatforms | jq_py "print(len(json.load(sys.stdin).get('platforms',[])))")
    BRAVO_COUNT=$(rpc_on "${CTX_B}" GetPlatforms | jq_py "print(len(json.load(sys.stdin).get('platforms',[])))")
    [ "${ALPHA_COUNT}" = "2" ] && [ "${BRAVO_COUNT}" = "2" ] \
        && pass "Fleet converged: both clusters see 2 platforms" \
        || fail "fleet state mismatch: alpha=${ALPHA_COUNT} bravo=${BRAVO_COUNT}"

    echo ""
    if ${FAILED}; then
        log "SOME TESTS FAILED"
        exit 1
    else
        log "All tests passed!"
    fi
}

# --- Main ---

case "${1:-all}" in
    create)  create_clusters ;;
    test)    run_test ;;
    cleanup) cleanup_clusters ;;
    all)
        trap cleanup_clusters EXIT
        create_clusters
        run_test
        ;;
    *)
        echo "Usage: $0 {create|test|cleanup|all}"
        exit 1
        ;;
esac
