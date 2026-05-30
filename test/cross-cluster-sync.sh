#!/usr/bin/env bash
# Cross-cluster CRDT sync test for peat-node.
#
# Creates two k3d clusters on a shared Docker network, deploys peat-node
# to each via the in-tree Helm chart, peers them via direct Iroh UDP
# addressing (k3d node hostnames on the shared network), and verifies
# bidirectional CRDT sync.
#
# Uses kubectl exec + curl (Connect protocol / HTTP+JSON). No Go
# toolchain needed.
#
# Prerequisites: k3d, helm, docker, kubectl, python3
#
# Relay model: this script uses the relay-off-by-default path that v0.1.1+
# ships. Peers reach each other via direct UDP at the k3d node container's
# hostname on the shared `peat-mesh-net` Docker network — no dependency
# on the public n0 relay or any external NAT-traversal infrastructure.
# The chart pins `PEAT_NODE_IROH_UDP_PORT` and sets `hostPort` so the
# pod's UDP socket is reachable from the other cluster's pods.
#
# Usage:
#   ./test/cross-cluster-sync.sh          # full lifecycle
#   ./test/cross-cluster-sync.sh create   # create clusters + deploy only
#   ./test/cross-cluster-sync.sh test     # run tests (clusters must exist)
#   ./test/cross-cluster-sync.sh cleanup  # destroy clusters

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="${SCRIPT_DIR}/.."
IMAGE="peat-node:dev"

ALPHA="peat-sync-alpha"
BRAVO="peat-sync-bravo"
NETWORK="peat-mesh-net"
IROH_PORT="51071"

# Demo shared key — same on both clusters so they're in one formation.
# 32 zero bytes, base64-encoded. Generate a real key for any non-test use.
SHARED_KEY="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

# --- Helpers ---

log()  { echo "==> $*"; }
pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; FAILED=true; }

# Execute an RPC via kubectl exec + curl inside the sidecar pod.
# NOTE on the default-body: do not use `${3:-{}}` — bash closes the
# parameter expansion at the first `}`, so the literal `{}` default
# leaves a trailing `}` attached to any caller-supplied body, breaking
# the JSON silently. Use a sentinel default instead.
rpc_on() {
    local context="$1" method="$2" body="${3-}"
    [ -z "${body}" ] && body='{}'
    kubectl --context "${context}" exec -n peat svc/peat-peat-node -c peat-node -- \
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

deploy_to() {
    local cluster="$1" node_id="$2"
    local ctx="k3d-${cluster}"
    kubectl --context "${ctx}" create namespace peat --dry-run=client -o yaml | \
        kubectl --context "${ctx}" apply -f -
    if ! helm --kube-context "${ctx}" upgrade --install peat \
        "${REPO_DIR}/chart/peat-node" \
        --namespace peat \
        --set image.repository=peat-node \
        --set image.tag=dev \
        --set image.pullPolicy=Never \
        --set listen="tcp://0.0.0.0:50051" \
        --set nodeId="${node_id}" \
        --set sharedKey="${SHARED_KEY}" \
        --set autoSync=true \
        --set irohUdpPort="${IROH_PORT}" \
        --set irohUdpHostPort=true \
        --wait --timeout 90s; then
        # Helm install timed out or rolled back. Capture diagnostics
        # before the cleanup trap nukes the cluster so the next CI
        # failure has something to investigate. Without this the
        # operator sees only "context deadline exceeded".
        echo "==> Helm install failed; capturing diagnostics for ${cluster}..."
        echo "--- kubectl get pods -n peat ---"
        kubectl --context "${ctx}" get pods -n peat -o wide 2>&1 || true
        echo "--- kubectl describe pods -n peat ---"
        kubectl --context "${ctx}" describe pods -n peat 2>&1 || true
        echo "--- kubectl get events -n peat (last 30) ---"
        kubectl --context "${ctx}" get events -n peat \
            --sort-by=.lastTimestamp 2>&1 | tail -30 || true
        echo "--- peat-node container logs (current) ---"
        kubectl --context "${ctx}" logs -n peat -l app.kubernetes.io/name=peat-node \
            -c peat-node --tail=200 2>&1 || true
        echo "--- peat-node container logs (previous, if crashed) ---"
        kubectl --context "${ctx}" logs -n peat -l app.kubernetes.io/name=peat-node \
            -c peat-node --tail=200 --previous 2>&1 || true
        return 1
    fi
}

create_clusters() {
    build_image

    # Both clusters share a Docker network so each cluster's node
    # container is reachable from the other's pods via Docker DNS at
    # `k3d-${CLUSTER}-server-0` on the `${NETWORK}` network.
    log "Creating k3d cluster: ${ALPHA} on network ${NETWORK}"
    k3d cluster create "${ALPHA}" --network "${NETWORK}" --wait 2>&1 | tail -1

    log "Creating k3d cluster: ${BRAVO} on network ${NETWORK}"
    k3d cluster create "${BRAVO}" --network "${NETWORK}" --wait 2>&1 | tail -1

    log "Loading image into clusters..."
    k3d image import "${IMAGE}" -c "${ALPHA}" 2>&1 | tail -1
    k3d image import "${IMAGE}" -c "${BRAVO}" 2>&1 | tail -1

    log "Deploying peat-node to ${ALPHA} via Helm..."
    deploy_to "${ALPHA}" "alpha-agent"

    log "Deploying peat-node to ${BRAVO} via Helm..."
    deploy_to "${BRAVO}" "bravo-agent"
}

cleanup_clusters() {
    log "Cleaning up..."
    k3d cluster delete "${ALPHA}" 2>/dev/null || true
    k3d cluster delete "${BRAVO}" 2>/dev/null || true
    docker network rm "${NETWORK}" 2>/dev/null || true
}

# --- Test ---

run_test() {
    local CTX_A="k3d-${ALPHA}"
    local CTX_B="k3d-${BRAVO}"
    FAILED=false

    # The chart's hostPort binds the pod's Iroh UDP socket on the k3d
    # node container's interface. Pods in the *other* cluster reach it
    # at the node container's IP on the shared Docker network.
    #
    # Resolve to a raw IP rather than passing the Docker DNS hostname:
    # iroh's address parser takes a slow discovery path on hostnames
    # (≈2 min before falling through to direct UDP), but accepts an
    # IP:port immediately. Pods *can* resolve the k3d hostname via
    # CoreDNS — the issue is just iroh's side, not Kubernetes.
    local ALPHA_IP BRAVO_IP
    ALPHA_IP=$(docker network inspect "${NETWORK}" \
        --format "{{range .Containers}}{{if eq .Name \"k3d-${ALPHA}-server-0\"}}{{.IPv4Address}}{{end}}{{end}}" \
        | cut -d/ -f1)
    BRAVO_IP=$(docker network inspect "${NETWORK}" \
        --format "{{range .Containers}}{{if eq .Name \"k3d-${BRAVO}-server-0\"}}{{.IPv4Address}}{{end}}{{end}}" \
        | cut -d/ -f1)
    [ -n "${ALPHA_IP}" ] || { echo "fatal: could not resolve k3d-${ALPHA}-server-0 IP"; exit 1; }
    [ -n "${BRAVO_IP}" ] || { echo "fatal: could not resolve k3d-${BRAVO}-server-0 IP"; exit 1; }
    local ALPHA_HOST="${ALPHA_IP}:${IROH_PORT}"
    local BRAVO_HOST="${BRAVO_IP}:${IROH_PORT}"

    # ── Test 1: Both nodes healthy ────────────────────────────────
    log "Test 1: Node health"

    ALPHA_ID=$(rpc_on "${CTX_A}" GetStatus | jq_py "print(json.load(sys.stdin)['nodeId'])")
    [ "${ALPHA_ID}" = "alpha-agent" ] && pass "Alpha node ID: ${ALPHA_ID}" || fail "unexpected Alpha ID: ${ALPHA_ID}"

    BRAVO_ID=$(rpc_on "${CTX_B}" GetStatus | jq_py "print(json.load(sys.stdin)['nodeId'])")
    [ "${BRAVO_ID}" = "bravo-agent" ] && pass "Bravo node ID: ${BRAVO_ID}" || fail "unexpected Bravo ID: ${BRAVO_ID}"

    ALPHA_ENDPOINT=$(rpc_on "${CTX_A}" GetStatus | jq_py "print(json.load(sys.stdin)['endpointAddr'])")

    # ── Test 2: Peer connection via direct UDP (no relay) ─────────
    log "Test 2: Cross-cluster peer connection via direct UDP at ${ALPHA_HOST}"

    rpc_on "${CTX_B}" ConnectPeer \
        "{\"endpointId\":\"${ALPHA_ENDPOINT}\",\"addresses\":[\"${ALPHA_HOST}\"]}" >/dev/null

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

    # ── Test 5: GetSyncStats reports non-zero byte counters ─────
    log "Test 5: GetSyncStats byte counters (real wire traffic)"

    # Proto3 JSON emits uint64 as string; bytesSent may be missing
    # entirely if 0 (proto3 default elision). Treat absent as 0.
    BYTES_A=$(rpc_on "${CTX_A}" GetSyncStats | jq_py "d=json.load(sys.stdin); print(int(d.get('bytesSent','0') or 0))")
    BYTES_B=$(rpc_on "${CTX_B}" GetSyncStats | jq_py "d=json.load(sys.stdin); print(int(d.get('bytesSent','0') or 0))")
    [ "${BYTES_A}" -gt 0 ] && pass "Alpha bytesSent=${BYTES_A}" || fail "Alpha bytesSent=${BYTES_A} (expected >0)"
    [ "${BYTES_B}" -gt 0 ] && pass "Bravo bytesSent=${BYTES_B}" || fail "Bravo bytesSent=${BYTES_B} (expected >0)"

    # ── Test 6: peat CLI inside the pod (QUICKSTART Path B) ───
    # Verifies the operator workflow the root QUICKSTART claims:
    # bootstrap a CLI credential bundle inside the sidecar, then
    # drive CRUD via `kubectl exec deploy/peat-peat-node -- peat …`.
    # Without this assertion the QUICKSTART Path B walkthrough would
    # have no executable contract — any drift in the CLI's wire
    # behaviour, the chart's deployment naming, or the in-pod
    # binary path would silently break the docs.
    log "Test 6: peat CLI in-pod workflow"

    # Bootstrap creds.yaml inside the alpha pod. App_id + shared_key
    # must match what the chart set the sidecar to. `deploy_to`
    # passes only --set sharedKey=…; appId defaults to the chart's
    # `peat-default` in values.yaml.
    if ! kubectl --context "${CTX_A}" exec -n peat deploy/peat-peat-node -c peat-node -- sh -c "
        cat > /tmp/creds.yaml <<EOF
app_id: peat-default
shared_key: ${SHARED_KEY}
peers:
  - \$(curl -s -X POST http://localhost:50051/peat.sidecar.v1.PeatSidecar/GetStatus \
      -H 'Content-Type: application/json' -d '{}' \
      | grep -o '\"endpointAddr\":\"[^\"]*\"' | cut -d'\"' -f4)@localhost:${IROH_PORT}
EOF
        chmod 600 /tmp/creds.yaml
    " >/dev/null 2>&1; then
        fail "could not bootstrap /tmp/creds.yaml in alpha pod"
    else
        pass "creds.yaml bootstrapped in alpha pod"
    fi

    # Offline sanity: schema list must work without joining a mesh.
    if kubectl --context "${CTX_A}" exec -n peat deploy/peat-peat-node -c peat-node -- \
        peat schema list 2>&1 | grep -q "capabilities"; then
        pass "peat schema list runs offline inside the pod"
    else
        fail "peat schema list failed inside the pod"
    fi

    # Drive a write through the CLI; assert the doc lands on the
    # sidecar's own store (via GetDocument over Connect RPC).
    if kubectl --context "${CTX_A}" exec -n peat deploy/peat-peat-node -c peat-node -- \
        peat --creds /tmp/creds.yaml --timeout 30s \
            create contacts --id cli-smoke --set name=via-cli --wait-for-sync \
            >/dev/null 2>&1; then
        pass "peat create via kubectl exec succeeded"
    else
        fail "peat create via kubectl exec failed"
    fi

    # Confirm the sidecar's own store has it (the CLI joined the
    # mesh and synced the write back to its peer).
    CLI_DOC=$(rpc_on "${CTX_A}" GetDocument '{"collection":"contacts","docId":"cli-smoke"}')
    if echo "${CLI_DOC}" | grep -q "via-cli"; then
        pass "GetDocument on alpha sees the CLI-authored doc"
    else
        fail "GetDocument on alpha missing CLI doc (got: ${CLI_DOC})"
    fi

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
