#!/usr/bin/env bash
# Cluster-to-cluster Peat sync test with UDS Remote Agent.
#
# Creates two k3d clusters, deploys UDS Remote Agent + peat-node to each,
# then runs the cross-cluster sync test.
#
# Prerequisites: k3d, docker, kubectl, go
# Optional: uds CLI (for full Zarf-based deployment)
#
# Usage:
#   ./setup.sh          # full lifecycle (create, deploy, test, cleanup)
#   ./setup.sh create   # create clusters only
#   ./setup.sh deploy   # deploy to existing clusters
#   ./setup.sh test     # run tests only
#   ./setup.sh cleanup  # destroy clusters

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PEAT_NODE_DIR="${SCRIPT_DIR}/../../../peat-node"
UDS_AGENT_DIR="${SCRIPT_DIR}/../../../uds-remote-agent"
PEAT_NODE_IMAGE="peat-node:dev"

# Cluster names and ports
ALPHA_CLUSTER="peat-alpha"
BRAVO_CLUSTER="peat-bravo"
ALPHA_AGENT_PORT=32582
ALPHA_PEAT_PORT=32551
BRAVO_AGENT_PORT=33582
BRAVO_PEAT_PORT=33551

NAMESPACE="zarf"
DEPLOYMENT="uds-remote-agent-deployment"

# --- Helpers ---

log() { echo "==> $*"; }

ensure_peat_node_image() {
    if ! docker image inspect "${PEAT_NODE_IMAGE}" &>/dev/null; then
        log "Building peat-node container image..."
        docker build -t "${PEAT_NODE_IMAGE}" "${PEAT_NODE_DIR}"
    else
        log "peat-node image already exists"
    fi
}

# --- Cluster Lifecycle ---

create_clusters() {
    log "Creating k3d cluster: ${ALPHA_CLUSTER}"
    k3d cluster create "${ALPHA_CLUSTER}" \
        -p "${ALPHA_AGENT_PORT}:${ALPHA_AGENT_PORT}@server:*" \
        -p "${ALPHA_PEAT_PORT}:${ALPHA_PEAT_PORT}@server:*" \
        --wait

    log "Creating k3d cluster: ${BRAVO_CLUSTER}"
    k3d cluster create "${BRAVO_CLUSTER}" \
        -p "${BRAVO_AGENT_PORT}:${BRAVO_AGENT_PORT}@server:*" \
        -p "${BRAVO_PEAT_PORT}:${BRAVO_PEAT_PORT}@server:*" \
        --wait

    # Load peat-node image into both clusters
    ensure_peat_node_image
    log "Loading peat-node image into clusters..."
    k3d image import "${PEAT_NODE_IMAGE}" -c "${ALPHA_CLUSTER}"
    k3d image import "${PEAT_NODE_IMAGE}" -c "${BRAVO_CLUSTER}"
}

cleanup_clusters() {
    log "Deleting k3d clusters..."
    k3d cluster delete "${ALPHA_CLUSTER}" 2>/dev/null || true
    k3d cluster delete "${BRAVO_CLUSTER}" 2>/dev/null || true
}

# --- Deployment ---

# Deploy UDS Remote Agent to a cluster (minimal, insecure mode for testing).
# In production you'd use the full Zarf package; here we use Helm directly
# for speed and to avoid needing the full UDS stack.
deploy_agent_to_cluster() {
    local cluster="$1"
    local agent_node_port="$2"
    local peat_node_port="$3"
    local peat_node_id="$4"

    log "Deploying to ${cluster}..."
    kubectl config use-context "k3d-${cluster}"

    # Create namespace
    kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

    # Create RBAC (matches the chart's rbac.yaml)
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: uds-remote-sa
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: uds-remote-sa-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: uds-remote-sa
  namespace: ${NAMESPACE}
EOF

    # Deploy UDS Remote Agent (insecure mode for testing)
    # Use Helm if the chart is available, otherwise apply minimal manifests
    if command -v helm &>/dev/null && [ -d "${UDS_AGENT_DIR}/chart" ]; then
        helm upgrade --install uds-remote-agent "${UDS_AGENT_DIR}/chart" \
            --namespace "${NAMESPACE}" \
            --set image.repository=ghcr.io/defenseunicorns/uds-remote-agent \
            --set image.tag=latest \
            --set nodePort="${agent_node_port}" \
            --set disableMtls=true \
            --set persistData=false \
            --set logLevel=debug \
            --wait --timeout 60s 2>/dev/null || {
                log "Helm deploy failed (may need image pull); continuing with patch..."
            }
    else
        log "Helm not available or chart not found; skipping UDS Remote Agent deploy"
        log "Deploy it manually, then re-run: $0 deploy"
        return 1
    fi

    # Patch the deployment to add peat-node as a sidecar container
    log "Patching deployment with peat-node..."
    kubectl -n "${NAMESPACE}" patch deployment "${DEPLOYMENT}" --type=json -p="[
        {
            \"op\": \"add\",
            \"path\": \"/spec/template/spec/containers/-\",
            \"value\": {
                \"name\": \"peat-node\",
                \"image\": \"${PEAT_NODE_IMAGE}\",
                \"imagePullPolicy\": \"Never\",
                \"env\": [
                    {\"name\": \"PEAT_NODE_LISTEN\", \"value\": \"tcp://0.0.0.0:50051\"},
                    {\"name\": \"PEAT_NODE_DATA_DIR\", \"value\": \"/data/peat-node\"},
                    {\"name\": \"PEAT_NODE_NODE_ID\", \"value\": \"${peat_node_id}\"},
                    {\"name\": \"PEAT_NODE_AUTO_SYNC\", \"value\": \"true\"},
                    {\"name\": \"RUST_LOG\", \"value\": \"peat_node=info,peat_mesh=info\"}
                ],
                \"ports\": [{\"containerPort\": 50051, \"protocol\": \"TCP\"}],
                \"volumeMounts\": [
                    {\"name\": \"peat-data\", \"mountPath\": \"/data/peat-node\"}
                ],
                \"livenessProbe\": {
                    \"tcpSocket\": {\"port\": 50051},
                    \"initialDelaySeconds\": 5,
                    \"periodSeconds\": 30
                },
                \"readinessProbe\": {
                    \"tcpSocket\": {\"port\": 50051},
                    \"initialDelaySeconds\": 3,
                    \"periodSeconds\": 10
                },
                \"resources\": {
                    \"requests\": {\"cpu\": \"50m\", \"memory\": \"64Mi\"},
                    \"limits\": {\"cpu\": \"500m\", \"memory\": \"256Mi\"}
                }
            }
        },
        {
            \"op\": \"add\",
            \"path\": \"/spec/template/spec/volumes/-\",
            \"value\": {\"name\": \"peat-data\", \"emptyDir\": {}}
        }
    ]"

    # Add NodePort service for peat-node
    kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: peat-node
spec:
  type: NodePort
  selector:
    app: uds-remote-agent-deployment
  ports:
    - name: grpc
      port: 50051
      targetPort: 50051
      nodePort: ${peat_node_port}
      protocol: TCP
EOF

    # Wait for rollout
    log "Waiting for rollout on ${cluster}..."
    kubectl -n "${NAMESPACE}" rollout status deployment/"${DEPLOYMENT}" --timeout=120s
}

deploy() {
    deploy_agent_to_cluster "${ALPHA_CLUSTER}" "${ALPHA_AGENT_PORT}" "${ALPHA_PEAT_PORT}" "alpha-node"
    deploy_agent_to_cluster "${BRAVO_CLUSTER}" "${BRAVO_AGENT_PORT}" "${BRAVO_PEAT_PORT}" "bravo-node"
}

# --- Test ---

run_test() {
    log "Running cross-cluster sync test..."
    cd "${SCRIPT_DIR}"

    export ALPHA_PEAT_ADDR="http://localhost:${ALPHA_PEAT_PORT}"
    export BRAVO_PEAT_ADDR="http://localhost:${BRAVO_PEAT_PORT}"
    export ALPHA_AGENT_ADDR="http://localhost:${ALPHA_AGENT_PORT}"
    export BRAVO_AGENT_ADDR="http://localhost:${BRAVO_AGENT_PORT}"

    go test -v -count=1 -timeout 120s ./...
}

# --- Main ---

case "${1:-all}" in
    create)
        create_clusters
        ;;
    deploy)
        deploy
        ;;
    test)
        run_test
        ;;
    cleanup)
        cleanup_clusters
        ;;
    all)
        trap cleanup_clusters EXIT
        create_clusters
        deploy
        run_test
        ;;
    *)
        echo "Usage: $0 {create|deploy|test|cleanup|all}"
        exit 1
        ;;
esac
