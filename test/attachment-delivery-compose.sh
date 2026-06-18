#!/usr/bin/env bash
# Functional test: real outbox -> inbox attachment delivery over Docker Compose,
# with byte-level content validation on the RECEIVE side.
#
# This is the regression guard PRD-006 v1 lacked: that surface passed every unit
# test and delivered no files (peat-protocol's receive-side observer hooks were
# never implemented; peat-node's inbox watcher closes the gap). A green run here
# proves a file sent from the sender's `--attachment-root` outbox arrives
# byte-identical in the receiver's `--attachment-inbox` directory via a real
# iroh transfer + the inbox watcher — and that we're connected to a peer first,
# so the COMPLETED status can't be the vacuous zero-peer short-circuit.
#
# Prereqs: docker (with compose), curl, jq, openssl.
# Image: builds `peat-node:dev` from the repo by default; override with
#   PEAT_NODE_IMAGE=ghcr.io/defenseunicorns/peat-node:vX.Y.Z to test a release.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${PEAT_NODE_IMAGE:-peat-node:dev}"
PROJECT="peat-attach-functest"
WORK="$(mktemp -d)"

log() { echo "==> $*"; }
fail() {
    echo "  ✗ $*" >&2
    echo "--- receiver logs (tail) ---" >&2
    (cd "$WORK" && docker compose -p "$PROJECT" logs receiver 2>&1 | tail -25 >&2) || true
    exit 1
}
cleanup() {
    (cd "$WORK" && docker compose -p "$PROJECT" down -v >/dev/null 2>&1) || true
    rm -rf "$WORK"
}
trap cleanup EXIT

rpc() {
    curl -sS -X POST -H 'content-type: application/json' \
        "http://127.0.0.1:$1/peat.sidecar.v1.PeatSidecar/$2" -d "$3"
}

# ---- Build the working-tree image unless a specific one was provided --------
if [ -z "${PEAT_NODE_IMAGE:-}" ] && ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "Building $IMAGE from $REPO_DIR"
    docker build -t "$IMAGE" "$REPO_DIR" >/dev/null
fi
log "Using image: $IMAGE"

# ---- Fixtures ---------------------------------------------------------------
K="$(head -c 32 /dev/urandom | base64)"
mkdir -p "$WORK/outbox" "$WORK/inbox"
head -c 2097152 /dev/urandom > "$WORK/outbox/payload.bin" # 2 MiB
SHA_B64="$(openssl dgst -sha256 -binary "$WORK/outbox/payload.bin" | base64)"
SIZE="$(wc -c < "$WORK/outbox/payload.bin" | tr -d ' ')"
log "Payload: ${SIZE} bytes, sha256(b64)=${SHA_B64}"

cat > "$WORK/docker-compose.yml" <<YAML
services:
  sender:
    image: ${IMAGE}
    environment:
      PEAT_NODE_NODE_ID: sender
      PEAT_NODE_APP_ID: attach-functest
      PEAT_NODE_SHARED_KEY: "${K}"
      PEAT_NODE_IROH_UDP_PORT: "51071"
      PEAT_NODE_DISABLE_MDNS: "true"
      PEAT_NODE_AUTO_SYNC: "true"
      PEAT_NODE_ATTACHMENT_ROOT: "outbox=/var/lib/peat/outbox"
      RUST_LOG: "peat_node=info"
    ports: ["50061:50051"]
    volumes: ["${WORK}/outbox:/var/lib/peat/outbox:ro"]
  receiver:
    image: ${IMAGE}
    environment:
      PEAT_NODE_NODE_ID: receiver
      PEAT_NODE_APP_ID: attach-functest
      PEAT_NODE_SHARED_KEY: "${K}"
      PEAT_NODE_IROH_UDP_PORT: "51072"
      PEAT_NODE_DISABLE_MDNS: "true"
      PEAT_NODE_AUTO_SYNC: "true"
      PEAT_NODE_ATTACHMENT_INBOX: "/var/lib/peat/inbox"
      RUST_LOG: "peat_node=info,peat_node::attachments=debug"
    ports: ["50062:50051"]
    volumes: ["${WORK}/inbox:/var/lib/peat/inbox"]
YAML

log "Bringing up sender + receiver"
(cd "$WORK" && docker compose -p "$PROJECT" up -d) >/dev/null

# ---- Wait for both gRPC servers -------------------------------------------
for i in $(seq 1 30); do
    if rpc 50061 GetStatus '{}' >/dev/null 2>&1 && rpc 50062 GetStatus '{}' >/dev/null 2>&1; then
        break
    fi
    sleep 1
    [ "$i" = 30 ] && fail "nodes did not become ready within 30s"
done

# ---- Peer both directions (AllNodes scope needs sender to know receiver) ----
EP_S="$(rpc 50061 GetStatus '{}' | jq -r .endpointAddr)"
EP_R="$(rpc 50062 GetStatus '{}' | jq -r .endpointAddr)"
log "Peering sender<->receiver"
rpc 50062 ConnectPeer "$(jq -nc --arg id "$EP_S" '{endpointId:$id,addresses:["sender:51071"]}')" >/dev/null
rpc 50061 ConnectPeer "$(jq -nc --arg id "$EP_R" '{endpointId:$id,addresses:["receiver:51072"]}')" >/dev/null
sleep 3

PEERS="$(rpc 50061 GetStatus '{}' | jq -r '.connectedPeers // 0')"
[ "${PEERS:-0}" -ge 1 ] || fail "sender reports ${PEERS:-0} peers — a COMPLETED here would be the vacuous zero-peer case, not real delivery"
log "sender connectedPeers=${PEERS}"

rpc 50061 StartSync '{}' >/dev/null
rpc 50062 StartSync '{}' >/dev/null
sleep 1

# ---- Send ------------------------------------------------------------------
log "SendAttachments (scope=allNodes)"
RESP="$(rpc 50061 SendAttachments "$(jq -nc --arg sha "$SHA_B64" --argjson size "$SIZE" \
    '{files:[{rootName:"outbox",relativePath:"payload.bin",sizeBytes:$size,sha256:$sha}],scope:{allNodes:{}}}')")"
DIST="$(echo "$RESP" | jq -r '.handles[0].distributionId // empty')"
[ -n "$DIST" ] || fail "SendAttachments returned no distribution_id: $RESP"
log "distribution_id=$DIST"

# ---- The assertion that matters: byte-identical file on the receiver --------
log "Polling receiver inbox for delivered bytes (up to 40s)"
RECV="$WORK/inbox/$DIST/payload.bin"
for i in $(seq 1 40); do
    if [ -f "$RECV" ]; then
        RSHA="$(openssl dgst -sha256 -binary "$RECV" | base64)"
        RSIZE="$(wc -c < "$RECV" | tr -d ' ')"
        [ "$RSIZE" = "$SIZE" ] || fail "received size $RSIZE != sent $SIZE"
        [ "$RSHA" = "$SHA_B64" ] || fail "received sha256 ($RSHA) != sent ($SHA_B64)"
        log "✓ delivered + content-validated in ~${i}s (${RSIZE} bytes, sha256 matches sender)"
        echo "PASS: attachment delivered outbox -> inbox with byte-identical content"
        exit 0
    fi
    sleep 1
done
fail "no file with matching content appeared in receiver inbox within 40s (dist=$DIST)"
