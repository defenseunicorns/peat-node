#!/usr/bin/env bash
# Functional test for the SEND-side outbox watcher (PEAT_NODE_ATTACHMENT_OUTBOX_WATCH).
#
# Proves the hands-off path: with the outbox watcher enabled on the sender, a
# file dropped into its --attachment-root is auto-distributed and lands
# byte-identical in the receiver's --attachment-inbox over a real two-node iroh
# transfer — with NO SendAttachments gRPC call. Complements
# attachment-delivery-compose.sh (which exercises the explicit RPC path).
#
# Prereqs: docker (with compose), curl, jq, openssl.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="${PEAT_NODE_IMAGE:-peat-node:dev}"
PROJECT="peat-outbox-functest"
WORK="$(mktemp -d)"

log() { echo "==> $*"; }
fail() {
    echo "  ✗ $*" >&2
    (cd "$WORK" && docker compose -p "$PROJECT" logs sender 2>&1 | tail -25 >&2) || true
    exit 1
}
cleanup() {
    (cd "$WORK" && docker compose -p "$PROJECT" down -v >/dev/null 2>&1) || true
    docker run --rm -v "$WORK":/w alpine sh -c 'rm -rf /w/* /w/.* 2>/dev/null' >/dev/null 2>&1 || true
    rm -rf "$WORK" 2>/dev/null || true
}
trap cleanup EXIT
rpc() {
    curl -sS -X POST -H 'content-type: application/json' \
        "http://127.0.0.1:$1/peat.sidecar.v1.PeatSidecar/$2" -d "$3"
}

if [ -z "${PEAT_NODE_IMAGE:-}" ] && ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    log "Building $IMAGE from $REPO_DIR"
    docker build -t "$IMAGE" "$REPO_DIR" >/dev/null
fi
log "Using image: $IMAGE"

K="$(head -c 32 /dev/urandom | base64)"
mkdir -p "$WORK/outbox" "$WORK/inbox"

cat > "$WORK/docker-compose.yml" <<YAML
services:
  sender:
    image: ${IMAGE}
    environment:
      PEAT_NODE_NODE_ID: sender
      PEAT_NODE_APP_ID: outbox-functest
      PEAT_NODE_SHARED_KEY: "${K}"
      PEAT_NODE_IROH_UDP_PORT: "51071"
      PEAT_NODE_DISABLE_MDNS: "true"
      PEAT_NODE_AUTO_SYNC: "true"
      PEAT_NODE_ATTACHMENT_ROOT: "outbox=/var/lib/peat/outbox"
      PEAT_NODE_ATTACHMENT_OUTBOX_WATCH: "true"
      PEAT_NODE_ATTACHMENT_OUTBOX_POLL_SECS: "1"
      RUST_LOG: "peat_node=info,peat_node::attachments=debug"
    ports: ["50061:50051"]
    volumes: ["${WORK}/outbox:/var/lib/peat/outbox:ro"]
  receiver:
    image: ${IMAGE}
    environment:
      PEAT_NODE_NODE_ID: receiver
      PEAT_NODE_APP_ID: outbox-functest
      PEAT_NODE_SHARED_KEY: "${K}"
      PEAT_NODE_IROH_UDP_PORT: "51072"
      PEAT_NODE_DISABLE_MDNS: "true"
      PEAT_NODE_AUTO_SYNC: "true"
      PEAT_NODE_ATTACHMENT_INBOX: "/var/lib/peat/inbox"
      RUST_LOG: "peat_node=info,peat_node::attachments=debug"
    ports: ["50062:50051"]
    volumes: ["${WORK}/inbox:/var/lib/peat/inbox"]
YAML

log "Bringing up sender (outbox-watch) + receiver"
(cd "$WORK" && docker compose -p "$PROJECT" up -d) >/dev/null

for i in $(seq 1 30); do
    if rpc 50061 GetStatus '{}' >/dev/null 2>&1 && rpc 50062 GetStatus '{}' >/dev/null 2>&1; then break; fi
    sleep 1
    [ "$i" = 30 ] && fail "nodes did not become ready within 30s"
done

EP_S="$(rpc 50061 GetStatus '{}' | jq -r .endpointAddr)"
EP_R="$(rpc 50062 GetStatus '{}' | jq -r .endpointAddr)"
log "Peering sender<->receiver"
rpc 50062 ConnectPeer "$(jq -nc --arg id "$EP_S" '{endpointId:$id,addresses:["sender:51071"]}')" >/dev/null
rpc 50061 ConnectPeer "$(jq -nc --arg id "$EP_R" '{endpointId:$id,addresses:["receiver:51072"]}')" >/dev/null
sleep 3
PEERS="$(rpc 50061 GetStatus '{}' | jq -r '.connectedPeers // 0')"
[ "${PEERS:-0}" -ge 1 ] || fail "sender reports ${PEERS:-0} peers — delivery would be vacuous"

# The whole point: DROP a file in the outbox. No SendAttachments call.
log "Dropping a file into the sender's outbox (NO SendAttachments)"
PAYLOAD="$WORK/outbox/dropped.bin"
head -c 1572864 /dev/urandom > "$PAYLOAD"   # 1.5 MiB
SHA_B64="$(openssl dgst -sha256 -binary "$PAYLOAD" | base64)"

log "Polling receiver inbox for the auto-distributed file (up to 40s)"
for i in $(seq 1 40); do
    RECV="$(find "$WORK/inbox" -name dropped.bin -type f 2>/dev/null | head -1)"
    if [ -n "$RECV" ]; then
        RSHA="$(openssl dgst -sha256 -binary "$RECV" | base64)"
        [ "$RSHA" = "$SHA_B64" ] || fail "received sha256 ($RSHA) != sent ($SHA_B64)"
        log "✓ outbox watcher auto-delivered + content-validated in ~${i}s (sha256 matches)"
        echo "PASS: file dropped in outbox -> peer inbox, byte-identical, no gRPC SendAttachments"
        exit 0
    fi
    sleep 1
done
fail "dropped file never appeared in receiver inbox within 40s (outbox watcher / fetch)"
