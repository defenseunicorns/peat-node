#!/usr/bin/env bash
# Peer the two nodes from the two-node compose. Both directions are
# needed: A's IrohFileDistribution::resolve_targets reads from A's
# blob_store.known_peers(), which is only populated by ConnectPeer
# *into* A. Without A → B as well, A's distribution doc's
# target_nodes is empty and B never delivers.

set -euo pipefail

A_BASE="${A_BASE:-http://127.0.0.1:50061}"
B_BASE="${B_BASE:-http://127.0.0.1:50062}"

call() {
  local base="$1"; shift
  local method="$1"; shift
  local body="$1"; shift
  curl -sS -X POST -H 'content-type: application/json' \
    "${base}/peat.sidecar.v1.PeatSidecar/${method}" -d "${body}"
}

EP_A=$(call "${A_BASE}" GetStatus '{}' | jq -r '.endpointAddr')
EP_B=$(call "${B_BASE}" GetStatus '{}' | jq -r '.endpointAddr')

echo ">>> A endpoint: ${EP_A}"
echo ">>> B endpoint: ${EP_B}"

# B → A (B learns about A).
echo ">>> ConnectPeer: B → A"
call "${B_BASE}" ConnectPeer "$(jq -n --arg id "${EP_A}" \
  '{ endpointId: $id, addresses: ["peat-node-attachments-a:51071"] }')" | jq .

# A → B (A learns about B, so its resolve_targets list includes B for
# AllNodes-scoped distributions).
echo ">>> ConnectPeer: A → B"
call "${A_BASE}" ConnectPeer "$(jq -n --arg id "${EP_B}" \
  '{ endpointId: $id, addresses: ["peat-node-attachments-b:51072"] }')" | jq .

echo
echo ">>> peered. Now run ./send.sh against the two-node compose."
echo ">>> Override the default 1-node ENDPOINT with: ENDPOINT=${A_BASE} ./send.sh"
