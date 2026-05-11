#!/usr/bin/env bash
# Bootstrap a two-node peat-node mesh: wait for both nodes to come up,
# fetch node-a's Iroh endpoint ID, then tell node-b to connect to it.
#
# Idempotent — if the nodes are already peered, the second ConnectPeer
# call is a no-op.

set -euo pipefail

NODE_A_URL="${NODE_A_URL:-http://localhost:50061}"
NODE_B_URL="${NODE_B_URL:-http://localhost:50062}"

call() {
  # Usage: call <base_url> <Method> <json_body>
  curl --silent --show-error --fail \
    -X POST "${1}/peat.sidecar.v1.PeatSidecar/${2}" \
    -H 'Content-Type: application/json' \
    -d "${3}"
}

wait_ready() {
  local url="$1"
  for _ in $(seq 1 30); do
    if call "$url" GetStatus '{}' >/dev/null 2>&1; then
      echo "  ready: $url"
      return 0
    fi
    sleep 1
  done
  echo "  TIMEOUT: $url did not become ready in 30s" >&2
  return 1
}

echo "Waiting for nodes to be ready..."
wait_ready "$NODE_A_URL"
wait_ready "$NODE_B_URL"

endpoint_a=$(call "$NODE_A_URL" GetStatus '{}' | jq -r .endpointAddr)
echo "node-a endpoint id: ${endpoint_a}"

echo "Peering node-b -> node-a..."
call "$NODE_B_URL" ConnectPeer "{\"endpointId\":\"${endpoint_a}\"}" >/dev/null
echo "  done"

# Auto-sync is on by default, but we also call StartSync explicitly so
# the script is correct even if PEAT_NODE_AUTO_SYNC=false.
call "$NODE_A_URL" StartSync '{}' >/dev/null
call "$NODE_B_URL" StartSync '{}' >/dev/null
echo "Sync started on both nodes."

echo
echo "Next: ./demo.sh"
