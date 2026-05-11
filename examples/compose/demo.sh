#!/usr/bin/env bash
# Write a document on node-a, poll node-b until it appears, print the result.
# Demonstrates CRDT sync across two peat-node instances.

set -euo pipefail

NODE_A_URL="${NODE_A_URL:-http://localhost:50061}"
NODE_B_URL="${NODE_B_URL:-http://localhost:50062}"

call() {
  local url="${1}/peat.sidecar.v1.PeatSidecar/${2}"
  local body
  local code
  body=$(curl --silent --show-error \
    -w '\n%{http_code}' \
    -X POST "$url" \
    -H 'Content-Type: application/json' \
    -d "${3}")
  code=$(printf '%s' "$body" | tail -n1)
  body=$(printf '%s' "$body" | sed '$d')
  if [ "$code" != "200" ]; then
    echo "ERROR: $url returned HTTP $code" >&2
    echo "  body: $body" >&2
    return 1
  fi
  printf '%s' "$body"
}

COLLECTION=hello
DOC_ID=world
PAYLOAD='{"msg":"sync via CRDT","from":"node-a"}'

echo "[node-a] PutDocument ${COLLECTION}/${DOC_ID}"
call "$NODE_A_URL" PutDocument \
  "{\"collection\":\"${COLLECTION}\",\"docId\":\"${DOC_ID}\",\"jsonData\":$(jq -Rs <<<"$PAYLOAD")}" \
  >/dev/null
echo "  wrote: ${PAYLOAD}"

echo "[node-b] Polling GetDocument ${COLLECTION}/${DOC_ID}..."
for i in $(seq 1 30); do
  body=$(call "$NODE_B_URL" GetDocument \
    "{\"collection\":\"${COLLECTION}\",\"docId\":\"${DOC_ID}\"}")
  data=$(echo "$body" | jq -r '.jsonData // empty')
  if [ -n "$data" ]; then
    echo "  PASS: node-b received it after ${i}s"
    echo "  data: ${data}"
    exit 0
  fi
  sleep 1
done

echo "  FAIL: node-b did not receive the document within 30s" >&2
echo "  Last response from node-b: ${body}" >&2
exit 1
