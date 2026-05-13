#!/usr/bin/env bash
# Send `outbox/hello.txt` through peat-node's SendAttachments RPC.
#
# Prereqs: docker compose up -d && curl + sha256sum + base64 + jq on PATH.
#
# What this exercises end-to-end:
#   - The attachment safety default (the RPC is enabled because the
#     compose file sets PEAT_NODE_ATTACHMENT_ROOT)
#   - Path validation against the allowlisted "outbox" root
#   - Streaming sha256 verification during blob ingest
#   - Distribution document creation (no peers → immediate Completed
#     via the watcher's zero-target short-circuit)
#   - GetAttachmentDistribution status lookup

set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:50051}"
FILE="${FILE:-hello.txt}"
LOCAL_PATH="$(dirname "$0")/outbox/${FILE}"

if [[ ! -f "${LOCAL_PATH}" ]]; then
  echo "no file at ${LOCAL_PATH}" >&2
  exit 1
fi

SIZE=$(wc -c < "${LOCAL_PATH}" | tr -d ' ')
SHA256_HEX=$(sha256sum "${LOCAL_PATH}" | cut -d' ' -f1)
# Proto3 JSON encodes the `bytes` field as base64.
SHA256_B64=$(printf '%s' "${SHA256_HEX}" | xxd -r -p | base64)

echo ">>> SendAttachments: ${FILE} (${SIZE} bytes, sha256=${SHA256_HEX:0:16}...)"

SEND_RESP=$(curl -sS -X POST \
  -H 'content-type: application/json' \
  "${ENDPOINT}/peat.sidecar.v1.PeatSidecar/SendAttachments" \
  -d "$(jq -n \
    --arg rel "${FILE}" \
    --arg sha "${SHA256_B64}" \
    --argjson size "${SIZE}" \
    '{
      files: [{
        rootName: "outbox",
        relativePath: $rel,
        sizeBytes: $size,
        sha256: $sha
      }],
      scope: { allNodes: {} }
    }')")

echo "${SEND_RESP}" | jq .

BUNDLE_ID=$(echo "${SEND_RESP}" | jq -r '.bundleId')
DIST_ID=$(echo "${SEND_RESP}" | jq -r '.handles[0].distributionId')
BLOB=$(echo "${SEND_RESP}" | jq -r '.handles[0].blobToken')

echo
echo ">>> bundle_id=${BUNDLE_ID}"
echo ">>> blob_token=${BLOB}"
echo ">>> distribution_id=${DIST_ID}"
echo

# Brief settle for the watcher's zero-peer short-circuit to fire.
sleep 0.5

echo ">>> GetAttachmentDistribution"
curl -sS -X POST \
  -H 'content-type: application/json' \
  "${ENDPOINT}/peat.sidecar.v1.PeatSidecar/GetAttachmentDistribution" \
  -d "$(jq -n --arg id "${DIST_ID}" '{ distributionId: $id }')" \
  | jq .
