#!/usr/bin/env bash
# Lightweight functional benchmark for peat-node's attachment surface.
#
# Drives a series of SendAttachments + GetAttachmentDistribution polls
# against an already-running peat-node, timing each round-trip end to
# end. Default suite: hello.txt (the in-repo baseline) plus generated
# 1 MiB / 10 MiB / 100 MiB files filled from /dev/urandom. Each generated
# file is regenerated on every run so iroh's content-addressed store
# can't dedup against a prior session.
#
# Usage:
#   ./send.sh                # default suite: 1 + 10 + 100 (MiB)
#   ./send.sh 1 5 25         # custom sizes (MiB)
#   ENDPOINT=http://other:port ./send.sh   # alternate target
#
# Prereqs:
#   - peat-node running and reachable at $ENDPOINT (default
#     http://127.0.0.1:50051). For the bundled compose:
#       docker compose up -d
#   - curl, openssl, base64, jq, awk on PATH.
#
# What's measured per file:
#   - send_ms      — wall clock from POST(SendAttachments) to response
#     received. Server-side this covers validate (filesystem stat),
#     ingest (full file read + sha256 verify + iroh blob create), and
#     distribute (creates the distribution document, starts the
#     watcher). For zero-peer scopes this dominates total time.
#   - complete_ms  — wall clock from the same POST to the first
#     GetAttachmentDistribution that returns a terminal status. For
#     zero-peer scopes the watcher's zero-target short-circuit fires
#     almost immediately after SendAttachments returns, so this is
#     roughly send_ms + one poll-interval.
#
# v1-honesty caveat: in a single-node compose with no peers, "COMPLETED"
# is the vacuous-zero-target case from peat-protocol's `is_complete()`.
# These numbers measure sender-side ingest + status-report latency, not
# real inter-peer transfer rate. The inter-peer transfer test needs a
# two-node setup once peat-protocol's receive-side observer hooks land.

set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:50051}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# OUTBOX_DIR defaults to ./outbox for the single-node compose. The
# two-node demo (docker-compose.two-node.yml) uses ./outbox-a; export
# OUTBOX_DIR=outbox-a (or set explicitly) before running.
OUTBOX="${SCRIPT_DIR}/${OUTBOX_DIR:-outbox}"
POLL_INTERVAL_SECS="${POLL_INTERVAL_SECS:-0.05}"
# Cap so a stuck distribution doesn't hang the benchmark forever.
POLL_TIMEOUT_SECS="${POLL_TIMEOUT_SECS:-120}"

now_ns() {
  # GNU `date +%s%N` gives nanosecond precision on Linux. macOS BSD
  # `date` ignores `%N` and emits a literal `N`; fall back to a Python
  # one-liner there. The script targets Linux first (the compose runs a
  # Debian container), but the host might be either.
  local ns
  ns=$(date +%s%N)
  if [[ "${ns}" == *N* ]]; then
    python3 -c 'import time; print(int(time.time_ns()))'
  else
    printf '%s' "${ns}"
  fi
}

elapsed_ms() {
  # $1 = start_ns, $2 = end_ns
  echo $(( ($2 - $1) / 1000000 ))
}

ensure_node_up() {
  if ! curl -sS -o /dev/null -X POST \
      -H 'content-type: application/json' \
      "${ENDPOINT}/peat.sidecar.v1.PeatSidecar/GetStatus" -d '{}'; then
    echo "ERROR: cannot reach peat-node at ${ENDPOINT}" >&2
    echo "       did you run 'docker compose up -d'?" >&2
    exit 1
  fi
}

generate_random_file() {
  local name="$1"
  local size_mib="$2"
  local path="${OUTBOX}/${name}"
  mkdir -p "${OUTBOX}"
  # `dd` is more universally available than `head -c` with multiplier
  # suffixes. `status=none` silences the per-block progress logging.
  dd if=/dev/urandom of="${path}" bs=1048576 count="${size_mib}" status=none
}

# Echoes a row to stdout: NAME BYTES SEND_MS COMPLETE_MS STATUS DIST_ID
benchmark_one() {
  local name="$1"
  local local_path="${OUTBOX}/${name}"
  if [[ ! -f "${local_path}" ]]; then
    echo "missing ${local_path}" >&2
    return 1
  fi
  local size
  size=$(wc -c < "${local_path}" | tr -d ' ')
  local sha_b64
  sha_b64=$(openssl dgst -sha256 -binary "${local_path}" | base64 | tr -d '\n')

  local send_start send_end
  send_start=$(now_ns)
  local resp
  resp=$(curl -sS -X POST -H 'content-type: application/json' \
    "${ENDPOINT}/peat.sidecar.v1.PeatSidecar/SendAttachments" \
    -d "$(jq -n \
      --arg rel "${name}" \
      --arg sha "${sha_b64}" \
      --argjson size "${size}" \
      '{ files: [{ rootName: "outbox", relativePath: $rel, sizeBytes: $size, sha256: $sha }], scope: { allNodes: {} } }')")
  send_end=$(now_ns)

  local dist_id
  dist_id=$(echo "${resp}" | jq -r '.handles[0].distributionId // empty')
  if [[ -z "${dist_id}" ]]; then
    printf '%-22s %12s  --  --  FAIL (response: %s)\n' "${name}" "${size}" "${resp}"
    return 1
  fi

  # Poll for terminal status. Timeout-bounded so the benchmark exits
  # even if the watcher gets stuck.
  local poll_deadline
  poll_deadline=$(( $(date +%s) + POLL_TIMEOUT_SECS ))
  local status="DISTRIBUTION_STATUS_UNSPECIFIED"
  while (( $(date +%s) < poll_deadline )); do
    local s
    s=$(curl -sS -X POST -H 'content-type: application/json' \
      "${ENDPOINT}/peat.sidecar.v1.PeatSidecar/GetAttachmentDistribution" \
      -d "$(jq -n --arg id "${dist_id}" '{ distributionId: $id }')" \
      | jq -r '.status // "DISTRIBUTION_STATUS_UNSPECIFIED"')
    case "${s}" in
      DISTRIBUTION_STATUS_COMPLETED|DISTRIBUTION_STATUS_PARTIAL|DISTRIBUTION_STATUS_FAILED|DISTRIBUTION_STATUS_CANCELLED)
        status="${s}"
        break
        ;;
    esac
    sleep "${POLL_INTERVAL_SECS}"
  done
  local complete_end
  complete_end=$(now_ns)

  local send_ms complete_ms
  send_ms=$(elapsed_ms "${send_start}" "${send_end}")
  complete_ms=$(elapsed_ms "${send_start}" "${complete_end}")

  # Right-align numeric columns so the human reading the output can
  # eyeball scale differences.
  printf '%-22s %12s  %8s ms  %10s ms  %-35s %s\n' \
    "${name}" "${size}" "${send_ms}" "${complete_ms}" "${status#DISTRIBUTION_STATUS_}" "${dist_id}"
}

main() {
  local sizes=(1 10 100)
  if (( $# > 0 )); then
    sizes=("$@")
  fi

  ensure_node_up

  echo ">>> Generating test files (${sizes[*]} MiB from /dev/urandom)"
  for mib in "${sizes[@]}"; do
    generate_random_file "test-${mib}mb.bin" "${mib}"
  done
  echo

  printf '%-22s %12s  %11s  %13s  %-35s %s\n' "FILE" "BYTES" "SEND" "COMPLETE" "STATUS" "DIST_ID"
  printf '%-22s %12s  %11s  %13s  %-35s %s\n' "----" "-----" "----" "--------" "------" "-------"

  benchmark_one "hello.txt"
  for mib in "${sizes[@]}"; do
    benchmark_one "test-${mib}mb.bin"
  done

  echo
  echo "Note: zero-peer scope — 'COMPLETED' is the vacuous-zero-target case."
  echo "      send_ms measures sender-side ingest (read + hash + iroh content-address)."
  echo "      complete_ms ≈ send_ms + one poll-interval for the watcher's zero-target shortcut."
}

main "$@"
