#!/usr/bin/env bash
# One-command TEST-03 proof for two independent Core NATS sites joined by Peat.
#
# Usage:
#   ./test/nats-bridge-e2e.sh       # clean run and teardown
#   ./test/nats-bridge-e2e.sh keep  # leave the isolated stack for debugging

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_DIR="${REPO_DIR}/examples/compose/nats-bridge"
BASE_COMPOSE="${COMPOSE_DIR}/docker-compose.yml"
LOCAL_COMPOSE="${COMPOSE_DIR}/docker-compose.local.yml"
FIXTURE="${COMPOSE_DIR}/vision-summary.json"
EXPECTED_FIXTURE="${EXPECTED_FIXTURE:-$FIXTURE}"
PROJECT="peat-nats-e2e-$$"
WORK="$(mktemp -d)"
KEEP="${1:-}"

STARTUP_DEADLINE_SECS="${STARTUP_DEADLINE_SECS:-240}"
BROKER_DEADLINE_SECS="${BROKER_DEADLINE_SECS:-60}"
PEER_DEADLINE_SECS="${PEER_DEADLINE_SECS:-90}"
BRIDGE_DEADLINE_SECS="${BRIDGE_DEADLINE_SECS:-60}"
RECEIVER_DEADLINE_SECS="${RECEIVER_DEADLINE_SECS:-30}"
DOCUMENT_DEADLINE_SECS="${DOCUMENT_DEADLINE_SECS:-90}"
RECEIPT_DEADLINE_SECS="${RECEIPT_DEADLINE_SECS:-60}"
QUIESCENCE_SECS="${QUIESCENCE_SECS:-10}"
QUERY_TIMEOUT_SECS="${QUERY_TIMEOUT_SECS:-60}"

EXPECTED_A_ID="fb3854f189c31fb61df2ace61f34ef7817fc90ccefbcfb94c3346e03b8b143fb"
EXPECTED_B_ID="84f5efd74c84c3ae848be186d7c2f169b94acda724ec6ee1506b321d9a504a4f"
SHARED_KEY="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
CURRENT_GATE="initialization"

compose() {
    PEAT_NODE_IMAGE=peat-node:nats-bridge-e2e \
        docker compose -p "${PROJECT}" \
        -f "${BASE_COMPOSE}" -f "${LOCAL_COMPOSE}" --profile local "$@"
}

log() { printf '==> %s\n' "$*"; }
pass() { printf '  PASS: %s\n' "$*"; }

diagnose() {
    printf 'FAIL: gate=%s\n' "${CURRENT_GATE}" >&2
    compose ps >&2 || true
    # One combined tail keeps the entire diagnostic payload at 120 lines.
    compose logs --no-color --tail 120 \
        nats-a nats-b peat-a peat-b receiver publisher 2>&1 | tail -n 120 >&2 || true
}

cleanup() {
    local status="$1"
    if [ "$status" -ne 0 ]; then
        diagnose
    fi
    if [ "${KEEP}" = "keep" ]; then
        log "Leaving Compose project ${PROJECT} up for debugging"
    else
        compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    rm -rf "${WORK}"
}
trap 'status=$?; trap - EXIT; cleanup "$status"; exit "$status"' EXIT

fail() {
    printf '  FAIL: %s\n' "$*" >&2
    return 1
}

validate_positive_integer() {
    local name="$1" value="$2"
    case "$value" in
        ''|*[!0-9]*|0) fail "${name} must be a positive integer" ;;
    esac
}

run_bounded() {
    local seconds="$1"
    shift
    "$@" &
    local command_pid=$!
    (
        sleep "$seconds"
        kill -TERM "$command_pid" 2>/dev/null || true
        sleep 2
        kill -KILL "$command_pid" 2>/dev/null || true
    ) &
    local watchdog_pid=$!
    local status=0
    wait "$command_pid" || status=$?
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    return "$status"
}

wait_for() {
    local gate="$1" seconds="$2"
    shift 2
    CURRENT_GATE="$gate"
    local deadline=$((SECONDS + seconds))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if "$@"; then
            pass "$gate"
            return 0
        fi
        sleep 1
    done
    fail "${gate} did not pass within ${seconds}s"
}

service_running() {
    local service="$1" container
    container="$(compose ps -q "$service" 2>/dev/null)"
    [ -n "$container" ] &&
        [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null)" = true ]
}

broker_healthy() {
    local service="$1"
    service_running "$service" &&
        compose exec -T "$service" \
            wget -q -T 2 -O /dev/null http://127.0.0.1:8222/healthz
}

node_status() {
    local service="$1"
    compose exec -T "$service" curl --silent --show-error --max-time 2 \
        -X POST -H 'content-type: application/json' \
        http://127.0.0.1:50051/peat.sidecar.v1.PeatSidecar/GetStatus -d '{}'
}

node_ready() {
    local service="$1" node_id="$2" endpoint_id="$3" status
    service_running "$service" || return 1
    status="$(node_status "$service" 2>/dev/null)" || return 1
    jq -e --arg node "$node_id" --arg endpoint "$endpoint_id" \
        '.nodeId == $node and .endpointAddr == $endpoint' \
        >/dev/null <<<"$status"
}

peer_connected() {
    local service="$1" status
    status="$(node_status "$service" 2>/dev/null)" || return 1
    jq -e '(.connectedPeers | type) == "number" and .connectedPeers >= 1' \
        >/dev/null <<<"$status"
}

exact_subscription_count() {
    local service="$1" evidence
    evidence="$(compose exec -T "$service" wget -q -T 2 -O - \
        'http://127.0.0.1:8222/subsz?subs=1&test=vision.summary' 2>/dev/null)" || return 1
    jq -er '
        if (.subscriptions_list | type) != "array" then
            error("malformed subscriptions_list")
        else
            [.subscriptions_list[] |
                if type == "string" then .
                elif type == "object" and (.subject | type) == "string" then .subject
                else error("malformed subscription entry")
                end
            ] as $subjects |
            [$subjects[] | select(. == "vision.summary")] | length
        end
    ' <<<"$evidence" 2>/dev/null
}

subscription_count_is() {
    local service="$1" expected="$2" actual
    actual="$(exact_subscription_count "$service")" || return 1
    [ "$actual" -eq "$expected" ]
}

write_query_credentials() {
    local service="$1" endpoint="$2" port="$3" path="$4"
    compose exec -T "$service" sh -ec '
        umask 077
        printf "app_id: nats-bridge-e2e\nshared_key: %s\npeers:\n  - %s@127.0.0.1:%s\n" \
            "$1" "$2" "$3" > "$4"
    ' sh "$SHARED_KEY" "$endpoint" "$port" "$path"
}

query_frames() {
    local service="$1" creds="$2" identity="$3" output="$4"
    compose exec -T "$service" peat \
        --creds "$creds" --as "$identity" --timeout "${QUERY_TIMEOUT_SECS}s" \
        --output json query frames > "$output"
}

valid_document() {
    local output="$1"
    jq -e --rawfile fixture "$EXPECTED_FIXTURE" '
        type == "object" and
        length == 1 and
        ([keys[] | select(startswith("frames:"))] | length) == 1 and
        ([.[]][0] |
            .kind == "peat.nats-bridge" and
            .version == 1 and
            .subject == "vision.summary" and
            .source_node_id == "edge-a" and
            .payload == $fixture)
    ' "$output" >/dev/null
}

documents_converged() {
    query_frames peat-a /tmp/phase5-query-a.yaml phase5-query-a "${WORK}/frames-a.json" \
        2>"${WORK}/query-a.stderr" || return 1
    query_frames peat-b /tmp/phase5-query-b.yaml phase5-query-b "${WORK}/frames-b.json" \
        2>"${WORK}/query-b.stderr" || return 1
    valid_document "${WORK}/frames-a.json" || return 1
    valid_document "${WORK}/frames-b.json" || return 1
    local key_a key_b
    key_a="$(jq -er 'keys[0]' "${WORK}/frames-a.json")" || return 1
    key_b="$(jq -er 'keys[0]' "${WORK}/frames-b.json")" || return 1
    [ "$key_a" = "$key_b" ] || return 1
    printf '%s\n' "$key_a" > "${WORK}/document-key"
}

delivery_stats() {
    compose exec -T receiver sh -ec '
        record=/results/deliveries
        [ -f "$record" ] || { printf "0 0 0\n"; exit 0; }
        awk '\''
            $0 == "match" { matches++ }
            $0 == "mismatch" { mismatches++ }
            END { printf "%d %d %d\n", NR, matches + 0, mismatches + 0 }
        '\'' "$record"
    '
}

one_exact_delivery() {
    local stats total matches mismatches
    stats="$(delivery_stats 2>/dev/null)" || return 1
    read -r total matches mismatches <<<"$stats"
    [ "$total" -eq 1 ] && [ "$matches" -eq 1 ] && [ "$mismatches" -eq 0 ]
}

unchanged_documents() {
    local original key_a key_b
    original="$(cat "${WORK}/document-key")"
    query_frames peat-a /tmp/phase5-query-a.yaml phase5-query-a-final \
        "${WORK}/frames-a-final.json" 2>"${WORK}/query-a-final.stderr" || return 1
    query_frames peat-b /tmp/phase5-query-b.yaml phase5-query-b-final \
        "${WORK}/frames-b-final.json" 2>"${WORK}/query-b-final.stderr" || return 1
    valid_document "${WORK}/frames-a-final.json" || return 1
    valid_document "${WORK}/frames-b-final.json" || return 1
    key_a="$(jq -er 'keys[0]' "${WORK}/frames-a-final.json")" || return 1
    key_b="$(jq -er 'keys[0]' "${WORK}/frames-b-final.json")" || return 1
    [ "$key_a" = "$original" ] && [ "$key_b" = "$original" ]
}

if [ -n "$KEEP" ] && [ "$KEEP" != keep ]; then
    fail "usage: $0 [keep]"
fi
for timeout_name in STARTUP_DEADLINE_SECS BROKER_DEADLINE_SECS PEER_DEADLINE_SECS \
    BRIDGE_DEADLINE_SECS RECEIVER_DEADLINE_SECS DOCUMENT_DEADLINE_SECS \
    RECEIPT_DEADLINE_SECS QUIESCENCE_SECS QUERY_TIMEOUT_SECS; do
    validate_positive_integer "$timeout_name" "${!timeout_name}"
done
command -v docker >/dev/null || fail "docker is unavailable"
command -v jq >/dev/null || fail "jq is unavailable"
[ -r "$EXPECTED_FIXTURE" ] || fail "expected fixture is unreadable"

CURRENT_GATE="clean-start"
compose down -v --remove-orphans >/dev/null 2>&1 || true

CURRENT_GATE="rendered-topology"
compose config --format json > "${WORK}/compose.json"
jq -e '
    def networks($service): (.services[$service].networks // {} | keys);
    def command_text($service): (.services[$service].command // [] | if type == "array" then join(" ") else . end);
    (networks("nats-a") | length) == 1 and
    (networks("nats-b") | length) == 1 and
    ([networks("nats-a")[]] - [networks("nats-b")[]] | length) == 1 and
    ([networks("nats-b")[]] - [networks("nats-a")[]] | length) == 1 and
    ([.services["nats-a"].ports[]?.target, .services["nats-b"].ports[]?.target]
        | map(select(. == 4222 or . == 8222)) | length) == 0 and
    ([command_text("nats-a"), command_text("nats-b")] | all(
        test("(^|[[:space:]])(-js|--jetstream|--cluster|--routes?|--gateway|--leafnode)([=[:space:]]|$)"; "i") | not))
' "${WORK}/compose.json" >/dev/null || fail "rendered topology permits a direct broker path"
pass "rendered topology isolates brokers and has no federation mode"

CURRENT_GATE="startup"
log "Building the current checkout and starting brokers plus Peat nodes"
run_bounded "$STARTUP_DEADLINE_SECS" compose up -d --build nats-a nats-b peat-a peat-b \
    >/dev/null || fail "current-checkout stack did not start within ${STARTUP_DEADLINE_SECS}s"

wait_for "nats-a private health" "$BROKER_DEADLINE_SECS" broker_healthy nats-a
wait_for "nats-b private health" "$BROKER_DEADLINE_SECS" broker_healthy nats-b
wait_for "peat-a process and identity" "$BROKER_DEADLINE_SECS" \
    node_ready peat-a edge-a "$EXPECTED_A_ID"
wait_for "peat-b process and identity" "$BROKER_DEADLINE_SECS" \
    node_ready peat-b edge-b "$EXPECTED_B_ID"

CURRENT_GATE="runtime-topology"
docker inspect "$(compose ps -q nats-a)" "$(compose ps -q nats-b)" \
    > "${WORK}/broker-inspect.json"
jq -e '
    (.[0].NetworkSettings.Networks | keys) as $a |
    (.[1].NetworkSettings.Networks | keys) as $b |
    ([$a[] | select(. as $network | $b | index($network))] | length) == 0 and
    ([.[].HostConfig.PortBindings // {} | keys[] |
        select(startswith("4222/") or startswith("8222/"))] | length) == 0
' "${WORK}/broker-inspect.json" >/dev/null || fail "runtime brokers share a network or publish NATS/monitor ports"
pass "runtime brokers have disjoint private networks"

wait_for "peat-a peer convergence" "$PEER_DEADLINE_SECS" peer_connected peat-a
wait_for "peat-b peer convergence" "$PEER_DEADLINE_SECS" peer_connected peat-b
wait_for "nats-a bridge subscription" "$BRIDGE_DEADLINE_SECS" \
    subscription_count_is nats-a 1
wait_for "nats-b bridge subscription" "$BRIDGE_DEADLINE_SECS" \
    subscription_count_is nats-b 1

NATS_B_BEFORE="$(exact_subscription_count nats-b)" || fail "nats-b bridge subscription evidence malformed"
[ "$NATS_B_BEFORE" -eq 1 ] || fail "nats-b bridge subscription count changed before receiver startup"

CURRENT_GATE="receiver-startup"
compose up -d --no-deps receiver >/dev/null
wait_for "nats-b receiver subscription" "$RECEIVER_DEADLINE_SECS" \
    subscription_count_is nats-b 2
NATS_B_AFTER="$(exact_subscription_count nats-b)" || fail "nats-b receiver subscription evidence malformed"
[ "$NATS_B_AFTER" -eq $((NATS_B_BEFORE + 1)) ] || fail "receiver did not add exactly one vision.summary subscription"

write_query_credentials peat-a "$EXPECTED_A_ID" 51071 /tmp/phase5-query-a.yaml
write_query_credentials peat-b "$EXPECTED_B_ID" 51072 /tmp/phase5-query-b.yaml

CURRENT_GATE="single-publication"
compose run --rm --no-deps -e PUBLISH_COUNT=1 -e PUBLISH_INTERVAL_SECS=1 publisher \
    >/dev/null || fail "single fixture publication failed"

wait_for "one same-key exact document per node" "$DOCUMENT_DEADLINE_SECS" documents_converged
wait_for "one byte-identical remote delivery" "$RECEIPT_DEADLINE_SECS" one_exact_delivery

CURRENT_GATE="raw-body-boundary"
if grep -Eq 'peat\.nats-bridge|source_node_id|frames:' "$EXPECTED_FIXTURE"; then
    fail "fixture overlaps bridge envelope metadata and cannot prove raw-body isolation"
fi
pass "remote raw body contains fixture bytes only"

CURRENT_GATE="delivery-quiescence"
deadline=$((SECONDS + QUIESCENCE_SECS))
while [ "$SECONDS" -lt "$deadline" ]; do
    one_exact_delivery || fail "delivery count changed during quiescence"
    sleep 1
done
one_exact_delivery || fail "delivery count changed at quiescence boundary"
unchanged_documents || fail "document count or key changed during quiescence"
pass "delivery and document state remained quiescent for ${QUIESCENCE_SECS}s"

printf '%s\n' 'PASS: one byte-identical vision.summary crossed only through Peat with one document per node and no loop'
