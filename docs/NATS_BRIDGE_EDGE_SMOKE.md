# NATS bridge edge smoke: Jetson Orin Nano and second host

This is the physical TEST-04 operator UAT. It requires an actual `linux/arm64` Jetson Orin Nano and a second `linux/amd64` machine on the same LAN. A local Compose or CI result is not a substitute.

Core NATS is at-most-once. There is no durable input, replay, subscriber acknowledgement or subscriber-delivery proof, global ordering, exactly-once delivery, or zero-loss overload guarantee. Peat persistence does not upgrade these semantics. The run below records one observation only.

## Prerequisites and fixed limits

Copy this repository's `examples/compose/nats-bridge/` directory to both hosts. Docker with Compose is the only runtime tooling required; the pinned `nats:2.14.3-alpine` and `natsio/nats-box:0.19.5` containers supply broker and NATS CLI functions. Do not build Rust on the Jetson.

Choose an actual released version and replace `vX.Y.Z`; never use a mutable image:

```sh
export PEAT_NODE_IMAGE=ghcr.io/defenseunicorns/peat-node:vX.Y.Z
```

The release manifest built by `.github/workflows/release.yml` contains both `linux/arm64` and `linux/amd64`. On each host record `docker image inspect "$PEAT_NODE_IMAGE" --format '{{.Os}}/{{.Architecture}}'` after pulling.

Generate one 32-byte base64 shared key on a trusted host and transfer it securely to the other host. Do not put it in a run record or diagnostic output:

```sh
export PEAT_SHARED_KEY="$(docker run --rm natsio/nats-box:0.19.5 sh -ec 'head -c 32 /dev/urandom | base64')"
docker pull "$PEAT_NODE_IMAGE"
A_ID="$(docker run --rm "$PEAT_NODE_IMAGE" peat-node derive-id --shared-key "$PEAT_SHARED_KEY" --node-id edge-a)"
B_ID="$(docker run --rm "$PEAT_NODE_IMAGE" peat-node derive-id --shared-key "$PEAT_SHARED_KEY" --node-id edge-b)"
```

`edge-a` and `edge-b` are stable identity inputs, not cosmetic names; changing either changes its endpoint ID. On each host create an untracked secret override so the checked-in topology uses the generated key:

```sh
umask 077
printf 'services:\n  peat-a:\n    environment:\n      PEAT_NODE_SHARED_KEY: ${PEAT_SHARED_KEY:?}\n  peat-b:\n    environment:\n      PEAT_NODE_SHARED_KEY: ${PEAT_SHARED_KEY:?}\n' > /tmp/nats-bridge-secret.yml
```

Do not configure broker federation. mDNS is disabled by the checked-in environment. Only Peat/Iroh UDP crosses the LAN.

## Site A — Jetson Orin Nano (`linux/arm64`)

Set the Jetson's real LAN IP and Site B coordinates. Open only UDP 51071 in the Jetson firewall:

```sh
cd examples/compose/nats-bridge
export A_LAN_IP=192.168.1.10
export B_LAN_IP=192.168.1.11
export PEAT_A_PEERS="${B_ID}@${B_LAN_IP}:51072"
COMPOSE_A='docker compose -f docker-compose.yml -f /tmp/nats-bridge-secret.yml --profile site-a'
```

Render and inspect before launch. The Site A profile must show peat-a `51071:51071/udp`; it must not host-publish broker 4222 or private monitor 8222:

```sh
$COMPOSE_A config
$COMPOSE_A run --rm preflight-site-a
```

Start only local nats-a and peat-a. The Site A publisher is held stopped and is deliberately absent from every startup command at this stage:

```sh
$COMPOSE_A up -d nats-a peat-a
$COMPOSE_A ps
```

Do not start `publisher` yet.

## Site B — second machine (`linux/amd64`)

Set the second host's real LAN IP and reciprocal Site A coordinates. Open only UDP 51072 in its firewall:

```sh
cd examples/compose/nats-bridge
export A_LAN_IP=192.168.1.10
export B_LAN_IP=192.168.1.11
export PEAT_B_PEERS="${A_ID}@${A_LAN_IP}:51071"
COMPOSE_B='docker compose -f docker-compose.yml -f /tmp/nats-bridge-secret.yml --profile site-b'
$COMPOSE_B config
$COMPOSE_B run --rm preflight-site-b
$COMPOSE_B up -d nats-b peat-b
$COMPOSE_B ps
```

The render must show peat-b `51072:51072/udp` only, with neither 4222 nor private 8222 host-published. On both sites, use bounded logs to confirm the expected reciprocal Peat peer is connected:

```sh
$COMPOSE_A logs --no-color --tail 120 peat-a
$COMPOSE_B logs --no-color --tail 120 peat-b
```

Do not print full environments or the shared key. A healthy process alone is insufficient; require peer connectivity before continuing.

Establish the receiver readiness gate from inside Site B's private broker network. NATS 2.14.3 `subscriptions_list` may contain objects with a string `.subject`; validate and normalize them before exact `vision.summary` filtering. Do not use global `num_subscriptions`.

```sh
subject_count_b() {
  $COMPOSE_B exec -T nats-b wget -q -T 2 -O- \
    'http://127.0.0.1:8222/subsz?subs=1&test=vision.summary' |
  docker run --rm -i ghcr.io/jqlang/jq:1.8.1 -er \
    '[.subscriptions_list[] | if type == "string" then . elif type == "object" and (.subject|type)=="string" then .subject else error("bad subscription entry") end | select(. == "vision.summary")] | length'
}
BASELINE="$(subject_count_b)"
test "$BASELINE" -eq 1
$COMPOSE_B up -d receiver
READY="$(subject_count_b)"
test "$READY" -eq 2
test $((READY - BASELINE)) -eq 1
```

Repeat `subject_count_b` with a bounded polling deadline if startup is not immediate. The required result is exact filtered count two and delta `+1`; only then is the receiver ready before publication.

## Start Site A publication only after receiver readiness

After reciprocal peer connectivity and Site B's count-two/delta `+1` gate pass, start the default infinite publisher on Site A. It uses `--no-templates` and publishes the checked-in fixture every 30 seconds:

```sh
$COMPOSE_A up -d publisher
```

On Site B, inspect only fixed comparison records, never payload output:

```sh
$COMPOSE_B exec -T receiver sh -ec 'tail -n 2 /results/deliveries'
```

Record timestamps for at least two `match` lines approximately 30 seconds apart. `match` means the verifier's raw stdin was byte-equal under `cmp` to the checked-in fixture, including its terminal newline. It is not subscriber acknowledgement.

## Bounded diagnostics and teardown

```sh
# Site A
$COMPOSE_A ps
$COMPOSE_A logs --no-color --tail 120 nats-a peat-a publisher
# Site B
$COMPOSE_B ps
$COMPOSE_B logs --no-color --tail 120 nats-b peat-b receiver
```

Diagnostics must not contain payload bodies, the shared key, credentials, or full environments. Stop publication first, then remove each site's containers and volumes:

```sh
$COMPOSE_A stop publisher
$COMPOSE_A down -v --remove-orphans
$COMPOSE_B down -v --remove-orphans
```

## Run record

Record only these non-secret fields:

- explicit image tag and observed Site A `linux/arm64` / Site B `linux/amd64` architecture;
- Jetson Site A and second-host Site B roles and LAN addresses;
- reciprocal `PEAT_A_PEERS` / `PEAT_B_PEERS` coordinates (endpoint IDs, LAN IPs, UDP 51071/51072; no key);
- rendered UDP mappings and confirmation that 4222/8222 remained private;
- reciprocal peer-connected timestamp;
- receiver baseline-one and ready-two/delta `+1` timestamp;
- timestamps for at least two `match` records about 30 seconds apart;
- Site A and Site B cleanup success.

Do not record the payload, shared key, credentials, full environment, or any durable, acknowledged, ordered, or exactly-once claim.
