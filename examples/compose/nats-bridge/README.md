# Core NATS bridge Compose proof

The release contract is the bounded, self-cleaning command run from the repository root:

```sh
./test/nats-bridge-e2e.sh
```

It builds the current checkout and proves the path `publisher -> nats-a -> peat-a -> Peat mesh -> peat-b -> nats-b -> receiver`. The two brokers share no network: `nats-a` is only on the internal `site-a` network, `nats-b` only on internal `site-b`, and only the Peat nodes join `mesh`.

## Manual local walkthrough

Prerequisites are Docker with Compose and host `jq`. Use the same base topology and local-build override as the harness:

```sh
cd examples/compose/nats-bridge
export PEAT_NODE_IMAGE=ghcr.io/defenseunicorns/peat-node:v0.0.0
COMPOSE='docker compose -f docker-compose.yml -f docker-compose.local.yml --profile local'
$COMPOSE down -v --remove-orphans
$COMPOSE build peat-a peat-b
$COMPOSE config
```

In the rendered config, `nats-a` and `nats-b` have no common network. Ports 4222 and 8222 are not host-published. Monitor port 8222 is private/site-local and is queried only from its broker container.

Start brokers and nodes, but keep both helpers stopped:

```sh
$COMPOSE up -d nats-a nats-b peat-a peat-b
```

Bound every loop in operator automation. `/healthz` proves broker availability; node HTTP/RPC availability and `connectedPeers >= 1` prove Peat connectivity. Neither node `GetStatus` nor broker health alone proves bridge readiness.

```sh
$COMPOSE exec -T nats-a wget -q -T 2 -O- http://127.0.0.1:8222/healthz
$COMPOSE exec -T nats-b wget -q -T 2 -O- http://127.0.0.1:8222/healthz
$COMPOSE ps
```

For bridge readiness, query `/subsz?subs=1&test=vision.summary`. NATS 2.14.3 may return `subscriptions_list` entries as objects with a string `.subject`; validate and normalize entries before exact filtering. The harness accepts a string entry too and rejects every other shape. An equivalent filter is:

```jq
[.subscriptions_list[] |
  if type == "string" then .
  elif type == "object" and (.subject | type) == "string" then .subject
  else error("invalid subscriptions_list entry") end |
  select(. == "vision.summary")] | length
```

Use it on each private monitor and require filtered count one before receiver startup. Snapshot the nats-b filtered count as the baseline. Never gate on global `num_subscriptions`: async-NATS `_INBOX.>` subscriptions may be present.

Start the receiver before any publication, then poll the same exact-subject filter until nats-b has filtered count two and delta `+1` from its baseline:

```sh
$COMPOSE up -d receiver
```

Only after that gate passes, perform one publication. The helper publishes fixture stdin with `--no-templates` and preserves its terminal newline:

```sh
PUBLISH_COUNT=1 $COMPOSE run --rm publisher
```

Query `frames` independently on peat-a and peat-b using the in-image `peat --output json query frames` pattern in `test/nats-bridge-e2e.sh`; fresh stores must contain one same-key document on each node. Inspect only the fixed raw comparison record:

```sh
$COMPOSE exec -T receiver sh -c 'tail -n 2 /results/deliveries'
```

An exact body produces `match`; the verifier uses `cmp` on stdin and never logs payload bytes. For the demonstration cadence, restart a receiver prepared for the desired count, prove its `+1` subscription delta again, then run the default continuous publisher (30 seconds):

```sh
$COMPOSE up -d publisher
```

Quiescence means both document cardinality/key and delivery-record count remain unchanged through the bounded quiet window after a one-shot run. It detects document and NATS echo loops; it does not strengthen delivery semantics.

Diagnostics must remain bounded and payload-safe:

```sh
$COMPOSE ps
$COMPOSE logs --no-color --tail 120 peat-a peat-b nats-a nats-b receiver
```

Do not print fixture contents, shared keys, credentials, or full environments. Tear down disposable state:

```sh
$COMPOSE down -v --remove-orphans
```

Core NATS is at-most-once. This bridge supplies no durable input, replay, subscriber acknowledgement or subscriber-delivery proof, global ordering, exactly-once delivery, or zero-loss overload guarantee. Peat persistence does not upgrade those semantics. A successful walkthrough is evidence for one observed run only.

For actual Jetson and second-host TEST-04 UAT, follow [the edge smoke procedure](../../../docs/NATS_BRIDGE_EDGE_SMOKE.md); do not infer a physical result from this local proxy.
