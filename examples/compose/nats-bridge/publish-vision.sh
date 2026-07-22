#!/bin/sh
# Publish the fixture's exact bytes; payload data never enters shell storage.
set -eu

FIXTURE=${FIXTURE:-/work/vision-summary.json}
NATS_URL=${NATS_URL:-nats://nats-a:4222}
NATS_SUBJECT=${NATS_SUBJECT:-vision.summary}
PUBLISH_COUNT=${PUBLISH_COUNT:-0}
PUBLISH_INTERVAL_SECS=${PUBLISH_INTERVAL_SECS:-30}

if [ ! -r "$FIXTURE" ]; then
  printf '%s\n' 'fixture is not readable' >&2
  exit 2
fi

case "$NATS_SUBJECT" in
  ''|*[[:space:]]*|*'*'*|*'>'*)
    printf '%s\n' 'NATS_SUBJECT must be a non-wildcard literal subject' >&2
    exit 2
    ;;
esac

case "$PUBLISH_COUNT" in
  ''|*[!0-9]*)
    printf '%s\n' 'PUBLISH_COUNT must be a nonnegative integer' >&2
    exit 2
    ;;
esac

case "$PUBLISH_INTERVAL_SECS" in
  ''|*[!0-9]*|0)
    printf '%s\n' 'PUBLISH_INTERVAL_SECS must be a positive integer' >&2
    exit 2
    ;;
esac

if ! command -v nats >/dev/null 2>&1; then
  printf '%s\n' 'nats CLI is unavailable' >&2
  exit 2
fi

sequence=0
while [ "$PUBLISH_COUNT" -eq 0 ] || [ "$sequence" -lt "$PUBLISH_COUNT" ]; do
  sequence=$((sequence + 1))
  # nats-box 0.19.5 interprets `--templates=false` as positional input;
  # `--no-templates` is its canonical negative boolean spelling.
  nats --server "$NATS_URL" pub --force-stdin --no-templates \
    "$NATS_SUBJECT" < "$FIXTURE"
  printf 'published sequence=%s cadence_seconds=%s\n' \
    "$sequence" "$PUBLISH_INTERVAL_SECS"

  if [ "$PUBLISH_COUNT" -ne 0 ] && [ "$sequence" -ge "$PUBLISH_COUNT" ]; then
    break
  fi
  sleep "$PUBLISH_INTERVAL_SECS"
done
