#!/bin/sh
# Compare translator stdin directly with the fixture and record no payload data.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
DELIVERY_RECORD=${1:-${DELIVERY_RECORD:-}}
FIXTURE=${FIXTURE:-"$SCRIPT_DIR/vision-summary.json"}

if [ ! -r "$FIXTURE" ]; then
  printf '%s\n' 'fixture is not readable' >&2
  exit 2
fi
if [ -z "$DELIVERY_RECORD" ]; then
  printf '%s\n' 'delivery record path is required' >&2
  exit 2
fi

case "$DELIVERY_RECORD" in
  */*) record_dir=${DELIVERY_RECORD%/*} ;;
  *) record_dir=. ;;
esac
if [ ! -d "$record_dir" ] || [ ! -w "$record_dir" ]; then
  printf '%s\n' 'delivery record directory is not writable' >&2
  exit 2
fi
if [ -e "$DELIVERY_RECORD" ] && [ ! -w "$DELIVERY_RECORD" ]; then
  printf '%s\n' 'delivery record is not writable' >&2
  exit 2
fi
if ! command -v cmp >/dev/null 2>&1; then
  printf '%s\n' 'byte comparator is unavailable' >&2
  exit 2
fi

if cmp -s - "$FIXTURE"; then
  printf '%s\n' match >> "$DELIVERY_RECORD"
else
  printf '%s\n' mismatch >> "$DELIVERY_RECORD"
  exit 1
fi
