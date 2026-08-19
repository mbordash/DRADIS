#!/bin/sh
# =============================================================================
# entrypoint.sh — selects which venue binary to exec.
#
# The three venues are mutually exclusive Cargo features, so each is a separate
# binary. A multi-venue image (the AWS Marketplace AMI) bakes all three into
# /app/bin and this script picks one at container start; a single-venue image
# (deploy-live.sh, local dev) bakes one and this script finds it automatically.
#
# Resolution order:
#   1. /app/data/venue   — the operator's choice, written by the Setup view.
#                          This is the authoritative source on the AMI, seeded
#                          on first boot by dradis-firstboot.sh.
#   2. $DRADIS_VENUE     — env fallback for deployments with no data volume.
#   3. the only baked    — single-venue images need no configuration at all.
#   4. intl              — historical default.
#
# The file outranks the env var on purpose. Switching venue from the Setup view
# means writing this file and calling POST /api/setup/restart; if a stale
# DRADIS_VENUE in the compose .env could override it, that switch would appear
# to succeed and silently do nothing.
# =============================================================================
set -e

BIN_DIR=/app/bin
VENUE_FILE=/app/data/venue

# Space-separated venue names baked into this image, e.g. "intl kalshi us".
# Word-split rather than `wc -l`, whose output is padded on some platforms —
# a padded count silently broke the single-venue auto-detect below.
BAKED=$(ls "$BIN_DIR" 2>/dev/null | sed 's/^dradis-//' | tr '\n' ' ')
# shellcheck disable=SC2086
baked_count=$(set -- $BAKED; echo $#)

if [ -r "$VENUE_FILE" ] && [ -s "$VENUE_FILE" ]; then
    venue=$(tr -d ' \t\r\n' < "$VENUE_FILE")
    source="$VENUE_FILE"
elif [ -n "${DRADIS_VENUE:-}" ]; then
    venue="$DRADIS_VENUE"
    source="DRADIS_VENUE"
elif [ "$baked_count" -eq 1 ]; then
    venue="$(echo $BAKED)"
    source="only venue in this image"
else
    venue=intl
    source="default"
fi

BIN="$BIN_DIR/dradis-$venue"
if [ ! -x "$BIN" ]; then
    echo "dradis: venue '$venue' (from $source) is not in this image." >&2
    echo "dradis: available venues: $BAKED" >&2
    echo "dradis: set DRADIS_VENUE or write one of the above to $VENUE_FILE." >&2
    exit 1
fi

echo "dradis: starting venue '$venue' (selected via $source)"
exec "$BIN" "$@"
