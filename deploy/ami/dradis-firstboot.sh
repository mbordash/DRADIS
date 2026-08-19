#!/usr/bin/env bash
# =============================================================================
# dradis-firstboot.sh — per-instance initialization (runs before every start;
# each step is individually guarded, so it is safe on every boot).
#
# Two independent jobs:
#
#   1. Seed the venue selection (/opt/dradis/data/venue) — once, from EC2
#      user data if the customer supplied it, otherwise the default. The AMI
#      ships binaries for all three venues; this file is what the container
#      entrypoint reads to pick one. Guarded by the file's own existence so a
#      venue later chosen in the Setup view survives every reboot.
#
#   2. Mint per-instance credentials (/opt/dradis/.env) — once.
#
# AWS Marketplace rules prohibit baked-in default passwords, so credentials
# are derived per instance:
#   CT_PASSWORD     = the EC2 instance ID (fetched via IMDSv2) — the standard
#                     Marketplace pattern; only someone who can see the
#                     instance in the AWS console knows it.
#   DRADIS_API_KEY  = 32 random hex bytes — internal CT-proxy → engine auth.
#
# Installed at /opt/dradis/dradis-firstboot.sh, invoked by dradis.service
# as ExecStartPre.
# =============================================================================
set -euo pipefail

ENV_FILE="/opt/dradis/.env"
DATA_DIR="/opt/dradis/data"
VENUE_FILE="$DATA_DIR/venue"
DEFAULT_VENUE="intl"

mkdir -p "$DATA_DIR"

# IMDSv2 token, reused for instance ID and user data (absent off-EC2).
TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
imds() {
    [ -n "$TOKEN" ] || return 1
    curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
         "http://169.254.169.254/latest/$1" || return 1
}

# ── 1. Venue selection ──────────────────────────────────────────────────────
# Customers who want a non-default venue without opening the UI can pass it in
# EC2 user data as a `dradis_venue=<intl|us|kalshi>` line. Everyone else gets
# the default and switches in the Setup view.
if [ ! -s "$VENUE_FILE" ]; then
    VENUE=""
    USER_DATA=$(imds user-data || true)
    if [ -n "$USER_DATA" ]; then
        VENUE=$(printf '%s\n' "$USER_DATA" \
                | sed -n 's/^[[:space:]]*dradis_venue[[:space:]]*=[[:space:]]*\([a-z]*\).*/\1/p' \
                | head -n1)
    fi
    case "$VENUE" in
        intl|us|kalshi) ;;
        "") VENUE="$DEFAULT_VENUE" ;;
        *)  echo "dradis-firstboot: ignoring unknown dradis_venue='$VENUE'" >&2
            VENUE="$DEFAULT_VENUE" ;;
    esac
    printf '%s\n' "$VENUE" > "$VENUE_FILE"
    echo "dradis-firstboot: venue set to '$VENUE' (change it in Setup → Venue)"
fi

# ── 2. Per-instance credentials ─────────────────────────────────────────────
[ -f "$ENV_FILE" ] && exit 0

INSTANCE_ID=$(imds meta-data/instance-id || true)
[ -n "$INSTANCE_ID" ] || INSTANCE_ID=$(openssl rand -hex 8)

API_KEY=$(openssl rand -hex 32)

umask 077
cat > "$ENV_FILE" <<EOF
# Generated on first boot by dradis-firstboot.sh — per-instance credentials.
# Control Tower login: user 'admin', password = this instance's EC2 instance ID.
CT_USERNAME=admin
CT_PASSWORD=$INSTANCE_ID
DRADIS_API_KEY=$API_KEY
EOF

echo "dradis-firstboot: credentials initialized (CT password = instance ID)"
