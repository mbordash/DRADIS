#!/usr/bin/env bash
# =============================================================================
# dradis-firstboot.sh — per-instance initialization (runs before every start;
# only acts when /opt/dradis/.env does not exist yet, i.e. the first boot of a
# fresh instance launched from the AMI).
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
[ -f "$ENV_FILE" ] && exit 0

# EC2 instance ID via IMDSv2 (fall back for non-EC2 test boots).
TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)
INSTANCE_ID=""
if [ -n "$TOKEN" ]; then
    INSTANCE_ID=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
                  "http://169.254.169.254/latest/meta-data/instance-id" || true)
fi
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

mkdir -p /opt/dradis/data
echo "dradis-firstboot: credentials initialized (CT password = instance ID)"
