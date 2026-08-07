#!/usr/bin/env bash
# =============================================================================
# provision.sh — runs ON the temporary EC2 builder instance (via build-ami.sh).
#
# Turns a fresh Ubuntu 24.04 box into the DRADIS AMI payload:
#   1. Install Docker Engine + compose plugin
#   2. Build the engine image for the requested venue (intl | us) and the
#      Control Tower image from the uploaded source tarball
#   3. Install /opt/dradis (compose file, first-boot script, systemd unit)
#   4. Remove the source tree and build caches
#
# Usage (invoked remotely): sudo bash provision.sh <intl|us>
# =============================================================================
set -euo pipefail

VENUE="${1:?usage: provision.sh <intl|us>}"
SRC_DIR="/tmp/dradis-src"

case "$VENUE" in
    intl) FEATURES="" ;;
    us)   FEATURES="--no-default-features --features us_retail" ;;
    *)    echo "unknown venue '$VENUE' (want intl|us)"; exit 1 ;;
esac

echo "── [1/4] Installing Docker Engine ──────────────────────────────────────"
if ! command -v docker >/dev/null 2>&1; then
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi
systemctl enable --now docker

echo "── [2/4] Building DRADIS images (venue=$VENUE) ─────────────────────────"
cd "$SRC_DIR"
docker build -t dradis-engine:latest \
    ${FEATURES:+--build-arg DRADIS_FEATURES="$FEATURES"} .
docker build -t dradis-control-tower:latest control-tower/

echo "── [3/4] Installing /opt/dradis runtime ────────────────────────────────"
mkdir -p /opt/dradis/data
install -m 0644 deploy/ami/docker-compose.yml    /opt/dradis/docker-compose.yml
install -m 0755 deploy/ami/dradis-firstboot.sh   /opt/dradis/dradis-firstboot.sh
install -m 0644 deploy/ami/dradis.service        /etc/systemd/system/dradis.service
# Record the build variant on the image for support / listing verification.
echo "venue=$VENUE" >  /opt/dradis/build-info
echo "built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> /opt/dradis/build-info
systemctl daemon-reload
systemctl enable dradis.service   # starts on customer first boot; NOT started now

echo "── [4/4] Cleaning build artifacts ──────────────────────────────────────"
rm -rf "$SRC_DIR"
docker builder prune -af >/dev/null
apt-get clean

echo "✅ Provisioning complete (venue=$VENUE). Ready for AMI snapshot."
