#!/usr/bin/env bash
# =============================================================================
# provision.sh — runs ON the temporary EC2 builder instance (via build-ami.sh).
#
# Turns a fresh Ubuntu 24.04 box into the DRADIS AMI payload:
#   1. Install Docker Engine + compose plugin
#   2. Build the engine image carrying EVERY venue binary, plus the Control
#      Tower image, from the uploaded source tarball
#   3. Install /opt/dradis (compose file, first-boot script, systemd unit)
#   4. Remove the source tree and build caches
#
# One AMI serves all three venues. They are mutually exclusive Cargo features,
# so the image holds one binary per venue and /app/entrypoint.sh execs the one
# the customer selected — see deploy/entrypoint.sh. This is what lets DRADIS be
# a single Marketplace listing instead of three, so reviews, subscribers and
# version updates concentrate in one place.
#
# Usage (invoked remotely): sudo bash provision.sh ["intl us kalshi"]
# =============================================================================
set -euo pipefail

# Space-separated venue list; override only to shorten a test build.
VENUES="${1:-intl us kalshi}"
SRC_DIR="/tmp/dradis-src"

for v in $VENUES; do
    case "$v" in
        intl|us|kalshi) ;;
        *) echo "unknown venue '$v' (want any of: intl us kalshi)"; exit 1 ;;
    esac
done

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

echo "── [2/4] Building DRADIS images (venues: $VENUES) ──────────────────────"
echo "     Each venue is a full Rust release build — expect roughly 3x the"
echo "     wall clock of a single-venue image."
cd "$SRC_DIR"
docker build -t dradis-engine:latest --build-arg DRADIS_VENUES="$VENUES" .
docker build -t dradis-control-tower:latest control-tower/
# Baked in rather than pulled on the customer's first boot: a fresh instance
# should come up without needing egress to Docker Hub, and without a pull delay
# on the very first thing a buyer does.
docker pull nginx:alpine

# Fail the build here rather than shipping an AMI that is missing a venue the
# Setup view will happily offer.
for v in $VENUES; do
    docker run --rm --entrypoint /bin/sh dradis-engine:latest \
        -c "test -x /app/bin/dradis-$v" \
        || { echo "❌ engine image is missing /app/bin/dradis-$v"; exit 1; }
done
echo "     ✓ verified binaries present for: $VENUES"
# The stack will not serve at all without this — nginx is the only listener.
docker image inspect nginx:alpine >/dev/null 2>&1 \
    || { echo "❌ nginx:alpine was not baked into the image"; exit 1; }

echo "── [3/4] Installing /opt/dradis runtime ────────────────────────────────"
mkdir -p /opt/dradis/data
install -m 0644 deploy/ami/docker-compose.yml    /opt/dradis/docker-compose.yml
install -m 0644 deploy/ami/nginx.conf           /opt/dradis/nginx.conf
install -m 0755 deploy/ami/dradis-firstboot.sh   /opt/dradis/dradis-firstboot.sh
install -m 0644 deploy/ami/dradis.service        /etc/systemd/system/dradis.service
# Record the build variant on the image for support / listing verification.
echo "venues=$VENUES" >  /opt/dradis/build-info
echo "built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> /opt/dradis/build-info
systemctl daemon-reload
systemctl enable dradis.service   # starts on customer first boot; NOT started now

echo "── [4/4] Cleaning build artifacts ──────────────────────────────────────"
rm -rf "$SRC_DIR"
docker builder prune -af >/dev/null
apt-get clean

echo "✅ Provisioning complete (venues: $VENUES). Ready for AMI snapshot."
