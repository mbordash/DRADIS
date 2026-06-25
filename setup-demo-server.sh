#!/usr/bin/env bash
# =============================================================================
# setup-demo-server.sh — One-time provisioning for the DRADIS demo box
#
# Installs Docker Engine on a fresh Ubuntu instance (AWS eu-west-1 / Ireland)
# and adds the `ubuntu` user to the docker group so deploy-demo.sh can run
# `docker` without sudo.
#
# Run this ONCE on a brand-new instance, then run ./deploy-demo.sh.
#
# Target server: 52.50.27.235  (dradis.live)
#
# Usage:
#   chmod +x setup-demo-server.sh
#   ./setup-demo-server.sh
# =============================================================================

set -euo pipefail

HOST="52.50.27.235"
USER="ubuntu"
KEY="~/.ssh/rustpolybot-ireland-key-2026.pem"
SSH_PORT="22"

echo " Provisioning Docker on $HOST (one-time setup)..."
echo ""

ssh -p "$SSH_PORT" -i "$KEY" "$USER@$HOST" bash -s << 'REMOTE'
    set -euo pipefail

    if command -v docker >/dev/null 2>&1; then
        echo "✅ Docker already installed: $(docker --version)"
    else
        echo " Installing Docker Engine via the official convenience script..."
        sudo apt-get update -y
        sudo apt-get install -y ca-certificates curl
        curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
        sudo sh /tmp/get-docker.sh
        rm -f /tmp/get-docker.sh
    fi

    echo " Enabling + starting the Docker service..."
    sudo systemctl enable docker
    sudo systemctl start docker

    echo " Adding '$USER' to the docker group (no sudo needed for docker)..."
    sudo usermod -aG docker "$(whoami)"

    echo ""
    echo "✅ Docker provisioned:"
    sudo docker --version
    sudo docker compose version 2>/dev/null || true
REMOTE

echo ""
echo "✅ Server provisioning complete on $HOST!"
echo ""
echo "ℹ️  The docker group membership takes effect on the NEXT SSH session."
echo "    deploy-demo.sh opens a fresh session, so you can run it now:"
echo ""
echo "    ./deploy-demo.sh"
