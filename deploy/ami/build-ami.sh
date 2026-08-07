#!/usr/bin/env bash
# =============================================================================
# build-ami.sh — builds a DRADIS AWS Marketplace AMI from the current repo.
#
# Two product variants (run once per listing):
#   ./deploy/ami/build-ami.sh --venue intl    → "DRADIS International" AMI
#   ./deploy/ami/build-ami.sh --venue us      → "DRADIS US" AMI
#
# What it does (everything is created fresh and torn down afterwards):
#   1. Resolves the latest Canonical Ubuntu 24.04 LTS base AMI via public SSM
#   2. Creates a throwaway key pair + security group (SSH from your IP only)
#   3. Launches a builder instance, uploads a clean `git archive` of HEAD
#      (tracked files only — .env / data / secrets can never leak into the AMI)
#   4. Runs provision.sh: Docker, engine + Control Tower images, /opt/dradis,
#      systemd unit (enabled, not started — first boot belongs to the customer)
#   5. Marketplace hygiene sweep: authorized_keys, SSH host keys, shell
#      histories, machine-id, cloud-init state, logs
#   6. Stops the instance, snapshots it into an AMI, terminates + cleans up
#
# Requirements: aws CLI v2 with credentials, git, ssh, a default VPC in REGION.
#
# Options:
#   --venue intl|us        (required) which product variant to bake
#   --region REGION        default: eu-west-1
#   --instance-type TYPE   default: c5.2xlarge (8 vCPU — Rust release build)
#   --version LABEL        default: git describe (AMI name suffix)
#   --keep-instance        don't terminate the builder on failure (debugging)
# =============================================================================
set -euo pipefail

REGION="eu-west-1"
INSTANCE_TYPE="c5.2xlarge"
VENUE=""
VERSION=""
KEEP_INSTANCE=false

while [ $# -gt 0 ]; do
    case "$1" in
        --venue)         VENUE="$2"; shift 2 ;;
        --region)        REGION="$2"; shift 2 ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
        --version)       VERSION="$2"; shift 2 ;;
        --keep-instance) KEEP_INSTANCE=true; shift ;;
        *) echo "unknown option: $1"; exit 1 ;;
    esac
done

case "$VENUE" in
    intl|us) ;;
    *) echo "usage: build-ami.sh --venue intl|us [--region R] [--instance-type T] [--version V]"; exit 1 ;;
esac

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
    echo "⚠️  Working tree has uncommitted changes — the AMI is built from git HEAD only."
    read -r -p "Continue? [y/N] " ans
    [ "${ans:-n}" = "y" ] || exit 1
fi

[ -n "$VERSION" ] || VERSION="$(git describe --tags --always 2>/dev/null || git rev-parse --short HEAD)"
STAMP="$(date -u +%Y%m%d-%H%M)"
AMI_NAME="dradis-${VENUE}-${VERSION}-${STAMP}"
TAG="dradis-ami-builder-${VENUE}-${STAMP}"

AWS() { aws --region "$REGION" "$@"; }

echo "═══ DRADIS AMI build: $AMI_NAME ($REGION, $INSTANCE_TYPE) ═══"

# ── 1. Base AMI (Canonical Ubuntu 24.04 LTS, x86_64, gp3) ────────────────────
BASE_AMI=$(AWS ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query 'Parameter.Value' --output text)
echo "Base AMI: $BASE_AMI"

# ── 2. Throwaway key pair + security group ───────────────────────────────────
KEY_NAME="$TAG"
KEY_FILE="$(mktemp -d)/${KEY_NAME}.pem"
AWS ec2 create-key-pair --key-name "$KEY_NAME" \
    --query 'KeyMaterial' --output text > "$KEY_FILE"
chmod 600 "$KEY_FILE"

MY_IP=$(curl -sf https://checkip.amazonaws.com)/32
VPC_ID=$(AWS ec2 describe-vpcs --filters Name=is-default,Values=true \
    --query 'Vpcs[0].VpcId' --output text)
SG_ID=$(AWS ec2 create-security-group --group-name "$TAG" \
    --description "temp DRADIS AMI builder" --vpc-id "$VPC_ID" \
    --query 'GroupId' --output text)
AWS ec2 authorize-security-group-ingress --group-id "$SG_ID" \
    --protocol tcp --port 22 --cidr "$MY_IP" >/dev/null

INSTANCE_ID=""
cleanup() {
    set +e
    if [ -n "$INSTANCE_ID" ]; then
        if [ "$KEEP_INSTANCE" = true ] && [ "${BUILD_OK:-false}" != true ]; then
            echo "⚠️  --keep-instance: builder $INSTANCE_ID left running for debugging."
        else
            echo "Terminating builder $INSTANCE_ID…"
            AWS ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null
            AWS ec2 wait instance-terminated --instance-ids "$INSTANCE_ID"
        fi
    fi
    AWS ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1
    rm -f "$KEY_FILE"
    # SG can only be deleted once the instance is gone.
    AWS ec2 delete-security-group --group-id "$SG_ID" >/dev/null 2>&1
}
trap cleanup EXIT

# ── 3. Launch the builder ────────────────────────────────────────────────────
INSTANCE_ID=$(AWS ec2 run-instances \
    --image-id "$BASE_AMI" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG}]" \
    --query 'Instances[0].InstanceId' --output text)
echo "Builder instance: $INSTANCE_ID"
AWS ec2 wait instance-running --instance-ids "$INSTANCE_ID"
HOST=$(AWS ec2 describe-instances --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Builder IP: $HOST — waiting for SSH…"

SSH=(ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
     -o ConnectTimeout=10 "ubuntu@$HOST")
for i in $(seq 1 30); do
    "${SSH[@]}" true 2>/dev/null && break
    [ "$i" = 30 ] && { echo "SSH never came up"; exit 1; }
    sleep 10
done

# ── 4. Upload clean source + provision ───────────────────────────────────────
echo "Uploading source (git archive HEAD)…"
git archive --format=tar.gz HEAD \
    | "${SSH[@]}" "mkdir -p /tmp/dradis-src && tar xz -C /tmp/dradis-src"

echo "Provisioning (Docker + image builds — this takes a while)…"
"${SSH[@]}" "sudo bash /tmp/dradis-src/deploy/ami/provision.sh $VENUE"

# ── 5. Marketplace hygiene sweep (last SSH command — it locks us out) ────────
echo "Scrubbing instance for Marketplace…"
"${SSH[@]}" 'sudo bash -s' <<'SCRUB'
set -e
sudo cloud-init clean --logs
sudo rm -f /etc/ssh/ssh_host_*                 # regenerated on customer boot
sudo rm -f /root/.ssh/authorized_keys
sudo truncate -s 0 /etc/machine-id
sudo rm -rf /tmp/* /var/tmp/* || true
sudo find /var/log -type f -exec truncate -s 0 {} \;
rm -f ~/.bash_history ~/.lesshst ~/.viminfo
sudo rm -f /home/ubuntu/.ssh/authorized_keys   # must be last — kills SSH access
SCRUB

# ── 6. Snapshot into an AMI ──────────────────────────────────────────────────
echo "Stopping builder and creating AMI…"
AWS ec2 stop-instances --instance-ids "$INSTANCE_ID" >/dev/null
AWS ec2 wait instance-stopped --instance-ids "$INSTANCE_ID"

AMI_ID=$(AWS ec2 create-image --instance-id "$INSTANCE_ID" \
    --name "$AMI_NAME" \
    --description "DRADIS $([ "$VENUE" = intl ] && echo International || echo US) — self-hosted, non-custodial automated prediction-market trading engine (provided AS IS)" \
    --query 'ImageId' --output text)
echo "AMI: $AMI_ID — waiting for 'available'…"
AWS ec2 wait image-available --image-ids "$AMI_ID"
AWS ec2 create-tags --resources "$AMI_ID" \
    --tags "Key=Name,Value=$AMI_NAME" "Key=dradis:venue,Value=$VENUE" \
           "Key=dradis:version,Value=$VERSION"

BUILD_OK=true
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "✅ $AMI_NAME"
echo "   AMI ID:  $AMI_ID   (region $REGION)"
echo "   Venue:   $VENUE"
echo ""
echo "Test it:   launch the AMI, open http://<public-ip>/ and log in with"
echo "           user 'admin' / password = the new instance's ID."
echo "Publish:   share the AMI with the AWS Marketplace scanner account via"
echo "           the seller portal (Assets → Amazon Machine Image)."
echo "═══════════════════════════════════════════════════════════════════"
