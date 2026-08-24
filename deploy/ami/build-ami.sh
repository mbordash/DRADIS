#!/usr/bin/env bash
# =============================================================================
# build-ami.sh — builds a DRADIS AWS Marketplace AMI from the current repo.
#
# ONE AMI for ONE Marketplace listing. The image carries a binary for every
# venue (Polymarket International, Polymarket US, Kalshi) and the customer
# picks theirs in the Setup view; see deploy/entrypoint.sh for the dispatch.
#
#   ./deploy/ami/build-ami.sh                      → all three venues
#   ./deploy/ami/build-ami.sh --venues "intl us"   → shorter test build
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
#   --profile NAME         AWS CLI profile (or set AWS_PROFILE)
#   --venues "LIST"        default: "intl us kalshi" (space-separated)
#   --region REGION        default: us-east-1 (where Marketplace sources AMIs)
#   --instance-type TYPE   default: c5.4xlarge (16 vCPU — three Rust builds)
#   --version LABEL        default: git describe (AMI name suffix)
#   --keep-instance        don't terminate the builder on failure (debugging)
# =============================================================================
set -euo pipefail

# AWS Marketplace sources an AMI from us-east-1, so an image registered anywhere
# else cannot be listed — this default is a correctness requirement, not a
# preference. eu-west-1 hosts only the public demo box, which is deployed by
# hand via deploy-demo.sh and is not this script's business.
#
# Note that every AWS call below passes --region "$REGION" explicitly, so
# AWS_DEFAULT_REGION in the environment has NO effect. Overriding the region
# means passing --region; exporting the variable silently does nothing.
REGION="us-east-1"
# Three sequential Rust release builds: 16 vCPU keeps the wall clock sane, and
# the instance is torn down at the end so the extra cost is minutes, not hours.
INSTANCE_TYPE="c5.4xlarge"
VENUES="intl us kalshi"
PROFILE=""
VERSION=""
KEEP_INSTANCE=false

while [ $# -gt 0 ]; do
    case "$1" in
        --venues)        VENUES="$2"; shift 2 ;;
        --profile)       PROFILE="$2"; shift 2 ;;
        --region)        REGION="$2"; shift 2 ;;
        --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
        --version)       VERSION="$2"; shift 2 ;;
        --keep-instance) KEEP_INSTANCE=true; shift ;;
        *) echo "unknown option: $1"; exit 1 ;;
    esac
done

for v in $VENUES; do
    case "$v" in
        intl|us|kalshi) ;;
        *) echo "unknown venue '$v'"
           echo "usage: build-ami.sh [--venues \"intl us kalshi\"] [--region R] [--instance-type T] [--version V]"
           exit 1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

if [ -n "$(git status --porcelain)" ]; then
    echo "⚠️  Working tree has uncommitted changes — the AMI is built from git HEAD only."
    read -r -p "Continue? [y/N] " ans
    [ "${ans:-n}" = "y" ] || exit 1
fi

# ── Preflight: the git archive must carry everything the images COPY ─────────
# The AMI is built from `git archive HEAD` — tracked files only. A file that is
# present locally but ignored is therefore invisible to the build even though
# `docker build .` works fine here, and the failure only surfaces on the remote
# builder, twenty minutes and one instance later. Three separate files were
# missing this way on the first real run (Cargo.lock, control-tower/Dockerfile,
# control-tower/package-lock.json), so check it locally instead.
REQUIRED_IN_ARCHIVE=(
    Dockerfile
    Cargo.toml
    Cargo.lock
    src/main.rs
    deploy/entrypoint.sh
    control-tower/Dockerfile
    control-tower/package.json
    control-tower/package-lock.json
    deploy/ami/provision.sh
    deploy/ami/docker-compose.yml
    deploy/ami/nginx.conf
    deploy/ami/dradis-firstboot.sh
    deploy/ami/dradis.service
)
ARCHIVE_LIST=$(git archive HEAD | tar -t)
MISSING=""
for f in "${REQUIRED_IN_ARCHIVE[@]}"; do
    printf '%s\n' "$ARCHIVE_LIST" | grep -qxF "$f" || MISSING="$MISSING $f"
done
if [ -n "$MISSING" ]; then
    echo "❌ Missing from \`git archive HEAD\` — the remote build would fail on these:"
    for f in $MISSING; do echo "     - $f"; done
    echo
    echo "   They may exist locally but be untracked or ignored. Diagnose with:"
    echo "     git check-ignore -v <path>     # names the rule, including ~/.gitignore_global"
    echo "     git ls-files --error-unmatch <path>"
    exit 1
fi

[ -n "$VERSION" ] || VERSION="$(git describe --tags --always 2>/dev/null || git rev-parse --short HEAD)"
STAMP="$(date -u +%Y%m%d-%H%M)"
AMI_NAME="dradis-${VERSION}-${STAMP}"
TAG="dradis-ami-builder-${STAMP}"

# EC2 CreateImage rejects any non-ASCII byte in Description or Name — and it
# rejects it at the very END of the build, after provisioning has succeeded and
# the instance has been stopped. An em dash cost one complete build, so the
# string lives here and is checked before anything is launched.
AMI_DESCRIPTION="DRADIS - self-hosted, non-custodial automated prediction-market trading engine for Polymarket International, Polymarket US and Kalshi (provided AS IS)"
if printf '%s%s' "$AMI_DESCRIPTION" "$AMI_NAME" | LC_ALL=C grep -q '[^ -~]'; then
    echo "❌ AMI name or description contains non-ASCII characters, which EC2"
    echo "   CreateImage rejects. Offending text:"
    printf '%s\n' "$AMI_DESCRIPTION" "$AMI_NAME" | LC_ALL=C grep -n '[^ -~]'
    exit 1
fi


# Built as an array so an empty PROFILE cannot inject an empty argument, and
# so the array is never empty (bash 3.2 + `set -u` errors on "${arr[@]}" when
# the array has no elements).
AWS_BASE=(--region "$REGION")
[ -n "$PROFILE" ] && AWS_BASE=(--profile "$PROFILE" "${AWS_BASE[@]}")
AWS() { aws "${AWS_BASE[@]}" "$@"; }

echo "═══ DRADIS AMI build: $AMI_NAME ($REGION, $INSTANCE_TYPE) ═══"

# ── 0. Preflight: the credentials must actually reach AWS ────────────────────
# A profile configured for an S3-compatible third party (Tigris, R2, MinIO, …)
# carries an `endpoint_url`, and botocore applies that to EVERY service — so an
# SSM or EC2 call is POSTed to object storage and comes back as a bare HTTP
# error with an empty body, naming nothing. Catch it here, before we create a
# key pair and pay for a builder instance.
if ! IDENTITY=$(AWS sts get-caller-identity --query Arn --output text 2>&1); then
    echo "❌ AWS credentials are not usable${PROFILE:+ for profile '$PROFILE'}:"
    echo "   ${IDENTITY:-(no response body)}"
    echo
    echo "   An HTTP status rather than an AWS error usually means a non-AWS"
    echo "   endpoint override is intercepting every service:"
    echo "     grep -n endpoint_url ~/.aws/config ~/.aws/credentials"
    echo
    echo "   Put real AWS keys in their own profile (no endpoint_url), then:"
    echo "     $0 --profile <name> --region $REGION"
    exit 1
fi
echo "Identity: $IDENTITY"

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
# Set before the trap is installed so cleanup() can distinguish "no image yet"
# from "image created, a later step failed".
AMI_ID=""
cleanup() {
    # +u as well as +e: a cleanup trap must never be the thing that fails.
    # It previously died on an unbound variable and leaked a running builder.
    set +e +u
    if [ -n "$INSTANCE_ID" ]; then
        if [ "${SNAPSHOT_READY:-false}" = true ] && [ "${BUILD_OK:-false}" != true ]; then
            # Everything expensive already succeeded: Docker, three Rust release
            # builds, the Marketplace scrub. Only the snapshot failed. Throwing
            # the instance away here means rebuilding all of it, so keep it —
            # stopped, it costs only its EBS volume.
            echo ""
            if [ -n "${AMI_ID:-}" ]; then
                # create-image already succeeded — the image EXISTS. Telling the
                # operator to re-snapshot here would silently create a second,
                # duplicate AMI (this is exactly what a create-tags failure looked
                # like on 2026-08-20).
                echo "⚠️  The AMI was created successfully; a later step failed."
                echo "    AMI: ${AMI_ID}  — do NOT re-run the build, it already exists."
                echo ""
                echo "    Finish it by hand (note '+' not ',' in the venues value —"
                echo "    a comma is the CLI's shorthand delimiter):"
                echo "      aws ${PROFILE:+--profile ${PROFILE} }--region ${REGION} ec2 create-tags --resources ${AMI_ID} \\"
                echo "          --tags Key=Name,Value=${AMI_NAME} \\"
                echo "                 \"Key=dradis:venues,Value=${VENUES// /+}\" \\"
                echo "                 Key=dradis:version,Value=${VERSION}"
            else
                echo "⚠️  Provisioning succeeded; only the snapshot failed."
                echo "    Builder ${INSTANCE_ID} is left STOPPED so you need not rebuild."
                echo ""
                echo "    Retry just the snapshot:"
                echo "      aws ${PROFILE:+--profile ${PROFILE} }--region ${REGION} ec2 create-image \\"
                echo "          --instance-id ${INSTANCE_ID} --name '${AMI_NAME}' \\"
                echo "          --description '${AMI_DESCRIPTION}'"
            fi
            echo ""
            echo "    When you are done, clean up:"
            echo "      aws ${PROFILE:+--profile ${PROFILE} }--region ${REGION} ec2 terminate-instances --instance-ids ${INSTANCE_ID}"
            echo "      aws ${PROFILE:+--profile ${PROFILE} }--region ${REGION} ec2 delete-key-pair --key-name ${KEY_NAME}"
            echo "      aws ${PROFILE:+--profile ${PROFILE} }--region ${REGION} ec2 delete-security-group --group-id ${SG_ID}"
            echo ""
            # Leave the key pair and security group in place too: deleting the
            # SG would fail anyway while the instance still references it.
            return
        elif [ "$KEEP_INSTANCE" = true ] && [ "${BUILD_OK:-false}" != true ]; then
            echo "⚠️  --keep-instance: builder $INSTANCE_ID left running for debugging."
        else
            # Braces are load-bearing: bash in a UTF-8 locale folds the
            # following ellipsis into the identifier, looks up a variable that
            # does not exist, and under `set -u` aborts the trap before the
            # instance is terminated.
            echo "Terminating builder ${INSTANCE_ID}…"
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
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3","DeleteOnTermination":true}}]' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG}]" \
    --query 'Instances[0].InstanceId' --output text)
echo "Builder instance: $INSTANCE_ID"
AWS ec2 wait instance-running --instance-ids "$INSTANCE_ID"
HOST=$(AWS ec2 describe-instances --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "Builder IP: $HOST — waiting for SSH…"

# ServerAlive* keeps a NAT or wifi blip from silently dropping a connection
# that is waiting on a long, quiet remote command.
SSH=(ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
     -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=10 \
     "ubuntu@$HOST")
for i in $(seq 1 30); do
    "${SSH[@]}" true 2>/dev/null && break
    [ "$i" = 30 ] && { echo "SSH never came up"; exit 1; }
    sleep 10
done

# Re-authorise our current public IP on the builder's security group.
#
# The group is opened to ONE address, captured once at build start. A dynamic
# lease, a VPN reconnect or a mobile handoff changes that address mid-build, and
# every subsequent SSH then hangs until it times out — packets are dropped, so
# it presents as "Operation timed out" rather than a refusal, which makes it
# look like a dead instance. Idempotent: a duplicate rule is not an error worth
# stopping for.
reauthorize_ip() {
    local ip
    ip="$(curl -sf https://checkip.amazonaws.com || true)"
    [ -n "$ip" ] || return 0
    ip="${ip}/32"
    [ "$ip" = "$MY_IP" ] && return 0
    echo "   public IP changed ($MY_IP -> $ip) — reopening SSH for it"
    AWS ec2 authorize-security-group-ingress --group-id "$SG_ID" \
        --protocol tcp --port 22 --cidr "$ip" >/dev/null 2>&1 || true
    MY_IP="$ip"
}

# Run a remote command, retrying through transient network failures.
#
# Everything after the source upload is expensive to redo, so a single dropped
# connection must not end the build (2026-08-20: it did, immediately after the
# upload, and cost the whole run).
ssh_try() {
    local attempt=1
    # </dev/null so ssh cannot swallow this script's own stdin — the dirty-tree
    # prompt and the log-follow loop both read from it.
    until "${SSH[@]}" "$@" < /dev/null; do
        [ "$attempt" -ge 5 ] && { echo "   ssh failed 5 times — giving up"; return 1; }
        echo "   ssh failed (attempt $attempt) — retrying in 10s"
        reauthorize_ip
        sleep 10
        attempt=$((attempt + 1))
    done
}

# As ssh_try, but feeds the remote command from a local file. A heredoc cannot
# be retried — its stdin is consumed by the first attempt.
ssh_try_file() {
    local src="$1"; shift
    local attempt=1
    until "${SSH[@]}" "$@" < "$src"; do
        [ "$attempt" -ge 5 ] && { echo "   ssh failed 5 times — giving up"; return 1; }
        echo "   ssh failed (attempt $attempt) — retrying in 10s"
        reauthorize_ip
        sleep 10
        attempt=$((attempt + 1))
    done
}

# ── 4. Upload clean source + provision ───────────────────────────────────────
echo "Uploading source (git archive HEAD)…"
git archive --format=tar.gz HEAD \
    | "${SSH[@]}" "mkdir -p /tmp/dradis-src && tar xz -C /tmp/dradis-src"

# A three-venue build runs for the better part of an hour with long silent
# stretches. Holding one SSH session open for all of it is fragile — any NAT
# timeout or wifi drop kills the connection and takes the build with it, which
# is exactly what happened on the first real run. Start it detached instead,
# then follow the log over short-lived connections that can each fail and be
# retried without losing the build.
echo "Provisioning (Docker + image builds — this takes a while)…"
RUNNER_LOCAL="$(mktemp)"
cat > "$RUNNER_LOCAL" <<RUNNER
#!/bin/bash
sudo bash /tmp/dradis-src/deploy/ami/provision.sh '$VENUES' > /tmp/provision.log 2>&1
echo \$? > /tmp/provision.rc
RUNNER
ssh_try_file "$RUNNER_LOCAL" "cat > /tmp/run-provision.sh" || exit 1
rm -f "$RUNNER_LOCAL"
ssh_try "chmod +x /tmp/run-provision.sh && \
    setsid nohup /tmp/run-provision.sh </dev/null >/dev/null 2>&1 & sleep 2" || exit 1

SENT=0            # lines of provision.log already mirrored locally
PROV_RC=""
while [ -z "$PROV_RC" ]; do
    if CHUNK=$("${SSH[@]}" "tail -n +$((SENT+1)) /tmp/provision.log 2>/dev/null" 2>/dev/null); then
        if [ -n "$CHUNK" ]; then
            printf '%s\n' "$CHUNK"
            SENT=$((SENT + $(printf '%s\n' "$CHUNK" | wc -l)))
        fi
        PROV_RC=$("${SSH[@]}" "cat /tmp/provision.rc 2>/dev/null" 2>/dev/null || true)
    else
        echo "   … lost contact with the builder — retrying (the build keeps running)"
        reauthorize_ip
    fi
    [ -z "$PROV_RC" ] && sleep 20
done

if [ "$PROV_RC" != 0 ]; then
    echo "❌ Provisioning failed on the builder (exit $PROV_RC)."
    echo "   Re-run with --keep-instance to keep the box and read /tmp/provision.log on it."
    exit 1
fi

# ── 5. Marketplace hygiene sweep (last SSH command — it locks us out) ────────
echo "Scrubbing instance for Marketplace…"
# Split deliberately: everything here is idempotent, so it can be retried
# through a dropped connection like every other post-upload step. The two
# removals that end our own access are NOT here — see below for why.
SCRUB_LOCAL="$(mktemp)"
cat > "$SCRUB_LOCAL" <<'SCRUB'
set -e
sudo cloud-init clean --logs
sudo rm -f /root/.ssh/authorized_keys
sudo truncate -s 0 /etc/machine-id
sudo rm -rf /tmp/* /var/tmp/* || true
sudo find /var/log -type f -exec truncate -s 0 {} \;
rm -f ~/.bash_history ~/.lesshst ~/.viminfo
SCRUB
ssh_try_file "$SCRUB_LOCAL" 'sudo bash -s' || exit 1
rm -f "$SCRUB_LOCAL"

# ── The two removals that end our own access — ONE session, confirmed ────────
#
# Host keys and the operator's authorized_keys are removed together, in a single
# connection, in this order, and the confirmation is echoed back over that same
# connection before it closes.
#
# Splitting them was a real bug (fixed 2026-08-21). The bulk scrub above used to
# delete /etc/ssh/ssh_host_* and the authorized_keys removal ran over a SECOND
# connection — which could no longer complete a handshake. The removal never
# happened, the confirmation never arrived, and the fallback concluded "SSH is
# closed, so the key must be gone". Four AMIs shipped with the builder's public
# key in /home/ubuntu/.ssh/authorized_keys, which is precisely what Marketplace
# scanning rejects.
#
# The lesson is in the fallback, not the ordering: once host keys are gone,
# "cannot connect" says nothing about authorized_keys. So there is no fallback
# now. Either the confirmation comes back or the build stops.
echo "Removing operator access…"
# Reports COUNTS, not a boolean. `test ! -f` answers true when the file merely
# cannot be seen — unreadable parent, permission error — so a check built on it
# reports success for a failure it never observed. Counting what is actually
# there makes a wrong answer visible in the build log instead of silent.
KEY_GONE=false
if OUT=$("${SSH[@]}" '
    sudo rm -f /etc/ssh/ssh_host_*
    sudo rm -f /home/ubuntu/.ssh/authorized_keys
    printf "AUTHKEYS=%s HOSTKEYS=%s\n" \
      "$(sudo ls -1 /home/ubuntu/.ssh/authorized_keys 2>/dev/null | wc -l | tr -d " ")" \
      "$(sudo ls -1 /etc/ssh/ssh_host_* 2>/dev/null | wc -l | tr -d " ")"
' 2>&1); then
    case "$OUT" in *"AUTHKEYS=0 HOSTKEYS=0"*) KEY_GONE=true ;; esac
fi
if [ "$KEY_GONE" != true ]; then
    echo "❌ Could not confirm the operator key was removed."
    echo "   Remote said: ${OUT:-(nothing)}"
    echo ""
    echo "   Refusing to snapshot. An AMI carrying /home/ubuntu/.ssh/authorized_keys"
    echo "   hands every customer the same login and fails Marketplace scanning."
    echo "   Re-run with --keep-instance to inspect the builder."
    exit 1
fi
echo "   ✓ operator key and host keys removed ($OUT)"

# ── 6. Snapshot into an AMI ──────────────────────────────────────────────────
echo "Stopping builder and creating AMI…"
AWS ec2 stop-instances --instance-ids "$INSTANCE_ID" >/dev/null
AWS ec2 wait instance-stopped --instance-ids "$INSTANCE_ID"
# From here on, a failure must not destroy the builder — see cleanup().
SNAPSHOT_READY=true

AMI_ID=$(AWS ec2 create-image --instance-id "$INSTANCE_ID" \
    --name "$AMI_NAME" \
    --description "$AMI_DESCRIPTION" \
    --query 'ImageId' --output text)
echo "AMI: $AMI_ID — waiting for 'available'…"
AWS ec2 wait image-available --image-ids "$AMI_ID"
# `+` and NOT `,` as the separator. In the CLI's shorthand syntax the comma is
# what delimits Key= from Value=, so "Value=intl,us,kalshi" is parsed as three
# shorthand tokens and the value arrives as a LIST — which create-tags rejects
# with a ParamValidation error, AFTER the image has already been created.
AWS ec2 create-tags --resources "$AMI_ID" \
    --tags "Key=Name,Value=$AMI_NAME" "Key=dradis:venues,Value=${VENUES// /+}" \
           "Key=dradis:version,Value=$VERSION"

BUILD_OK=true
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "✅ $AMI_NAME"
echo "   AMI ID:  $AMI_ID   (region $REGION)"
echo "   Venues:  $VENUES"
echo ""
echo "Test it:   launch the AMI, open http://<public-ip>/ and log in with"
echo "           user 'admin' / password = the new instance's ID."
echo "Publish:   share the AMI with the AWS Marketplace scanner account via"
echo "           the seller portal (Assets → Amazon Machine Image)."
echo "═══════════════════════════════════════════════════════════════════"
