#!/usr/bin/env bash
# =============================================================================
# publish-version.sh — submits a built AMI as a new version of the DRADIS
# AWS Marketplace listing, through the Marketplace Catalog API.
#
# Replaces the two manual seller-portal steps (share the AMI with the scanner
# account, fill in the "Add new version" form) with one StartChangeSet call of
# type AddDeliveryOptions, then waits for AWS to scan the image and report.
#
# What goes into the version, and where it comes from:
#   VersionTitle      --version (the bare number, "1.5.1")
#   ReleaseNotes      the bullets under "## <version>" in docs/RELEASES.md,
#                     above its "### Detail" line, plus the upgrade link;
#                     or the whole of --notes-file verbatim
#   UsageInstructions the block between the two rule lines of docs/USAGE.md
#                     (the file says so at its top); or --usage-file verbatim
#   AMI source        --ami, plus the OS name/version, SSH user, scanning port,
#                     recommended instance type and security-group rules COPIED
#                     from the listing's newest existing version, so a release
#                     never changes them by accident
#   AccessRoleArn     the AWSMarketplaceAmiIngestion role in this account
#                     (--role to override)
#
# Options:
#   --profile NAME     AWS CLI profile (or set AWS_PROFILE)
#   --ami ID           the AMI to publish (us-east-1, owned by this account)
#   --version LABEL    version title; a leading "v" is dropped
#   --product ID       Marketplace product id (default: the one public AmiProduct)
#   --role ARN         ingestion role (default: AWSMarketplaceAmiIngestion here)
#   --notes-file F     release notes verbatim instead of docs/RELEASES.md
#   --usage-file F     usage text verbatim instead of docs/USAGE.md
#   --dry-run          build and print the change set, submit nothing
#   --validate         ask AWS to validate the change set without creating the
#                      version (Intent=VALIDATE); nothing is published
#   --no-wait          submit and exit; print the change-set id to poll later
#
# Needs: aws CLI v2, python3. Permissions: see iam-policy.json
# (MarketplaceCatalogPublish and PassAmiIngestionRoleToMarketplace).
# The scan takes AWS roughly an hour; --no-wait if you would rather not hold a
# terminal, then `aws marketplace-catalog describe-change-set` with the id.
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
# The Catalog API lives in us-east-1 whatever region the caller is in, and
# Marketplace only ingests AMIs from us-east-1.
REGION="us-east-1"
PROFILE=""
AMI_ID=""
VERSION=""
PRODUCT_ID=""
ROLE_ARN=""
NOTES_FILE=""
USAGE_FILE=""
INTENT="APPLY"
WAIT=true
DRY_RUN=false

while [ $# -gt 0 ]; do
    case "$1" in
        --profile)    PROFILE="$2"; shift 2 ;;
        --ami)        AMI_ID="$2"; shift 2 ;;
        --version)    VERSION="$2"; shift 2 ;;
        --product)    PRODUCT_ID="$2"; shift 2 ;;
        --role)       ROLE_ARN="$2"; shift 2 ;;
        --notes-file) NOTES_FILE="$2"; shift 2 ;;
        --usage-file) USAGE_FILE="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        --validate)   INTENT="VALIDATE"; shift ;;
        --no-wait)    WAIT=false; shift ;;
        *) echo "unknown option: $1"; exit 1 ;;
    esac
done

[ -n "$AMI_ID" ]  || { echo "need --ami"; exit 1; }
[ -n "$VERSION" ] || { echo "need --version"; exit 1; }
VERSION="${VERSION#v}"

AWS() {
    if [ -n "$PROFILE" ]; then
        aws --profile "$PROFILE" --region "$REGION" "$@"
    else
        aws --region "$REGION" "$@"
    fi
}
CATALOG() { AWS marketplace-catalog "$@" --catalog AWSMarketplace; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# ── 1. The AMI must exist here and be ours ───────────────────────────────────
ACCOUNT="$(AWS sts get-caller-identity --query Account --output text)"
AMI_STATE="$(AWS ec2 describe-images --image-ids "$AMI_ID" \
    --query 'Images[0].[State,OwnerId,Name]' --output text 2>/dev/null || true)"
read -r STATE OWNER AMI_NAME <<<"$AMI_STATE"
if [ "${STATE:-}" != "available" ] || [ "${OWNER:-}" != "$ACCOUNT" ]; then
    echo "❌ $AMI_ID is not an available image owned by $ACCOUNT in $REGION (state '${STATE:-none}', owner '${OWNER:-none}')"
    exit 1
fi
[ -n "$ROLE_ARN" ] || ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/AWSMarketplaceAmiIngestion"

# ── 2. The product, and its newest version to copy settings from ─────────────
if [ -z "$PRODUCT_ID" ]; then
    CATALOG list-entities --entity-type AmiProduct --output json > "$WORK/entities.json"
    PRODUCT_ID="$(python3 -I - "$WORK/entities.json" <<'EOF'
import json, sys
ents = [e for e in json.load(open(sys.argv[1]))["EntitySummaryList"] if e.get("Visibility") == "Public"]
if len(ents) != 1:
    sys.stderr.write(f"expected exactly one public AmiProduct, found {len(ents)}: "
                     + ", ".join(f"{e['EntityId']} ({e['Name']})" for e in ents) + "\n"
                     + "name the product with --product\n")
    sys.exit(1)
print(ents[0]["EntityId"])
EOF
)"
fi
CATALOG describe-entity --entity-id "$PRODUCT_ID" --output json > "$WORK/product.json"

# ── 3. Release notes and usage text ──────────────────────────────────────────
if [ -n "$NOTES_FILE" ]; then
    cp "$NOTES_FILE" "$WORK/notes.txt"
else
    python3 -I - "$REPO/docs/RELEASES.md" "$VERSION" > "$WORK/notes.txt" <<'EOF'
import re, sys
md = open(sys.argv[1], encoding="utf-8").read()
ver = sys.argv[2]
m = re.search(r"^## " + re.escape(ver) + r"\b[^\n]*\n(.*?)(?=^## |\Z)", md, flags=re.M | re.S)
if not m:
    sys.stderr.write(f"docs/RELEASES.md has no '## {ver}' section\n")
    sys.exit(1)
section = m.group(1).split("### Detail")[0]
bullets = [line.rstrip() for line in section.splitlines() if line.startswith("- ")]
if not bullets:
    sys.stderr.write(f"the '## {ver}' section has no bullets above '### Detail'\n")
    sys.exit(1)
sys.stdout.write("\n".join(bullets) + "\n\nUpdate instructions at https://dradis.live/docs/upgrade")
EOF
fi
if [ -n "$USAGE_FILE" ]; then
    cp "$USAGE_FILE" "$WORK/usage.txt"
else
    python3 -I - "$REPO/docs/USAGE.md" > "$WORK/usage.txt" <<'EOF'
import re, sys
md = open(sys.argv[1], encoding="utf-8").read()
parts = re.split(r"^-{3,}\s*$", md, flags=re.M)
if len(parts) < 3:
    sys.stderr.write("docs/USAGE.md needs the usage block between two '---' rule lines\n")
    sys.exit(1)
block = parts[1].strip("\n")
if len(block) > 4000:
    sys.stderr.write(f"usage block is {len(block)} characters; the Marketplace field holds 4,000\n")
    sys.exit(1)
sys.stdout.write(block)
EOF
fi

# ── 4. The change set ────────────────────────────────────────────────────────
python3 -I - "$WORK/product.json" "$WORK/notes.txt" "$WORK/usage.txt" \
    "$PRODUCT_ID" "$AMI_ID" "$VERSION" "$ROLE_ARN" > "$WORK/changeset.json" <<'EOF'
import json, sys
product_json, notes_path, usage_path, product_id, ami_id, version, role_arn = sys.argv[1:8]
product = json.load(open(product_json))
details = product.get("DetailsDocument") or json.loads(product["Details"])
versions = sorted(details.get("Versions", []), key=lambda v: v.get("CreationDate", ""))
if not versions:
    sys.stderr.write("the product has no versions to copy settings from\n")
    sys.exit(1)
if any(v.get("VersionTitle") == version for v in versions):
    sys.stderr.write(f"version {version} already exists on {product_id}\n")
    sys.exit(1)
if any(ami_id == s.get("Image") for v in versions for s in v.get("Sources", [])):
    sys.stderr.write(f"{ami_id} is already the source of an existing version\n")
    sys.exit(1)
newest = versions[-1]
source = newest["Sources"][0]
os_ = source["OperatingSystem"]
option = newest["DeliveryOptions"][0]
rec = option.get("Recommendations", {})
groups = [
    {"IpProtocol": g["Protocol"], "FromPort": g["FromPort"], "ToPort": g["ToPort"], "IpRanges": g["CidrIps"]}
    for g in rec.get("SecurityGroups", [])
]
notes = open(notes_path, encoding="utf-8").read().strip("\n")
usage = open(usage_path, encoding="utf-8").read().strip("\n")
ami_details = {
    "AmiSource": {
        "AmiId": ami_id,
        "AccessRoleArn": role_arn,
        "UserName": os_["Username"],
        "OperatingSystemName": os_["Name"],
        "OperatingSystemVersion": os_["Version"],
        "ScanningPort": os_.get("ScanningPort", 22),
    },
    "UsageInstructions": usage,
    "RecommendedInstanceType": rec.get("InstanceType", "t3.medium"),
    "SecurityGroups": groups,
}
if option.get("AccessEndpointUrl"):
    ami_details["AccessEndpointUrl"] = option["AccessEndpointUrl"]
change_set = [{
    "ChangeType": "AddDeliveryOptions",
    "Entity": {"Type": "AmiProduct@1.0", "Identifier": product_id},
    "DetailsDocument": {
        "Version": {"VersionTitle": version, "ReleaseNotes": notes},
        "DeliveryOptions": [{"Details": {"AmiDeliveryOptionDetails": ami_details}}],
    },
}]
json.dump(change_set, sys.stdout, indent=1)
sys.stderr.write(
    f"copying from version {newest['VersionTitle']}: {os_['Name']} {os_['Version']}, user {os_['Username']}, "
    f"port {os_.get('ScanningPort', 22)}, {rec.get('InstanceType', 't3.medium')}, "
    f"{len(groups)} security-group rule(s)\n"
    f"release notes {len(notes)} chars, usage {len(usage)} chars\n"
)
EOF

echo "═══ Marketplace: $PRODUCT_ID ← version $VERSION from $AMI_ID ($AMI_NAME) [$INTENT] ═══"
if [ "$DRY_RUN" = true ]; then
    cat "$WORK/changeset.json"
    echo ""
    echo "(dry run: nothing submitted)"
    exit 0
fi
CHANGE_SET_ID="$(CATALOG start-change-set \
    --change-set "file://$WORK/changeset.json" \
    --change-set-name "Add new version $VERSION" \
    --intent "$INTENT" \
    --query ChangeSetId --output text)"
echo "change set: $CHANGE_SET_ID"

if [ "$WAIT" != true ]; then
    echo "Poll with: aws marketplace-catalog describe-change-set --catalog AWSMarketplace --change-set-id $CHANGE_SET_ID --region $REGION${PROFILE:+ --profile $PROFILE}"
    exit 0
fi

# ── 5. Wait for AWS to scan and apply ────────────────────────────────────────
# PREPARING → APPLYING → SUCCEEDED | FAILED | CANCELLED. A scan is normally
# about an hour; give it three before giving up on the wait (the change set
# itself keeps running; only this terminal stops watching).
DEADLINE=$(( $(date +%s) + 3 * 3600 ))
LAST=""
while :; do
    CATALOG describe-change-set --change-set-id "$CHANGE_SET_ID" --output json > "$WORK/status.json"
    STATUS="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["Status"])' "$WORK/status.json")"
    if [ "$STATUS" != "$LAST" ]; then
        echo "$(date '+%H:%M:%S') $STATUS"
        LAST="$STATUS"
    fi
    case "$STATUS" in
        SUCCEEDED)
            if [ "$INTENT" = "VALIDATE" ]; then
                echo "✅ change set validated; nothing was published (drop --validate to publish)"
            else
                echo "✅ version $VERSION is on the listing"
                echo "   AMI alias: /aws/service/marketplace/$PRODUCT_ID/$VERSION"
            fi
            exit 0 ;;
        FAILED|CANCELLED)
            python3 -I - "$WORK/status.json" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"❌ {d['Status']}: {d.get('FailureCode') or ''} {d.get('FailureDescription') or ''}".rstrip())
for change in d.get("ChangeSet", []):
    for err in change.get("ErrorDetailList") or []:
        print(f"   {err.get('ErrorCode')}: {err.get('ErrorMessage')}")
EOF
            exit 1 ;;
    esac
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
        echo "⏳ still $STATUS after three hours; keep polling with describe-change-set --change-set-id $CHANGE_SET_ID"
        exit 2
    fi
    sleep 60
done
