# DRADIS Marketplace AMI toolkit

Builds the two AWS Marketplace product AMIs from the current repo:

| Variant | Command | Engine build |
|---|---|---|
| DRADIS International | `./deploy/ami/build-ami.sh --venue intl` | default (`intl_clob`) |
| DRADIS US | `./deploy/ami/build-ami.sh --venue us` | `--no-default-features --features us_retail` |

They are **separate listings on purpose**: the jurisdiction warning (US persons
must not use the International build) attaches to the listing artifact itself.

## What's inside the AMI

- Ubuntu 24.04 LTS + Docker Engine
- `dradis-engine:latest` and `dradis-control-tower:latest` images, pre-built
- `/opt/dradis/` — compose file, data dir, first-boot script
- `dradis.service` — enabled systemd unit; the stack starts on the customer's
  first boot (zero-credential graceful boot: engine parks idle until Setup
  is completed)

## First-boot behavior (customer instance)

`dradis-firstboot.sh` generates `/opt/dradis/.env` once per instance:
Control Tower login is `admin` / *the EC2 instance ID* (Marketplace forbids
baked-in default passwords), plus a random internal `DRADIS_API_KEY`.
Port 80 (Control Tower) is the only public surface; the engine API stays on
localhost. The operator then completes the Setup view → AlphaGate
acknowledgment → credentials → restart engine.

## Requirements

`aws` CLI v2 with credentials, `git`, `ssh`, a default VPC in the target
region. The builder instance (default `c5.2xlarge`) exists only for the
duration of the build (~20–30 min) and is terminated automatically.

The AMI is built from `git archive HEAD` — tracked files only, so local
`.env`, `data/`, and other gitignored secrets can never leak into the image.
