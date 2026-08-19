# DRADIS Marketplace AMI toolkit

Builds **one** AWS Marketplace AMI, for **one** listing, carrying **every**
venue:

```bash
./deploy/ami/build-ami.sh                      # intl + us + kalshi (the product)
./deploy/ami/build-ami.sh --venues "intl us"   # shorter test build
```

The three venues are mutually exclusive Cargo features, so each is a separate
binary. The image bakes one per venue under `/app/bin/` and
`deploy/entrypoint.sh` execs the one the customer selected.

### Why one listing and not three

Reviews, ratings and subscriber counts cannot be merged after the fact, so
three listings would split the product's social proof three ways and leave two
of them sitting at zero. Every version would also need its own AWS review cycle
per listing. And a customer who starts on Kalshi and wants to try Polymarket US
switches in the UI instead of finding and subscribing to a second product.

The jurisdiction warning that used to justify separate listings now attaches to
the **venue**, not the artifact: `AlphaGate` names the selected venue in the
acknowledgment the customer must record before trading, and the International
copy tells a US person to switch venues rather than to buy something else.

## Venue selection on a customer instance

Resolution order, implemented in `deploy/entrypoint.sh`:

1. `/opt/dradis/data/venue` — the operator's choice, written by the Setup
   view's **Trading Venue** card. Authoritative.
2. `$DRADIS_VENUE` — env fallback for deployments with no data volume.
3. The only baked venue, if the image carries just one (production, dev).
4. `intl`.

The file outranks the env var deliberately: a venue switch from the UI writes
that file, so a stale `DRADIS_VENUE` in the compose `.env` overriding it would
make the switch appear to succeed and silently do nothing.

`dradis-firstboot.sh` seeds the file once, reading an optional
`dradis_venue=<intl|us|kalshi>` line from EC2 user data so a customer can pick
at launch without opening a browser. Anything unrecognised falls back to the
default and logs why.

**Venue is not a live config knob.** It selects which binary runs, so it takes
effect on restart — the Setup card says so and pairs the change with an
explicit restart rather than implying it applies on save.

## What's inside the AMI

- Ubuntu 24.04 LTS + Docker Engine
- `dradis-engine:latest` (one binary per venue) and
  `dradis-control-tower:latest`, pre-built
- `/opt/dradis/` — compose file, data dir, first-boot script
- `dradis.service` — enabled systemd unit; the stack starts on the customer's
  first boot (zero-credential graceful boot: engine parks idle until Setup
  is completed)

`provision.sh` asserts every requested binary is present in the image before
the snapshot is taken, so a build that silently dropped a venue fails there
rather than shipping a Setup view offering a venue that cannot start.

## First-boot behavior (customer instance)

`dradis-firstboot.sh` generates `/opt/dradis/.env` once per instance:
Control Tower login is `admin` / *the EC2 instance ID* (Marketplace forbids
baked-in default passwords), plus a random internal `DRADIS_API_KEY`.
Port 80 (Control Tower) is the only public surface; the engine API stays on
localhost. The operator then completes the Setup view → AlphaGate
acknowledgment → venue → credentials → restart engine.

## Requirements

`aws` CLI v2 with credentials, `git`, `ssh`, a default VPC in the target
region. The builder instance exists only for the duration of the build and is
terminated automatically.

Defaults reflect the three-venue build: `c5.4xlarge` (16 vCPU) on a 60 GB
volume, because each venue is a full Rust release build and cargo rebuilds the
dependency graph whenever the feature set changes. Expect roughly three times
the wall clock of a single-venue image.

The AMI is built from `git archive HEAD` — tracked files only, so local
`.env`, `data/`, and other gitignored secrets can never leak into the image.
