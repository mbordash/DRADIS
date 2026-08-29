# Upgrading DRADIS

DRADIS ships as an immutable image, so there is no in-place upgrade. Upgrading
means launching an instance of the new version and moving your configuration
across. The Setup view has an **Instance Migration** panel that packages
everything transferable into a single bundle for exactly this purpose.

Budget about fifteen minutes, and read the wallet warning below before you
start. It is the one step that can cost money if you skip it.

## Before you begin

Do the export while the old instance is still healthy. The bundle is produced by
the running engine, so an instance you have already stopped or terminated cannot
give you one. If you are upgrading because something is broken, export first
anyway; a bundle from a misbehaving instance is still better than retyping every
credential.

## The procedure

**1. Export the bundle from the running instance.**

Open the Control Tower, go to **Setup**, and find the **📦 Instance Migration**
panel. Export the config bundle and save the file somewhere safe.

The bundle contains your venue API credentials, your wallet private key, your
admin password hash, your Raptor signal keys, and your global and per-squadron
configuration. Treat the file exactly as you would treat the keys themselves.

**2. Launch an instance of the new version.**

Subscribe to or launch the new version from AWS Marketplace as you did the first
time. Use the same instance type unless you have a reason to change it.

Give it a Name tag carrying the new version, for example `dradis-v1.0.5`. For
the next few minutes two DRADIS instances will be running against the same
wallet and you will need to stop exactly one of them; two unnamed rows in the
EC2 console is how the wrong one gets stopped. A one-click Marketplace launch
leaves the Name tag blank, because the instance is created in your account and
the seller cannot tag it. The Control Tower footer also shows the running
version, so an open tab is never ambiguous.

Do not enter any credentials on the new instance. The import will supply them.

**3. Import the bundle.**

Log in to the new instance's Control Tower with user `admin` and the password
shown as the new instance's EC2 instance ID, then go to **Setup → 📦 Instance
Migration** and import the bundle you saved in step 1.

**4. Restart the engine.**

Setup will prompt for a restart. Take it. The engine comes back in roughly 30 to
60 seconds with your credentials and configuration applied.

After this restart, log in with **your own admin password**, not the instance
ID. The password travels in the bundle, which is why the instance ID stops
working at this point. Your browser session does not carry over, because each
instance mints its own session-signing key.

**5. Verify, then stop the old instance.**

Confirm the new instance shows the configuration you expect: your venue is
selected, your strategy settings match, and the Setup view reports your
credentials as present.

Then stop the old instance **before** the new one starts trading.

## Stop the old instance. This part is not optional.

Your wallet private key travels in the bundle, so after an import **both
instances control the same wallet**. Two DRADIS engines on one wallet interfere
with each other in ways that lose money:

- Each engine cancels every open order on the wallet at startup, because from
  its point of view those are leftovers from a previous session. The new
  instance will cancel the old instance's live resting quotes while the old
  instance still believes they are working.
- Each engine reconciles the wallet's on-chain holdings against its own
  database and adopts anything it does not recognize. Both will therefore claim
  the same positions and manage them independently, with two sets of stops and
  two sets of take-profits against one set of shares.

Stopping the old instance is the whole mitigation. Sequence the cutover so the
two never trade at once.

## What does not transfer

The bundle carries configuration, not history. The new instance starts fresh on:

- Trade history and the tradelog
- Open position records (the positions themselves are on-chain and belong to the
  wallet, so they are not lost; the new instance re-adopts them from the chain)
- Profit and loss history, so the dashboard chart starts empty
- The trained GBoost model, which begins collecting data again from zero

None of this affects your money. It affects what the dashboard can show you
about the past. If your P&L history matters to you, keep the old instance's
database before terminating it: it lives at `/opt/dradis/logs/*.db`.

## Rolling back

Until you terminate the old instance, rolling back is just stopping the new
instance and starting the old one again. This is why step 5 says to verify
before you stop the old one, and why terminating it should be a separate,
later decision.

## Troubleshooting

**"bundle is for the 'X' venue build but this instance is 'Y'"**

The image carries binaries for Polymarket International, Polymarket US and
Kalshi, and the bundle records which one the old instance was running. Select
the matching venue on the new instance in Setup, restart, then import again.

**"unsupported bundle schema_version N"**

The bundle format is newer than the build you are importing into. This happens
if you try to import into an older version. Import into a build at least as new
as the one that produced the bundle.

**A setting did not come across**

Configuration is round-tripped through the current schema on import, so a
setting that was removed in the new version is dropped, and a setting that was
added takes its default. This is intended. Check the new or changed settings in
that version's release notes and set them explicitly if the defaults do not
suit you.

**The dashboard looks empty after the upgrade**

Expected. See "What does not transfer" above. The wallet balance and any
on-chain positions should appear within a minute of the restart; the P&L chart
will not backfill.
