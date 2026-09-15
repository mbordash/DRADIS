# Upgrading DRADIS

DRADIS ships as an immutable image, so there is no in-place upgrade. Upgrading
means launching an instance of the new version and moving your data across.

From version 1.2 the Setup view has a **🚚 Move to a New Instance** panel that
does the whole move. On the old instance it retires the engine, so it stops
trading, and builds one backup holding your trade ledger, your open positions
with the strategies that manage them, the GBoost models and training data, and
your credentials and settings. On the new instance it restores all of it.

If the instance you are upgrading from runs 1.1.x or earlier, it has no such
panel. Follow [Upgrading from 1.1.x or earlier](#upgrading-from-11x-or-earlier)
instead.

Budget about fifteen minutes, plus the time to download the backup and upload
it again. With GBoost training data included it runs to a few hundred megabytes.

## Upgrading from 1.2 or later

**1. Launch an instance of the new version, and leave it unconfigured.**

Subscribe to or launch the new version from AWS Marketplace as you did the first
time. Use the same instance type unless you have a reason to change it.

Give it a Name tag carrying the new version, for example `dradis-v1.2.0`. For a
while two DRADIS instances will be running and you will need to tell them apart;
two unnamed rows in the EC2 console is how the wrong one gets terminated. A
one-click Marketplace launch leaves the Name tag blank, because the instance is
created in your account and the seller cannot tag it. The Control Tower footer
also shows the running version, so an open tab is never ambiguous.

Do not enter any credentials on the new instance. The backup supplies them.

**2. Retire and back up the old instance.**

On the old instance, open the Control Tower, go to **Setup → 🚚 Move to a New
Instance** and choose **Retire and back up**.

Retiring stops the old instance trading at once. It refuses every new order,
cancels its resting orders and stands its squadrons down, and it stays retired
across restarts until you choose **Resume trading here**. Your open positions
are not sold: they stay in the wallet, and the backup carries the records of
which strategy manages each one.

**Until the new instance is running from the backup, no stop or exit can fire on
those open positions.** Every order is refused while retired, sells included, so
a position that moves against you in the meantime is not stopped out. The
confirmation shows how many positions are open. Retire when there are none, or
accept that risk for the time the move takes.

The backup is a snapshot taken when you retire. A settlement the old instance
books after that is not in it; the new instance books the same settlement itself
once it is running.

Leave **Include GBoost training data** ticked unless the download size is a
problem. Without it the models still move, so GBoost keeps trading, but the new
instance spends four to five hours rebuilding its training data before it can
retrain.

When the panel shows the backup, choose **Download backup**. The file holds your
wallet private key, your venue and signal credentials and your full trade
history. Treat it exactly as you would treat the keys themselves.

The browser holds the whole download in memory before saving it. On a computer
with little memory, or for a very large backup, leave the training data out.

**3. Restore on the new instance.**

Log in to the new instance's Control Tower with user `admin` and the password
shown as the new instance's EC2 instance ID. The Setup tab on a fresh instance
first asks you to create a **Setup password**. This is a second, separate
password that protects the Setup view, and it is yours to choose; it does not
come from the old instance.

If the old instance traded on Polymarket US or Kalshi, select that venue in Setup
and restart before restoring.

Go to **Setup → 🚚 Move to a New Instance** and choose **Restore from backup**.
The upload is checked before anything changes: the backup must come from the same
venue, from the same or an older version, and every file must match the checksum
recorded when it was made. The panel then shows what the backup holds: trades,
open positions, files, and whether training data is included.

Choose **Apply and restart**. The engine restarts, applies the backup before it
opens any database, and comes back in about a minute. The panel then reports the
restore, and keeps whatever it replaced on the new instance under
`logs/migration/pre-restore-*`.

**4. Verify.**

Check that the trade log and P&L history are there, that open positions show
with the strategies you expect, that the GBoost card reports its model, and that
Setup shows your credentials as present.

The Control Tower login stays `admin` with the new instance ID, and Setup asks
for the Setup password you created in step 3. Your browser's Setup session does
not carry over, because each instance mints its own session-signing key, so
expect to log in to Setup once more.

**5. Terminate the old instance when you are satisfied.**

The old instance is retired, so it does not trade while you verify, and there is
no race to stop it. Terminating it is a separate decision you can take later.

To roll back before you terminate it, stop the new instance first, then choose
**Resume trading here** on the old one. Never resume the old instance while the
new one is running: two engines would trade the same wallet.

## Why two engines must never trade one wallet

Your wallet private key travels in the backup and in the config bundle, so after
a restore or an import **both instances control the same wallet**. Two DRADIS
engines on one wallet interfere with each other in ways that lose money:

- Each engine cancels every open order on the wallet at startup, because from
  its point of view those are leftovers from a previous session. The new
  instance will cancel the old instance's live resting quotes while the old
  instance still believes they are working.
- Each engine reconciles the wallet's on-chain holdings against its own
  database and adopts anything it does not recognize. Both will therefore claim
  the same positions and manage them independently, with two sets of stops and
  two sets of take-profits against one set of shares.

Retiring the old instance in step 2 is what prevents this on 1.2 and later. On
older versions, stopping the old instance is the whole mitigation.

## Upgrading from 1.1.x or earlier

These versions can export your credentials and settings but not your data.

**1. Export the config bundle from the running instance.** Open **Setup → 📦
Instance Migration**, export the config bundle and save the file somewhere safe.
Do this while the instance is still healthy: the bundle is produced by the
running engine. It carries your venue API credentials, wallet private key,
Raptor signal keys and global and per-squadron configuration, but not the Setup
password.

**2. Launch an instance of the new version** as in step 1 above, and do not
enter any credentials on it.

**3. Import the bundle.** Log in as in step 3 above and create a Setup password,
then go to **Setup → 📦 Config Bundle** and import the file.

**4. Restart the engine** when Setup prompts. It comes back in 30 to 60 seconds
with your credentials and configuration applied.

**5. Verify, then stop the old instance before the new one starts trading.** See
[Why two engines must never trade one wallet](#why-two-engines-must-never-trade-one-wallet).

The bundle carries configuration, not history, so the new instance starts fresh
on the trade log, the P&L chart and the trained GBoost model, which begins
collecting data again from zero. Open positions are not lost: they belong to the
wallet, and the new instance re-adopts them from the chain. If your history
matters to you, keep the old instance's databases before terminating it; they
live at `/opt/dradis/logs/*.db`.

## Check your trading mode after upgrading to 1.0.6 or later

Read this before starting a 1.0.6 engine if you upgraded from 1.0.5 or earlier.

In those versions the Control Tower's GHOST/LIVE button wrote only the
instance-wide setting and did not reach squadrons that were already running. If
you pressed LIVE on an older instance, the screens said LIVE while the squadrons
went on simulating. Many operators are in that state without knowing it.

From 1.0.6 the two are kept in agreement, and the instance-wide setting is the
one that wins. So the first time a 1.0.6 engine starts, any squadron that was
quietly still simulating begins **trading real money**, with no further
confirmation, because that is what the saved setting has been asking for.

Before you start the new instance, open Setup and confirm the mode is what you
actually intend. If you are not sure which state you were in, switch to ghost
mode first, start the engine, watch the Console for a few minutes, and go live
deliberately once you can see what it is doing.

One related consequence: a 1.0.5 trade log could not record whether a trade was
simulated, and displayed every completed trade as real. If your P&L history was
built on an older instance it may mix simulated and real results with no way to
tell them apart. Trades recorded from 1.0.6 onward carry the distinction.

## Troubleshooting

**"the backup is from a 'X' instance but this instance runs 'Y'"**

The image carries binaries for Polymarket International, Polymarket US and
Kalshi, and the backup records which one the old instance ran. Select the
matching venue on the new instance in Setup, restart, then restore again.

**"the backup was made by version X and this instance runs Y; restore it on the same or a newer version"**

A backup restores onto the version that made it or a newer one, never an older
one, because an older build would open a database it has never seen.

**"... does not match its checksum: the archive is damaged or was altered"**

The file changed after the old instance made it, most often from an interrupted
download. Download the backup again from the old instance; it is still there.

**"this instance already has N trade(s); restoring replaces its ledger"**

The new instance has traded, so the panel asks before replacing its databases.
Confirm to go ahead; the replaced files are kept under
`logs/migration/pre-restore-*`. If you did not expect trades there, check that
you are on the instance you meant to restore.

**"this instance is retired for migration; restore the backup on the new instance"**

You are on the old instance. Restore on the new one.

**The last restore failed**

The panel shows the error and where the files it had already replaced were moved.
A failed restore is never retried at the next restart, so nothing changes further
on its own. To recover, upload the same backup again, confirm the overwrite and
apply it. If it fails again, contact support with the message shown.

**"not enough free disk space ..."**

The instance checks its disk before a backup and before accepting an upload. A
restore needs room for the upload, its extracted copy and the files it replaces;
a backup needs room for a copy of the data and the archive. Free space on the
volume, or leave the training data out, and try again.

**HTTP 413 when uploading a backup**

nginx refused the upload for its size before it reached DRADIS. This happens only
on an instance installed before 1.2 and updated in place, which keeps its old
proxy configuration. Copy `deploy/ami/nginx.conf` to `/opt/dradis/nginx.conf`
and restart the `dradis-proxy` container.

**The old instance stays retired and you have decided not to upgrade**

Choose **Resume trading here** on the old instance. The engine restarts and its
squadrons come back.

**"bundle is for the 'X' venue build but this instance is 'Y'"**

The config bundle equivalent of the venue message above: select the matching
venue, restart, then import again.

**"unsupported bundle schema_version N"**

The bundle format is newer than the build you are importing into. Import into a
build at least as new as the one that produced the bundle.

**A setting did not come across**

Configuration is round-tripped through the current schema, so a setting that was
removed in the new version is dropped, and a setting that was added takes its
default. This is intended. Check the new or changed settings in that version's
release notes and set them explicitly if the defaults do not suit you.
