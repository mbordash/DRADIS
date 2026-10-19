# DRADIS releases

Customer-facing notes, one entry per Marketplace version, newest first. The
short form at the top of each entry is what goes in the AWS Marketplace
version's release notes field; the detail below it is the full account.

## 1.4.0 (2026-10-18)

- Kalshi: shard balances and funding controls in the Setup tab.
- Kalshi: the spot mark is now in dollars (BTC/USDT / USDC/USDT), so strike comparisons and GBoost features match the dollar settlement index.
- Squadrons sharing an account no longer duplicate each other's positions.
- GBoost records and trains on Kalshi and Polymarket US.
- Kalshi crypto squadrons prefer the hourly market.
- Time Decay book-settled gate.
- Console support bundle.
- Venue-switch configuration seeding; fixes.
- Kalshi operators: check shard balances after upgrading.

### Detail

**Kalshi exchange shards.** Kalshi keeps a separate balance per exchange shard
(crypto on shard 2, sports moving to shard 3 series by series, most other
markets on shard 0) and API orders cannot borrow from another shard. The Setup
tab shows the balance per shard, marks the shards your deployed squadrons'
markets trade on as the venue reports them, and lets you move funds between
shards or set Kalshi's automatic rebalancing target, each behind the Setup
password with a confirmation. The engine refuses a buy its shard cannot cover
and says so on the strategy's card instead of retrying, and routes cancels to
the shard the order rests on.

**Dollar mark on Kalshi.** Kalshi's crypto markets settle on CF Benchmarks'
dollar indices, while Binance quotes bitcoin in USDT, which is not always worth
a dollar. The engine now publishes its spot level in dollars on Kalshi (Binance
BTC/USDT divided by USDC/USDT, from the same feed), so strike comparisons use
the currency the market settles in. While the conversion rate is unavailable
the level reads as not connected rather than as the USDT price. GBoost's bars,
live and in its training store, are restated in dollars the same way on Kalshi;
a Kalshi store built before this release is refetched in dollars on the next
training pass. Polymarket markets, which resolve on the Binance USDT candle by
rule, are unchanged in every path.

**Squadrons sharing an account.** A squadron's position sweep no longer adopts
a holding that another squadron on the same account already owns, so several
squadrons on one Kalshi or Polymarket US account do not produce duplicate
position rows or duplicate settlement bookings.

**GBoost on every venue.** GBoost scores Kalshi's one-hour markets and
Polymarket US short-dated markets, trading its shadow lane until a model
trained for that venue passes the holdout. On venues that publish no price
history (Kalshi, Polymarket US) the engine records each hourly market it flies,
minute by minute with the trade tape and the settlement, and builds its
training store from those records; the strategy card shows the count toward
the first training run.

**Kalshi market selection.** The crypto squadron prefers Kalshi's one-hour
above/below listing as its primary market, with the daily market on the same
coin beside it for the slower strategies.

**Time Decay.** Book-settled gate: a resting bid is placed only when the two
legs of the book agree within a configurable net theta
(`time_decay_max_net_theta`, default 0.05), so a mid-flip tick cannot buy a
lone leg.

**Console.** A Support bundle button downloads the logs and configuration a
support request needs, so no shell access is required to ask for help.

**Setup.** Switching venue on an instance seeds the new venue's configuration
from the one you were running, and the Setup tab says so. The sports ledger
switch sits on the Odds API card face. Sports-only settings are hidden on
crypto and politics squadrons.

**Fixes.** Venue switch and sports squadron switch bugs; GBoost pipeline
registration on Kalshi and Polymarket US; live configuration changes apply to
Kalshi and Polymarket US squadrons without a restart.

**Upgrading.** Follow the Setup tab's Move to a New Instance flow as before.
Kalshi operators: check the Setup tab's shard balances after the move and fund
the crypto shard before expecting fills.
