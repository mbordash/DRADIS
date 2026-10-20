# DRADIS releases

Customer-facing notes, one entry per Marketplace version, newest first. The
short form at the top of each entry is what goes in the AWS Marketplace
version's release notes field; the detail below it is the full account.

## 1.4.2 (unreleased)

- Control Tower: a finished Kalshi sports or politics market no longer lingers in the "market data has stopped" banner or as stale vipers in the health strip after its squadron retires.
- Polymarket International: "Retire and back up" no longer restarts the engine on the first click; the backup now builds on the first attempt.
- Kalshi: simulated (ghost mode) positions on a finished sports or politics market are booked as settlements from the venue's result, so a ghost game no longer holds its squadron and blocks the class.

### Detail

**Retire and back up on Polymarket International.** The crypto squadron was
registered with its asset loop's own cancellation token, so standing every
squadron down for a migration backup also ended the loop, the process exited
and Docker restarted it already retired, with the backup task gone. The first
click showed "Retired for migration" and never offered a download; the second
click worked. The squadron now has its own token: an operator Stand Down still
ends the asset loop as before, while a retirement leaves it idling so the
process stays up for the backup.

**Simulated settlements on Kalshi.** Ghost mode has no settlement path of its
own, and 1.4.1's retirement never stands down a squadron that holds a position.
A simulated position on a finished game would therefore have pinned its
squadron, and the seeder skips a class that already has one. Kalshi states the
result in the market record, so once the venue reports a yes or no result
(status determined or finalized), simulated positions on it are booked as
settlements, marked simulated, charging the entry fee a live entry of the same
kind would have paid (taker entries pay it, post-only entries do not), and the
squadron retires as usual. A position with a real open-position row on the same
token is never booked this way. An undetermined, voided or non-binary result
books nothing, and such a position keeps its squadron until the operator stands
it down.

**Stale market state after retirement.** The first night 1.4.1 retired a
finished game, the squadron stood down on time but the Control Tower kept
reporting its market as dark and its four vipers as stale for hours, because
the engine's per-market registries were only cleared on crypto rotation. Every
Kalshi market loop now clears its own entries when it ends, whichever way it
ends. Nothing changes in trading.

## 1.4.1 (2026-10-19)

- Kalshi: sports and politics squadrons stand down when the venue closes or finalizes their market, instead of waiting for the market's stated close.
- Kalshi: a sports squadron also retires once its game is over by the bookmaker board, the order book and the ledger, as on Polymarket International.

### Detail

**Kalshi finished games.** Kalshi dates a game market's close at its latest
possible expiration, two days after the game, and finalizes the market a few
hours after the final. The engine ended a pinned market only on that stated
close, so a finished game held the sports squadron, blocked the sports slot for
the next slate, and kept the "market data has stopped" banner up for two days.
Pinned squadrons now ask the venue once a minute whether the market still
trades and stand down, once flat, after the venue has said no for the
retirement grace (five minutes by default). Sports squadrons additionally apply
the game-over rule already used on Polymarket International: kick-off well
past, the game off the bookmaker board, and the book dark or priced as decided.
Nothing changes for crypto hourly markets, whose stated close is exact. The new
retirement paths never stand down a squadron that holds a position; the
exchange settles the position and the settlement sweep books it. A halted game
(Kalshi status "inactive") is not read as closed.

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
