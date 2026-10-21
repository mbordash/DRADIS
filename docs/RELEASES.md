# DRADIS releases

Customer-facing notes, one entry per Marketplace version, newest first. The
short form at the top of each entry is what goes in the AWS Marketplace
version's release notes field; the detail below it is the full account.

## 1.5.0 (unreleased)

- Includes the 1.4.3 changes below (GBoost venue handling, young-store training, Kalshi hourly strike pinning), which ran on production from 2026-10-20 but were never listed as a Marketplace version.
- New Anchor viper: on hourly crypto markets it takes the index level from the market's own price and the move from the oracle feed, prices the contract through its gamma, and buys the cheap side at the ask when the anchored fair clears it net of fee, holding to settlement with no exit order. It trades the way every viper does, simulated in ghost mode and with real orders otherwise, and keeps its own ledger of each fill's model reading beside a never-bought control lane, which is the record the strategy is judged on. Off by default, on the Anchor card of every crypto squadron.
- Simulated settlements: a simulated Anchor position held through an hourly close is booked at the venue's result and released, where a simulated hold had no settlement path before.

### Detail

**Why another viper.** October's range regime (realized volatility near 31%
annualized, seven in ten hours moving under a quarter of a percent) left the
directional vipers idle, as they should be. Research on 307 settled Kalshi hours
and 75 fully recorded ones found no volatility premium to sell: the hourly ladder
is calibrated within two points at every price and every level-based range
structure nets zero or less after the taker fee. It also found that a fair-value
model fed from an external feed plus a fixed basis is anti-informative late in
the hour, because the venue settles on an index the engine never sees and a
small level error is a large share of the remaining sigma. What the market is
slow at is the move, not the level.

**What Anchor does.** It reads the index level the market's own YES mid implies
under the normal model, keeps a rolling median of those readings from one to ten
minutes ago as its anchor, and prices the contract from the oracle's move since
then. When the anchored fair clears the ask by four cents net of fee, inside the
last thirty to five minutes of the hour, within one sigma of the strike and at an
ask between 0.20 and 0.60, it buys ten contracts with a fill-and-kill order and
holds to settlement. Every input is already in the strategy context, so the same
code runs on Polymarket International, Polymarket US and Kalshi; the anchor
absorbs whatever offset sits between the feed and each venue's settlement index.

**Order management.** Anchor sends its entry to the squadron's dispatcher like
every other viper, so the instance's ghost switch decides whether the fill is
simulated or real, the position appears on the positions table either way, and
the venue's own settlement sweep books a real position at the result. It never
sends an exit. The exposure cap counts every fill from the moment it is bought
until its market settles, so one hour's hold occupies the cap in the next and
cannot stack without limit. An order that does not fill is not asked for again
for thirty seconds.

**The ledger, and why it stays.** The backtest's record came from one week, one
regime and fills read off prints other takers received. So every fill is also
written to the viper's own ledger with the model's reading at the signal (fair,
anchor, sigma, distance from the strike, seconds to close, depth at the ask),
beside a control lane that records the side the model called expensive by the
same margin and never buys it. The go/no-go for leaving the viper on with real
money is read from that ledger: it needs a positive entry lane against a
negative control lane over at least three hundred hours, per venue. Its card
shows the anchor, the fair against the ask, and the running record in whichever
mode the instance is in.

**Simulated settlements.** Ghost mode had no settlement path for a position held
through an hourly close: the Polymarket International patrol cleared the
simulated row at rotation unscored, and the Kalshi and Polymarket US loops left
it in place. Anchor books its own simulated settlements from the venue's result,
through the same simulated-settlement path the sports squadrons use, so a
simulated hold is scored and released like a real one. Each fill is stamped
simulated or real when it is made, and that stamp, not the switch's position at
the close, decides who books it, so flipping ghost mode mid-hold can neither
double-book a position nor lose one.

## 1.4.3 (unreleased)

- GBoost: an instance that moved venues no longer keeps the previous venue's model in service indefinitely; the first model trained on the new venue's own recorded markets takes over in the shadow lane, where its record counts toward real money.
- GBoost: on venues that record their own hourly markets (Kalshi, Polymarket US), training starts after two days of settled records instead of a week, with a holdout fold sized to the young store.
- Kalshi: the crypto squadron settles on one hourly strike about five minutes into the window and holds it to the close instead of hopping to a hotter strike every few minutes, so the vipers' per-market warmups stop restarting and the training recorder captures most of the hour.

### Detail

**A model from the previous venue.** A migration backup carries the serving
GBoost model across, and the viper loads it: the venue stamp is read, not
enforced, so the model scores and records in the shadow lane. The lane decision
checks the venue before the record, so that record can never promote it, and the
trainer would have compared each new candidate against it on holdout skill,
keeping the carried model whenever a candidate trained on days of data scored
worse than one trained on months. Production has carried such a model since
moving to Kalshi on October 18. A serving model whose venue or currency stamp
bars it from real money here is now treated like one the viper refuses: the
first candidate the store can produce replaces it, in the shadow lane if it has
not cleared the gate, and the shadow record that counts starts then. The gate
and the shadow record decide real money exactly as before, and Auto Adopt still
decides whether a candidate goes into service: with it off, the candidate waits
in the candidate file for the operator.

**Training on a young store.** A venue without public price history records its
own hourly markets, and training waited for a week of settled records. With the
default fourteen-day holdout every record of that week was inside the fold, so
the first cycle that could fit anything was two weeks out. The wait is now two
days, and the fold is a third of the recorded span (never under a day) until the
store is at least three folds deep, after which the configured fold applies
unchanged. A cycle that still lacks rows reports that on the GBoost card with
the counts, as before.

**One strike per hour on Kalshi.** Kalshi lists a ladder of strikes for each
hourly window and the crypto squadron followed volume across it, rescanning
every two minutes and moving whenever another strike was 5,000 contracts busier.
On October 20 that was about a dozen moves in seventy minutes. Each move tears
the squadron down and rebuilds it, restarting the warmups Momentum, Convergence
and Trend Reversal run on a new market, and the training recorder, which samples
mids only while the squadron sits on a market and writes at the close, kept a
median of five minutes of mids per hour, below what a training row needs. The
squadron now settles on a strike at its first rescan after minute five and holds
it to the close, even when trading moves to a neighboring strike later in the
hour; that strike's book then goes quiet and the squadron may sit idle until the
close, which is the trade-off this makes. The first five minutes still follow
volume, since every strike is empty at the top of the hour and the first pick is
a guess, and a market of another underlying is still judged by volume at any
minute, so a quiet hour's first pick gives way once BTC is clearly busier. The
held market's volume is now refreshed on every rescan rather than frozen at
selection, which had made it look colder than its neighbors. A better cadence
appearing (an hourly market while flying a 15-minute one) still rotates at once,
and a market the operator deployed was never rotated.

## 1.4.2 (2026-10-20)

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
