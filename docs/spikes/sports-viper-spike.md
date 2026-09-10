# Sports Raptor/Viper design spike

Status: design spike, no production code. Written 2026-09-09. Author: Claude (Fable), for Michael's go/no-go.

Governing premise, quoted from Michael: "Stop deploying politics and sports is dead against the DRADIS premise - there is signal and we need to invent raptor/viper to find them."

Every claim below is tagged as one of:

- **[E]** evidence-backed: something I measured today, read from a primary source, or read in the code.
- **[R]** reasoned: follows from [E] facts by arithmetic or by mechanism, but not itself measured.
- **[G]** guess: plausible, unmeasured, and could be wrong.

## 0. Recommendation in one screen

1. **The premise is right and the current deployment is not testing it.** [E] The Sports Raptor publishes a de-vigged multi-book consensus that no viper reads, and the two vipers that do run on sports squadrons (Arbitrage, Maker) are gated off by construction on sports books. The sports squadron is inert because it is asking the wrong questions of the market, not because the market is empty.
2. **Polymarket sports moneylines are not thin.** [E] Tonight's MLB moneylines carry $250k-$560k of book liquidity with 1c spreads and 0.001 tick sizes; the US Open match had 45,000 shares of bids within 3c of the touch. The brief's assumption "sports markets are thinner than crypto" is wrong for the top-of-slate games DRADIS's auto-deploy actually lands on. That cuts both ways: deep books are good for resting and bad for mispricing.
3. **Pre-game, there is no static mispricing to harvest.** [E, small sample] Across 10 pre-game MLB games priced by 9 US books, the de-vigged consensus sat within 1.5 points of the Polymarket mid on every game (mean absolute gap 0.6 points), inside a 1c spread. A taker FairValue analogue needs +1.3 to +1.75 points over mid just to cover the fee. It would never fire, and that is the correct outcome.
4. **The signal, if it exists, is in the dynamics, not the level.** [R] Three candidate sources: (a) the book consensus leading Polymarket on news-driven line moves (starting pitcher scratches, injuries, weather); (b) in-play, where most sports volume lives and where a book or live-event feed with sub-minute latency would lead a 50ms patrol loop; (c) the consensus as an adverse-selection shield for a resting maker, which is a defensive use of the raptor rather than an alpha source. Only (a) is measurable with the free-tier Odds API, and tonight's 92-minute time series produced no reading at all: no line moved by even half a point and Polymarket did not tick once (section 2.4). The lead/lag thesis is untested, not refuted.
5. **Maker-first is not just cheaper, it flips the sign of the fee term.** [E, arithmetic] On Polymarket International sports, a taker at mid 0.50 needs the true probability to exceed the mid by +1.75 points to break even holding to settlement; a maker at the bid needs it to exceed the mid by -0.69 points (it can be slightly wrong and still break even, because it collects the half-spread and a 15% rebate). The maker advantage is 2.4 points at mid, ~1.9 points at 0.20/0.80. Every point of that is real only if the resting bid is not adversely selected, which is the entire risk of the design.
6. **Proposed instrument:** a market-linked Sports Raptor v2 (one line feed per deployed market, matched by team names and start time, publishing `fair_yes`, feed age, book count, dispersion, drift, in-play flag) and a maker-first "Bookline" viper that rests a post-only GTC bid at `min(best_bid, fair - required_edge)`, holds to fee-free settlement, pulls the bid on any raptor-side signal that the line is moving or stale, and stays idle when the gap is inside the edge (which, pre-game, is nearly always).
7. **Sequencing:** Phase 0 is a line ledger, not a viper: the v2 raptor writes (t, fair, mid, bid, ask, book ages) rows for every deployed sports market and the market's eventual result, for weeks, at zero trading risk. Phase 1 is the viper in ghost mode with an explicit caveat that DRADIS's ghost fill model is pessimistic for makers. Phase 2 is live at $10/market on MLB/NBA/NFL moneylines only. Explicit go/no-go statistics are in section 8. **Go on Phase 0 now; Phase 1 and 2 are gated on the ledger.**
8. **Two latent defects found on the way, worth fixing regardless:** Adama hands every event-market squadron `yes_fee_bps: 0, no_fee_bps: 0` while the venue charges 0.05 on sports takers (section 1.4); and the Polymarket US and Kalshi auto-deploy "sports" slot landed on a Miami temperature market and a "Steve Kerr out before Oct 25" market respectively (section 1.5).

## 1. What exists today (all [E], read from code and logs at commit fa0f795)

### 1.1 The Sports Raptor

`src/raptors/sports.rs` polls The Odds API (`ODDS_API_KEY`, free tier) for the configured sport key (default `upcoming`, region `us`, market `h2h`), every `sports_poll_secs` (default 7200s, 2 hours). Each poll it selects the **single nearest-commencing event that has priced h2h odds across all sports**, removes each book's two-way overround by normalizing `1/decimal_odds` to sum to 1, averages the reference (first-listed) outcome across books, and publishes:

| field | meaning |
| --- | --- |
| `consensus_prob` | vig-free consensus probability of the first-listed outcome |
| `line_drift` | change vs previous poll, same event id only |
| `book_dispersion` | max - min of per-book de-vigged probabilities |
| `num_books` | contributing books, 0 = no data |

The raptor is correct as far as it goes, and its credential handling (query-string key redaction in error paths) is careful. Two structural facts make it unusable as a trading signal in its current form:

- **It is not linked to any market.** The event it tracks is whichever game starts next across all sports. The sports squadron it is attached to is on whatever market Adama's seeder picked by liquidity (tonight: a Feyenoord match and a US Open WTA match on intl; the raptor was on `upcoming`, i.e. the next MLB game). The two have nothing to do with each other.
- **Nothing consumes it.** `consensus_prob` outside the raptor appears only in `src/api/server.rs` telemetry (`sports_consensus_prob`, `sports_line_drift`). `StrategyContext` carries no raptor fields at all; vipers read raptor values through `MarketSnapshot` (`tide_coherence`, `cvd_ratio`, `funding_rate`, ...), and there is no sports field there. The registry row says so: `("sports", "Sports Raptor (line movement, observe-only)", 1)`.

There is also a Tennis Raptor (`src/raptors/tennis.rs`, Live Tennis API, free tier 100 req/day) publishing live score state for one tracked match, likewise observe-only and likewise not tied to a market. It is the in-play sibling of what this spike proposes and I come back to it in section 3.

### 1.2 What a sports squadron runs and why it is inert

`src/helpers/db.rs` taxonomy: `("sports", "arbitrage"), ("sports", "maker")`. The intl soak log (`logs/dradis-intl.log`, 30,935 lines, through 2026-09-09 18:06 EDT) confirms it: `Squadron [sports-open] classified as 'sports' -> raptors=["sports"], vipers=["arbitrage", "maker"]` (18 times). Not one `MakerQuote` or Arbitrage entry line names a sports market across the whole log.

The Maker's own code says why. `src/vipers/maker_impl.rs:100-125`:

```rust
// spread is below the ROUND-TRIP FEE, no setting rescues it: quoting
// would buy a guaranteed loss. Polymarket International's event markets
// quote a tenth of a cent against fees two to seventeen times larger ...
let fee_floor = crate::venues::round_trip_fee_pct(bid_price) * bid_price;
if spread < fee_floor { return Some(("spread_below_fee", ...)) }
```

and it has a test for exactly this case, `an_event_book_spread_is_below_the_fee_floor` (line 1987). The Maker assumes a taker round trip (`round_trip_fee_pct = 2 * taker_fee_rate() * (1 - p)`) and refuses any book whose spread cannot pay for two taker fees. On a 1c-spread sports book that is every book, all the time. Note also that `taker_fee_rate()` reads the global `intl_taker_fee_rate` knob (default 0.07, the crypto rate), not the market's own schedule.

Arbitrage needs `YES_ask + NO_ask < 1` on a deep book. On these books the two asks sum to 1.006-1.010. It never fires either.

So the squadron patrols, hits the venue's `accepting_orders` check every minute (the log shows it politely retiring when the match ends), and does nothing. That is honest idleness, but it is idleness caused by running crypto-calibrated order-book vipers on a market class they were never designed for.

### 1.3 Market shape on Polymarket International (measured 2026-09-09 ~22:00Z)

| market | liquidity | 24h volume | bid/ask | tick |
| --- | --- | --- | --- | --- |
| US Open ATP: Khachanov vs Blockx (in play) | $338,688 | $3,142,043 | 0.79 / 0.80 | 0.01 |
| Astros vs Phillies (pre-game) | $547,434 | $64,102 | 0.42 / 0.43 | 0.001 |
| Guardians vs Orioles (pre-game) | $530,989 | $45,021 | 0.52 / 0.53 | 0.001 |
| Pirates vs White Sox (pre-game) | $420,713 | $48,156 | 0.45 / 0.46 | 0.001 |
| Rockies vs Yankees (pre-game) | $103,143 | $13,360 | 0.31 / 0.32 | 0.001 |

The tennis book had 45,137 shares of bids and 27,755 shares of asks within 3c of the touch. The 24h-volume gap between the in-play tennis match ($3.1M) and the pre-game MLB games ($10k-$65k) is the clearest single fact in this spike: **sports volume on Polymarket is overwhelmingly in-play** [E for these markets, R as a generalization].

Market records carry the fields a market-linked raptor needs: `outcomes` are the team names verbatim (`["Washington Nationals", "San Diego Padres"]`, identical strings to The Odds API's `home_team`/`away_team`), `gameStartTime`, `sportsMarketType: "moneyline"`, event `slug` (`mlb-wsh-sd-2026-09-09`), event `series[].slug` (`mlb`), event `gameId`, and `teams`. Soccer markets are a different shape: `"Will Chelsea FC win on 2026-09-09?"` is a YES/NO market over a three-way event (draw exists), so the de-vig must be three-way and the mapping is team -> YES.

Resolution rules (from the market `description`): "If the game is postponed, this market will remain open until the game has been completed. If the game is canceled entirely, with no make-up game, or ends in a tie, this market will resolve 50-50." Polymarket stamps `endDate` a week after the game; the CLOB's `accepting_orders` is the real close, which DRADIS already polls.

### 1.4 Fees, from primary sources

Polymarket International (docs.polymarket.com, help center article 13364478, and the market record itself): `fee = shares * rate * p * (1 - p)`, taker only, makers never charged. Sports rate **0.05** (crypto 0.07, politics 0.04, geopolitics 0). The Gamma market record for tonight's games carries `feeType: sports_fees_v3, feeSchedule: {rate: 0.05, takerOnly: true, rebateRate: 0.15}`. Maker rebates (docs.polymarket.com/programs/maker-rebates): sports pool is **15%** of taker fees in the market, split pro rata by fee-equivalent among makers whose liquidity was taken, paid daily in pUSD, $1 minimum. The same record shows `rewardsMaxSpread: 4.5, rewardsMinSize: 50`: the separate liquidity-rewards program pays for orders that merely rest within 4.5c of mid at 50+ shares. I did not find the reward rate; it is an unmeasured second maker income stream [G on size].

The CLOB's `maker_base_fee`/`taker_base_fee` fields read 1000 on both a sports token and a crypto token and `GET /fee-rate` returns `{"base_fee": 1000}` for both; those fields do not carry the category rate (py-clob-client issue #326 documents the same contradiction, with NHL returning 0 and NBA/MLB 1000). **The market-level `feeSchedule` is the source of truth**, and DRADIS currently does not read it: `src/cag/adama.rs:148-149` builds every event-market `MarketConfig` with `yes_fee_bps: 0, no_fee_bps: 0`. This cannot cause order rejections: the CLOB V2 upgrade (changelog, 2026-04-17) removed `feeRateBps` from the signed order and sets fees at match time, and DRADIS's intl order builder accordingly ignores the value (`src/venues/intl/orders.rs:286`, parameter `_fee_rate_bps`). What it does affect [R] is every piece of DRADIS arithmetic that reads `yes_fee_bps`/`no_fee_bps`: the Arbitrage early-exit threshold on sports squadrons believes taker exits are free, `fee_headroom` sizing is unpadded, and any fee-aware exit a sports viper inherits from FairValue would be mis-fed unless it reads the market rate itself.

Polymarket US (docs.polymarket.us/fees, effective 2026-07-01): `fee = theta * C * p * (1-p)`, taker theta **0.06**, maker theta **-0.0125** (a rebate applied at the point of trade), no per-category differences, banker's rounding to the cent.

Kalshi (secondary sources, consistent with `src/venues/kalshi/trader.rs:30`): taker `ceil(0.07 * C * p * (1-p))` per contract, maker fee `0.0175 * C * p * (1-p)` on the markets that charge one, and Kalshi turned maker fees on for resting orders on 2026-08-19. The per-market list lives on kalshi.com/fee-schedule, which rate-limited me twice; whether `KXMLBGAME`/`KXNFLGAME` moneylines charge the maker fee is unverified [G]. Section 4.2 assumes they do, which is the conservative case.

### 1.5 The other venues' "sports" slot

`logs/dradis-us.log`: Polymarket US auto-deployed its sports squadron on `"Highest temperature in Miami on August 30?"` (`tc-temp-miahigh-...`), because the US venue's discovery bucketed it under `sports` (`categories seen: sports=84, climate=4`). `logs/dradis-kalshi.log`: Kalshi's sports slot went to `"Will Steve Kerr be out before Oct 25, 2026?"`. Neither is a game market. Any sports viper needs a `sportsMarketType == moneyline` (or venue equivalent: Kalshi `KXMLBGAME`/`KXNFLGAME` series, Polymarket US's game markets) filter in discovery, or it will be handed weather.

## 2. Is the signal real and tradeable? (evidence)

### 2.1 What I measured and how

A throwaway probe (`sports_probe.py`, scratchpad only, not committed) polls The Odds API for `baseball_mlb` (`regions=us`, `markets=h2h`, one credit per poll), pulls Polymarket's MLB moneylines from Gamma (`/events?tag_slug=mlb`), matches events by the unordered pair of team names plus game date, de-vigs each book two-way, averages across books, and records `(consensus, sharp-book reading from LowVig/BetOnline, book count, dispersion, newest book `last_update`, Polymarket mid, bid, ask, liquidity)` per team per poll. It ran every 5 minutes through tonight's pre-game window and every 20 minutes overnight against tomorrow's slate. Budget: the key had 344 credits left at start; the run spends about 38. All numbers in this section are from that probe unless stated.

Caveats up front: one sport, one evening, US books only (Pinnacle is in the `eu` region and would cost a second credit per poll), a polling cadence far too slow to see anything in-play, and no resolved outcomes yet. This is a first reading, not a backtest.

### 2.2 Cross-section: does the book consensus disagree with Polymarket pre-game?

Ten games, all with 9 books, sampled 22:03Z, 30-100 minutes before first pitch:

| team (Polymarket side) | consensus | sharp (LowVig) | PM mid | gap (cons - mid) | PM ask edge (cons - ask) |
| --- | --- | --- | --- | --- | --- |
| Orioles | 0.479 | 0.481 | 0.475 | +0.004 | -0.001 |
| Phillies | 0.579 | 0.575 | 0.575 | +0.004 | -0.001 |
| Marlins | 0.533 | 0.537 | 0.535 | -0.002 | -0.007 |
| Red Sox | 0.659 | 0.661 | 0.665 | -0.006 | -0.011 |
| Yankees | 0.685 | 0.687 | 0.685 | +0.000 | -0.005 |
| Braves | 0.530 | 0.533 | 0.535 | -0.005 | -0.010 |
| Royals | 0.478 | 0.476 | 0.485 | -0.007 | -0.012 |
| Brewers | 0.548 | 0.558 | 0.555 | -0.007 | -0.012 |
| White Sox | 0.550 | 0.546 | 0.545 | +0.005 | +0.000 |
| Dodgers | 0.710 | 0.714 | 0.725 | -0.015 | -0.020 |

Mean absolute gap 0.55 points, max 1.5 points, and the consensus was on the wrong side of the Polymarket ask (negative "ask edge") on 9 of 10. **[E] Polymarket's pre-game MLB moneylines track the de-vigged US book consensus to within the spread.** A first version of the probe without the date match paired today's markets with tomorrow's Odds API events and showed two 6-point "gaps"; they vanished when the bug was fixed. I mention it because a market-linked raptor will have exactly that failure mode (section 6).

Polymarket's own 24-hour price paths for the same tokens (CLOB `prices-history`, 10-minute fidelity) moved 1-2 points over the whole day. Pre-game MLB lines are quiet unless there is news.

### 2.3 What that already rules out

A taker instrument. At mid 0.50 a taker who holds to settlement needs the true probability to exceed the mid by +1.75 points (half spread + 1.25c fee). The largest gap in the sample was 1.5 points and it was in the wrong direction. Adding the 8c base edge FairValue uses mid-session would make the threshold unreachable by an order of magnitude. This part is the same lesson as the Momentum/Convergence fee work, with a cleaner answer: the market and the books agree, so there is nothing to cross for.

### 2.4 Time series: does the book lead Polymarket when the line moves?

**Result: no qualifying events, so the lead/lag thesis is untested by this spike.** [E]

Sample: 18 polls at a clean 5-minute cadence from 22:03Z to 23:35Z (one interval is 10 minutes because a poll at 22:34Z failed on a TLS handshake timeout; that interval is excluded from the between-poll statistics), 26 pre-game team-series with 7 or more books, 280 rows, 236 usable intervals.

- The de-vigged consensus never moved by 1 point between polls. The largest single-interval move was 0.29 points; the largest whole-window drift was 0.5 points (Pirates 0.450 to 0.455, White Sox 0.550 to 0.545).
- The Polymarket mid did not move at all: 0.00 points in all 236 intervals. I checked that this was the market and not the sampler by pulling the CLOB's own 1-minute price history for eight of the tokens over the same window: 736 minute-points, zero price changes.
- Level agreement held throughout: mean absolute gap 0.57 points, maximum 1.52 points, mean signed gap 0.00.
- The paper-maker statistic (S2 in section 8) is therefore also empty: 254 hypothetical bid-polls at each of four edges, zero fills under the pessimistic rule, because nothing ever traded through a resting bid.

What this does and does not say. It says that on a quiet pre-game MLB evening the books and Polymarket agree to within the spread and neither moves, which is exactly the regime in which a Bookline viper should be idle, and it is a small confirmation that the idleness conditions in section 3.2 would have kept it idle. It says nothing about who leads when a line actually moves, because no line moved. The question needs news events (scratched starters, injuries, weather) and those arrive a few times a week per sport, not per evening. That is why Phase 0 is a ledger over weeks and why the S1 threshold in section 8 requires a minimum count of moves before anyone reads the ratio.

### 2.5 The honest verdict on the thesis

The thesis in the brief, "the de-vigged book consensus leads the prediction-market price," is **untested by this spike at the level that matters** (minutes, on news) and **false at the level I could test** (the pre-game level, where they agree). What I can say with confidence:

- [E] There is no pre-game level edge on liquid MLB moneylines.
- [R] Whatever edge exists is in the first minutes after news and in play, both of which need a faster feed than a 5-minute (never mind 2-hour) poll of a free API.
- [R] The most robust use of the book consensus is defensive: an independent fair value that tells a resting maker when to get out of the way. That use does not need the book to lead; it needs the book not to lag by more than a poll interval, which is a much weaker requirement.

## 3. What is the instrument?

### 3.1 Raptor v2: a market-linked line feed

Keep the name Sports Raptor; change what it tracks. Per deployed sports market, not per sport:

1. **Match** the Polymarket market to an Odds API event. Sport key from the Gamma event `series[].slug` via a small static table (`mlb -> baseball_mlb`, `nfl -> americanfootball_nfl`, `nba -> basketball_nba`, `nhl -> icehockey_nhl`, `atp/wta -> tennis_atp_*`/`tennis_wta_*`, ...). Event by exact team-name match on both names plus `commence_time` within 12 hours of `gameStartTime`, using the **quota-free** `/v4/sports/{sport}/events` endpoint. No match means no signal, and the squadron stays idle; never fall back to "nearest event".
2. **Map sides** explicitly: Polymarket outcome index 0 is the market's YES token and its name must equal the Odds API outcome name used for `fair_yes`. For YES/NO soccer markets, de-vig three-way and take the named team's probability. Refuse to publish if the names do not match byte-for-byte after a whitelisted normalization (this is the single most dangerous bug class in the design; section 6).
3. **Poll** `/v4/sports/{sport}/events/{id}/odds` (or the sport-wide `/odds` and filter; both cost one credit per region per market) at a cadence set by a knob and by phase: slow (15-30 min) far from start, fast (1-5 min) in the last two hours, off once in play unless an in-play feed is configured.
4. **Publish** per market: `fair_yes` (consensus), `sharp_yes` (a designated sharp book if present), `num_books`, `dispersion`, `feed_age_secs` (now minus the newest book `last_update`), `drift_1h` (change in `fair_yes` over the last hour, from the raptor's own history), `secs_to_start`, `in_play`. Same `watch` pattern as today; the patrol copies the fields into `MarketSnapshot` alongside `tide_coherence` and friends, which is how every other raptor reaches a viper.
5. **Ledger** every poll to a new SQLite table (`sports_line_ledger`: ts, condition_id, fair_yes, sharp_yes, num_books, dispersion, feed_age, pm_bid, pm_ask, pm_mid, in_play), and on market close record the result. This is Phase 0's whole output.

Budget [E]: one credit per poll per market. Free tier = 500/month, so 15-minute polling for a 4-hour pre-game window on one market per day is the ceiling (480/month) with nothing left for matching mistakes. The $30/month 20K tier supports 5-minute polling on three markets all day. Adding Pinnacle (`eu` region) doubles the cost per poll. The raptor should read `x-requests-remaining` (it already does) and degrade its cadence rather than run the key dry.

### 3.2 Viper: "Bookline" (name is a placeholder), maker-first

What carries over from FairValue (`src/vipers/fairvalue_impl.rs`):

- **The shape:** compute a fair value for each side, compare to the market, act only on a gap that exceeds a required edge; refuse to act when the model cannot price (no feed, stale feed, too few books). FairValue's "coin-flip guard" becomes "not enough books / dispersion too high".
- **The horizon-scaled edge** (`required_edge`: base * sqrt(T / taper), capped). Here T is seconds to game start rather than to expiry, and the reason is the same: the further away, the more the line can move against a resting bid before it fills. Sports adds a second scaling input FairValue does not have: the raptor's own measured line volatility (`drift_1h`), which is the sports equivalent of the realized-vol sampler.
- **Hold-to-settlement as the default profit path** ("settle_snipe" posture). Sports resolve within hours and settlement is fee-free, so the resting-take-profit ask is a bonus, not the plan.
- **The resting take-profit** (`MakerRestingExit`, post-only ask at fair + edge, repriced only beyond a threshold). Direct reuse.
- **Model-reversal exit as an EV test, not a percentage stop.** FairValue's `settle_snipe_exit` (sell only when bid net of fee >= model settlement value) is exactly the right exit rule here; a percentage stop on a binary that resolves in three hours is the wrong instrument for the same reason FairValue's header explains.

What does not carry over:

- **The model.** There is no Black-Scholes and no continuous underlying. Fair value is the book consensus, a number produced by other people's models and money, and its error bars come from `dispersion` and `num_books`, not from sigma.
- **Entry is a resting bid, never an ask lift.** FairValue's entry crosses the spread with a FAK. Bookline's entry is a `MakerQuote`-style post-only GTC bid on one side, priced at `min(best_bid, fair - required_edge)` rounded to tick: join the queue if the queue is already at or below fair minus edge, otherwise sit under it and wait. If `best_bid >= fair - edge` the viper does nothing this tick and logs the reason. This is the maker-first requirement made concrete: the viper never pays the fee and never pays the spread.
- **The pull rules are the raptor's real job.** Emit `MakerCancel` on any of: `feed_age_secs > max`, `num_books < min`, `dispersion > max` (books disagree = news in flight), `fair` moved against the bid by more than a threshold since placement, `secs_to_start < pull_before_start` (line goes in-play and the free feed goes stale), `in_play` flip, venue `accepting_orders` false. The Maker viper already has the mechanics (unfilled-quote pull is free; confirmed fills are managed separately) and the patrol already owns quote epochs and fill-watches; nothing new is needed in the patrol for this.
- **One position per market, one side, small.** No two-sided quoting: that is the Maker's job and it is fee-gated for good reasons on these books. Bookline takes a view (the book's view) on one side.
- **Sizing knob is per market,** with a hard cap on simultaneous sports positions, because these are correlated only by sport and resolve as coin flips at whatever probability was paid.

### 3.3 In-play (what would make the big volume tradeable) [R/G]

Most Polymarket sports volume is in-play (section 1.3). In-play the relevant fair value moves on every pitch, point, or possession, and the free Odds API cadence is useless. Two routes, both out of scope for a first version but they are the answer to "what would make it tradeable":

- **Live-event raptors** (the Tennis Raptor is the prototype): a scoreboard feed plus a simple in-game win-probability model (tennis has good closed-form models; baseball has run-expectancy tables) gives a fair value that updates in seconds. The Tennis Raptor's free tier (100 req/day) is enough to develop against, not to run.
- **A paid in-play odds feed.** The Odds API's paid tiers include in-play odds; their in-play update interval is not stated on the pages I could reach [G]. Sharp in-play lines from a sub-minute feed against a 50ms patrol loop is the version of this thesis that has teeth, and it is a budget decision, not a design one.

Both attach to the same viper: Bookline does not care where `fair_yes` comes from, only how old it is and how many sources agree.

## 4. Economics

### 4.1 Break-even arithmetic, hold to settlement

Polymarket International sports, `rate = 0.05`, rebate 15%, 1c spread. "Edge over mid needed" is the amount by which the true probability must exceed the market mid for the trade to have zero expected value:

| mid | ask | taker fee/sh | taker: edge over mid needed | bid | maker rebate/sh | maker: edge over mid needed | maker advantage (pts) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.20 | 0.205 | 0.81c | +1.31 | 0.195 | +0.12c | -0.62 | 1.93 |
| 0.35 | 0.355 | 1.14c | +1.64 | 0.345 | +0.17c | -0.67 | 2.31 |
| 0.50 | 0.505 | 1.25c | +1.75 | 0.495 | +0.19c | -0.69 | 2.44 |
| 0.65 | 0.655 | 1.13c | +1.63 | 0.645 | +0.17c | -0.67 | 2.30 |
| 0.80 | 0.805 | 0.78c | +1.28 | 0.795 | +0.12c | -0.62 | 1.91 |

Read the maker column carefully: a filled bid at 0.495 breaks even if the true probability is 0.4931. The maker does not need to be right; it needs to not be picked off by more than 0.69 points on average. That is the whole game.

For a stop-loss path (crossing to the bid to exit) the taker fee returns on the way out: the required edge over a random baseline for a tp/sl round trip is `fees / (tp + sl)`, which at mid with 5c tp and 5c sl is 25 points on sports (35 on crypto). That is why Bookline's exits are settlement, a resting ask, or an EV-tested sale, and never a percentage stop.

The 0.001-tick MLB books quote 0.6-1.0c spreads; the maker advantage scales with the spread, so on a 0.6c book it is about 2.0 points at mid rather than 2.4.

### 4.2 Venue comparison (same arithmetic, 1c spread)

| venue | taker rate | maker | taker edge over mid at 0.50 | maker edge over mid at 0.50 | maker advantage |
| --- | --- | --- | --- | --- | --- |
| Polymarket International (sports) | 0.05 | 0, +15% rebate pool | +1.75 | -0.69 | 2.44 |
| Polymarket US | 0.06 | -0.0125 (rebate at trade) | +2.00 | -0.81 | 2.81 |
| Kalshi | 0.07 (ceil to cent) | +0.0175 where charged | +2.25 | -0.06 | 2.31 |

Polymarket US is the best maker venue on paper (guaranteed rebate at trade, no pool pro-rating). Kalshi's maker fee eats most of the half-spread. The instrument is venue-portable in the sense that matters: it is a `MakerQuote`-shaped entry with `MakerCancel` pulls and a `MakerRestingExit`, all of which exist behind the `Execution` trait on all three venues, and the raptor is venue-neutral. What is not portable is discovery (section 1.5) and the fee model, which today is a single global rate per venue and needs to become per-market (section 7).

### 4.3 What the signal must supply

Pre-game the level gap is ~0.5 points and the maker needs about -0.7 points; so a bid resting at `best_bid` has positive expected value **before adverse selection** by roughly 1.2 points at mid, i.e. about 1.2c per share, plus rebates. Against that, every fill happens because someone chose to sell into the bid. The fill population is [G]: retail hedging and noise (benign), bots arbitraging a book move Polymarket has not made yet (fatal, several points each), and in-play flow after the line has moved (fatal). The viper is worth running if and only if the mean adverse move on filled bids is under about 1 point after the pull rules have done their work. Nothing in tonight's evidence measures that number; Phase 0 and Phase 1 exist to measure it.

Put differently: the signal does not need to supply alpha. It needs to supply a fast enough "get out" that the bid is not on the book when informed flow arrives. A 5-minute feed will not be fast enough for bot flow; it may be fast enough for lineup news, which propagates over minutes.

## 5. What could go wrong (adversarial)

1. **Side inversion.** A single string mismatch between the Odds API outcome name and the Polymarket outcome name inverts every trade (bidding 0.49 on a 0.31 side). Mitigation: byte-equal names after a tiny normalization table, refuse otherwise; a startup assertion that `fair_yes + fair_no` is within 0.5 points of 1; a ledger row check in Phase 0 that `|fair - mid|` is under 10 points before any entry is even ghost-simulated. My own probe produced 6-point phantom gaps from a date mismatch within its first minute of life.
2. **Adverse selection is the design's whole risk.** A resting bid fills when the market moves through it; on a deep sports book the market moves through it because the line moved. DRADIS's ghost-mode fill model (`helpers/ghost_quotes::take_crossed`) simulates a fill only when the ask crosses the bid, i.e. it simulates **only** the adverse fills and none of the benign queue fills. Ghost results for Bookline will therefore be a pessimistic lower bound, which is the right direction for a safety test but the wrong instrument for estimating live P&L. Say this in the Phase 1 report before anyone reads a ghost number.
3. **Feed staleness and quota.** Free tier is 500 credits/month and the raptor already warns at 50. A stale line is worse than no line: the viper must treat `feed_age_secs > max` as "no model" and pull, exactly as the Tennis Raptor doc says. Books also pull their lines at start; `num_books` collapsing is a signal, not a glitch.
4. **Start, pause, postponement.** Polymarket keeps a postponed market open; the Odds API event may vanish or move. Rule: any change in the matched event's `commence_time`, or the event disappearing, pulls the bid and marks the market unmatched until re-matched. Cancellation resolves 50-50, which turns a 0.30 bid into a +20c windfall and a 0.70 bid into a -20c loss; small size makes that noise, not ruin.
5. **Resolution while a bid rests.** The venue stops accepting orders at the end of the game; DRADIS already polls `accepting_orders` and retires the squadron. An unfilled bid at that moment is canceled by the venue; a filled one is a position that settles. No new failure mode, but the pull-before-start rule should fire well before the game ends anyway.
6. **The `fee_bps: 0` defect (section 1.4).** Orders are not at risk (CLOB V2 sets fees at match time), but every fee-aware exit rule Bookline inherits from FairValue would compute with a zero rate on a market that charges 0.05. Bookline must take its rate from the market's `feeSchedule`, and Adama should populate `yes_fee_bps`/`no_fee_bps` from the same field so the Arbitrage gate on sports squadrons stops believing taker exits are free.
7. **Correlated exposure.** Ten MLB bids on one evening are ten independent coin flips at 0.45-0.55; that is fine at $10 each and dangerous at $100 each. Cap simultaneous sports positions and total sports exposure with knobs.
8. **Gambling drift.** A viper that rests bids on every game every night is a betting bot with extra steps. The design's idleness conditions (no match, stale feed, gap inside edge, dispersion high, in play) should leave it quoting rarely; if Phase 1 shows it quoting on most games, the edge knob is too tight and the design has failed the DRADIS premise rather than passed it.
9. **Settlement risk.** UMA resolution on sports has been reliable and the resolution source is official statistics; the 50-50 tie/cancel rule is the only unusual outcome. Low, not zero [G].

## 6. Politics (out of scope, one paragraph)

The raptor pattern generalizes: "an independent, de-vigged, multi-source probability for the deployed market, with feed age and source count." For politics the sources would be other prediction markets and polling aggregates rather than sportsbooks, and the horizon is months, which changes the viper (there is no settlement in hours, so the resting-take-profit becomes the main exit). Nothing in Bookline is politics-specific; the raptor's matching layer is.

## 7. Scope and sequencing

### Phase 0: line ledger (go now; no trading; ~2-3 days of work) [R on effort]

- Raptor v2 matching and per-market publishing (section 3.1), `sports_line_ledger` table, result capture on close.
- Discovery filter: `sportsMarketType == moneyline` on intl; venue equivalents on Polymarket US and Kalshi, or leave those venues on Phase 0 telemetry only.
- Fix Adama to read `feeSchedule.rate` into `yes_fee_bps`/`no_fee_bps` for event markets and make the Maker's fee floor use the market rate rather than the global knob. This is a correctness fix independent of the viper.
- Control Tower: show the matched event, feed age, book count, and gap per sports squadron (the telemetry fields already exist for the global snapshot; they become per-squadron).
- Odds API tier: recommend the $30/month 20K plan for the duration of Phase 0 so the ledger can poll at 5 minutes rather than 15.

Go/no-go statistics to compute from the ledger after 3-4 weeks (roughly 300-500 games across MLB, NFL, NBA start):

- S1 (lead/lag): of consensus moves >= 1 point between polls, the fraction where Polymarket had made less than half the move at the same poll and most of it by the next. Do not read the ratio until there are at least 30 such moves (tonight produced zero in 236 intervals). Target: >= 60% book-led. Below 40% (Polymarket leads) kills Bookline on the free feed and the only path forward is in-play.
- S2 (paper maker P&L): simulate a bid at `fair - e` for e in {0, 0.5, 1, 2} points, filled when the Polymarket ask later trades at or below it (the pessimistic rule), held to the recorded result, with rebates. Target: mean P&L per share > 0 with the 95% interval excluding zero at some e.
- S3 (idleness): fraction of market-hours in which a bid would have rested at all. Target: under 30%; the viper should be quiet.

### Phase 1: Bookline in ghost mode (only if S1 and S2 pass)

- The viper per section 3.2, `sports_bookline_enabled` off by default, ghost only, with the pessimistic-fill caveat printed in its startup banner and in the report.
- Knobs (all DynamicConfig, all in the Control Tower, defaults as `pub const` in all three `config.*.rs.example` files): `bookline_enabled`, `bookline_base_edge`, `bookline_min_edge`, `bookline_edge_taper_secs`, `bookline_min_books`, `bookline_max_dispersion`, `bookline_max_feed_age_secs`, `bookline_pull_on_adverse_drift`, `bookline_pull_before_start_secs`, `bookline_trade_size_usdc`, `bookline_max_exposure_usdc`, `bookline_max_open_markets`, `bookline_resting_tp_edge`, `bookline_settle_hold`, and the raptor's `sports_poll_secs_far`/`sports_poll_secs_near`/`sports_near_window_secs`/`sports_sharp_book`.
- Position keying, fill-watch, quote epochs, `MakerCancel`, `MakerRestingExit`: reuse as-is.

### Phase 2: live, small (only if Phase 1 ghost P&L is non-negative under the pessimistic model)

- Polymarket International only, MLB/NBA/NFL moneylines only, `bookline_trade_size_usdc = 10`, `bookline_max_open_markets = 3`, settlement hold on.
- After the first 30 live fills, read the rows: mean (fair_30min_after_fill - fill_price) is the adverse-selection number the whole design rests on. If it is worse than -1 point, stop and go back to the ledger.
- Venue expansion (Polymarket US first, for the at-trade rebate) after Phase 2 has 100 fills.

## 8. Things to verify before implementation

- The Odds API in-play update interval on paid tiers (not stated in the docs I could reach). `/events/{id}/odds` is priced like `/odds`: unique markets returned times regions [E, docs].
- Whether Kalshi's `KXMLBGAME` series charges the maker fee.
- The liquidity-rewards rate on sports markets (`rewardsMaxSpread 4.5`, `rewardsMinSize 50`): if it is material, a "rest inside the reward band, outside the adverse band" posture is a second, lower-risk sports instrument and arguably belongs to the Maker viper with a per-market fee model rather than to Bookline.

## Appendix A: what was run today

- Read-only: The Odds API `/v4/sports` (free) and `baseball_mlb` h2h polls at one credit each (20 through the phase-1 window; the overnight phase against tomorrow's slate was still running at 20-minute cadence when this was written and will spend about 18 more, leaving roughly 305 of the key's 344 starting credits), Gamma `/events` and `/markets`, CLOB `/markets`, `/book`, `/fee-rate`, `/prices-history`, Kalshi public `/markets`.
- Throwaway scripts in the session scratchpad only: `sports_probe.py`, `run_probe.sh`, `analyze_pairs.py`. Nothing was added to the repository besides this document. No production behavior, config, or viper code was touched. The overnight rows in `pairs.csv` were not analyzed for this document; the script can be re-run on them.

## Appendix B: raw cross-section (22:03Z, pre-game, 9 books)

See section 2.2. Full CSV rows including the in-play games and the low-book-count games (where the consensus was 1-3 books and the gaps were 1-3 points, which is a book-count effect, not an edge) are in the scratchpad `pairs.csv`; not committed.
