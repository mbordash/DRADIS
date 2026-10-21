# Sideways-regime viper design spike

Status: research spike, no production code. Written 2026-10-21. Author: Claude (Fable), for Michael's go/no-go.

Prompt, quoted from Michael: "the markets have been sideways this month, can you research another trading method for this regime and propose a new viper".

Every claim below is tagged:

- **[E]** evidence-backed: measured today from public Kalshi, Binance or Coinbase data, from the production recorder's tapes, or read in the code.
- **[R]** reasoned: follows from [E] facts by arithmetic or mechanism, not itself measured.
- **[G]** guess: plausible, unmeasured, could be wrong.

## 0. Recommendation in one screen

1. **October is a range regime.** [E] BTC hourly realized volatility is 31% annualized against 35% in September and 37% in August; 71% of October hours moved less than 0.25%; the month opened at $83,509 and sat at $83,282 on Oct 21. The existing vipers are built for velocity, drift, funding skew or settlement convergence, and the last 24 hours produced zero real fills. That is the gates working, not failing.
2. **The obvious range trades do not pay on Kalshi.** [E] Across 307 settled October hours the hourly ladder is calibrated to within two points at every price, its implied volatility is slightly *below* trailing realized (median $192 against $219 of remaining-window sigma), and every level-based structure I tested nets zero or less after the 0.07 quadratic taker fee: the two-strike range ("condor"), the at-the-money straddle of resting bids, resting bids on the wings, and backing favorites. Section 2 has the tables. A "sell volatility" viper would be built on a premium that is not there.
3. **An absolute fair-value model is anti-informative against Kalshi's tape.** [E] On 75 recorded hours (337,549 prints), a normal model fed Binance one second ahead of each print and the stablecoin-corrected basis calls prints cheap that then win 38% against a 47% price. The market knows the settlement level better than any single external feed. Swapping Binance for Coinbase USD (a CF Benchmarks constituent) makes the model neutral, not informative.
4. **A market-anchored model is informative, and the edge lives exactly where a range regime keeps the market.** [E] Anchor the model's level to the market's own prices from a few minutes earlier, then let Binance's move since the anchor drive fair through the contract's gamma. Buy candidates (4 cents of edge net of fee) then win 42% at an average price of 0.38, +2.8 cents per contract held to settlement, while the control group the model calls expensive loses 3.4 cents. The edge concentrates at minutes 30 to 45 (+13.4 cents), within half a sigma of the strike (+6.4 cents), and on the cheap side at 0.20 to 0.60 (+10.3 and +4.6 cents). Those are the conditions a sideways market produces for most of every hour and a trending market produces for a few minutes.
5. **It is a settlement edge, not a latency edge.** [E] Exiting through a resting take-profit at fair within five minutes erases it (−0.04 cents). The market does not converge to the model before the close; it is wrong by a few points and stays wrong until settlement resolves it. [R] So the viper holds to settlement and sizes for binary variance.
6. **Proposed instrument: the Anchor viper**, section 4, built 2026-10-21 as a shadow lane. A taker that buys the cheap side of the squadron's hourly market when the anchored fair clears the ask by a fixed edge, inside minutes 30 to 55, within one sigma of the strike, at prices 0.20 to 0.60, ten contracts, held to settlement, and only while the trailing-hour realized vol is under 35% annualized (section 3.6: the regime gate holds on both venues). No new raptor, no new venue surface, no change to any existing gate. Venue-neutral by construction; the same model is informative on Polymarket International over 828 hours.
7. **Sequencing: Phase 0 is a shadow lane, not a live viper.** [E] The hour-level record is 36 of 71 hours positive with a mean of +$1.60 and a worst hour of −$46.56 at ten contracts per signal minute. That is one week, one regime, and fills read off prints someone else got. Go/no-go statistics for the shadow lane are in section 4. **Go on Phase 0 now; Phase 1 is gated on the ledger.**

## 1. The regime [E]

Binance BTCUSDT hourly klines, public data endpoint.

| Month | Hours | Realized vol (annualized) | Median hourly range | Hours with abs return under 0.25% |
|---|---|---|---|---|
| 2026-08 | 744 | 37.0% | 0.37% | 72% |
| 2026-09 | 720 | 34.9% | 0.41% | 66% |
| 2026-10 (to Oct 21 03:00Z) | 491 | 31.3% | 0.36% | 71% |

Weekly ranges: Oct 5 week $80,394 to $86,999; Oct 12 week $79,800 to $84,352; Oct 19 week (partial) $81,200 to $83,714. Nothing in the engine reads this directly; the Price raptor's `hist_vol` is the in-process equivalent.

## 2. What was tested and rejected

Data: every settled `KXBTCD` hourly from Oct 8 to Oct 20 (307 hours), eight strikes per hour at $100 steps around the Binance-rounded spot, one-minute candlesticks from Kalshi's public series endpoint (bid, ask and prints per minute), settlement from the market record. Fees: taker 0.07·p·(1−p) per contract, maker 0.0175·p·(1−p) where a maker variant is quoted. Scripts and cached data are in the session scratchpad (`condor_bt.py`, `condor_rows.json`, `kc/`).

### 2.1 The ladder is calibrated and volatility is fairly priced [E]

Minute-10 mids against settlement, both legs, all strikes:

| Mid | n | Realized |
|---|---|---|
| 0.1 | 325 | 0.083 |
| 0.2 | 146 | 0.151 |
| 0.3 | 99 | 0.263 |
| 0.5 | 114 | 0.509 |
| 0.7 | 149 | 0.738 |
| 0.8 | 177 | 0.785 |
| 0.9 | 294 | 0.905 |

Longshots at 0.2 to 0.3 are two to five points rich, the classic favorite-longshot bias, and it is already smaller than the fee needed to harvest it (section 2.5). Fitting a normal to each hour's ladder at minute 10 gives a remaining-window sigma with median $192; the trailing 24 hours of Binance returns scaled to the same horizon give $219. The ratio's quartiles are 0.59, 0.88, 1.12. Kalshi is not overpricing volatility in October. The fit also puts the market's implied index a median $93 below Binance USDT, consistent with the CF Benchmarks basis the Price raptor already corrects for.

### 2.2 Two-strike range (buy YES at K−j, buy NO at K+j, pays 2 inside the band, 1 outside) [E]

Centered on the price-implied at-the-money strike. "Maker" assumes a fill at our bid with no adverse selection, which section 2.4 shows is not a safe assumption.

| Width | Minute | n | Implied inside | Realized inside | Taker net per pair | Maker net per pair |
|---|---|---|---|---|---|---|
| ±$100 | 10 | 93 | 0.398 | 0.441 | +1.4c | +5.6c |
| ±$100 | 20 | 81 | 0.442 | 0.383 | −8.6c | −4.5c |
| ±$200 | 10 | 166 | 0.712 | 0.729 | +0.1c | +3.3c |
| ±$200 | 20 | 152 | 0.748 | 0.789 | +2.7c | +5.8c |
| ±$300 | 10 | 121 | 0.810 | 0.777 | −4.5c | −1.6c |
| ±$300 | 20 | 112 | 0.853 | 0.804 | −5.9c | −3.2c |

Splitting by the implied-over-realized volatility ratio (the regime gate a "sell vol" viper would use) does not produce a stable sign in either half. Rejected.

### 2.3 At-the-money oscillation straddle [E]

Rest YES and NO bids at the same price b on the strike nearest 0.50 from minute 10, cancel three minutes before close, hold fills to settlement. A fill is counted when a print trades at or below the bid.

| b | Both legs filled | One leg | Net per hour |
|---|---|---|---|
| 0.30 | 36% | 64% | −5.0c |
| 0.35 | 51% | 49% | −1.8c |
| 0.40 | 60% | 40% | −4.2c |
| 0.45 | 74% | 26% | −5.0c |

When only one bid fills, that leg settles in the money **1% of the time** (n=97). Passive bids at the strike are selected against almost perfectly: the side that gets hit is the side that is losing. Rejected, and a warning for any future passive design at the money.

### 2.4 Resting bids on the wings [E]

NO bid one cent inside the ask on the strike above, YES bid one cent inside on the strike below, from minute m0, held to settlement, lone legs included. Sample sizes 69 to 168 hours per cell. Results flip sign between adjacent cells: ±$100 at minute 10 nets +4.2c per hour, the same at minute 20 nets −7.4c; ±$200 is +1.3c to +3.0c; ±$300 is −3.3c to −4.9c. No robust edge. Rejected.

### 2.5 Backing favorites [E]

Buying the side whose ask is in 0.80 to 0.90 at minute 10 wins 86.4% (n=368) for +0.5c per contract after the taker fee; every other band is negative. Real, and too thin to build on. Rejected.

## 3. The finding: anchor the model to the market, let gamma do the work

### 3.1 Data

The GBoost recorder's Kalshi store: 75 closed hours Oct 17 to 21 with full tapes (median 5,186 prints per hour, 337,549 prints, about 18.9 million contracts), the strike and the settlement. Binance one-second klines for each hour and the hour before; Coinbase BTC-USD one-minute candles for the robustness runs. Every print is a two-sided opportunity: a taker buy at p means a resting ask at p we could have lifted; a taker sell at p means a resting bid at p, which on Kalshi is an ask at 1−p on the other leg. Scripts: `tape_bt.py`, `tape_robust.py`, `tape_anchor.py` in the scratchpad.

### 3.2 The absolute model loses [E]

Fair = Φ(ln((S − basis)/K) / (σ√T)), S the Binance close one second before the print, basis the $93 median from section 2.1, σ the trailing hour's realized vol from five-second returns. Prints the model calls at least 4c cheap net of fee: n=52,821 across 75 hours, win 38.1%, average price 0.468, **−9.9c per contract**. Prints it calls at least 4c expensive: win 57.5% at 0.494, +6.8c. A three-second lead changes nothing. Coinbase USD at one-minute granularity with no basis: +0.07c in the 0.30 to 0.70 band, neutral. Widening σ by 1.5× does not rescue either. The market's price is a better estimate of the settlement index than any single external feed with a fixed offset, and near the strike late in the hour a $15 level error is a quarter of the remaining sigma, so the unanchored model is noise there. [R] This is the regime FairValue runs in on Kalshi, and it is consistent with FairValue finding no edge.

### 3.3 The anchored model wins, modestly and unevenly [E]

Two anchoring rules, both out of sample for the prints they score:

- **Fixed anchor:** fit the basis per hour on prints from minutes 2 to 10 (the fitted basis has median $100 against Binance, interquartile $80 to $110; median $0 against Coinbase, interquartile −$20 to +$20). Applied to minutes 10 to 59: in the 0.30 to 0.70 price band, prints the model calls 5c cheap win 54.0% at 0.463, **+6.1c** (n=59,130, 61 hours); the control group wins 44.8% at 0.518, −8.7c. Minutes 50 to 59 buy candidates: +6.0c (n=40,902).
- **Rolling anchor** (what a live loop would compute): every print with 0.15 < p_yes < 0.85 implies an index level S_impl = K·exp(Φ⁻¹(p_yes)·σ√T); the anchor is the median of (Binance − S_impl) over prints 60 to 600 seconds old. Applied from minute 12 with the Binance close one second before each print:

| Buy candidates (edge net of fee) | n | Hours | Win | Avg price | Net per contract, settle |
|---|---|---|---|---|---|
| ≥ 2c | 51,868 | 71 | 42.3% | 0.408 | +0.3c |
| ≥ 4c | 31,912 | 71 | 42.3% | 0.383 | +2.8c |
| ≥ 6c | 20,889 | 69 | 40.0% | 0.351 | +3.8c |
| ≥ 10c | 9,272 | 59 | 32.0% | 0.271 | +3.9c |
| Control: ≥ 4c expensive | 72,023 | 71 | 53.6% | 0.557 | −3.4c |

Where the ≥ 4c edge lives:

| Slice | n | Hours | Net per contract |
|---|---|---|---|
| Minutes 12 to 30 | 2,728 | 43 | −6.5c |
| Minutes 30 to 45 | 6,078 | 56 | **+13.4c** |
| Minutes 45 to 55 | 9,765 | 65 | +1.9c |
| Minutes 55 to 60 | 13,341 | 52 | +0.5c |
| z = abs(ln(S/K))/(σ√T) under 0.5 | 15,455 | 66 | **+6.4c** |
| z 0.5 to 1 | 10,467 | 65 | +1.6c |
| z 1 to 2 | 5,605 | 65 | −5.0c |
| Price 0.20 to 0.40 | 8,339 | 65 | **+10.3c** |
| Price 0.40 to 0.60 | 6,723 | 63 | +4.6c |
| Price 0.60 to 0.80 | 5,173 | 53 | −0.4c |
| Price 0.80 to 1.00 | 2,382 | 45 | −7.0c |
| Price under 0.20 | 9,295 | 60 | −0.9c |

Reading: the market under-reacts to Binance's move since a few minutes ago when spot is near the strike mid-hour, and it over-reacts on the expensive side (buying the side the market already favors above 0.60 loses). The last five minutes, where FairValue's settlement snipe lives, carry almost nothing once anchored.

### 3.4 Exit, capacity and variance [E]

- **Resting take-profit at fair within 300 seconds, else settle:** hit rate 57%, net −0.04c against +2.8c for settle-only. The disagreement with the market persists to the close; selling into the market before then gives the edge back. Hold to settlement.
- **Capacity:** every one of the 71 hours produced signals; median 16 signal minutes per hour; a median 22,806 contracts printed inside those minutes. Ten contracts per signal is a rounding error against the flow.
- **Hour-level record** at ten contracts on the first print of every signal minute, held to settlement: mean +$1.60 per hour, median +$0.22, worst −$46.56, best +$64.21, 36 of 71 hours positive. One entry per hour restricted to minutes 45 and later at ≥ 6c edge: +$0.08 per hour. The positive mean is carried by fat tails and by the 30 to 45 minute block. [R] At the hour level this is not yet distinguishable from zero; at the print level it is, but prints cluster within hours. The shadow lane exists to settle that.

### 3.5 Caveats that stand regardless [E unless marked]

- One week, one regime (Oct 17 to 21), 75 hours. The fixed-anchor variant beat the rolling one late in the hour (+6c against +0.5c in minutes 50 to 59), which means part of what the fixed anchor captures is the market's level drifting away from its early calibration over the hour. Whether that is basis drift or slow mispricing, the rolling anchor re-absorbs it. [G] A slower anchor (20 to 30 minutes) may sit between the two.
- Fills are read from prints another taker received. DRADIS would need to be at the resting order first; the print size (median 10 contracts) is a lower bound on what rested, and the live book's depth at the ask is the real constraint.
- Fees assume the quadratic taker schedule at 0.07, which is what `KXBTCD` publishes (`fee_type quadratic, fee_multiplier 1`) and what the engine books on Polymarket International by default (`INTL_TAKER_FEE_RATE`); Polymarket US books 0.06. The viper reads the live rate through `venues::taker_fee_per_share`, so a venue's schedule change reaches it without a code change.
- Settlement is an index the model never sees: CF Benchmarks BRTI on Kalshi, Chainlink on Polymarket International, unverified on Polymarket US. Anchoring is what makes that tolerable on every venue, so the anchor's health is the viper's first gate.
- On Kalshi the 1.4.3 hourly pin matters: before it, a rotation every two minutes would have reset the anchor. Polymarket's hourly is one market per hour, so there is nothing to pin.

### 3.6 Does it travel? Polymarket International, 828 hours [E]

Added 2026-10-21 after the venue-centric draft above. The demo box has run the same recorder against Polymarket International's "Bitcoin Up or Down" hourly since June: 3,048 closed hours with prints (median about 600 per hour) and minute mids. The identical rolling-anchor method (strike = the Binance one-second open at the window start, the engine's own approximation of the Chainlink open; the anchor absorbs the offset; fee 0.07 quadratic, the engine's `INTL_TAKER_FEE_RATE` default) was run on three slices: October 1 to 21 (494 hours, the range regime), the week of August 17 (168 hours, 59.7% realized vol) and the week of August 10 (168 hours, 19.3%). 402,678 prints. Scripts `pm_anchor.py`, `pm_anchor2.py` in the scratchpad; tapes in `pm_tapes/`.

**The model is informative on Polymarket too, with a different shape.** Buy candidates at ≥4c edge: n=54,020 over 770 hours, win 61.7% at 0.588, **+1.65c per contract**; control (≥4c expensive) −1.19c. Resting take-profit at fair within 300 s: +2.2c, better than settlement here (Kalshi: worse). By price band the money sits at 0.40 to 0.60 (+3.4c) and above (0.6 to 0.8 +1.8c, 0.8 to 1 +1.75c); 0.20 to 0.40 is flat (+0.06c) and below 0.20 negative (−2.5c). By minute block 12 to 30 is positive (+2.25c), unlike Kalshi. By moneyness z 1 to 2 is positive (+3.0c) where Kalshi's was negative. The Kalshi gate (minutes 30 to 55, z ≤ 1, ask 0.20 to 0.60) applied to Polymarket gives +1.9c per contract but **−$0.37 per hour** at ten contracts per signal minute (477 hours, 193 positive): first-print-of-minute fills are worse than the print-weighted average. The Kalshi tuning does not transfer; the model does.

**The regime is the common factor.** Bucketing every signal by the trailing-hour realized vol at the time (annualized):

| vol at signal | Polymarket buys | Polymarket control | Kalshi buys | Kalshi control |
|---|---|---|---|---|
| under 25% | +1.3c (n=39,771, 567 h) | −0.7c | +1.1c (n=21,840, 54 h) | −2.5c |
| 25 to 35% | +5.9c (n=6,584, 173 h) | −5.6c | +3.9c (n=7,006, 24 h) | −7.6c |
| 35 to 50% | −0.45c (n=4,522, 107 h) | −0.8c | −4.6c (n=1,313, 8 h) | +7.8c |
| over 50% | −0.3c (n=3,143, 66 h) | +1.1c | +24.9c (n=1,753, 4 h) | −13.7c |

Below 35% the buys win and the control loses on both venues; between 35 and 50% the model is noise on Polymarket and inverted on Kalshi; above 50% the samples are too few to read. This is the thesis stated as one number: the market reprices a fast tape faster than a one-to-ten-minute anchor can follow. The viper therefore carries a regime gate, `anchor_max_vol_annualized` (default 0.35), which the Phase 0 ledger will test directly since the lane records nothing above it. Within the gate, by venue: Polymarket's 0.40 to 0.60 band is positive in every minute block (+4.5c, +4.5c, +9.8c for 12 to 30, 30 to 45, 45 to 55) and its 0.20 to 0.40 band is mixed (+5.7c, −7.8c, +4.4c); Kalshi's small cells point at 0.20 to 0.60 in minutes 30 to 45 (+40.9c, +27.7c) with 12 to 30 negative. The compiled defaults keep the Kalshi gate (production's venue); the knobs are per squadron, so the demo box's Polymarket squadron can run the wider band (0.40 to 0.80, minutes 12 to 55) and its ledger will say which is right.

**What this does not say.** The high-vol week is one week; the per-cell tables below 35% rest on tens of hours on Kalshi; fills are still read from other takers' prints; and Polymarket US has no tapes yet (the recorder runs there untested live), so its record starts with Phase 0.

## 4. The proposed viper: Anchor

Working name. A taker that buys the cheap side of the squadron's hourly market when a market-anchored normal model clears the ask by a fixed edge, under conditions the evidence supports, held to settlement.

### 4.1 Thesis

The market's own recent prices are the best estimate of the settlement index level. Binance's move since then, mapped through the contract's gamma near the strike, is information the market absorbs slowly. In a range regime spot sits near a strike for most of every hour, so the viper is busy; in a trend the strike runs away within minutes and the viper is idle by construction (the moneyness gate), which is the correct posture.

### 4.2 Inputs, all present in `StrategyContext` today [E]

- `snapshot.oracle_price`: the USD mark (`MarkDenomination::Usd` on Kalshi).
- `snapshot.yes_bid/yes_ask/no_bid/no_ask` and depths: the anchor samples and the entry ask.
- `snapshot.hist_vol` (60-minute realized vol from the Price raptor), or a self-sampled σ as FairValue keeps; FairValue's `sigma_floor` logic should be shared rather than copied (make it `pub(crate)`).
- `market.strike`, `market.close_time`, `market.window_secs`.
- `dynamic_config` for the knobs.

### 4.3 Anchor estimator [R, from 3.3]

Each tick with a two-sided book and 0.15 < mid_yes < 0.85: S_impl = K·exp(Φ⁻¹(mid_yes)·σ√T); push (now, oracle − S_impl) to a ring buffer. Anchor = median of samples aged 60 to 600 seconds. Ready when at least 20 samples span at least 300 seconds. Not ready, or no two-sided book, or oracle 0: idle with reason "anchor not ready". A market change (rotation) clears the buffer.

### 4.4 Fair and edge

fair_yes = Φ(ln((oracle − anchor)/K) / (σ√T)); fair_no = 1 − fair_yes. For each side: edge = fair − ask − taker_fee(ask). Buy the side with the larger edge when it clears `anchor_min_edge`.

### 4.5 Gates (compiled consts for the spike, knobs before Phase 1)

- Seconds to close between 300 and 1800 (minutes 30 to 55). Nothing before minute 30: that block lost 6.5c.
- Moneyness z = abs(ln((oracle − anchor)/K))/(σ√T) ≤ 1.0.
- Ask in 0.20 to 0.60. The expensive side is never bought.
- `anchor_min_edge` 0.04 net of fee.
- Existing OBI veto and `has_price_feed` rule apply unchanged.
- One position per side per market per hour; no re-entry after an exit.
- Size `anchor_trade_size_contracts` 10 (about $2 to $6); `anchor_max_exposure_usdc` 12.

### 4.6 Exit

Settlement only, per 3.4. No take-profit, no percentage stop. The shadow ledger records counterfactual stops the way FairValue's stop shadow does, so a stop is added only if the ledger shows it pays.

### 4.7 Plumbing (as built, 2026-10-21)

`src/vipers/anchor_impl.rs` implements `Strategy` (venue "Hourly", risk model "Taker at the ask, hold to settlement (simulated)"). Phase 0 emits no `Entry` signal: the would-be buy goes to the `anchor_shadow` ledger (`db::anchor_shadow_*`), settles through `venues::core::resolve_leg`, and is booked to the trade list as a ghost row. The expensive side is recorded in a `control` lane the same way, sampled on every tick inside the gates whatever the entry rule decided, and never booked. Reviewed independently 2026-10-21 (no path to a venue order; math confirmed; the review's fixes, in-memory holds, market keying by `gboost_planb::market_key`, a ghost filter on the real-settlement duplicate check, are in). Registry entry, `strategy_names`, kind `anchor` in `VIPER_KINDS` and the crypto class taxonomy; `ENABLE_ANCHOR_TRADING` plus fourteen knobs (the fifteenth, `anchor_max_vol_annualized`, is the regime gate from 3.6) in `config.rs` and all three `config.*.rs.example`, `DynamicConfig` fields with defaults, the `Anchor` group in `config_schema.rs`, all three profiles in `profiles.json`, `types.ts`, and the card in `VIPER_DEFS`. Refusal reasons through the viper status snapshot; the card's detail line carries the anchor, the fair against the ask and the lane's running record. Nothing in the file names a venue (`vipers::venue_agnostic_guard`). The live path (`StrategySignal::Entry`, FairValue's FAK) is Phase 1 and is not in this build.

Alternative worth one line: the anchored fair is a strong candidate feature for GBoost plan B once the Kalshi store has trained its first model.

## 5. Phases and go/no-go

**Phase 0, shadow lane** (as Bookline runs on the board: simulated, never places an order). The viper evaluates every tick and writes a ledger row per would-be entry: time, market, side, ask, ask depth, fair, anchor, σ, z, seconds to close; and at close the settlement and the P&L at the ask. It runs on every crypto squadron's hourly market on whichever venue the instance trades, so production (Kalshi) and the demo box (Polymarket International) each build their own record, and the go/no-go below is judged per venue.

Go to Phase 1 when all of these hold on the ledger:

1. At least 300 hours and 150 would-be entries, spanning at least one week where realized vol is above 40% annualized (a non-range week).
2. Net per contract after the taker fee ≥ +3c over the whole ledger, and positive on the 30 to 45 minute block alone.
3. At least 55% of hours with a signal positive.
4. The control group (would-be entries on the expensive side, logged but never taken) negative.
5. Depth at the ask at signal time ≥ 10 contracts on at least 80% of entries.

**Phase 1, live**: ten contracts, $12 max exposure, settlement exits only, enabled per venue only where that venue's ledger passed. Review after 200 live entries.

## 6. Sources

- Kalshi public API: `GET /trade-api/v2/series/KXBTCD/markets/{ticker}/candlesticks?period_interval=1`, `GET /markets?event_ticker=`, `GET /series/KXBTCD` (fee type and multiplier).
- Kalshi fee schedule (taker 0.07·C·P·(1−P), maker 0.0175, series multipliers): https://kalshi.com/docs/kalshi-fee-schedule.pdf
- Favorite-longshot bias on Kalshi, 300,000+ contracts: https://cepr.org/voxeu/columns/economics-kalshi-prediction-market
- Bitcoin implied volatility index and variance risk premia (Alexander and Imeraj): https://sro.sussex.ac.uk/id/eprint/91094/
- Binance public data endpoint (`data-api.binance.vision`, 1s and 1h klines); Coinbase Exchange public candles (BTC-USD, 60s).
- Production recorder store `/opt/dradis/logs/gboost_planb/kalshi/btc/markets/` (75 closed hours with tapes as of 2026-10-21 00:00 ET).
