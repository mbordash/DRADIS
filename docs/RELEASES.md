# DRADIS releases

Customer-facing notes, one entry per Marketplace version, newest first. The
short form at the top of each entry is what goes in the AWS Marketplace
version's release notes field; the detail below it is the full account.

## 1.7.2 (unreleased)

- Ultra-short market (new, off by default): each crypto squadron can also fly the venue's 15-minute market on its underlying, beside its hourly and window markets, replacing it at every 15-minute close. Switch it on in Setup (Deployment, Ultra-short Market). Only vipers that declare it trade it: today, script vipers whose market is set to Ultra-short on the Build a Viper page. Every built-in viper keeps its own market. The squadron page shows the 15-minute market beside the others. Kalshi first; on a venue or underlying with no 15-minute market the squadron leaves it empty, and those vipers say so on their cards.
- Build a Viper: a script chooses which of its squadron's markets it trades (hourly, window/daily or ultra-short). Its ctx describes that market, and its exits follow each position's own market.
- Up or Down markets whose title gives a time range ("10:15PM-10:30PM ET") are read as that length: a 15-minute or four-hour window is no longer taken for an hour. The Markets page's model strike for a four-hour window now starts at the window's open.

## 1.7.1 (2026-11-07)

- Kalshi: the crypto squadron no longer trades the wrong market for two minutes after each close. Kalshi lists the next window about 15 seconds after the last one closes, and the squadron chose its next market before that, so it took whatever was listed (the 17:00 daily between 15-minute windows, or a 15-minute market at the top of the hour) and switched back on its next two-minute rescan. Each switch stood the squadron down and canceled its resting orders, and the strategies that trade only hourly markets sat out. After a close the squadron now waits up to 90 seconds for the right next market: the new hour's market at the top of the hour, the next 15-minute window in the hour before Kalshi's 17:00 daily close (which has no hourly market), and otherwise a market like the one that closed.
- Build a Viper: a script no longer sees a price of $0 before the price feed's first print. For the first moment after a start, ctx.oracle and the move signals read 0, so a script comparing the price with a strike saw the market as decided: on one instance a paper viper bought one second after a restart for that reason. Every price signal is now () until the feed has a price, ctx.has_price_feed is false, and the engine refuses a script's new entries in that moment; exits keep running.
- Build a Viper: Check and Save refuse a script that reads a parameter the viper does not define, and name it ("the script reads ctx.params.basis, which is not one of this viper's parameters"). Such a script used to save and then fail on every tick with an error about an operator, not the parameter. Comments, strings and map methods such as ctx.params.len() do not count. Scripts already saved keep running as before.
- Build a Viper templates, from their first day of paper trades. Plain Momentum's stop now waits 60 seconds (Min Hold Secs) before it can fire, so it no longer sells the bounce right after the move it bought, and it is a fraction of the entry price (Stop Loss Pct, 0.35) rather than a fixed 10 cents. Late Favorite buys only clear favorites (Min Fair 0.75), caps how sure its model may be (Fair Cap 0.95, since large moves are more common than its normal curve assumes), starts from a typical hourly move of 0.3% instead of 0.5%, and records the price, strike and distance in each entry reason. Vipers already made from a template are unchanged.
- The Trade Log shows the venue, class and underlying of script vipers' paper trades; they showed a dash.
- Hardened two file paths in the Build a Viper API so a script name can never reach outside the scripts folder (reported by code scanning; the names were already checked before use).

## 1.7.0 (2026-11-07)

- Build a Viper (new): write your own strategy as a script in Control Tower, save it, and it runs from the next tick, with no rebuild. A script defines entry(ctx) and exit(ctx, pos), reads the squadron's market, its signals and its own parameters, and returns a hold, a buy, a sale or a take-profit. Start from a blank script or one of two worked examples (Plain Momentum, Late Favorite). Check finds every problem with the manifest and the script at once. Test run evaluates the script against a squadron's latest market data without sending anything.
- The Trade Log shows script vipers' paper trades beside the real ones, with a 📝 paper badge and a Show/Hide filter. They stay out of every total (Realized, Unrealized, Net, the summary cards, the chart, the auto-scaler and the advisor), and the footer gives them a line of their own. The CSV export stays real trades only. Ghost mode keeps its 👻 ghost badge; Build a Viper now says ghost, not simulated, for the instance setting.
- On the squadron page a script viper's card sits with the built-in vipers, and the squadron's strategies badge counts it.
- A saved viper joins every running squadron of its classes on that squadron's next tick, on every venue, rather than when the squadron next moves to a new market (on the hour for a crypto squadron, days for a sports or politics one). Where it runs, under the editor, lists those squadrons and what the viper is doing on each: joining, waiting for a market, running with its last hold reason and refusals, or failing; it also says when no squadron of its classes is running or its venues leave this one out.
- A new viper trades on paper, in its own ledger: taker fills at the ask and bid with the venue's fee, and settlement at the venue's result. Nothing reaches the venue; the Trade Log shows it with a paper badge, out of every total. Its Live switch sends real orders only while the instance is live as well, behind a confirmation that shows its cap and its paper record.
- The engine owns every check that protects money, whatever the script returns: a required exposure cap counted across every squadron, the daily drawdown limit, taker limits no more than 2 cents past the touch, the venue's minimum size, one position per market, and a sandbox with no file, network or clock access and a hard bound on each call's work.
- Switching a viper off, or taking it off Live, stops new entries only: what it holds keeps its stop and take-profit until it closes. A take-profit is watched by the engine and sold as a taker on every venue, and it survives a restart.
- Scripts live in the data folder and travel in the instance backup. Each squadron page shows a card for every scripted viper, with its mode, its paper record and a per-squadron on/off switch. Guide: docs/SCRIPTED_VIPERS.md.

- Settings that only one venue uses are now shown only on that venue. A Kalshi instance's Setup > Risk & Engine tab used to show an Engine card made entirely of Polymarket settings (the two Polymarket fee rates, the order book price-change feed, the USDC.e collateral sweep); on Kalshi that card no longer appears. The same applies to two Polymarket International settings on strategy cards (Momentum and Convergence Resting Take Profit), and the Kalshi Sports Game Series setting no longer appears on Polymarket instances. Nothing about trading changes, and the engine still accepts every setting, so a config bundle moves between venues as before. The LLM Advisor is no longer offered settings the venue ignores.

- FairValue's Stop Counterfactual Recorder now works on Kalshi and Polymarket US. For each position FairValue's stop closes, it records what holding to settlement would have returned instead, so the stop's width can be tuned from evidence. It looked the result up only on Polymarket International, so on the other venues no stop could ever be scored. It now asks each venue for its own settlement, as GBoost's and Anchor's simulated trades already do. A stop that fills without a matching record now says so in the log instead of being skipped silently.
- Kalshi: standing down the last Helm squadron no longer turns the viper health ribbon red. Helm squadrons share one status entry, and nothing cleared it after the last one ended, so the ribbon reported "1 stale/error — check squadron detail" for a squadron that was no longer on screen. The last Helm squadron to stand down now clears it.

## 1.6.0 (2026-11-05)

- Capital Auto-Scaler (new, off by default): sizes each strategy by its own recent results in its own squadron. While on, a strategy's entry orders grow while it makes money on the capital it risks and shrink while it loses, between a floor and a ceiling (defaults 0.5× and 2×). The measure is return on capital, realized profit or loss over the dollars risked across its last 30 closed trades, not win rate, which on a yes/no contract says nothing without the price paid. Nothing moves before 20 closed trades. Growth stops at a per-order dollar limit (default $25, both legs of a pair together; 0 means never grow), at the strategy's own Max Exposure counting what it already holds, and while the daily drawdown breaker is tripped; shrinking stops at the venue's minimum order size. Exits and Helm entries are never scaled, and simulated and real results are never mixed. Set it in Setup → Risk & Engine, where a table shows each strategy's record and the multiplier it earns; each strategy card shows the multiplier in force. The settings are instance-wide and not part of the risk profiles, so applying a profile never changes them.
- Setup is split into tabs: Venue, Credentials, Signals, Risk & Engine, AI & Alerts, Backup & Move, Support. Each tab has its own link (`#setup/<tab>`), unsaved credential edits survive switching tabs and are marked with a dot, and Save and Restart stay on every tab.
- Time Decay on Kalshi and Polymarket US: the second bid of a pair now stays on the book. Both bids are placed together, and the first to fill was leaving its partner resting for only 60 seconds before the engine canceled it as stale, a window meant for Arbitrage, so nearly every Time Decay entry on these venues became a one-sided position. On 2026-11-03 a NO bid filled, its YES partner was canceled a minute later, and the market then traded back through the partner's price. The partner now rests until the strategy's own exits, the hourly rotation or the market's close end it, as it always has on Polymarket International.
- Time Decay in the balanced profile now enters only when a settled order book offers at least 3¢ of theta (Min Net Profit 0.03, Max Entry 0.50), sizes $5 a leg (Max Exposure $10), and holds a one-sided position to settlement instead of stopping it out (Min Hold Secs 1800). A balanced 0.49/0.50 book offers 1¢ against a one-sided loss of a dollar or more. The conservative and aggressive profiles are unchanged.
- Drawdown limit is now daily: the loss that pauses new entries is counted from midnight ET and clears at the next midnight. It used to count from engine start and stayed tripped until a restart, so one bad evening could keep every strategy idle for days (on one live instance a $3.64 loss at 8 pm paused trading for 19 hours, until a redeploy cleared it). Session P&L as shown in the header, chart and advisor is unchanged.
- Anchor now obeys the drawdown limit. It was the only strategy that never checked it, so it could keep buying after its own losses had paused everything else. Its control lane, which records and never trades, still records.
- Price feed: a reconnect to Binance is no longer reported as a velocity gap. After an outage the first tick has no one-second history, and the gap warning measured the whole outage as "no confirmed 1s velocity … while ticks arrive" although no tick had arrived; the reconnect already logs its own warning. A gap that persists after the reconnect still warns, timed from the first tick back.
- Docker: the Control Tower and proxy containers now cap their logs (32 MB × 3 files, as the engine already did); unbounded logs could fill a long-running instance's disk.
- Docker: the image can be built with another profile for a non-Marketplace build (`--build-arg DRADIS_PROFILE=balanced|aggressive|local`, plus `DRADIS_FORCE_GHOST=true` to compile simulation on), and records the profile it compiled in `/app/build-profile`. The default is unchanged: conservative.
- Dependencies: the GBoost library `perpetual` is pinned to exactly 3.0.0-rc.2, so a routine dependency update cannot move it to 3.0.0 final without a deliberate change.

## 1.5.8 (2026-11-04)

- Kalshi: the market rotation stays on one underlying. The rotation picked the busiest market across every configured series, and by default that list held both BTC and ETH series, so at a quiet hour start the BTC squadron could hand over to an ETH squadron for a couple of minutes and back. While it did, the BTC squadron's vipers stopped evaluating, and the ETH squadron ran its own configuration, which may have strategies enabled that were never tuned for it. The rotation now flies only the underlying of the first configured series and never switches underlyings mid-run; any series for a different underlying is ignored, with a warning in the log at startup. The default series list is now `KXBTC15M,KXBTCD`. To fly ETH instead, set `KALSHI_SERIES` with an ETH series first.
- Price feed: quieter, more useful gap warnings. Binance's ticker stream routinely skips a single one-second update; for that one tick the 1-second velocity reads zero (unconfirmed), as before, but the engine logged a warning each time, up to once a minute per asset. Single skipped ticks are now counted and summarized in the log once a minute at info level. A warning is logged only when the 1-second velocity stays unconfirmed for 5 seconds while updates keep arriving, followed by an info line when the feed recovers. Trading behavior is unchanged.
- Control Tower: a Realized P&L card on the dashboard shows the profit or loss booked by closed trades over the same window as the portfolio chart, with the trade count ("12 trades over the last 7d"). The Session P&L card is unchanged. The trade statistics endpoint, `/api/trades/stats`, takes an optional `hours` parameter for this; without it the figures stay lifetime, so the Trade Log and squadron pages are unchanged.
- Performance: the portfolio chart's history queries use a new database index on the snapshot time. Before, each chart load read the whole snapshot table, which grows by roughly 8,700 rows a day, so long-running instances loaded the chart more slowly over time. The index is added automatically at startup, and an older version of the engine ignores it.

## 1.5.7 (2026-11-02)

- Anchor: an optional take-profit exit. Anchor holds every fill to settlement, so a position that ran well into profit could still settle at zero; on 2026-11-02 a live NO bought at 0.34 had a bid near 0.82 (about +140%) and expired worthless. A new "Take Profit Exit" switch on the Anchor card (off by default, so existing behavior is unchanged) sells a held position at the bid, as a taker, when the sale is a profit after both fees and either the gain reaches "Take Profit At" (default +100%, zero turns it off) or the bid, net of the exit fee, beats the model's live fair value by "Exit Over Fair Value" (default 0.02). A position that is never in profit still rides to settlement. The Anchor ledger now records the best bid each fill saw while held, whether the switch is on or off, so the two approaches can be compared on the same trades, and a holding sold before its close is scored from its sale and never booked a second time at settlement.
- Kalshi and Polymarket US: a taker buy is confirmed as soon as the venue's order response says it traded. Until now only a resting order was confirmed that way, and a taker buy waited for the live fill feed; when the feed reported the fill before the engine had recorded the position, nothing confirmed it afterwards. An unconfirmed position still showed and settled normally, but the Anchor take-profit exit, which sells only a confirmed holding, ignored it: on 2026-11-02 a NO bought at 0.36 reached a 0.92 bid with the exit switched on and was not sold.
- Polymarket International: moved to Polymarket's Data API v2. Polymarket retired Data API v1 on 2026-11-02, and every v1 call now answers 410 Gone, which stopped chain-sync (the check of the engine's positions against the wallet), the settlement of resolved positions, orphaned-pair detection, the venue-income ledger, the Markets page trade tape and the GBoost trainer's trade history. The bundled Polymarket SDK still calls v1, so the engine now reads v2 directly: snake_case fields, results wrapped in a data list, cursor paging. Two things the engine sees that it did not before: wallets with more than 100 open positions are read in full (v1 returned the first 100), and holdings down to 0.1 shares are listed (v1 left out anything under 1 share). A positions response the engine cannot fully read is treated as an error and skipped for that cycle, never as a shorter list, because a missing holding would look like a sold one. Kalshi and Polymarket US are unaffected.
- Control Tower: the portfolio banner's change figure follows the chart's range. It read "vs session start", which is the engine's own session, the time since the process last started, so a deploy or a restart reset it to $0.00 while the chart beside it showed a week of gains. On a live account it is now the change in portfolio value over the chart window, labeled with that window ("over the last 7d", or "since Oct 17" when the history is shorter), from the first snapshot in view to the live value; its tooltip says that deposits and withdrawals count. Ghost mode keeps the session figure, since a simulated account's value never moves. The chart's dashed baseline is labeled "Window start" with its value, and the Session P&L card's caption names the engine start time it counts from instead of a percentage measured from a different clock.

## 1.5.6 (2026-10-31)

- Control Tower: the portfolio chart has a range control. It showed the last 24 hours and nothing else; it now offers 24h, 3d, 7d, 30d and All, greys out the windows the instance has not been running long enough to fill, says where the history begins when a window reaches past the first snapshot, and labels the time axis with the date once the window is longer than a day. The engine serves any window up to a year (`/api/pnl/history?hours=`) at the same point budget as before, so a month draws as quickly as a day, and reports the oldest snapshot it holds (`/api/pnl/span`). Exit markers on the longer windows come from a larger trade list, and the chart says when that list was cut short.
- Kalshi and Polymarket US: contracts the account holds above what a strategy bought through its own orders are now adopted. When a strategy held a leg through its own fill and the account came to hold more on that leg (an operator buy, or a fill from a quote the engine had lost track of), the extra contracts belonged to no row: not the strategy's, which is sized from its own fills and is never moved by a wallet reading, and not an adopted one, because the adoption sweep only ran on legs no row claimed at all. Collateral was the only sign. The extra contracts are now adopted as a ChainAdopted position under the strategy's squadron once the difference has stood for a minute and no order is still working on the leg, at the cost the account's average implies for them, and from then on they follow the account like any other adopted position. The engine also now holds every account-side reconciliation on a leg off while any of its own orders is still working there, whichever squadron placed it, and while a squadron's in-memory position and its ledger row disagree about what it holds, which is logged after a minute as a fill the ledger never recorded.
- Kalshi and Polymarket US: a sell the venue filled in part is booked at what sold. A fill-and-kill exit that filled, say, 4 of 10 contracts used to book the whole position as sold and drop it, leaving 6 contracts in the account with no row: they came back as an adopted holding and were booked a second time when they left. The round trip is now booked for the contracts that sold, with that share of the entry fee, and the remainder keeps its position and row, with the strategy selling it on its next pass.
- Marketplace image: the operating system is brought up to date with Ubuntu's security fixes when the image is built, the Control Tower runs on Node.js 22 (Node.js 20 reached end of life in April 2026 and its bundled Brotli decoder carries CVE-2026-111497 with no fix on that line), and the nginx proxy image is rebuilt on Alpine's current packages. The first 1.5.6 image was refused by the Marketplace security scan for that CVE; this one is what the scan approved. The engine's TLS library (rustls 0.23.46) and a Control Tower build-time CSS tool (postcss-selector-parser 6.1.5) are also updated for their published advisories.
- Control Tower: the footer, the page description and the Close Now and Flatten warnings no longer name Polymarket on every venue. The footer reads "DRADIS Trading Automation Control Tower" on every build, with the latency pill beside it naming the venue this engine trades; the fee warnings say venue taker fees apply, since Kalshi's fee schedule is not Polymarket's two percent.

## 1.5.5 (2026-10-29)

- Kalshi and Polymarket US: a resting quote is now tracked from the moment the venue accepts it. A good-till-canceled bid that rests on the book (a Maker quote, a Time Decay theta bid, a FairValue or Basis maker entry) was acknowledged by the venue with nothing filled, and the engine treated that as nothing having happened: the order stayed on the book with nothing watching it, survived the squadron rotating to another market, and when it filled later the contracts appeared as a "ChainAdopted" holding under whichever squadron noticed first. Meanwhile the Maker, seeing its own bid as the new best bid, quoted one tick higher every tick until the account ran out of collateral (five bids in two seconds on 2026-10-29). Now the quote is carried as a pending position at the size and price placed, exactly as it always was on Polymarket International: the venue's fill feed confirms it by order id and sizes it to what actually traded; rotation, stand-down and the Maker's own quote-pull cancel it and clear the pending row; a quote nothing pulls is canceled after thirty minutes; a second quote is not placed while one already rests on the leg; and a quote is only ever confirmed by its own order (the venue is asked about the order itself when it leaves the open-orders list), never by contracts another strategy or squadron holds on the same leg. An exit signalled on a quote that never filled cancels the order instead of trying to sell contracts that are not held. The Trade Log shows a resting quote as a pending position until it fills or is pulled.
- Kalshi and Polymarket US: a position the engine adopted from the account now follows the account. The sweep that adopts a holding no strategy claims sized it once, at adoption, and never again, so a holding that grew afterwards (more untracked bids filling, as on 2026-10-29, when the engine showed 56.16 contracts while the account held 116.56) or shrank (an operator sale) stayed at its first size with collateral the only sign of the difference. The adopted row now follows the account: once a change has stood for a minute it is resized to what the account holds beyond the strategies' own confirmed fills, at the remainder's own cost, and when adopted contracts leave the account (an operator sale, or Kalshi netting an opposite-side fill against them) the contracts that left are booked at the row's last mark, with the row closed once none remain. When the strategies' own rows claim more than the account holds, nothing is changed and the engine says so once. Contracts a strategy filled through its own order are never moved by a wallet reading, and a quote still resting on the market stops the resize until it fills or is pulled. Two squadrons sharing one account could also both adopt the same holding when they noticed it in the same pass, leaving two rows for one position and booking its exit twice; adoption now happens once across every squadron, and duplicate rows an earlier release wrote are collapsed to the oldest.

## 1.5.4 (2026-10-27)

- Trade log: Close Now now works on Kalshi and Polymarket US. The button was shown on every venue, but the engine behind it existed only in the Polymarket International build, so pressing it on the other two answered "Close Now failed:" with nothing after the colon and left the position open. The exit now goes through the same venue layer every strategy trades through: it reads the live bid, sells one tick under it as a fill-or-kill order (the position either closes in full or stays exactly as it was), books the fill the venue reports net of both legs' fees, and hands the closed position to the trading loop so no strategy keeps managing shares that were sold. A simulated (ghost) position is booked at the bid without touching the venue. While the operator's order is in flight the engine's own stop on that position stands aside, so the same contracts cannot be sold twice. When the engine refuses, the message carries the HTTP status and the engine's reason.
- Polymarket International and Polymarket US: an immediate (fill-and-kill or fill-or-kill) order the venue accepted but did not fill is now reported as unfilled. It used to be reported as filled at the requested size, which could book a close that never happened.
- Anchor: a position closed by hand now closes Anchor's own ledger row at the sale price, so the settlement sweep no longer books a second, simulated settlement for it.
- Ghost mode: a simulated position held through an hourly or daily market's close is now settled and scored. Only event markets (sports, politics) had a simulated settlement path; a hold-to-settlement strategy on a crypto hourly (GBoost's settlement posture, FairValue's settle-hold) was dropped at rotation on Polymarket International and left open forever on Kalshi and Polymarket US, so simulated P&L understated exactly the strategies ghost mode exists to evaluate. The engine now asks the venue for the result once the market has closed and books the hold as a simulated settlement at the final price, with the fee it was charged at entry.
- Naked-leg repair: a flatten order the venue killed without filling is no longer booked as a sale. The leg stays held and the next reconcile tries again.
- Support bundle: the file now carries each squadron's own configuration beside the instance-wide one, every on/off switch first and then whatever else the squadron overrides. A squadron's loop reads its own row, so a bundle that printed only the instance-wide row could show every strategy enabled while the Viper status said "disabled in config"; a customer's bundle did exactly that.
- GBoost card, from a customer's instance: an HTTP 451 from Binance's futures host (it refuses US addresses by policy) is shown as a region note rather than a standing "last error"; the affected feature columns were already zeroed by design, and the card now says so. The refusal ledger no longer prints `last cycle # #:# ET` when no model is in service: the pipeline's status stays on the detail line and the refusal key is stable. A training cycle that found too little data is retried within the hour instead of after the full retrain interval, so an operator who changes the plan is not left without a model for a day. A posture mismatch names the knob that set the posture (`gboost_planb_exit_posture`).

## 1.5.3 (2026-10-24)

- Backup and restore: a backup whose database was large enough for the archiver to store it as a "sparse" tar entry could not be restored; the restore refused it as "not a regular file" with every byte intact. A Marketplace customer hit this moving a 1.16 GB ledger from 1.5.1 to 1.5.2. Backups now always write plain entries, and the restore accepts the sparse ones already in customers' hands, so archives made by an earlier version restore (the archiver recorded every byte of the customer's; all 305 checksums matched).
- Database size: the engine wrote a 12 KB "session start" configuration snapshot every five seconds for as long as a sports squadron had no game to deploy on, which is the normal state of an instance without a working Odds API key. The same customer's eight-day-old ledger was 1.1 GB with 67 trades. The snapshot is now written once per start, and every start thins the history to one snapshot per session, so backups shrink to a few megabytes.
- Sports: a failing bookmaker feed (The Odds API) is now reported, on every venue and for both the free event list and the paid odds call, so a canceled key (HTTP 401) and an exhausted quota (HTTP 429 on the odds call only) both show. The engine warns in the log on the first failure and every ten minutes after, with the HTTP status the API returned, and the Control Tower shows an amber banner once the feed has failed twice in a row. Until now a canceled or exhausted key left the sports slot silently idle.
- Trade log: an open position whose market has no bid now says "no bid" with the current ask instead of a dash in the price and P&L cells. A dash read as a stuck trade; the position simply cannot be sold until it settles.
- Status: on Kalshi, a finished pinned market (a game that ended) no longer stays listed under its vipers on `/api/status` after its squadron is reaped. Polymarket US still lists it until the next restart.
- Control Tower: Next.js 15.5.28 (GHSA-2464-fhr4-w9xw, GHSA-p3m5-59mp-jv5r; neither was exploitable on the shipped image, which runs the fixed `sharp` on Alpine and has no Server Actions), and the image optimizer route now sits behind the login like every other route.

**Detail**

*The sparse tar entry.* The backup writer uses the `tar` crate's path appender, which detects sparse files by default and writes them as GNU sparse entries. The database copy a backup takes (`VACUUM INTO`) had just been written, and under ext4's delayed allocation it reported fewer allocated blocks than its size, so the 1.16 GB file went in as typeflag `S`. The restore's extractor accepted regular entries only. The archive was otherwise perfect: gzip intact, all 305 checksums matching, the database passing SQLite's integrity check. The fix is on both sides: the writer asks for plain entries, and the reader accepts sparse entries, which the crate expands on read and the manifest checksum then verifies. The same release stops the last GBoost training candidate travelling in backups under the venue-nested layout, where the exclusion had missed it since 1.4.0.

*The five-second snapshots.* `DynamicConfig::load_or_default` recorded a full-config "session start" row on every call. The auto-deploy seeder calls it on every pass in which a class still needs seeding, every five seconds, and a sports slot with no games is exactly that for as long as the feed is dead. The snapshot moved to a startup-only entry point; the seeder and the Polymarket US wing read the live config broadcast instead; and a prune at every start keeps the newest snapshot per session, which on a thin history removes nothing. The freed pages stay in the database file until a `VACUUM`; backups compact on their own.

## 1.5.2 (2026-10-23)

- Trade log: a settlement is no longer skipped as "already recorded" because an earlier market with the same question text, side and share count had one. Kalshi hourly markets with the same strike share their question text across hours, so a win could be dropped against the previous hour's loss on the same text; the account balance was right and the trade log was not. Settlement rows now name the contract they closed and the check keys on it. The skipped booking, when it is a genuine duplicate, is logged where the operator can find it.
- Trade log: settlements on Kalshi and Polymarket US are now net of the entry fee, with the fee in the Fees column. The fee was kept in memory but never written to the open position's row, so every settlement booked on these venues was gross with a zero fee, about seven cents a trade at five Kalshi contracts, in the direction of flattering the record.
- Kalshi: the three squadron loops no longer trip the venue's one-per-second limit on the balance endpoint every sync, and a balance read that fails leaves a squadron's available collateral at its last reading instead of zero. Until now a failed read told every collateral-gated viper on that squadron that the account was empty until the next sync.
- Anchor viper: two new gates from the first twelve live trades, both on the Anchor card. Min Strike Distance (default 0.10 sigma) keeps the viper out of entries where the anchored level sits on the strike, where the model has no opinion and any discount is the market's directional view; the first three such entries all lost. Max Anchor Offset (default one sigma of the anchor's ten-minute sample window, about $80 on a 23% tape) refuses an entry when the market's implied level sits that far from the price feed, which means the feed moved inside the sample window and the anchor is the lag rather than the level; one such entry faded a $211 rally and lost. Every Anchor ledger row now records the spread of the anchor's samples and the feed's range over the window, so these rules can be judged on the record.

### Detail

**A settlement dropped by a name-keyed check.** When a held contract settles, the venue sweep books a settlement row unless one already exists for the same market, side and share count. The market is the question text, and the trades table carried no contract id, so the check could not tell two markets with the same text apart. On 2026-10-22 an Anchor position on the 7 PM ET Bitcoin hour lost and the next position, same strike text, same side, same five contracts, won the 8 PM hour; the win was skipped against the loss, the position row was released, the balance moved, and the trade log showed the pair as a net loss. Rows written by the sweep and by the simulated-settlement paths now carry the contract id, and a row that names a different contract never covers another position, whatever its price, size or result. Rows written before this version, and by writers that do not know the contract, are matched the old way, bounded to rows written after the position opened, which is the only time a re-booking of that position could have happened. The column is added on upgrade and older rows keep working. One narrow case remains across the upgrade itself: a settlement booked by the previous version after the next hour's position had already opened, on the same text, side and size, still covers that position; from then on every row names its contract and the case cannot recur. Simulated settlements no longer count as a booking of a real position on the same contract. Nothing is added back for a settlement that was already dropped; the operator can insert the row by hand from the Anchor ledger.

**Anchor's first twelve live trades.** Seven wins and five losses on Kalshi hourly Bitcoin markets, five contracts each. Two losses were the strategy's own kind of bet, a cheap tail side at 0.20 that did not pay, against three of the same bet that did. Three losses were entered in the first seconds of the window with the anchored level within 0.07 sigma of the strike. At the strike the gamma model prices both sides at 0.50 whatever the anchor reads, so the whole discount it saw was the market skewing one side, which is the crowd's view and not the model's; the control lane at the same distance went two and three, coin flips. One of those three was also an anchor failure: Bitcoin ran $211 in the ten minutes the anchor samples, the market lagged, every sample said the market priced the index far below the feed, the median came out at $124 where every other row in the ledger sits inside plus or minus $25, and the model bought NO at 0.37 against a 0.53 fair straight into a rally that added another $400. The research note behind the viper had already flagged that a rolling anchor re-absorbs a move. The two gates are the smallest rules that separate these entries from the wins: a minimum distance from the strike of 0.10 sigma, which the near-strike wins at 0.12 and 0.18 clear, and a cap on the anchor's offset at one sigma of the anchor's own sample window, scaled by that window rather than by the time to close so the bar is the same at every minute of the hour, which blocks exactly one of the 26 rows the ledger holds, the one that lost, while the largest honest offset on the record sits at 0.4. Both gates apply to the control lane too, so the two lanes stay comparable. The ledger gains the interquartile range of the anchor's samples and the feed's range over the sample window on every row, so the next version of these rules can be read off the record rather than argued.

**Balance reads on Kalshi.** Kalshi meters the balance endpoint in a bucket of its own: one call per second on the Basic and Advanced tiers, no burst. Every squadron loop reads the balance on its 30 second dashboard sync, and loops started by the same restart stay aligned to the second, so with three squadrons two of the three reads came back rate-limited on every cycle, over a thousand times in one day. Loops syncing within a few seconds of each other now share one reading; anything that moves money still forces a fresh one. The failure path mattered more than the warning: a failed read set that squadron's available collateral to zero until the next successful sync, so Arbitrage, FairValue, Basis, GBoost and Helm on two of the three squadrons believed the account was empty for most of every minute. A failed read now keeps the last reading and says so, and leaves the session P&L figure, which the drawdown guard reads, at its last measured value instead of recomputing it from a total that was never read. The same rule is applied on Polymarket US.

**Fees on Kalshi and Polymarket US settlements.** The fill path keeps the venue's fee on the in-memory position so a strategy exit can net it, but the row in the open positions table was written before the fee was known and never updated afterwards on these two venues. The settlement sweep reads the fee from that row and, finding none, books the gross result with a zero fee, by design, so as not to invent a cost. The fill path now writes the fee to its own row as soon as the venue reports it. Settlements booked before this version stay gross; the Fees column says 0 for them.

## 1.5.1 (2026-10-22)

- Trade log: simulated rows (instance ghost mode, the GBoost and Anchor shadow lanes) no longer count toward the Realized, Unrealized and Net figures under the table, which now agree with the lifetime cards above it; simulated results get their own labeled line.
- Trade log: GBoost's simulated settlements are filed under their venue the moment they are written instead of waiting for the next restart to fill the column, and their asset badge matches every other viper's.
- Trade log: open positions on Kalshi and Polymarket US now show a live bid and unrealized P&L. The live-quote endpoint behind the in-flight rows was built for Polymarket International only, so on the other venues every open position sat with no mark until it settled.
- Session P&L on Kalshi and Polymarket US: the session now has one starting point, the account at engine start, cash plus open positions at cost. Every squadron measures from it, so the header figure, the P&L chart and the LLM Advisor's briefing stop restarting from zero at each market rotation and stop disagreeing with each other. The session drawdown guard reads the same baseline, so on these venues it now covers the whole run, as it always has on Polymarket International, instead of clearing at the next rotation.
- LLM Advisor: each trade and open-position row in the briefing names its market class and whether it was money, the briefing says which class the squadron under review flies, session totals count real rows only with simulated rows on their own line, and the enabled-flags summary names every viper, with `n/a` for the ones this squadron's class does not fly. Knobs of vipers with an `_enabled` switch (Anchor, Helm) are no longer offered on squadrons that cannot fly them.

### Detail

**One session baseline.** On Kalshi and Polymarket US each squadron runs its own loop, and that loop is re-entered for every market it rotates onto, so each squadron read a fresh starting balance every market, usually every hour. Its session P&L restarted from zero at every rotation, and the squadrons disagreed with each other whenever one had rotated since the last settlement: on a live Kalshi instance two were writing +$1.45 and one 0.00 into the same snapshot table. The dashboard header, the chart and the advisor all read the newest row, so the figure alternated between the two every sync, and the advisor reasoned about a session that had made nothing. The reading was also cash alone while the figure it is compared against is cash plus positions at cost, so a squadron that rotated with an Anchor hold open started that hour in profit by the hold's cost. The first reading of the process, cash plus open positions at cost, is now the baseline for every squadron for the life of the run, which is what Polymarket International's per-asset session has always done. Two consequences are worth knowing: the session drawdown guard, the larger of 1% and $4 of the baseline, now accumulates across the whole run on these venues and stays tripped until a restart, where it used to clear hourly; and a deposit or withdrawal during the run moves the session figure, since the baseline cannot tell it from trading.

**What the advisor is told.** The advisor briefs one squadron at a time with the whole instance's trades, because risk is shared at the account, but nothing in the briefing said so. On 2026-10-22 the politics and sports passes read Anchor's crypto settlements as their own history, found `anchor_enabled=false` in their own config, and reported the viper disabled mid-session. The briefing now states that the trade and open-position lists span the instance and names the class the squadron flies; each row carries its class and a live or simulated mark; the session totals and per-strategy lines count real rows only, with simulated rows totaled separately the way the trade log footer now does; the enabled-flags line names every viper rather than the first seven and shows `n/a` for a viper the squadron's class does not fly; and a viper whose switch is spelled `anchor_enabled` rather than `enable_anchor` is withheld from squadrons outside its class the way the others always were.

**Marks on Kalshi and Polymarket US.** The Trade Log marks an in-flight row from `/api/positions/quotes`, which asked the Polymarket International client directly and was compiled for that venue alone; on the other builds the route did not exist, the dashboard's poll got a 405, and nothing else refreshes a stored mark there (the 300 s chain-sync sweep is also Polymarket International's). The first real Anchor fill on a Kalshi instance showed the gap: five NO contracts in flight with no unrealized P&L for the whole hour. The endpoint now reads both ends of the book through the venue trait, the way Helm's validator does, so Kalshi derives the NO leg from its YES book and Polymarket US answers from its own quote. A zero from an empty side is still reported as missing rather than as a price.

**Simulated money in the footer.** The lifetime cards at the top of the Trade Log have excluded simulated rows since the GBoost shadow lane first booked them on a live instance, but the per-filter footer under the table still summed every loaded row. On a live Kalshi instance with 44 simulated settlements and 6 real trades the footer read a realized loss eleven times the real one. The footer now splits the same way the cards do: Realized, Unrealized and Net are real rows only, and a Simulated line appears beneath them whenever the filter includes ghost rows, with the row count and the words "not money".

**Venue on GBoost shadow rows.** Every trade writer files a row under the venue bound to its shard. GBoost's shadow lane names its shard by asset ("btc"), which on Kalshi is not a shard (the Kalshi database is one shard, keyed "kalshi"), so the lookup missed and the row's venue stayed NULL until the next restart backfilled it. The settlement writer already fell back to the venue bound to the database file itself; the trade and entry writers now do the same. The same lane wrote its underlying in upper case, which the Control Tower's badge lookup did not match, so GBoost rows wore a generic badge beside every other viper's bitcoin one. It writes lower case now and the upgrade folds existing rows.

## 1.5.0 (2026-10-21)

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

## 1.4.3 (not listed; included in 1.5.0)

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
