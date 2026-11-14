# Build a Viper: your own strategies as scripts

Build a Viper lets you write a trading strategy, save it, and have it run from
the next tick. There is nothing to compile. Each strategy is a short script in
[Rhai](https://rhai.rs), a small embedded language with Rust-like syntax. The
engine runs it every tick against the squadron's market, alongside the
built-in Vipers.

A Marketplace instance ships compiled binaries and cannot build the engine,
so scripts are the way to add your own Viper there. If you build DRADIS from
source and want a compiled Rust Viper instead, see
[CUSTOM_STRATEGY.md](CUSTOM_STRATEGY.md).

**Status: new.** The engine side is covered by unit tests and by end-to-end
tests that run real ticks through the paper ledger and the order path. Until
you have run a script yourself on paper, treat it as untested on your venue.

---

## The safety model, first

A script can only ask. The engine decides what actually happens.

Two words, two things. **Ghost** is the instance setting: every viper's orders
go through the order path with simulated fills, and the Trade Log badges them
👻 ghost. **Paper** is one script viper you have not yet trusted with orders:
its own ledger, on a live or a ghost instance alike, badged 📝 paper. On a
ghost instance, turning a script's Live switch on moves it from paper to ghost.

- **Paper until you say otherwise.** A new viper trades in its own *paper
  ledger*. Its fills are recorded there and never reach the venue or the
  position book, whether your instance is live or in ghost mode. The Trade Log
  shows them with a 📝 paper badge, kept out of every total. It reaches the
  order path only when you turn on its **Live** switch.
- **Real money needs two switches.** With Live on, its orders are real only
  while the instance itself is live. On an instance in ghost mode they are the
  instance's ordinary ghost orders.

- **The cap is the engine's.** Every viper declares a maximum exposure. The
  engine counts what the viper holds across every squadron, fees included,
  and refuses any order that would go over. It refuses; it never resizes.
  Your own cap on the Settings tab can lower that limit, never raise it.
- **The daily drawdown limit** stops a viper's entries while it is on Live,
  like every other viper's. Paper entries are exempt.
- **Order sanity.** Prices must be strictly between 0 and 1, on the one-cent
  tick. A buy's limit is never more than 2¢ over the ask, and a sale's never
  more than 2¢ under the bid. Size must be at least the venue's minimum (whole
  contracts on Kalshi and Polymarket US). A viper holds one position per
  market, either side.
- **Exits outlive the switches.** Turning a viper off, or taking it off Live,
  stops new entries only. Whatever it holds keeps its stop and take-profit
  until the position closes or settles.
- **Sandboxed.** A script has no file, network or clock access (`ctx.now_secs`
  and the `ctx.et_*` fields are its only clock), cannot `eval` or import, and each call is stopped after
  about half a millisecond of work. A script that errors, loops or returns
  something malformed simply sits out that tick, with the error as its
  reason on the viper card.
- **Its own size.** The Capital Auto-Scaler never resizes a scripted viper,
  and the risk profiles never change its settings.

---

## Quick start

1. Open **Build a Viper** from the main menu.
2. Under **Templates**, pick **Plain Momentum**, then change its name
   (e.g. `my_momentum`).
3. Press **Check**, then **Save (starts on paper)**.
4. Every running squadron of the viper's classes picks it up on its next
   tick, on every venue, without a restart. **Where it runs**, under the editor,
   lists those squadrons and what the viper is doing on each: joining, waiting
   for a market, running with its last hold reason, or failing. Once it has
   ticked on a squadron, the **Test run** tab can run your script against that
   squadron's latest market data without sending anything.
5. Watch **Where it runs**, the **Paper ledger** tab and the viper's card on
   the squadron page.
6. When the paper record says so, and not before, use **Settings → Live**.

---

## Anatomy of a viper

A viper is two files in the data folder, `vipers/<name>.rhai` and
`vipers/<name>.json`. On an AMI they live in `/opt/dradis/data/vipers/`. The
page writes both, and they travel in the instance backup.

### The manifest

```json
{
  "name": "my_momentum",
  "display_name": "My Momentum",
  "description": "Velocity threshold, taker in and out.",
  "classes": ["crypto"],
  "venues": [],
  "max_exposure_usdc": 5,
  "params": {
    "velocity":  { "type": "decimal", "default": 0.001, "min": 0.0001, "max": 0.05,
                   "description": "Five-second oracle move that triggers an entry, as a fraction of the price (0.001 = $100 at $100,000)." },
    "contracts": { "type": "int", "default": 5, "min": 1, "max": 100,
                   "description": "Contracts per entry." }
  }
}
```

- `name`: 3 to 32 characters of `a-z`, `0-9` and `_`, starting with a letter.
  It is fixed once saved, and the viper reports as `Script:<name>`.
  `templates` and `validate` are reserved.
- `market`: which of the squadron's markets the script trades: `hourly` (the
  default, the squadron's primary), `window` (its window/daily market) or
  `ultra_short` (its 15-minute market, where the venue lists one and the
  squadron flies it). `ctx` describes that market: its book, strike, close
  time, `window_secs` and age. On a squadron without it the script holds with
  the reason.
- `classes`: the market classes whose squadrons attach this viper: `crypto`,
  `sports`, `politics`, `unknown`. A Helm squadron never attaches one.
- `venues`: `kalshi`, `intl_clob`, `us_retail`. Empty means every venue.
- `max_exposure_usdc`: required, above 0 and at most 1000.
- `params`: up to 32 parameters, each `decimal`, `int` or `bool`, with an
  optional range. Names follow the same rule as the viper's (3 to 32
  characters of `a-z`, `0-9` and `_`, starting with a letter), and may not be
  a word the language reserves, such as `default`, `fn` or `go`; the page
  tells you if one is. You change their values on the Settings tab, without
  editing the script. A value that does not fit its type or range is ignored
  and the default applies. Check and Save refuse a script that reads
  `ctx.params.<name>` for a name not in this list, so add a parameter before
  (or with) the code that reads it. A comment or a string that mentions one
  does not count, and neither do map methods such as `ctx.params.len()`.

### The script

A script must define both functions:

```rust
// Called every tick while the squadron has a market.
fn entry(ctx) {
    if ctx.secs_to_close == () || ctx.secs_to_close < 300 {
        return #{ hold: "too close to the close" };
    }
    let edge = 0.50 - ctx.yes.ask - taker_fee(ctx.yes.ask);
    if edge < ctx.params.min_edge {
        return #{ hold: `edge ${edge} below ${ctx.params.min_edge}` };
    }
    #{ buy: "yes", price: ctx.yes.ask, shares: ctx.params.contracts,
       take_profit: ctx.yes.ask + 0.10, reason: `edge ${edge}` }
}

// Called every tick for each position this viper holds.
fn exit(ctx, pos) {
    let book = if pos.side == "yes" { ctx.yes } else { ctx.no };
    if book.bid > 0 && book.bid <= pos.avg_entry - 0.15 {
        return #{ sell: book.bid, reason: "stop" };
    }
    ()
}
```

**What `entry(ctx)` may return**

| Return | Meaning |
|---|---|
| `()` or `#{ hold: "why" }` | Sit out this tick. The reason shows on the viper card. |
| `#{ buy: "yes" \| "no", price, shares, take_profit?, reason? }` | A taker buy. Paper fills at the ask, up to the size shown there. |

**What `exit(ctx, pos)` may return**

| Return | Meaning |
|---|---|
| `()` | Hold. |
| `#{ sell: price, reason? }` | A taker sale at or under the bid (never more than 2¢ under). |
| `#{ take_profit: price }` | Set or move the take-profit. |

**Take-profit.** Kalshi and Polymarket US have no resting exit order, so on
every venue (and on paper) the engine watches the bid and sells as a taker
once it reaches your target. The target stays armed until the position is
gone, so a sale that does not fill is retried on the next tick. It survives
a restart.

**Memory.** `this` is the viper's own map, kept between ticks, separately on
each squadron. It survives a save and a squadron rebuild. **Settings → Reset
memory** clears it.

```rust
if this.last_entry != () && ctx.now_secs - this.last_entry < 600 {
    return #{ hold: "cooldown" };
}
this.last_entry = ctx.now_secs;
```

**Trading hours.** To fly only certain hours, check the Eastern clock in
`entry` and leave `exit` alone, so a position opened inside the window is
still managed after it closes. With `start_hour` and `end_hour` as
parameters, the operator can change the window from the viper card:

```rust
let h = ctx.et_hour;
let inside = if ctx.params.start_hour <= ctx.params.end_hour {
    h >= ctx.params.start_hour && h < ctx.params.end_hour
} else {                                  // overnight, e.g. 22 to 6
    h >= ctx.params.start_hour || h < ctx.params.end_hour
};
if !inside || ctx.et_weekday > 5 { return #{ hold: "outside trading hours" }; }
```

### Numbers

Every number with a decimal point is an exact decimal, the same type the
engine trades in, so `0.42 + 0.10` is exactly `0.52`. Whole numbers are
integers. The two mix freely in arithmetic and comparisons, and every helper
below accepts either. `x.sqrt()`, `x.exp()` and `x.ln()` can return up to 28
decimal places, so round a result before printing it; `x.round(dp)` rounds halves to the even digit (2.25 becomes 2.2).

### `ctx`: what a script can read

Every price is a decimal, and every time is a whole number of seconds. A
value that does not exist for this market, or a signal whose feed is not
running on this squadron, is `()`, not 0.

| Field | Type |
|---|---|
| `market_name`, `venue` | strings |
| `market_class` | `"crypto"`, `"sports"`, `"politics"`, `"unknown"`, or `()` |
| `strike` | decimal, or `()` |
| `secs_to_close`, `window_secs` | integers, or `()` |
| `market_age_secs`, `book_age_secs` | integers |
| `now_secs` | integer: the tick's clock (Unix seconds) |
| `et_hour`, `et_minute`, `et_weekday` | integers: the same clock in US/Eastern, daylight saving applied. Hour 0-23, minute 0-59, weekday 1 (Monday) to 7 (Sunday) |
| `yes`, `no` | each `#{ bid, ask, bid_depth, ask_depth, bid_depth_total, ask_depth_total, obi }` |
| `has_price_feed` | bool: true while the squadron's price feed is attached **and** has a price. False on a squadron with no feed, and for the first moments after a start, before the feed's first print; every signal below is then `()`, and on a squadron with a feed the engine refuses new entries until it prints (exits keep running). A comparison with `()` is false, not an error, so test `ctx.oracle == ()` before relying on an `else` branch. During a mid-session outage the feed keeps its last price |
| `oracle` | the price feed's level, in dollars, or `()` (see `has_price_feed`) |
| `velocity`, `velocity_1s` | the oracle's move over the last five seconds and the last second, **in dollars** (not a fraction: divide by `ctx.oracle` for one) |
| `acceleration` | the change in `velocity` since the previous tick, in dollars |
| `drift_10m`, `drift_60m` | the oracle's move over the last 10 and 60 minutes, in dollars; 0 until that much history exists |
| `hist_vol` | a 0-1 **flatness index**, not a move: the spread of tick-to-tick log returns over the last hour divided by 2%, capped at 1. About 0.003 for ordinary BTC, near 0 when the feed is frozen. Good for "is anything happening"; for an hourly move in price terms, use a parameter |
| `funding_rate`, `institutional_pulse`, `cvd_ratio`, `oi_delta_pct` | derivatives and ETF-flow signals |
| `tide_coherence`, `tradfi_velocity`, `vix_proxy` | macro signals (BTC, US market hours) |
| `sports` | `#{ yes, no }`, each `#{ consensus, num_books, dispersion, odds_age_secs, secs_to_start, league }`, or `()` |
| `collateral`, `day_pnl` | the account |
| `ghost` | true when this viper's orders are not real right now |
| `exposure`, `max_exposure` | this viper's open cost basis across every squadron, and its cap |
| `min_shares` | the venue's smallest order |
| `params` | your parameters, by name |

`ctx.sports` is filled on every venue once the sports ledger has matched the
market to a game on its board. It is `()` until then, and whenever the odds
feed is not running.

### `pos`: the position in `exit`

`side` (`"yes"`/`"no"`), `shares`, `avg_entry`, `entry_fee`, `held_secs`,
`take_profit` (or `()`), `market_name`.

### Helpers

| Function | Does |
|---|---|
| `taker_fee(price)` | The venue's taker fee per contract at that price. |
| `round_tick(price)` | Rounds to the one-cent tick. |
| `norm_cdf(z)` | The standard normal CDF (to about 7 decimal places), for option-style fair values. |
| `clamp(x, lo, hi)`, `min(a, b)`, `max(a, b)` | On any numbers. |
| `to_decimal(n)` | An integer as a decimal. |

---

## Saving, editing and deleting

- **Saving is all-or-nothing.** The manifest is checked and the script
  compiled before anything is written. If anything is wrong you get every
  problem at once, and the running version keeps running.
- **An edit applies from the next tick,** including to positions the viper
  already holds. Each paper row records the version that opened it and the
  version that closed it. A real order's log entry names the version that
  sent it.
- **A new viper may not take an existing viper's name.** Open that viper
  instead to edit it.
- **Narrowing a viper that holds a real position is refused.** A save may not
  drop a class or this venue while the viper holds one, because the viper
  would detach from the squadron and stop managing it.
- **Deleting** is refused while the viper holds a real position. Switch it
  off, so its exits keep running, and delete it once those positions have
  closed. Its open paper positions are closed unscored, and the record says
  why.

## Switches

- **Enabled** (Settings tab) sets every squadron. Each squadron's viper card
  also has its own **On here / Off here** switch. Off stops entries only.
- **Live** and **your cap** are instance-wide. A squadron cannot disagree
  with them: the engine re-applies the instance's values whenever a squadron
  loads its config.

## The paper ledger

Paper is meant to be a fair rehearsal, so its fills are a taker's. An entry
fills at the ask, no deeper than the size shown there, and pays the venue's
taker fee. A sale fills at the bid and pays the fee again. A take-profit sells
at the bid once the bid reaches it. A position still open when its market
closes is settled at the venue's own result, with no fee. If the venue never
reports a result, the position is written off unscored after 24 hours.

Real fills can be worse than paper ones: a real order can miss, part-fill or
move the book, and paper assumes none of that. Read a paper record as an
upper bound on what the same script would have made.

The Settings → Live confirmation shows the paper record behind the decision,
and warns when it is under 20 trades.

## Test run

The **Test run** tab evaluates `entry()`, and `exit()` for each paper position
on that market, against the last market data a squadron handed any script.
It uses this viper's parameters and a copy of its memory. Nothing is sent and
nothing is recorded. It can run the editor's unsaved code. A squadron records
its market data each tick that any saved viper runs there, so on a fresh
instance save a viper first (it starts on paper); it joins its squadrons on
their next tick.

## Backups

The instance backup carries `data/vipers/*.rhai` and `*.json`, and a restore
puts them back before the engine loads them. Their settings (switches, cap,
parameter values) are part of the config and travel with it.

## Sharing: export, import and the community registry

- **Export.** The Export button saves the viper as a bundle file
  (`<name>.dradis-viper.json`): manifest, source and a content digest. Use it
  to move a viper between your own instances or to send one in a support
  ticket.
- **Import a file.** "Import a viper file…" reads a bundle in the browser and
  hands it to the engine, which checks the digest, the script API level and
  the save checks. You review the readme (shown as plain text), parameters,
  full source and, when the name is taken, a diff against your viper. You then
  import it under a free name or replace yours.
- **Community.** "Browse community vipers" fetches the registry index from
  dradis.live (only when you click it; the engine never polls). Review fetches
  that version, and Import goes through the same review as a file. Registry
  versions are signed. The review shows **signed · dradis.live** and the
  verified publisher, and the engine refuses a bundle whose signature does not
  check out, however it arrived. Withdrawn versions are refused from the
  registry and flagged on the copies you installed. An installed copy shows
  "Update to x.y.z" when a newer version is published.

Every import arrives **switched off on every squadron and on paper**. A
replacement keeps the viper's on/off switches, goes back to paper and clears
its memory, and is refused while the viper holds a position. The card shows
where an imported viper came from: **Imported** while it is unchanged,
**Imported, edited** once you change it.

To publish a viper of your own, open a pull request to
[dradis-contrib](https://github.com/mbordash/dradis-contrib) (see its
CONTRIBUTING.md). After review and merge, a maintainer publishes it to the
registry.

## API

Everything the page does goes through the engine's REST API, behind the API
key like every other config write:

| Method | Path | |
|---|---|---|
| GET | `/api/vipers/scripts` | Scripts, scripts that failed to load, squadrons with test-run data |
| GET | `/api/vipers/scripts/templates` | Starter templates |
| POST | `/api/vipers/scripts/validate` | `{ manifest, source }` → problems |
| GET | `/api/vipers/scripts/{name}` | Manifest, source, version, paper summary and recent paper rows |
| PUT | `/api/vipers/scripts/{name}[?create=true]` | Save; `create` refuses an existing name |
| DELETE | `/api/vipers/scripts/{name}` | Delete (refused while holding a real position) |
| POST | `/api/vipers/scripts/{name}/dry-run` | `{ squadron, source?, manifest? }` |
| POST | `/api/vipers/scripts/{name}/reset-state` | Clear the viper's memory |
| GET | `/api/vipers/scripts/{name}/export` | The viper as a bundle file |
| POST | `/api/vipers/import/check` | `{ bundle, name? }` → what an import would do (changes nothing) |
| POST | `/api/vipers/import` | `{ bundle, name?, replace? }` → import (off, on paper) |
| GET | `/api/vipers/registry` | The community registry index, signature checked, with what is installed here |
| GET | `/api/vipers/registry/{publisher}/{name}/{version}` | One published version, signature and index checked (400 invalid, 404 unlisted, 410 withdrawn) |

Switches and parameter values are config, set with
`PATCH /api/config { "custom_vipers": { "<name>": { "enabled": …, "live": …, "max_exposure_usdc": …, "params": { … } } } }`.
The engine merges each named script's fields into its existing settings, so
a patch that names one viper leaves the others alone. Within one viper,
`params` is replaced as a whole, so send every override you want to keep.
`null` removes a script's settings.

## Troubleshooting

- **The viper never appears on a squadron.** **Where it runs** says why: no
  squadron of its classes is running, or its venues leave out this venue. A
  squadron that shows **joining** for more than a few seconds is not ticking
  (no market yet, or a fault); its squadron detail says which.
- **I narrowed its classes and it still runs on a squadron.** A squadron it was
  already running on keeps it until that squadron moves to a new market;
  **Where it runs** marks it. (While the viper holds a real position, a save
  that narrows its classes or venues is refused.)
- **"script error: …" on the card.** The script raised an error or returned
  something malformed. The message names the function and the line.
- **"Too many operations".** The script did too much work in one call. Look
  for a loop that does not end.
- **"exposure cap: …".** The order would take the viper over its cap. This
  counts what it holds on every squadron.
- **"buy price … is below the ask".** A taker order at that limit would not
  fill. Use `ctx.yes.ask` (or `ctx.no.ask`).
- **"already holds YES/NO on this market".** One position per market.
