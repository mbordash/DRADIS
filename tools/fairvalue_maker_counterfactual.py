#!/usr/bin/env python3
"""Maker-fill counterfactual for FairValue entries, from Polymarket's public tape.

E34 (post-only FairValue entries) turns on one number the engine cannot record
for itself: had the entry rested one tick below the ask instead of crossing it,
WOULD it have filled, WHEN, and what did the market do next? Every taker entry
DRADIS makes is also a free experiment in that question, because Polymarket's
public trade tape shows every taker print on the market. This tool replays each
FairValue round trip against that tape and reports:

  * maker ENTRY counterfactual: crossing taker volume at (entry − 1 tick) after
    the entry, the time a queue-blind resting bid would have filled, and the
    traded-price path after that fill (TP touch, SL touch, settlement);
  * maker TP EXIT counterfactual: the time a resting ask at entry × (1 + TP)
    would have been lifted, and whether that precedes the realized exit;
  * the taker fees each leg actually paid, i.e. what each counterfactual saves.

Assumptions, stated once here and printed in the output header:

  * `side` on the data-api tape is the TAKER's side, one row per taker fill.
    Verified against DRADIS's own FAK prints (entry BUY / exit SELL rows match
    the ledger to the share).
  * A resting bid on token T at price b is crossed by a taker SELL on T at
    ≤ b, and (with --complement, the default) by a taker BUY on the other
    outcome at ≥ 1 − b: Polymarket matches a YES buy against a NO buy by
    minting the pair. Same-token-only volume is reported alongside so the
    conservative reading is always visible.
  * Queue-blind: the resting order is assumed to sit at the front of its level.
    Real fills arrive later than this, never earlier, so "would not have
    filled" here is a hard no and "would have filled at t" is an optimistic t.

Usage:
  python3 tools/fairvalue_maker_counterfactual.py --db logs/btc-dradis.db
  python3 tools/fairvalue_maker_counterfactual.py --db logs/btc-dradis.db --json-out logs/analysis/fv-maker.json

Network: Gamma (token → market), data-api (tape), CLOB (nothing). Read-only,
public endpoints, no credentials. Both refuse Python's default User-Agent.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
UA = "Mozilla/5.0 (compatible; dradis-tools/1.0)"
PAGE = 1000
MIN_ORDER_SHARES = 1.0  # mirrors config::MIN_ORDER_SHARES for "first touch"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

def http_json(url: str, retries: int = 3):
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:  # noqa: PERF203
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after {retries} attempts: {last}")


def resolve_market(token_id: str) -> Optional[dict]:
    """Gamma's `clob_token_ids` filter answers for open markets without a flag and
    for settled ones only with `closed=true`; try both."""
    for extra in ("", "&closed=true"):
        rows = http_json(f"{GAMMA}/markets?clob_token_ids={token_id}{extra}")
        if rows:
            return rows[0]
    return None


def fetch_tape(condition_id: str, oldest_needed_ts: int, cache_dir: Optional[Path]) -> List[dict]:
    """All taker prints on a market, newest first from the API, returned oldest
    first. Pages until the tape reaches back past `oldest_needed_ts`."""
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cached = cache_dir / f"{condition_id}.json"
        if cached.exists():
            return json.loads(cached.read_text())
    out: List[dict] = []
    offset = 0
    while True:
        page = http_json(f"{DATA_API}/trades?market={condition_id}&limit={PAGE}&offset={offset}")
        if not page:
            break
        out.extend(page)
        oldest = min(int(t["timestamp"]) for t in page)
        if len(page) < PAGE or oldest < oldest_needed_ts:
            break
        offset += PAGE
    out.sort(key=lambda t: int(t["timestamp"]))
    if cache_dir is not None:
        (cache_dir / f"{condition_id}.json").write_text(json.dumps(out))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Arithmetic (mirrors the engine)
# ─────────────────────────────────────────────────────────────────────────────

def taker_fee(rate: float, price: float, shares: float) -> float:
    """`venues::intl::taker_fee`: rate · p · (1 − p) · shares, takers only."""
    if price <= 0 or price >= 1 or shares <= 0:
        return 0.0
    return rate * price * (1 - price) * shares


def floor_tick(p: float, tick: float) -> float:
    return math.floor(p / tick + 1e-9) * tick


def ceil_tick(p: float, tick: float) -> float:
    return math.ceil(p / tick - 1e-9) * tick


def parse_ts(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def hms(ts: Optional[int]) -> str:
    return "—" if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S")


# ─────────────────────────────────────────────────────────────────────────────
# Ledger
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RoundTrip:
    entry_id: int
    entry_ts: int
    token_id: str
    market: str
    side: str
    entry_price: float
    shares: float
    trade_id: Optional[int] = None
    exit_ts: Optional[int] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    fees: Optional[float] = None
    reason: str = ""


def load_round_trips(db: Path, strategy: str) -> List[RoundTrip]:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    entries = con.execute(
        "SELECT id, ts, token_id, market, side, entry_price, shares FROM entries "
        "WHERE strategy = ? ORDER BY id", (strategy,)).fetchall()
    trades = con.execute(
        "SELECT id, ts, market, side, entry_price, exit_price, shares, pnl, fees, reason FROM trades "
        "WHERE strategy = ? AND COALESCE(ghost, 0) = 0 ORDER BY id", (strategy,)).fetchall()
    used: set = set()
    out: List[RoundTrip] = []
    for e in entries:
        rt = RoundTrip(e["id"], parse_ts(e["ts"]), e["token_id"], e["market"], e["side"],
                       float(e["entry_price"]), float(e["shares"]))
        # Same market + side, closed after the entry, nearest in time. Entry
        # price is matched loosely: the ledger books the venue's fill price.
        best = None
        for t in trades:
            if t["id"] in used or t["market"] != e["market"] or t["side"] != e["side"]:
                continue
            tts = parse_ts(t["ts"])
            if tts < rt.entry_ts or abs(float(t["entry_price"]) - rt.entry_price) > 0.02:
                continue
            if best is None or tts < parse_ts(best["ts"]):
                best = t
        if best is not None:
            used.add(best["id"])
            rt.trade_id = best["id"]
            rt.exit_ts = parse_ts(best["ts"])
            rt.exit_price = float(best["exit_price"])
            rt.pnl = float(best["pnl"])
            rt.fees = float(best["fees"]) if best["fees"] is not None else None
            rt.reason = best["reason"]
            # The ledger's share count is the venue's actual fill; prefer it.
            rt.shares = float(best["shares"])
        out.append(rt)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Counterfactuals
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Print:
    ts: int
    ours: bool          # on our token (True) or the complement (False)
    taker_side: str     # BUY / SELL, the taker's side
    price: float        # as traded on THAT token
    size: float

    def price_on_ours(self) -> float:
        return self.price if self.ours else 1.0 - self.price


@dataclass
class Fill:
    ts: Optional[int]
    cum_same_token: float
    cum_with_complement: float
    first_touch_ts: Optional[int]


def crossing_bid_volume(tape: List[Print], bid: float, t0: int, t1: Optional[int], shares: float,
                        complement: bool) -> Fill:
    """Taker volume that would have hit a resting BID at `bid` between t0 and t1."""
    same = both = 0.0
    fill_ts = touch_ts = None
    for p in tape:
        if p.ts < t0 or (t1 is not None and p.ts > t1):
            continue
        hit = 0.0
        if p.ours and p.taker_side == "SELL" and p.price <= bid + 1e-9:
            hit = p.size
            same += hit
        elif (not p.ours) and complement and p.taker_side == "BUY" and p.price >= (1.0 - bid) - 1e-9:
            hit = p.size
        if hit <= 0:
            continue
        both += hit
        if touch_ts is None and both >= MIN_ORDER_SHARES:
            touch_ts = p.ts
        if fill_ts is None and both >= shares:
            fill_ts = p.ts
    return Fill(fill_ts, same, both, touch_ts)


def crossing_ask_volume(tape: List[Print], ask: float, t0: int, t1: Optional[int], shares: float,
                        complement: bool) -> Fill:
    """Taker volume that would have lifted a resting ASK at `ask` between t0 and t1."""
    same = both = 0.0
    fill_ts = touch_ts = None
    for p in tape:
        if p.ts < t0 or (t1 is not None and p.ts > t1):
            continue
        hit = 0.0
        if p.ours and p.taker_side == "BUY" and p.price >= ask - 1e-9:
            hit = p.size
            same += hit
        elif (not p.ours) and complement and p.taker_side == "SELL" and p.price <= (1.0 - ask) + 1e-9:
            hit = p.size
        if hit <= 0:
            continue
        both += hit
        if touch_ts is None and both >= MIN_ORDER_SHARES:
            touch_ts = p.ts
        if fill_ts is None and both >= shares:
            fill_ts = p.ts
    return Fill(fill_ts, same, both, touch_ts)


@dataclass
class PathAfterFill:
    max_price: float
    max_ts: Optional[int]
    min_price: float
    min_ts: Optional[int]
    tp_touch_ts: Optional[int]
    sl_touch_ts: Optional[int]


def path_after(tape: List[Print], t0: int, fill_price: float, tp_pct: float, sl_pct: float) -> PathAfterFill:
    """Traded-price path on our token after t0, expressed on our token."""
    tp_level = fill_price * (1 + tp_pct)
    sl_level = fill_price * (1 - sl_pct)
    mx, mn = -1.0, 2.0
    mx_ts = mn_ts = tp_ts = sl_ts = None
    for p in tape:
        if p.ts < t0:
            continue
        px = p.price_on_ours()
        if px > mx:
            mx, mx_ts = px, p.ts
        if px < mn:
            mn, mn_ts = px, p.ts
        if tp_ts is None and px >= tp_level:
            tp_ts = p.ts
        if sl_ts is None and px <= sl_level:
            sl_ts = p.ts
    return PathAfterFill(mx, mx_ts, mn, mn_ts, tp_ts, sl_ts)


def find_own_print(tape: List[Print], side: str, price: float, shares: float, near_ts: int,
                   window: int = 45) -> Optional[Print]:
    """DRADIS's own FAK print: same taker side, same price to half a tick, same
    size to 1%, within `window` seconds of the ledger timestamp."""
    best = None
    for p in tape:
        if not p.ours or p.taker_side != side or abs(p.ts - near_ts) > window:
            continue
        if abs(p.price - price) > 0.006 or abs(p.size - shares) > max(0.02, 0.01 * shares):
            continue
        if best is None or abs(p.ts - near_ts) < abs(best.ts - near_ts):
            best = p
    return best


@dataclass
class Report:
    trade: RoundTrip
    condition_id: str
    settled_our_side: Optional[float]
    market_close_ts: Optional[int]
    own_entry_print_ts: Optional[int]
    own_exit_print_ts: Optional[int]
    t0: int
    t_exit: Optional[int]
    entry_fee_paid: float
    exit_fee_paid: float
    maker_bid: float
    bid_fill_before_exit: Fill
    bid_fill_any: Fill
    path_after_bid_fill: Optional[PathAfterFill]
    maker_ask: float
    ask_lift: Fill
    notes: List[str] = field(default_factory=list)


def analyze(rt: RoundTrip, args, cache_dir: Optional[Path]) -> Optional[Report]:
    m = resolve_market(rt.token_id)
    if m is None:
        print(f"  entry #{rt.entry_id}: token not found on Gamma — skipped", file=sys.stderr)
        return None
    cid = m["conditionId"]
    token_ids = json.loads(m.get("clobTokenIds") or "[]")
    outcome_prices = json.loads(m.get("outcomePrices") or "[]")
    our_idx = token_ids.index(rt.token_id) if rt.token_id in token_ids else None
    settled = None
    if our_idx is not None and len(outcome_prices) > our_idx and m.get("closed"):
        settled = float(outcome_prices[our_idx])
    close_ts = parse_ts(m["endDate"]) if m.get("endDate") else None

    raw = fetch_tape(cid, rt.entry_ts - 600, cache_dir)
    tape = [Print(int(t["timestamp"]), t["asset"] == rt.token_id, t["side"].upper(),
                  float(t["price"]), float(t["size"])) for t in raw]

    notes: List[str] = []
    own_entry = find_own_print(tape, "BUY", rt.entry_price, rt.shares, rt.entry_ts)
    t0 = own_entry.ts if own_entry else rt.entry_ts - 2
    if own_entry is None:
        notes.append("own entry print not found on tape; using ledger ts − 2s")

    own_exit = None
    t_exit: Optional[int] = None
    if rt.exit_ts is not None:
        is_settlement = "Settlement" in rt.reason or (rt.exit_price is not None and rt.exit_price >= 0.999)
        if is_settlement:
            t_exit = close_ts or rt.exit_ts
            notes.append("exited by settlement (no taker exit, exit fee 0)")
        else:
            own_exit = find_own_print(tape, "SELL", rt.exit_price or 0.0, rt.shares, rt.exit_ts)
            t_exit = own_exit.ts if own_exit else rt.exit_ts
            if own_exit is None:
                notes.append("own exit print not found on tape; using ledger ts")

    entry_fee = taker_fee(args.fee_rate, rt.entry_price, rt.shares)
    exit_fee = 0.0
    if rt.exit_price is not None and rt.exit_price < 0.999 and t_exit is not None and own_exit is not None:
        exit_fee = taker_fee(args.fee_rate, rt.exit_price, rt.shares)
    elif rt.exit_price is not None and rt.exit_price < 0.999 and "Settlement" not in rt.reason:
        exit_fee = taker_fee(args.fee_rate, rt.exit_price, rt.shares)

    bid = floor_tick(rt.entry_price - args.tick, args.tick)
    fill_before_exit = crossing_bid_volume(tape, bid, t0, t_exit, rt.shares, args.complement)
    fill_any = crossing_bid_volume(tape, bid, t0, close_ts, rt.shares, args.complement)
    path = path_after(tape, fill_any.ts, bid, args.tp_pct, args.sl_pct) if fill_any.ts else None

    ask = ceil_tick(rt.entry_price * (1 + args.tp_pct), args.tick)
    lift = crossing_ask_volume(tape, ask, t0, close_ts, rt.shares, args.complement)

    return Report(rt, cid, settled, close_ts, own_entry.ts if own_entry else None,
                  own_exit.ts if own_exit else None, t0, t_exit, entry_fee, exit_fee,
                  bid, fill_before_exit, fill_any, path, ask, lift, notes)


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def print_report(r: Report, args) -> None:
    t = r.trade
    print(f"\n{'─' * 96}")
    print(f"entry #{t.entry_id}  {t.market}  {t.side} @ ${t.entry_price:.4f} × {t.shares:.2f}"
          f"   (${t.entry_price * t.shares:.2f} notional)")
    exit_desc = "OPEN" if t.exit_ts is None else f"${t.exit_price:.4f}  pnl ${t.pnl:+.4f}  fees ${t.fees or 0:.4f}  [{t.reason[:60]}]"
    print(f"  ledger exit : {exit_desc}")
    settled = "n/a (open)" if r.settled_our_side is None else f"${r.settled_our_side:.4f} for our side"
    print(f"  settlement  : {settled}   market close {hms(r.market_close_ts)}Z")
    print(f"  tape anchors: entry print {hms(r.own_entry_print_ts)}Z  exit print {hms(r.own_exit_print_ts)}Z"
          f"   (window {hms(r.t0)}Z → {hms(r.t_exit)}Z)")
    print(f"  taker fees  : entry ${r.entry_fee_paid:.4f}  exit ${r.exit_fee_paid:.4f}"
          f"   ({(r.entry_fee_paid / (t.entry_price * t.shares)) * 100:.1f}% of notional on entry)")
    for n in r.notes:
        print(f"  note        : {n}")

    fb, fa = r.bid_fill_before_exit, r.bid_fill_any
    print(f"\n  MAKER ENTRY  bid ${r.maker_bid:.2f} (entry − 1 tick), post-only, queue-blind")
    print(f"    before realized exit : crossing vol {fb.cum_with_complement:.2f} sh"
          f" (same-token {fb.cum_same_token:.2f})  → "
          + (f"FILLED {hms(fb.ts)}Z ({fb.ts - r.t0}s after entry)" if fb.ts else "NOT filled"))
    print(f"    over whole market    : crossing vol {fa.cum_with_complement:.2f} sh"
          f" (same-token {fa.cum_same_token:.2f})  → "
          + (f"FILLED {hms(fa.ts)}Z ({fa.ts - r.t0}s after entry)" if fa.ts else "NEVER filled"))
    if r.path_after_bid_fill:
        p = r.path_after_bid_fill
        tp_lvl = r.maker_bid * (1 + args.tp_pct)
        sl_lvl = r.maker_bid * (1 - args.sl_pct)
        first = "TP first" if (p.tp_touch_ts and (p.sl_touch_ts is None or p.tp_touch_ts < p.sl_touch_ts)) \
            else ("SL first" if p.sl_touch_ts else "neither touched")
        print(f"    path after that fill : max ${p.max_price:.3f} @{hms(p.max_ts)}Z  min ${p.min_price:.3f} @{hms(p.min_ts)}Z"
              f"  | TP ${tp_lvl:.3f} touch {hms(p.tp_touch_ts)}Z  SL ${sl_lvl:.3f} touch {hms(p.sl_touch_ts)}Z  → {first}")
    print(f"    fee saved if filled  : ${r.entry_fee_paid:.4f}")

    lf = r.ask_lift
    print(f"\n  MAKER TP EXIT  ask ${r.maker_ask:.2f} (entry × {1 + args.tp_pct:.2f}, ceil to tick), rested from entry")
    lifted = f"LIFTED {hms(lf.ts)}Z ({lf.ts - r.t0}s after entry)" if lf.ts else "NEVER lifted"
    before = ""
    if lf.ts and r.t_exit is not None:
        before = "  — before the realized exit" if lf.ts <= r.t_exit else "  — after the realized exit"
    print(f"    crossing vol {lf.cum_with_complement:.2f} sh (same-token {lf.cum_same_token:.2f})  → {lifted}{before}")
    print(f"    fee saved if lifted  : ${r.exit_fee_paid:.4f} (the realized taker exit fee)")


def summary(reports: List[Report]) -> None:
    n = len(reports)
    if n == 0:
        return
    filled_any = sum(1 for r in reports if r.bid_fill_any.ts)
    filled_before = sum(1 for r in reports if r.bid_fill_before_exit.ts)
    lifted = sum(1 for r in reports if r.ask_lift.ts)
    entry_fees = sum(r.entry_fee_paid for r in reports)
    exit_fees = sum(r.exit_fee_paid for r in reports)
    print(f"\n{'═' * 96}")
    print(f"{n} round trip(s)   taker fees paid: entry ${entry_fees:.4f}  exit ${exit_fees:.4f}  total ${entry_fees + exit_fees:.4f}")
    print(f"maker entry (bid = entry − 1 tick): would have filled before the realized exit in {filled_before}/{n},"
          f" at some point in the market's life in {filled_any}/{n}")
    print(f"maker TP exit (ask = entry × (1+TP)): would have been lifted in {lifted}/{n}")
    adverse = [r for r in reports if r.bid_fill_any.ts and r.path_after_bid_fill
               and r.path_after_bid_fill.sl_touch_ts
               and (r.path_after_bid_fill.tp_touch_ts is None
                    or r.path_after_bid_fill.sl_touch_ts < r.path_after_bid_fill.tp_touch_ts)]
    print(f"of the maker-entry fills, {len(adverse)} touched the SL level before the TP level")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="logs/btc-dradis.db", help="DRADIS SQLite database (opened read-only)")
    ap.add_argument("--strategy", default="FairValueStrategy")
    ap.add_argument("--tick", type=float, default=0.01)
    ap.add_argument("--fee-rate", type=float, default=0.07, help="intl_taker_fee_rate (default 0.07)")
    ap.add_argument("--tp-pct", type=float, default=0.20, help="fairvalue_target_profit_pct")
    ap.add_argument("--sl-pct", type=float, default=0.15, help="fairvalue_stop_loss_pct")
    ap.add_argument("--no-complement", dest="complement", action="store_false",
                    help="count only same-token crossing prints (ignore mint/merge matches)")
    ap.add_argument("--cache-dir", type=Path, default=None, help="cache tapes as JSON per condition id")
    ap.add_argument("--json-out", type=Path, default=None)
    ap.add_argument("--entry-id", type=int, action="append", help="limit to these entries.id values")
    args = ap.parse_args()

    db = Path(args.db)
    if not db.exists():
        print(f"no such database: {db}", file=sys.stderr)
        return 2
    trips = load_round_trips(db, args.strategy)
    if args.entry_id:
        trips = [t for t in trips if t.entry_id in set(args.entry_id)]
    print(f"{args.strategy}: {len(trips)} entries in {db}")
    print(f"assumptions: tape side = taker side; complement matching {'ON' if args.complement else 'OFF'};"
          f" queue-blind; tick {args.tick}; fee rate {args.fee_rate}; TP {args.tp_pct:.0%}; SL {args.sl_pct:.0%}")

    reports: List[Report] = []
    for rt in trips:
        try:
            r = analyze(rt, args, args.cache_dir)
        except Exception as e:  # noqa: BLE001 — one bad market must not sink the run
            print(f"  entry #{rt.entry_id}: {e}", file=sys.stderr)
            continue
        if r is None:
            continue
        reports.append(r)
        print_report(r, args)
    summary(reports)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps([asdict(r) for r in reports], indent=2, default=str))
        print(f"\nwrote {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
