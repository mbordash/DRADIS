#!/usr/bin/env python3
"""Parse DRADIS session logs into trade analytics with heartbeat context.

Usage example:
  python tools/session_parser.py --input session.file --csv-out logs/analysis/trades.csv --json-out logs/analysis/trades.json
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


ENTRY_RE = re.compile(
    r"(?:GHOST_MODE ENTRY|ENTRY) \[(?P<strategy>[^\]]+)\]: "
    r"(?P<market>.*?) \| \$(?P<price>\d+\.\d+) x (?P<shares>\d+\.?\d*)"
)
EXIT_RE = re.compile(
    r"EXIT \[(?P<strategy>[^\]]+)\]: (?P<market>.*?) \| "
    r"shares=(?P<shares>\d+\.?\d*), bid=\$(?P<bid>\d+\.\d+) \| (?P<reason>.+)$"
)
PNL_RE = re.compile(r"Position closed \[(?P<strategy>[^\]]+)\]: PnL \$(?P<pnl>-?\d+\.\d+)")
HB_RE = re.compile(
    r"Heartbeat \| Ask Sum \$(?P<ask_sum>\d+\.\d+).*?"
    r"Bid Sum \$(?P<bid_sum>\d+\.\d+).*?"
    r"Binance: \$(?P<binance>\d+\.\d+) \| "
    r"OBI Y=(?P<obi_y>-?\d+\.\d+) N=(?P<obi_n>-?\d+\.\d+)"
)
TS_RE = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
ASSET_RE = re.compile(r"^\[(?P<asset>[A-Z]+)\]\s+")


@dataclass
class Heartbeat:
    ts: datetime
    ask_sum: float
    bid_sum: float
    binance: float
    obi_y: float
    obi_n: float


@dataclass
class Entry:
    ts: datetime
    asset: str
    strategy: str
    market: str
    price: float
    shares: float


@dataclass
class Exit:
    ts: datetime
    asset: str
    strategy: str
    market: str
    shares: float
    bid: float
    reason: str


@dataclass
class Trade:
    asset: str
    strategy: str
    market: str
    entry_ts: Optional[str]
    exit_ts: str
    hold_seconds: Optional[int]
    entry_price: Optional[float]
    exit_bid: float
    shares: Optional[float]
    pnl: Optional[float]
    reason: str
    entry_ask_sum: Optional[float]
    entry_bid_sum: Optional[float]
    entry_binance: Optional[float]
    entry_obi_y: Optional[float]
    entry_obi_n: Optional[float]
    entry_hb_age_sec: Optional[int]
    exit_ask_sum: Optional[float]
    exit_bid_sum: Optional[float]
    exit_binance: Optional[float]
    exit_obi_y: Optional[float]
    exit_obi_n: Optional[float]
    exit_hb_age_sec: Optional[int]


def parse_ts(line: str) -> Optional[datetime]:
    m = TS_RE.search(line)
    if not m:
        return None
    return datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")


def parse_asset(line: str) -> str:
    m = ASSET_RE.match(line)
    return m.group("asset") if m else "UNK"


def nearest_hb_before(ts: datetime, hbs: List[Heartbeat], hb_times: List[datetime]) -> Tuple[Optional[Heartbeat], Optional[int]]:
    idx = bisect.bisect_right(hb_times, ts) - 1
    if idx < 0:
        return None, None
    hb = hbs[idx]
    return hb, int((ts - hb.ts).total_seconds())


def parse_lines(lines: List[str]) -> Tuple[List[Entry], List[Exit], Dict[str, List[Heartbeat]], List[Tuple[datetime, str, str, float]]]:
    entries: List[Entry] = []
    exits: List[Exit] = []
    heartbeats: Dict[str, List[Heartbeat]] = {}
    pnls: List[Tuple[datetime, str, str, float]] = []

    for line in lines:
        ts = parse_ts(line)
        if not ts:
            continue
        asset = parse_asset(line)

        hm = HB_RE.search(line)
        if hm:
            heartbeats.setdefault(asset, []).append(
                Heartbeat(
                    ts=ts,
                    ask_sum=float(hm.group("ask_sum")),
                    bid_sum=float(hm.group("bid_sum")),
                    binance=float(hm.group("binance")),
                    obi_y=float(hm.group("obi_y")),
                    obi_n=float(hm.group("obi_n")),
                )
            )
            continue

        em = ENTRY_RE.search(line)
        if em:
            entries.append(
                Entry(
                    ts=ts,
                    asset=asset,
                    strategy=em.group("strategy"),
                    market=em.group("market"),
                    price=float(em.group("price")),
                    shares=float(em.group("shares")),
                )
            )
            continue

        xm = EXIT_RE.search(line)
        if xm:
            exits.append(
                Exit(
                    ts=ts,
                    asset=asset,
                    strategy=xm.group("strategy"),
                    market=xm.group("market"),
                    shares=float(xm.group("shares")),
                    bid=float(xm.group("bid")),
                    reason=xm.group("reason"),
                )
            )
            continue

        pm = PNL_RE.search(line)
        if pm:
            pnls.append((ts, asset, pm.group("strategy"), float(pm.group("pnl"))))

    return entries, exits, heartbeats, pnls


def correlate_trades(
    entries: List[Entry], exits: List[Exit], pnls: List[Tuple[datetime, str, str, float]], heartbeats: Dict[str, List[Heartbeat]]
) -> List[Trade]:
    entry_q: Dict[Tuple[str, str], List[Entry]] = {}
    for e in sorted(entries, key=lambda x: x.ts):
        entry_q.setdefault((e.asset, e.strategy), []).append(e)

    pending_exit: Dict[Tuple[str, str], List[Tuple[Exit, Optional[Entry]]]] = {}
    for ex in sorted(exits, key=lambda x: x.ts):
        key = (ex.asset, ex.strategy)
        en = entry_q.get(key, []).pop(0) if entry_q.get(key) else None
        pending_exit.setdefault(key, []).append((ex, en))

    completed: List[Trade] = []
    hb_cache: Dict[str, Tuple[List[Heartbeat], List[datetime]]] = {
        k: (v, [h.ts for h in v]) for k, v in heartbeats.items()
    }

    pnl_sorted = sorted(pnls, key=lambda x: x[0])
    for pnl_ts, asset, strategy, pnl_value in pnl_sorted:
        key = (asset, strategy)
        if not pending_exit.get(key):
            continue
        ex, en = pending_exit[key].pop(0)
        hb_list, hb_times = hb_cache.get(asset, ([], []))
        en_hb, en_age = (None, None)
        if en and hb_list:
            en_hb, en_age = nearest_hb_before(en.ts, hb_list, hb_times)
        ex_hb, ex_age = nearest_hb_before(ex.ts, hb_list, hb_times) if hb_list else (None, None)

        hold = int((ex.ts - en.ts).total_seconds()) if en else None
        completed.append(
            Trade(
                asset=asset,
                strategy=strategy,
                market=ex.market,
                entry_ts=en.ts.isoformat(sep=" ") if en else None,
                exit_ts=ex.ts.isoformat(sep=" "),
                hold_seconds=hold,
                entry_price=en.price if en else None,
                exit_bid=ex.bid,
                shares=en.shares if en else ex.shares,
                pnl=pnl_value,
                reason=ex.reason,
                entry_ask_sum=en_hb.ask_sum if en_hb else None,
                entry_bid_sum=en_hb.bid_sum if en_hb else None,
                entry_binance=en_hb.binance if en_hb else None,
                entry_obi_y=en_hb.obi_y if en_hb else None,
                entry_obi_n=en_hb.obi_n if en_hb else None,
                entry_hb_age_sec=en_age,
                exit_ask_sum=ex_hb.ask_sum if ex_hb else None,
                exit_bid_sum=ex_hb.bid_sum if ex_hb else None,
                exit_binance=ex_hb.binance if ex_hb else None,
                exit_obi_y=ex_hb.obi_y if ex_hb else None,
                exit_obi_n=ex_hb.obi_n if ex_hb else None,
                exit_hb_age_sec=ex_age,
            )
        )

    return completed


def write_csv(path: Path, trades: List[Trade]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(trades[0]).keys()) if trades else [])
        if trades:
            writer.writeheader()
            for t in trades:
                writer.writerow(asdict(t))


def write_json(path: Path, trades: List[Trade]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump([asdict(t) for t in trades], f, indent=2)


def print_summary(trades: List[Trade]) -> None:
    print(f"completed_trades: {len(trades)}")
    if not trades:
        return
    pnl_values = [t.pnl for t in trades if t.pnl is not None]
    wins = [p for p in pnl_values if p > 0]
    losses = [p for p in pnl_values if p < 0]
    total = sum(pnl_values)
    print(f"total_pnl: {total:.4f}")
    print(f"win_rate: {(len(wins) / len(pnl_values) * 100):.2f}%")
    print(f"avg_win: {(sum(wins) / len(wins)):.4f}" if wins else "avg_win: 0")
    print(f"avg_loss: {(sum(losses) / len(losses)):.4f}" if losses else "avg_loss: 0")

    by_reason: Dict[str, int] = {}
    for t in trades:
        by_reason[t.reason] = by_reason.get(t.reason, 0) + 1
    print("exit_reasons:")
    for reason, count in sorted(by_reason.items(), key=lambda x: x[1], reverse=True):
        print(f"  {count:>3}  {reason}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Parse DRADIS session logs with heartbeat context.")
    p.add_argument("--input", default="session.file", help="Path to session log file")
    p.add_argument("--asset", default=None, help="Filter by asset tag, e.g. BTC")
    p.add_argument("--strategy", default=None, help="Filter by strategy name, e.g. GboostStrategy")
    p.add_argument("--csv-out", default=None, help="Optional output CSV path")
    p.add_argument("--json-out", default=None, help="Optional output JSON path")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    log_path = Path(args.input)
    if not log_path.exists():
        raise SystemExit(f"Input file not found: {log_path}")

    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    entries, exits, heartbeats, pnls = parse_lines(lines)
    trades = correlate_trades(entries, exits, pnls, heartbeats)

    if args.asset:
        trades = [t for t in trades if t.asset.upper() == args.asset.upper()]
    if args.strategy:
        trades = [t for t in trades if t.strategy == args.strategy]

    print_summary(trades)

    if trades and args.csv_out:
        write_csv(Path(args.csv_out), trades)
        print(f"wrote_csv: {args.csv_out}")
    if args.json_out:
        write_json(Path(args.json_out), trades)
        print(f"wrote_json: {args.json_out}")

    # Print a compact trade timeline with heartbeat context age for quick triage.
    for idx, t in enumerate(trades, 1):
        print(
            f"{idx:02d} {t.asset} {t.strategy} | {t.entry_ts or 'NA'} -> {t.exit_ts} "
            f"| pnl={t.pnl:+.4f} | hold={t.hold_seconds}s | reason={t.reason} "
            f"| hb_age(entry/exit)={t.entry_hb_age_sec}/{t.exit_hb_age_sec}s"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

