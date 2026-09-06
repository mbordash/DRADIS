# Session Parser

`session_parser.py` parses DRADIS `session.file` logs into completed trades and attaches nearest heartbeat context before entry/exit.

## Quick Run

```zsh
python tools/session_parser.py --input session.file --asset BTC --strategy GboostStrategy
```

## Save Outputs

```zsh
python tools/session_parser.py \
  --input session.live \
  --csv-out logs/analysis/trades.csv \
  --json-out logs/analysis/trades.json
```

## Notes

- Matches `ENTRY`/`EXIT`/`Position closed` per `(asset, strategy)` using FIFO pairing.
- Adds heartbeat context fields (`ask_sum`, `bid_sum`, `binance`, `obi_y`, `obi_n`) and heartbeat age in seconds.
- Works with ghost mode and real runs.


---

# FairValue Maker Counterfactual

`fairvalue_maker_counterfactual.py` replays every FairValue round trip in a DRADIS database against Polymarket's public trade tape and answers the question E34 (post-only entries) turns on: had the entry rested one tick below the ask instead of crossing it, would it have filled, when, and what did the price do next. It also reports whether a resting take-profit ask at entry x (1 + TP) would have been lifted, and the taker fee each leg actually paid.

## Quick Run

```zsh
python3 tools/fairvalue_maker_counterfactual.py --db logs/btc-dradis.db
python3 tools/fairvalue_maker_counterfactual.py --db logs/btc-dradis.db --cache-dir logs/analysis/tape-cache --json-out logs/analysis/fv-maker.json
```

## Notes

- Read-only: opens the database with `mode=ro` and calls only public Gamma and data-api endpoints (no credentials).
- The tape's `side` is the taker's side. The tool locates DRADIS's own FAK prints on the tape (same price, size and second) and anchors the counterfactual window to them.
- Queue-blind by construction: "would not have filled" is a hard no, "filled at t" is the earliest possible t.
- `--no-complement` counts only same-token crossing prints; the default also counts mint/merge matches from the other outcome.
- Defaults mirror the intl profile (`--fee-rate 0.07`, `--tp-pct 0.20`, `--sl-pct 0.15`, `--tick 0.01`); pass the squadron's live values when they differ.
