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

