#!/usr/bin/env python3
"""Kalshi exchange-shard balances and transfers for a DRADIS operator.

Kalshi runs separate matching engines ("exchange shards"). New crypto and
commodity events open on shard 2, sports on shard 3, everything else on shard 0.
Orders placed through the Kalshi app move collateral between shards for you;
orders placed through the API do not, so an engine whose funds all sit on shard 0
has every crypto order rejected with `insufficient_shard_balance`.

This script signs requests with the same credentials the engine uses
(`KALSHI_API_KEY_ID` plus `KALSHI_PRIVATE_KEY` inline or `KALSHI_PRIVATE_KEY_PATH`,
`KALSHI_BASE_URL` to override the host). On a DRADIS instance it reads them from
/opt/dradis/data/secrets.env by itself (`--secrets FILE` for another path), so:

    sudo python3 kalshi_shards.py balance
    sudo python3 kalshi_shards.py allocation
    sudo python3 kalshi_shards.py transfer --from-shard 0 --to-shard 2 --dollars 50      # dry run
    sudo python3 kalshi_shards.py transfer --from-shard 0 --to-shard 2 --dollars 50 --yes
    sudo python3 kalshi_shards.py set-allocation 0:40,2:40,3:20 --yes

`transfer` and `set-allocation` print the account, host and request, and stop
unless `--yes` is given. If a transfer's response is lost, rerun it with the
printed `--client-transfer-id`: Kalshi refuses a repeat of an accepted id, so the
retry cannot move the money twice. `set-allocation` follows Kalshi's documented
target-balance-allocation endpoint (percentages per exchange index).
The amount is in dollars (`--dollars 50`, `--dollars 12.50`). The balance endpoint
is limited to one call per second.
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from decimal import Decimal, ROUND_HALF_UP

PROD_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEMO_BASE_URL = "https://external-api.demo.kalshi.co/trade-api/v2"

# Where Kalshi opens new events (docs: Series Shard Assignments). Labels only.
SHARD_NAMES = {0: "default", 1: "combos", 2: "crypto and commodities", 3: "sports"}


def base_url() -> str:
    if os.environ.get("KALSHI_BASE_URL"):
        return os.environ["KALSHI_BASE_URL"].rstrip("/")
    if os.environ.get("KALSHI_DEMO", "") in ("1", "true", "yes"):  # exactly as the engine tests it
        return DEMO_BASE_URL
    return PROD_BASE_URL


def api_root_path(url: str) -> str:
    """The path prefix Kalshi signs: everything after the host."""
    without_scheme = url.split("://", 1)[1]
    return "/" + without_scheme.split("/", 1)[1] if "/" in without_scheme else ""


DEFAULT_SECRETS = "/opt/dradis/data/secrets.env"


def unescape(v: str) -> str:
    out, it = [], iter(v)
    for c in it:
        if c != "\\":
            out.append(c)
            continue
        n = next(it, "")
        out.append({"n": "\n", "r": "\r", "t": "\t", "\\": "\\"}.get(n, "\\" + n))
    return "".join(out)


def load_secrets(path: str) -> None:
    """Load the engine's secrets file into the environment, as the engine does.

    Values are `KEY=VALUE` one per line with `\\n` escapes; files written before
    escaping existed hold the PEM across several raw lines, which are glued back.
    The file is not shell syntax (the PEM line has spaces and no quotes), so it
    cannot be sourced. The file wins over the shell environment, as it does for
    the engine, so this script acts on the account the engine trades with.
    """
    with open(path, encoding="utf-8") as f:
        lines = f.read().splitlines()
    pending = None
    for raw in lines:
        if pending is not None:
            pending[1].append(raw.rstrip())
            if raw.strip().startswith("-----END"):
                os.environ[pending[0]] = "\n".join(pending[1])
                pending = None
            continue
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = (s.strip() for s in line.split("=", 1))
        if val.startswith("-----BEGIN") and "-----END" not in val:
            pending = (key, [val])
            continue
        os.environ[key] = unescape(val)
    if pending is not None:
        os.environ[pending[0]] = "\n".join(pending[1])


def load_pem() -> str:
    inline = os.environ.get("KALSHI_PRIVATE_KEY", "").strip()
    if inline:
        return inline.replace("\\n", "\n") + "\n"
    path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "").strip()
    if path:
        with open(path, encoding="utf-8") as f:
            return f.read()
    sys.exit("set KALSHI_PRIVATE_KEY (inline PEM) or KALSHI_PRIVATE_KEY_PATH")


def sign(pem: str, message: bytes) -> str:
    """RSA-PSS SHA-256, salt length = digest length, base64; Kalshi's scheme."""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        key = serialization.load_pem_private_key(pem.encode(), password=None)
        sig = key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode()
    except ImportError:
        pass
    # No `cryptography` module: the openssl CLI signs the same way. The key
    # touches disk only inside a private temp file that is removed right after.
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".pem") as kf:
        os.chmod(kf.name, 0o600)
        kf.write(pem)
        key_path = kf.name
    try:
        out = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sigopt", "rsa_padding_mode:pss",
             "-sigopt", "rsa_pss_saltlen:digest", "-sign", key_path],
            input=message, capture_output=True, check=True,
        ).stdout
    finally:
        os.unlink(key_path)
    return base64.b64encode(out).decode()


class Client:
    def __init__(self) -> None:
        self.key_id = os.environ.get("KALSHI_API_KEY_ID", "").strip()
        if not self.key_id:
            sys.exit("set KALSHI_API_KEY_ID")
        self.pem = load_pem()
        self.base = base_url()

    def request(self, method: str, path: str, body=None, query: str = ""):
        url = f"{self.base}{path}" + (f"?{query}" if query else "")
        ts = str(int(time.time() * 1000))
        # Kalshi signs timestamp + METHOD + path from the API root, no query string.
        signed = f"{ts}{method}{api_root_path(self.base)}{path}".encode()
        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sign(self.pem, signed),
            "Accept": "application/json",
            "User-Agent": "dradis-kalshi-shards/1",
        }
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"raw": raw}
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # No status: the request may or may not have reached Kalshi.
            return 0, {"raw": f"no response: {e}"}


def ok(status: int) -> bool:
    return 200 <= status < 300


def fail(status: int, payload) -> None:
    sys.exit(f"Kalshi HTTP {status}: {json.dumps(payload)}")


def show_target(c: "Client") -> None:
    """Say which account and host a write goes to before it goes."""
    print(f"account key {c.key_id[:8]}... at {c.base}")


def cmd_balance(c: Client, _args) -> None:
    status, b = c.request("GET", "/portfolio/balance")
    if not ok(status):
        fail(status, b)
    print(f"total balance        ${b.get('balance_dollars', '?')}")
    pv = b.get("portfolio_value")
    if pv is not None:
        print(f"portfolio value      ${Decimal(pv) / 100:.2f}")
    breakdown = b.get("balance_breakdown")
    if breakdown is None:
        print("per-shard breakdown  not returned (subaccount-restricted API key?)")
        return
    print("per shard:")
    for row in sorted(breakdown, key=lambda r: r.get("exchange_index", 0)):
        idx = row.get("exchange_index")
        print(f"  shard {idx:<2} ${str(row.get('balance', '?')):>12}  {SHARD_NAMES.get(idx, '')}")
    present = {r.get("exchange_index") for r in breakdown}
    for idx in (2, 3):
        if idx not in present:
            print(f"  shard {idx:<2} {'(no balance yet)':>13}  {SHARD_NAMES[idx]}")


def cmd_allocation(c: Client, _args) -> None:
    status, a = c.request("GET", "/portfolio/target_balance_allocation")
    if not ok(status):
        fail(status, a)
    rows = a.get("allocations") or []
    if not rows:
        print("automatic rebalancing: off (no target allocation set)")
    else:
        print("automatic rebalancing: on")
        for r in rows:
            print(f"  shard {r['exchange_index']:<2} {r['percent']:>3}%  {SHARD_NAMES.get(r['exchange_index'], '')}")
    print(f"resting margin reservation: {a.get('resting_margin_reservation', '?')}")


def centicents(dollars: str) -> int:
    """Kalshi's transfer amount unit is the centicent: one ten-thousandth of a dollar."""
    amt = Decimal(dollars)
    if amt <= 0:
        sys.exit("amount must be positive")
    return int((amt * 10000).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def cmd_transfer(c: Client, args) -> None:
    if args.from_shard == args.to_shard:
        sys.exit("source and destination shard are the same")
    body = {
        "source": "event_contract",
        "destination": "event_contract",
        "source_exchange_shard": args.from_shard,
        "destination_exchange_shard": args.to_shard,
        "amount": centicents(args.dollars),
        # Kalshi rejects a repeat of an accepted id on the same route, so a
        # retry with the same id cannot move the money twice.
        "client_transfer_id": args.client_transfer_id or str(uuid.uuid4()),
    }
    show_target(c)
    print(f"move ${Decimal(args.dollars):.2f} from shard {args.from_shard} ({SHARD_NAMES.get(args.from_shard, '')}) "
          f"to shard {args.to_shard} ({SHARD_NAMES.get(args.to_shard, '')})")
    print("POST /portfolio/intra_exchange_instance_transfer")
    print(json.dumps(body, indent=2))
    if not args.yes:
        print("dry run: add --yes to send it")
        return
    status, r = c.request("POST", "/portfolio/intra_exchange_instance_transfer", body)
    if status == 0:
        sys.exit(f"{r['raw']}\nThe transfer may still have gone through. Run `balance`; to retry safely, "
                 f"repeat this command with --client-transfer-id {body['client_transfer_id']}")
    if not ok(status):
        fail(status, r)
    print(f"accepted, transfer id {r.get('transfer_id', '?')} (processed asynchronously; run `balance` in a few seconds)")


def parse_allocation(spec: str):
    rows, seen = [], set()
    for part in spec.split(","):
        idx, pct = part.split(":")
        idx, pct = int(idx), int(pct)
        if idx < 0 or not 0 <= pct <= 100:
            sys.exit(f"bad entry {part!r}: shard index must be 0 or more, percent 0 to 100")
        if idx in seen:
            sys.exit(f"shard {idx} listed twice")
        seen.add(idx)
        rows.append({"exchange_index": idx, "percent": pct})
    if sum(r["percent"] for r in rows) != 100:
        sys.exit("percentages must total 100")
    return rows


def cmd_set_allocation(c: Client, args) -> None:
    body = {"allocations": parse_allocation(args.spec), "resting_margin_reservation": args.reservation}
    show_target(c)
    print("POST /portfolio/target_balance_allocation")
    print(json.dumps(body, indent=2))
    if not args.yes:
        print("dry run: add --yes to send it")
        return
    status, r = c.request("POST", "/portfolio/target_balance_allocation", body)
    if not ok(status):
        fail(status, r)
    print("target allocation set; Kalshi rebalances every 10 seconds")


def main() -> None:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--secrets", metavar="FILE",
                        help=f"engine secrets file to read credentials from (default {DEFAULT_SECRETS} when present)")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter, parents=[common])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("balance", help="total balance and the per-shard breakdown", parents=[common])
    sub.add_parser("allocation", help="show the automatic rebalancing target, if any", parents=[common])
    t = sub.add_parser("transfer", help="move funds between two shards (dry run without --yes)", parents=[common])
    t.add_argument("--client-transfer-id", metavar="UUID",
                   help="reuse the id of a transfer whose outcome is unknown, so a retry cannot move the money twice")
    t.add_argument("--from-shard", type=int, required=True)
    t.add_argument("--to-shard", type=int, required=True)
    t.add_argument("--dollars", required=True, help="amount in dollars, e.g. 50 or 12.50")
    t.add_argument("--yes", action="store_true", help="send the transfer")
    s = sub.add_parser("set-allocation", help="set automatic rebalancing, e.g. 0:40,2:40,3:20", parents=[common])
    s.add_argument("spec")
    s.add_argument("--reservation", choices=("none", "max", "sum"), default="sum",
                   help="collateral a rebalance leaves for resting orders (default sum)")
    s.add_argument("--yes", action="store_true", help="send it")
    args = p.parse_args()
    secrets = args.secrets or (DEFAULT_SECRETS if os.path.exists(DEFAULT_SECRETS) else None)
    if secrets:
        try:
            load_secrets(secrets)
        except PermissionError:
            sys.exit(f"cannot read {secrets}: run with sudo, or pass --secrets with a readable copy")
        except FileNotFoundError:
            sys.exit(f"no secrets file at {secrets}")
    c = Client()
    {"balance": cmd_balance, "allocation": cmd_allocation,
     "transfer": cmd_transfer, "set-allocation": cmd_set_allocation}[args.cmd](c, args)


if __name__ == "__main__":
    main()
