#!/usr/bin/env python3
"""Generate src/profiles.json from the three config.*.rs.example files.

A "profile" is a complete DynamicConfig merge-patch body: every runtime-tunable
field with the value that profile's example config would compile in. The engine
embeds the output via include_str! and applies a profile through the same
validated DynamicConfig::apply_patch path used by PATCH /api/config.

Usage: python3 tools/generate-profiles.py   (run from repo root)
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DYN = ROOT / "src/helpers/dynamic_config.rs"
OUT = ROOT / "src/profiles.json"

PROFILES = {
    "conservative": ROOT / "src/config.conservative.rs.example",
    "balanced":     ROOT / "src/config.balanced.rs.example",
    "aggressive":   ROOT / "src/config.aggressive.rs.example",
}

DESCRIPTIONS = {
    "conservative": "The recommended starting point: capital preservation first — smaller position sizes, tighter stops, fewer concurrent strategies. Build confidence here, then move up the risk ramp.",
    "balanced":     "The next step up: moderate sizing and risk limits across all core strategies.",
    "aggressive":   "Maximum opportunity capture: larger sizes, wider stops, all strategies enabled. Expect higher drawdowns.",
}


def field_const_map() -> dict[str, str]:
    """Parse the Default impl: field_name: config::CONST_NAME."""
    src = DYN.read_text()
    m = re.search(r"impl Default for DynamicConfig \{.*?\n\}", src, re.S)
    if not m:
        sys.exit("Default impl not found in dynamic_config.rs")
    pairs = re.findall(r"^\s+([a-z0-9_]+):\s+config::([A-Z0-9_]+)", m.group(0), re.M)
    return dict(pairs)


def const_values(path: Path) -> dict[str, object]:
    """Parse `pub const NAME: TYPE = VALUE;` lines into JSON-ready values."""
    out: dict[str, object] = {}
    text = path.read_text()
    for name, ty, raw in re.findall(
        r"pub const ([A-Z0-9_]+):\s*([A-Za-z0-9_&' <>]+?)\s*=\s*(.+?);", text
    ):
        ty = ty.strip()
        raw = raw.strip()
        if ty == "Decimal":
            dm = re.match(r"dec!\(\s*(-?[0-9._]+)\s*\)", raw)
            if dm:
                # Decimal serializes as a JSON string in DynamicConfig.
                out[name] = dm.group(1).replace("_", "")
        elif ty == "bool":
            out[name] = raw == "true"
        elif ty in ("i64", "u64", "u32", "i32", "usize", "u16", "u8"):
            im = re.match(r"(-?[0-9_]+)", raw)
            if im:
                out[name] = int(im.group(1).replace("_", ""))
        elif ty == "f64":
            fm = re.match(r"(-?[0-9._]+)", raw)
            if fm:
                out[name] = float(fm.group(1).replace("_", ""))
        elif ty.startswith("&") and "str" in ty:
            sm = re.match(r'"(.*)"', raw)
            if sm:
                out[name] = sm.group(1)
        # Other types (arrays, tuples, complex exprs) are not DynamicConfig
        # material; skip silently.
    return out


# Fields a profile must NOT carry.
#
# A profile expresses risk appetite. `ghost_mode` expresses whether real money
# moves at all, which is a different kind of decision and not one that should
# change as a side effect of picking a risk level. Leaving it in meant a new
# operator following the Setup view's own advice — "start with conservative" —
# silently disarmed simulation on their first run, because `apply_profile`
# patches every field a profile declares.
NON_PROFILE_FIELDS = {"ghost_mode"}


def main() -> None:
    mapping = {f: c for f, c in field_const_map().items() if f not in NON_PROFILE_FIELDS}
    result = {"schema_version": 1, "profiles": {}}
    missing_report: list[str] = []

    for pname, ppath in PROFILES.items():
        consts = const_values(ppath)
        values: dict[str, object] = {}
        for field, const in mapping.items():
            if const in consts:
                values[field] = consts[const]
            else:
                missing_report.append(f"{pname}: {field} <- config::{const}")
        result["profiles"][pname] = {
            "label": pname.capitalize(),
            "description": DESCRIPTIONS[pname],
            "values": values,
        }

    if missing_report:
        print("WARNING — consts referenced by DynamicConfig but absent from example file(s):", file=sys.stderr)
        for line in missing_report:
            print(f"  {line}", file=sys.stderr)

    counts = {p: len(v["values"]) for p, v in result["profiles"].items()}
    OUT.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n")
    print(f"Wrote {OUT} — field counts: {counts} (DynamicConfig maps {len(mapping)} fields)")


if __name__ == "__main__":
    main()
