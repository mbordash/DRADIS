# Contributing to DRADIS

Pull requests are welcome. DRADIS trades real money, so the bar for changes to
the execution path is high — please read this before opening one.

## Licensing and the CLA

DRADIS is **dual-licensed**: AGPL-3.0 for open-source use, with commercial
licenses available for hosted/SaaS and proprietary deployments (see
[README](../README.md#license)).

That model only works if the maintainer holds the right to license *every* line
in the tree under both sets of terms — including code you write. So before your
first pull request can be merged, you'll be asked to sign the
[Contributor License Agreement](CLA.md).

It's a one-time, automated step: a bot comments on your pull request with a link
and the exact phrase to reply with. You keep ownership of your contribution; the
CLA grants a license, it does not assign copyright.

**If you write code on an employer's time or equipment**, please check Section 4
of the CLA before signing. In many jurisdictions that work belongs to your
employer by default, which means it isn't yours to license.

## Before you open a pull request

Run the checks the CI runs:

```bash
# Rust — all three venue builds must compile, since only one is active per build
cargo test --verbose
cargo check --no-default-features --features us_retail
cargo check --no-default-features --features kalshi

# Control Tower
cd control-tower && npx tsc --noEmit && npx next build
```

A change that compiles under the default `intl_clob` feature can still break the
`us_retail` or `kalshi` builds — the venue feature gates are mutually exclusive,
so please check all three.

## Commit messages

Please do **not** include AI co-author trailers — `Co-Authored-By: Claude ...`,
`Co-Authored-By: Copilot ...` or similar — in commit messages.

This is not a position on using AI assistance; use whatever tools you like. It is
about what the trailer does on GitHub: those addresses resolve to real GitHub
accounts, so a commit carrying one renders with the assistant as a linked
contributor avatar beside the humans who wrote the code. DRADIS credits people.

If you use an assistant that adds the trailer automatically, strip it before
pushing — `git commit --amend` on the last commit, or an interactive rebase for
older ones.

## Config files

`src/config.rs` is gitignored and holds live tuning. If you add a constant there,
add it to **all three** of `src/config.conservative.rs.example`,
`src/config.balanced.rs.example`, and `src/config.aggressive.rs.example` — those
are the committed templates, and CI builds from the balanced profile.

If the constant should be tunable at runtime, wire it as a `DynamicConfig` field
rather than a bare constant, register it in `src/api/config_schema.rs`, and
regenerate `src/profiles.json` with `python3 tools/generate-profiles.py`.

## Changes to the execution path

For anything touching order placement, fill accounting, or P&L
(`src/squadron/patrol_impl.rs`, `src/venues/`), please describe in the pull
request:

- what the venue actually returns, not what the docs say it returns, and
- how you verified it — a log excerpt from a demo/ghost run is ideal.

Several past bugs in this area were silent: the code compiled, the numbers looked
plausible, and the error only surfaced as a slow P&L drift. Ghost mode
(`GHOST_MODE = true` in `config.rs`) and Kalshi's demo environment
(`KALSHI_DEMO=1`) both let you exercise these paths without risking funds.

## Strategy changes

If you change a Viper's entry or exit logic, say which live trades motivated it.
The config files carry dated triage comments explaining why each threshold is
what it is — please add to that record rather than replacing values silently.
