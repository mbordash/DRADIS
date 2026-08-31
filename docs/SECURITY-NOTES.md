# Dependency advisories: what is fixed, and what is accepted

DRADIS is sold as a commercial AMI, so its public advisory list is something a
buyer's security review can read. This file records what was updated, what was
deliberately left, and why — so "open high severity" on the repository page can
be answered with a reason rather than a shrug.

Reviewed 2026-08-21 against GitHub Dependabot and `npm audit`.

## Why they appeared when they did

Dependabot needs a lockfile to resolve transitive versions. `Cargo.lock` and
`control-tower/package-lock.json` were untracked until 2026-08-19, when they were
committed so the Marketplace AMI could build reproducibly from `git archive`.
The exposure was always present; committing the lockfiles made it visible. That
is the tracked state working as intended.

## Fixed

| Package | Was | Now | Severity |
|---|---|---|---|
| `next` | 15.5.16 | 15.5.23 | 4 high, 5 moderate |
| `postcss` (direct) | 8.5.14 | 8.5.26 | 2 high, 2 moderate |
| `nanoid` | 3.3.12 | 3.3.18 | 2 high |
| `sharp` | 0.34.5 | 0.35.3 | 1 high |
| `openssl` | 0.10.78 | 0.10.81 | 1 high, 2 moderate |
| `quinn-proto` | 0.11.14 | 0.11.17 | 1 high |
| `serde_with` | 3.19.0 | 3.22.0 | 1 moderate |

Every one was a patch or minor bump inside the same major. No API changes were
required and no code was modified.

`sharp` needed an `overrides` entry in `control-tower/package.json`: Next pins it
to `^0.34.3`, so `npm update` cannot reach the fixed 0.35 line. The override is
safe here because the package is never actually invoked — see below.

## Accepted, with reasons

### postcss 8.4.31, vendored inside Next

`node_modules/next/node_modules/postcss` remains at 8.4.31. Next depends on that
**exact** version and vendors its own copy, and npm honours the pin over an
`overrides` entry — verified, not assumed. Forcing it would mean fighting the
framework's own resolution on every install.

Four advisories apply to it:

- XSS via unescaped `</style>` in CSS stringify output
- Arbitrary file read via an attacker-controlled source map
- Path traversal in previous source-map auto-loading
- An incomplete fix for the above

All four require **attacker-controlled CSS or source-map input**. The Control
Tower compiles its own Tailwind sources at build time, inside the Docker build,
from files in this repository. It never processes CSS from a user, a customer, or
the network, and the compiled output ships as static assets. There is no path by
which a customer's instance feeds untrusted CSS to postcss.

Resolution is Next's to ship. Re-check when the framework bumps its pin.

### sharp, if you are wondering why it is here at all

Next lists `sharp` as an optional dependency for its image optimizer. The Control
Tower imports no `<Image>` component anywhere — `next/image` appears only as a
path exclusion in the middleware matcher — so the optimizer never runs and sharp
is never loaded. It was updated regardless, because an unreachable advisory still
costs credibility on a listing.

## Re-checking

```
gh api "repos/mbordash/DRADIS/dependabot/alerts?state=open&per_page=100" \
  --jq '.[] | [.security_advisory.severity, .dependency.package.name] | @tsv' | sort | uniq -c
```

```
cd control-tower && npm audit
```

Rust advisories are not covered by `npm audit`; `cargo update -p <crate>` handles
them individually, which is preferable to a blanket `cargo update` on a
trading system where a surprise transitive bump is its own risk.
