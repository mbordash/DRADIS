<!--
Thanks for contributing to DRADIS. The checklist below is the same set of things
a review would raise anyway — running through it first usually saves a round trip.
-->

## What this changes

<!-- One or two sentences. If it adds a Raptor or Viper, say what signal it reads
     and whether anything consumes it yet. -->

## Why

<!-- Link the issue if there is one. -->

---

### Checklist

- [ ] **All three venue builds compile.** Only one venue is active per build, so a
      change that passes the default `intl_clob` build can still break the others:
      ```bash
      cargo test --verbose
      cargo check --no-default-features --features us_retail
      cargo check --no-default-features --features kalshi
      ```
- [ ] **Control Tower builds**, if the change touches it:
      ```bash
      cd control-tower && npx tsc --noEmit && npx next build
      ```
- [ ] **New constants are in all three profile templates** —
      `src/config.conservative.rs.example`, `src/config.balanced.rs.example` and
      `src/config.aggressive.rs.example`. `src/config.rs` is gitignored, so CI
      builds from the balanced template and a missing constant fails only there.
- [ ] **A new credential is registered in the Setup UI** — add it to
      `MANAGED_KEYS` in `src/api/setup.rs`, and for a Raptor key also to
      `RAPTOR_SOURCES` with a `signup_url`. Keep the `.env.example` entry too;
      headless deployments provision without a browser.
- [ ] **No AI co-author trailers in the commit messages.** See CONTRIBUTING.md —
      those addresses resolve to real GitHub accounts and render as linked
      contributor avatars.
- [ ] **CLA signed.** First-time contributors: comment the phrase the bot asks
      for. It is a one-off, and it is what lets DRADIS stay dual-licensed.
