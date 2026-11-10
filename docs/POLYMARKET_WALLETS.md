# Polymarket International wallets

DRADIS separates the EOA signer, the trading funder holding collateral and positions, and the contract executing on-chain calls.

| Configuration | CLOB signature type | Order identity | On-chain transport |
|---|---:|---|---|
| `gnosis_safe` (default) | 2 | maker = Safe, signer = EOA | Existing single-owner Safe transaction |
| `poly1271` | 3 | maker = funder, signer = funder | Signed Deposit Wallet batch through the Polymarket relayer |

## Configuration

```dotenv
POLYMARKET_WALLET_TYPE=poly1271
POLYMARKET_FUNDER_ADDRESS=<your deployed Deposit Wallet address>
POLYMARKET_PRIVATE_KEY=<EOA owner key>
POLYGON_RPC_URL=<Polygon RPC endpoint>
POLYMARKET_RELAYER_API_KEY=<relayer API key>
POLYMARKET_RELAYER_API_KEY_ADDRESS=<relayer key owner address>
# Optional; HTTPS only:
# POLYMARKET_RELAYER_URL=https://relayer-v2.polymarket.com
```

Keep credentials server-side. Never commit actual dotenv files. Poly1271 requires an explicit valid, nonzero funder different from the EOA; there is no Safe derivation fallback. `POLYMARKET_SIGNATURE_TYPE` does not select the wallet mode. Safe accounts may omit the new settings.

Startup loads `.env`, then `$DRADIS_DATA_DIR/secrets.env`, whose values override matching settings. Local Intl instances normally use `data-intl/secrets.env`. Wallet and relayer settings are available in Setup and take full effect after restart. Invalid wallet inputs fail validation. Missing or malformed relayer credentials block LIVE while balance reads, Setup and Ghost remain usable.

## Balance and readiness

CLOB authentication uses the EOA. Balance requests explicitly carry the selected signature type; a successful API credential derivation alone does not establish wallet ownership. `/api/portfolio` reads CLOB even before a strategy session exists. A valid zero remains `collateral_source: "live"`; failed or timed-out reads use the existing snapshot fallback. An on-chain balance never substitutes for a failed CLOB read.

`GET /api/venue/capabilities` reports wallet type, signature type, trading address, simulation support, execution and settlement readiness, blockers and effective Ghost mode. Selecting LIVE refreshes wallet deployment, signer authorization, RPC, relayer access and existing approvals. Readiness checks do not grant approvals or move funds. Missing prerequisites produce a reason and leave Ghost/configuration controls available. Poly1271 starts in Ghost, even if a previous configuration selected LIVE.

No cache refresh is required on every startup. SDK 0.7.0's `update_balance_allowance()` does not check HTTP status; its return value alone cannot establish a successful refresh.

## Execution and recovery

Orders use the SDK's V2 payload types, signable-order builder and signing implementation, including Poly1271 ERC-7739 wrapping and the appropriate exchange domain. Existing amount quantization remains in the order helper. Actual matched quantities are distinct from signed working quantities. Batch acceptance is tracked per leg. Cancellations verify funder ownership and the returned canceled IDs; a rejected cancellation is never treated as success.

A private guarded CLOB client checks current Ghost/read-only state at every mutation boundary. Placement records are durable and scoped by funder and token before submission. A lost or ambiguous response retains the claim across restarts and blocks fresh exposure until reconciliation. An accepted order remains claimed until its owner and fills have durable bookkeeping. An unknown response or a missing order is not proof of rejection; investigate retained claims rather than deleting the journal to retry.

The wallet executor preserves Safe transport and uses signed Deposit Wallet batches for Poly1271. It verifies the deployed owner-derived wallet, chain, nonce, successful receipt and observable operation effect. Durable atomic intent claims prevent concurrent or restarted settlement from blindly submitting a second transaction. Read-only recovery can confirm known completed effects while in Ghost. Unrecognized wallet contracts, unsupported protocols, unverified ownership and unresolved submissions fail closed.

Trading in this SDK integration covers CTF Exchange V2 markets. Protocol V2 books declare `version: "v2"` and require Exchange V3; they are rejected at discovery and again before V2 signing/submission. Unknown or unreadable protocol metadata also blocks submission.

Supported redemption routing distinguishes legacy CTF standard/negative-risk positions from Protocol V2 positions. The latter require their actual condition/outcome encoding and Router approval. Completion requires the held outcome balances to reach zero before ledger cleanup. The collateral sweep uses the selected wallet transport; a Deposit Wallet never receives a Safe `execTransaction` call. Automatic approval provisioning, wallet deployment and session-key delegation are outside this integration.

## Read the balance without starting the engine

From the instance working directory, run `cargo run --locked --example polymarket-balance`. It loads the same dotenv/managed secrets precedence and reads authenticated CLOB collateral, printing JSON. It creates no trading session, relayer, order, approval or settlement task; failed authentication/reads exit unsuccessfully. Its local order journal may be initialized. Use a protected terminal and keep credentials out of command arguments.

## Build and verify safely

```bash
cp src/config.balanced.rs.example src/config.rs  # on a fresh checkout only
cargo test --locked
cargo check --locked --no-default-features --features us_retail
cargo check --locked --no-default-features --features kalshi
cargo build --release --locked --bin dradis
```

Preserve an existing customized `src/config.rs`. Start with Ghost enabled and read `/api/portfolio` plus `/api/venue/capabilities` on the configured API port, using a protected API client when authentication is enabled. For local Intl instances `VENUE=intl ./start-local.sh` installs `target/release/dradis-intl`; editing source or environment files alone does not replace the running process.

Offline signing, synthetic local HTTP/RPC and database tests establish construction and recovery behavior. They do not establish acceptance of an actual exchange order or a funded settlement transaction. Do not infer real-money production acceptance from these tests. Configure missing prerequisites locally and inspect capability blockers before considering an operator-authorized LIVE session.

References: [wallet authentication](https://docs.polymarket.com/trading/wallets-auth), [position management](https://docs.polymarket.com/trading/positions/manage), [contract addresses](https://docs.polymarket.com/resources/contracts).
