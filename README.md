# RustPolyBot 🤖

A high-frequency hybrid trading bot for Polymarket. This bot combines "Perfect-Hedge" arbitrage with "Oracle-Lag" momentum trading to maximize opportunities in both low-volatility and trending markets.

---

## ⚠️ CRITICAL WARNING - READ BEFORE USE

**THIS SOFTWARE IS EXPERIMENTAL AND FOR EDUCATIONAL PURPOSES ONLY.**

### Legal & Geographic Restrictions
- **US Citizens**: Polymarket is **NOT permitted for use by US citizens inside the United States**. Check your local laws before using this software.
- **Jurisdiction**: Verify that prediction markets and automated trading are legal in your country/region before deployment.
- **Compliance**: Users are responsible for ensuring that their deployment location and trading activities comply with all applicable local, national, and international laws and regulations.

### Financial Risk
- **You will likely lose money.** Momentum trading introduces **directional risk**—you are no longer always hedged.
- **This is NOT a guaranteed profit tool.** Arbitrage opportunities are rare, and momentum trades can be "whiplashed" by market reversals.
- **Treat this as gambling, not investing.** You should never spend money you cannot afford to lose.

### Market Dynamics
- **Polymarket is dominated by sophisticated bots**, many far more advanced than this one.
- **The odds are stacked against retail traders.** This bot is a proof-of-concept, not a money-making machine.

**Use at your own risk.** 🎲

---

## Features

- **Hybrid Strategy Engine**:
    - **Arbitrage**: Simultaneously buys 'YES' and 'NO' when the sum is < $1.00 for a riskless profit.
    - **Momentum**: Executes one-sided speculative trades when Binance prices move sharply before Polymarket adjusts.
- **Advanced Safety Filters**:
    - **Strike Buffer**: Momentum trades only fire when price is safely away from the strike price to avoid choppy oscillations.
    - **Directional Lock**: Prevents buying the opposite side of an open momentum position to avoid "accidental arbitrage" at a loss.
    - **Price Cap**: Automatically stops momentum buying if the token price exceeds a healthy risk/reward ratio.
- **Comprehensive Exit Management**:
    - **Tight Take Profit**: Captures quick spikes.
    - **Stop Loss**: Limits downside on failed moves.
    - **Momentum Reversal**: Exits immediately if the external oracle trend reverses.
    - **Safety Ceiling**: Bot exits immediately if the bid reaches a safety ceiling to ensure capital recycling.
- **Self-Healing Nonce Sync**: Automatically detects "invalid nonce" errors, re-synchronizes with the CLOB API, and retries the trade.
- **Telegram Monitoring**: Real-time alerts for trade failures and emergency circuit breakers.
- **Ghost Mode Testing**: Includes a `GHOST_MODE` flag to simulate all trades in the logs without spending real capital.
- **Binance Oracle Integration**: Streams real-time ticker data to detect "Oracle Lag" in milliseconds.
- **Ultra-Parallel Execution**: Pre-signs and posts legs simultaneously using Rust's `tokio` runtime.

---

## Geographic Deployment & Latency

Latency is a critical factor for the bot's success, particularly for the "Oracle Lag" strategy.

- **The Strategy**: High-frequency trades rely on submitting an order before the exchange price fully adjusts to external market moves.
- **Research Deployment**: Users must research the exchange's infrastructure and identify the optimal geographic region for deployment to minimize network round-trip time.
- **Compliance First**: When choosing a deployment region, ensure that your hosting provider and the selected jurisdiction are compliant with your legal obligations.

---

## Quick Start

### Prerequisites
- **Rust 1.91+** or **Docker**
- **Polygon Wallet**: An EOA with USDC and MATIC (for gas).
- **Telegram Bot** (Optional): For remote monitoring.

### Configuration (`.env`)

| Variable | Description | Default |
|----------|-------------|---------|
| `TRADE_SIZE_USDC` | Size for hedged arbitrage trades. | `10` |
| `MOMENTUM_TRADE_SIZE_USDC` | Size for speculative momentum trades. | `5` |
| `CRYPTO_FILTER` | Target asset (`btc`, `eth`, or `sol`). | `btc` |
| `POLYMARKET_PRIVATE_KEY` | Your Polygon EOA private key. | `REQUIRED` |
| `TELEGRAM_BOT_TOKEN` | Your Telegram Bot API token. | `OPTIONAL` |
| `TELEGRAM_CHAT_ID` | Your Telegram Chat ID. | `OPTIONAL` |

### Deployment
1. **Test first in Ghost Mode**:
   Ensure `pub const GHOST_MODE: bool = true;` is set in `src/config.rs`. Run the bot and watch the logs for `👻 GHOST MODE` signals.

2. **Go Live**:
   Change `GHOST_MODE` to `false` in `src/config.rs` and rebuild:
   ```bash
   cargo build --release
   ./target/release/rustpolybot
   ```

---

## FAQ & Troubleshooting

### Why are my orders being rejected even when the signal is right?
Order rejections are often due to **Price Slippage** caused by network latency. The bot uses Fill-or-Kill (FAK) orders. If the order takes too long to reach the exchange, the market price may have moved beyond your limit price, causing the exchange to "Kill" the order. Reducing latency by deploying in an optimal geographic region is the primary fix for this.

### Why did the bot fail with "invalid nonce"?
Nonces are sequence numbers used to prevent replay attacks. If you use your wallet elsewhere (e.g., via the Polymarket UI), the bot's local counter will fall behind. RustPolyBot automatically detects this, fetches the correct nonce from the API, and retries the trade.

### Why does the bot keep scanning but not trading?
- **Spread/Fees**: The arbitrage logic requires the margin to be higher than `ARBITRAGE_PROFIT_THRESHOLD` *after* accounting for fees.
- **Momentum Thresholds**: Asset-specific price moves and buffers must be met as defined in `config.rs`.
- **Ghost Mode**: Check if `GHOST_MODE` is still enabled.

### The bot says "Emergency Stopping". What happened?
If the bot encounters **3 consecutive persistent failures** (meaning a trade failed even after a nonce re-sync), it will trigger a circuit breaker and shut down. This is a safety feature to protect your wallet. You will receive a Telegram notification if this happens.

---

## Performance Tuning (HFT Mode)

To achieve the lowest possible latency (~15-30ms execution), apply these host-level optimizations if running on Linux:

### 1. Host Kernel Tuning
```bash
sudo sysctl -w net.ipv4.tcp_fastopen=3
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0
```

### 2. Docker Container
Run with high-priority resource allocations: `--network host --cpus="1.0" --cpu-shares=1024`

---

## Future Enhancements (TODO)

- [ ] **Maker Support**: Transition to earning rebates by placing limit orders at the best bid.
- [ ] **Book Walking**: Analyze order book depth up to 5 levels to prevent slippage.
- [ ] **Multi-Outcome Arbitrage**: Support markets with 3+ outcomes.

---

**Happy (and safer) trading! 🚀**
