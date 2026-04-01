# RustPolyBot 🤖

A high-frequency hybrid trading bot for Polymarket. This bot combines "Perfect-Hedge" arbitrage with "Oracle-Lag" momentum trading to maximize opportunities in both low-volatility and trending markets.

---

## ⚠️ CRITICAL WARNING - READ BEFORE USE

**THIS SOFTWARE IS EXPERIMENTAL AND FOR EDUCATIONAL PURPOSES ONLY.**

### Legal & Geographic Restrictions
- **US Citizens**: Polymarket is **NOT permitted for use by US citizens inside the United States**. Check your local laws before using this software.
- **Jurisdiction**: Verify that prediction markets and automated trading are legal in your country/region before deployment.

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
    - **Arbitrage**: Simultaneously buys 'YES' and 'NO' when the sum is < $1.00 for a risk-free profit.
    - **Momentum**: Executes one-sided speculative trades when Binance prices move sharply ($50+ for BTC) before Polymarket adjusts.
- **Advanced Safety Filters**:
    - **Strike Buffer**: Momentum trades only fire when price is safely away from the strike (e.g. Strike + $50) to avoid choppy oscillations.
    - **Directional Lock**: Prevents buying the opposite side of an open momentum position to avoid "accidental arbitrage" at a loss.
    - **Price Cap**: Automatically stops momentum buying if the token price exceeds $0.75, ensuring a healthy risk/reward ratio.
- **Automated Profit Taking**: Automatically sells one-sided momentum positions when the bid reaches $0.93 to lock in gains and recycle capital for the next session.
- **Ghost Mode Testing**: Includes a `GHOST_MODE` flag to simulate all trades in the logs without spending real capital.
- **Binance Oracle Integration**: Streams real-time ticker data to detect "Oracle Lag" in milliseconds.
- **Response-Based Accounting**: Uses the exchange's direct API response for 100% accurate fill detection.
- **Ultra-Parallel Execution**: prep-signs-posts legs simultaneously using Rust's `tokio` runtime, achieving latency as low as 20ms per leg.

---

## Performance Tuning (HFT Mode)

To achieve the lowest possible latency, the following host and container optimizations should be considered:

### 1. Ubuntu Host (Kernel Tuning)
The Linux kernel can be tuned for aggressive TCP performance by applying these `sysctl` settings:
- `net.ipv4.tcp_fastopen=3`: Enables data exchange during the initial handshake.
- `net.core.rmem_max / wmem_max`: Increases network buffers to prevent micro-stuttering.
- `net.ipv4.tcp_slow_start_after_idle=0`: Keeps the TCP connection "hot" and ready to fire.

### 2. Docker Container (Overhead Reduction)
Containers are deployed with high-priority resource allocations:
- `--network host`: Bypasses the Docker virtual bridge for direct access to the AWS network card.
- `--cpus="1.0"`: Reserves a full physical CPU core for the bot.
- `--cpu-shares=1024`: Assigns maximum priority to the bot process.

### 3. DNS Pinning
The bot resolves `clob.polymarket.com` once at startup and "pins" the IP address in memory, saving ~20ms of lookup time on every trade.

---

## Quick Start

### Prerequisites
- **Rust 1.91+** or **Docker**
- **AWS Server**: Research the proper region required for ~15ms peering to the exchange.
- **Minimum $10 USDC**: Due to exchange minimum order sizes.

### Configuration (`.env`)

| Variable | Description | Default |
|----------|-------------|---------|
| `TRADE_SIZE_USDC` | Size for hedged arbitrage trades. | `10` |
| `MOMENTUM_TRADE_SIZE_USDC` | Size for speculative momentum trades. | `5` |
| `CRYPTO_FILTER` | Target asset (`btc`, `eth`, or `sol`). | `btc` |
| `POLYMARKET_PRIVATE_KEY` | Your Polygon EOA private key. | `REQUIRED` |

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

## Future Enhancements (TODO)

- [ ] **Maker Support**: Transition to earning rebates by placing limit orders at the best bid.
- [ ] **Book Walking**: Analyze order book depth up to 5 levels to prevent slippage.
- [ ] **Multi-Outcome Arbitrage**: Support markets with 3+ outcomes where the sum is < $1.00.

---

**Happy (and safer) trading! 🚀**
