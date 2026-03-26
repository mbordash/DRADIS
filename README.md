# RustPolyBot 🤖

A high-frequency, "Perfect-Hedge" arbitrage bot for Polymarket. This bot simultaneously monitors both sides of a market ('YES' and 'NO') to find and exploit mispricings, locking in guaranteed profits by buying a "synthetic dollar" for less than $1.00.

---

## ⚠️ CRITICAL WARNING - READ BEFORE USE

**THIS SOFTWARE IS EXPERIMENTAL AND FOR EDUCATIONAL PURPOSES ONLY.**

### Legal & Geographic Restrictions
- **US Citizens**: Polymarket is **NOT permitted for use by US citizens inside the United States**. Check your local laws before using this software.
- **Jurisdiction**: Verify that prediction markets and automated trading are legal in your country/region before deployment.

### Financial Risk
- **You will likely lose money.** Potentially a lot of money. This is not a "free money" machine. Arbitrage opportunities are rare and fleeting.
- **This is NOT a guaranteed profit tool.** Trading crypto prediction markets is inherently risky.
- **Treat this as gambling, not investing.** You should never spend money you cannot afford to lose.
- **Start with minimal position sizes** (e.g., $8 per trade is the current minimum to meet exchange requirements) for testing only.

### Market Dynamics
- **Polymarket is dominated by sophisticated bots**, many far more advanced than this one.
- **Professional traders and algorithms are already operating** on these markets at scale.
- **The odds are stacked against retail traders.** This bot is a proof-of-concept, not a money-making machine.

### What This Software Is
- **Educational project**: Demonstrates how to interact with Polymarket APIs.
- **Proof of concept**: Shows a classic risk-neutral arbitrage strategy.
- **Learning tool**: For understanding market micro-structure, order execution, and parallel latency.

### What This Software Is NOT
- A financial advisor or investment recommendation.
- A guaranteed profit system.
- A substitute for understanding market mechanics.
- Advice to spend money on Polymarket.

### Disclaimer
By using this software, you agree that:
- You understand the financial risks involved.
- You are operating in a jurisdiction where this is legal.
- You take full responsibility for any losses.
- The authors bear no liability for your trading outcomes.

**Use at your own risk.** 🎲

---

## Features

- **Response-Based Accounting**: Uses the exchange's direct API response (`taking_amount`) for 100% accurate fill detection. No more "ghost" positions.
- **Automatic Position Cleanup**: Periodically scans and removes expired markets from memory, preventing exposure bloat and ensuring accurate risk limits.
- **Perfect-Hedge Sizing**: Calculates the exact number of shares to buy on both sides to ensure a risk-neutral position. Payoff is guaranteed regardless of market outcome.
- **Ultra-Parallel Execution**: Uses Rust's `tokio::join!` to prepare, sign, and post YES and NO orders simultaneously, minimizing "legging risk".
- **Dual WebSocket Feeds**: Maintains two persistent, low-latency WebSocket connections for real-time order book data, now with a 60-second "Heartbeat" log.
- **Smart Exit Logic**: Automatically "works" the bid price for emergency flattening and early profit taking, avoiding expensive market-sell "dumps".
- **Strict Liquidity & Expiry Filters**: Automatically filters for high-volume markets (>$5,000) and avoids the final 15 minutes of trading.
- **Exchange Minimum Enforcement**: Automatically enforces the exchange's minimum order size (5 shares) to prevent API rejections.

### Telegram Notifications (Optional)
To receive real-time alerts on your phone:
1. Create a bot using [@BotFather](https://t.me/botfather) to get your `TELEGRAM_BOT_TOKEN`.
2. Get your `TELEGRAM_CHAT_ID`.
3. Add these to your `.env` file.

## Architecture

```
┌─────────────────────────────────────────────┐
│      RustPolyBot - Arbitrage Scalper        │
├─────────────────────────────────────────────┤
│  Core Logic (main.rs)                       │
│  • Response-Based Fill Detection            │
│  • Automatic Position Cleanup Task          │
│  • Ultra-Parallel Order Execution           │
│  • Perfect-Hedge Sizing & Heartbeats        │
├─────────────────────────────────────────────┤
│  Risk Engine (risk.rs)                      │
│  • Pre-trade sum-based validation           │
│  • Session-level drawdown protection        │
├─────────────────────────────────────────────┤
│  Configuration Module (config.rs)           │
│  • Arbitrage & Sizing Parameters            │
│  • Exchange Minimums & Volume Filters       │
├─────────────────────────────────────────────┤
│  External APIs                              │
│  • Polymarket Gamma API (market discovery)  │
│  • CLOB API (order execution & data)        │
└─────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- **Rust 1.91+** or **Docker**
- **Polymarket Account**: With USDC collateral on Polygon
- **Minimum $10 USDC**: Due to exchange minimum order sizes.

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/RustPolyBot.git
   cd RustPolyBot
   ```

2. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your values:
   # - POLYMARKET_PRIVATE_KEY: Your trading private key
   # - TRADE_SIZE_USDC: Total position size (e.g., 8)
   ```

3. **Build and run**
   ```bash
   cargo build --release
   ./target/release/RustPolyBot
   ```

### Docker Deployment

1. **Deploy**
   ```bash
   ./deploy-multi.sh
   ```

2. **Monitor**
   ```bash
   ./status.sh        # Check status
   ./logs-all.sh      # View live logs and Heartbeats
   ```

## Configuration (`src/config.rs`)

- `ARBITRAGE_PROFIT_THRESHOLD`: Min margin to trigger entry (default: 0.035 or 3.5%).
- `MAX_SUM_PRICE_FOR_ENTRY`: Max combined price allowed (default: 0.975).
- `MIN_ORDER_SHARES`: Minimum shares per order (default: 5.0).
- `MIN_MARKET_VOLUME`: Minimum 24hr volume (default: $5,000).

## Environment Variables

Create a `.env` file with:

```bash
POLYMARKET_PRIVATE_KEY=<your_hex_private_key>

# Total USDC per trade. MUST be high enough to buy at least 5 shares on each side.
# At $0.50/share, you need $5.00 minimum. Recommended: $8.00+.
TRADE_SIZE_USDC=8

RUST_LOG=info,rustpolybot=info
```

## Market Strategy

1. **Scan**: Finds liquid (>$5k vol) hourly markets.
2. **Monitor**: Subscribes to order books via WebSockets.
3. **Heartbeat**: Every 60s, logs the current arbitrage sum.
4. **Detect**: If `Ask(YES) + Ask(NO) < 0.965` (1.0 - 0.035 threshold), an opportunity exists.
5. **Execute**: Sells both legs **simultaneously** using Ultra-Parallel tasks.
6. **Accounting**: Uses the `taking_amount` from the exchange for perfect position tracking.
7. **Cleanup**: Periodically removes expired markets from internal tracking.
8. **Hold**: Holds until expiry ($1.00 payoff) or early exit if `Bid(YES) + Bid(NO) > 0.995`.

---

**Happy (and safer) trading! 🚀**
