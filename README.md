# RustPolyBot 🤖

A high-frequency, mean-reversion arbitrage bot for Polymarket. This bot simultaneously monitors both sides of a market ('YES' and 'NO') to find and exploit arbitrage opportunities, locking in near-risk-free profits.

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
- **Start with minimal position sizes** (e.g., $1 per trade) for testing only.

### Market Dynamics
- **Polymarket is dominated by sophisticated bots**, many far more advanced than this one.
- **Professional traders and algorithms are already operating** on these markets at scale.
- **The odds are stacked against retail traders.** This bot is a proof-of-concept, not a money-making machine.

### What This Software Is
- **Educational project**: Demonstrates how to interact with Polymarket APIs.
- **Proof of concept**: Shows a classic mean-reversion/arbitrage strategy.
- **Learning tool**: For understanding market micro-structure, order execution, and latency.

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

- **Arbitrage Engine**: The core logic continuously monitors the YES and NO sides of a market. If `Ask(YES) + Ask(NO) < (1.0 - Profit Margin)`, it executes simultaneous BUY orders on both to lock in the difference.
- **Dual WebSocket Feeds**: Maintains two persistent, low-latency WebSocket connections to receive real-time order book data for both tokens in a market, crucial for accurate arbitrage calculation.
- **Strict Liquidity Filter**: Automatically filters for and trades only in markets with significant 24-hour volume (configurable, default >$1,500) to ensure orders can be filled.
- **Simplified Risk Management**: Focuses on maximum exposure limits and session-level drawdown, removing complex and unnecessary stop-loss logic for a hedged strategy.
- **Centralized Configuration**: All trading parameters are decoupled into a single, tunable `config.rs` module, centered around the `ARBITRAGE_PROFIT_THRESHOLD`.
- **Docker Deployment**: Multi-stage builds with minimal Alpine image (~50MB) and one-click deployment scripts for BTC, ETH, and SOL markets.

### Telegram Notifications (Optional)
To receive real-time alerts on your phone:
1. Create a bot using [@BotFather](https://t.me/botfather) to get your `TELEGRAM_BOT_TOKEN`.
2. Get your `TELEGRAM_CHAT_ID` (google how to do this).
3. Add these to your `.env` file.

## Architecture

```
┌─────────────────────────────────────────────┐
│      RustPolyBot - Arbitrage Scalper        │
├─────────────────────────────────────────────┤
│  Core Logic (main.rs)                       │
│  • Dual WebSocket Management (YES/NO)       │
│  • Arbitrage Calculation                    │
│  • Simultaneous Order Execution             │
├─────────────────────────────────────────────┤
│  Risk Engine (risk.rs)                      │
│  • Pre-trade validation                     │
│  • Exposure & drawdown checks               │
├─────────────────────────────────────────────┤
│  Configuration Module (config.rs)           │
│  • Arbitrage & Sizing Parameters            │
│  • API endpoints & Timeouts                 │
├─────────────────────────────────────────────┤
│  External APIs                              │
│  • Polymarket Gamma API (market discovery)  │
│  • CLOB API (order execution & data)        │
└─────────────────────────────────────────────┘
```

**Deployment**: 3 independent Docker containers, each monitoring a single crypto type.
- **rustpolybot-btc**: BTC hourly markets
- **rustpolybot-eth**: ETH hourly markets
- **rustpolybot-sol**: SOL hourly markets

## Quick Start

### Prerequisites

- **Rust 1.91+** (for local development) or **Docker** (for deployment)
- **Polymarket Account**: With USDC collateral on Polygon
- **SSH Access**: To your deployment server (for remote deployment)

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
   # - POLYMARKET_PRIVATE_KEY: Your Polymarket trading account private key
   # - TRADE_SIZE_USDC: Position size in USDC per side of the arbitrage
   ```

3. **Build and run**
   ```bash
   cargo build --release
   ./target/release/RustPolyBot
   ```

### Docker Deployment

1. **Prepare deployment scripts**
   ```bash
   cp deploy-multi.sh.example deploy-multi.sh
   # ... and other scripts
   chmod +x *.sh
   ```

2. **Customize for your server**
   Edit the scripts and update `HOST`, `USER`, `KEY`, and `REMOTE_DIR`.

3. **Deploy**
   ```bash
   ./deploy-multi.sh
   ```

4. **Monitor**
   ```bash
   ./status.sh        # Check container status
   ./logs-all.sh      # View live logs from all 3 containers
   ./stop-all.sh      # Stop all containers
   ```

## Configuration

All trading parameters are centralized in `src/config.rs`. The most important ones for the arbitrage strategy are:

- `ARBITRAGE_PROFIT_THRESHOLD`: The minimum profit margin required to trigger a trade. A value of `0.01` means the bot looks for at least a 1-cent profit per pair of shares.
- `MAX_SHARE_PRICE_FOR_ENTRY`: Prevents the bot from entering a trade if either the YES or NO side is too expensive (e.g., >85¢), as these markets are less likely to offer arbitrage opportunities.
- `TRADE_COOLDOWN_SECS`: A brief pause after each trade to prevent spamming orders.
- `MIN_MARKET_VOLUME`: The minimum 24-hour volume required to consider a market liquid enough to trade.

To modify parameters, edit `src/config.rs` and redeploy.

## Environment Variables

Create a `.env` file with:

```bash
# Your Polymarket trading account private key (REQUIRED)
POLYMARKET_PRIVATE_KEY=<your_hex_private_key>

# Position size in USDC per side (REQUIRED)
# A value of 3 means $3 on YES and $3 on NO for a total of $6 per trade.
TRADE_SIZE_USDC=3

# Logging level (optional, default: info)
RUST_LOG=info,rustpolybot=info

# Crypto filter (optional, set automatically by deploy scripts)
# CRYPTO_FILTER=btc
```

**⚠️ IMPORTANT**: Never commit `.env` to git. It contains your private key!

## Market Selection & Strategy

The bot's strategy is now purely mathematical:

1. **Scan** for active, liquid hourly crypto markets.
2. **Subscribe** to the real-time order books for both the `YES` and `NO` tokens of the most liquid market.
3. **Calculate** the combined cost to buy one of each: `Ask(YES) + Ask(NO)`.
4. **Check for Profit**: If the combined cost is less than `$1.00` by at least the `ARBITRAGE_PROFIT_THRESHOLD`, an opportunity exists.
5. **Execute**: Fire simultaneous `BUY` orders for both tokens to capture this "synthetic dollar" for less than a dollar.
6. **Hold**: The position is now fully hedged. The bot holds the pair of shares until the market resolves, at which point the pair is worth exactly $1.00.

## Utility Scripts

- `deploy-multi.sh`: Deploys all 3 containers.
- `status.sh`: Shows real-time status.
- `logs-all.sh`: Streams combined logs.
- `stop-all.sh`: Stops all running containers.

## Project Structure

```
RustPolyBot/
├── src/
│   ├── main.rs          # Core arbitrage logic & orchestration
│   ├── risk.rs          # Simplified RiskEngine for exposure checks
│   ├── config.rs        # Centralized configuration for arbitrage
│   └── lib.rs           # Module exports
├── ... (other project files)
```

## Building & Testing

### Local Build
```bash
cargo build --release
```

### Docker Build
```bash
docker build -t RustPolyBot .
```

Before deploying with significant capital, test with `TRADE_SIZE_USDC=1`.

## Troubleshooting

### No Trades Happening
This is normal. Arbitrage opportunities are rare and depend on market inefficiency. The bot is correctly waiting for a profitable spread. Check the logs for:
- `"Arbitrage opportunity found!"` to see if it's detecting but failing trades.
- `"No suitable market found"` if liquidity is too low across all markets.

---

**Happy (and safer) trading! 🚀**
