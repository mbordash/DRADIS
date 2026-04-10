# RustPolyBot 🤖

A high-frequency hybrid trading bot for Polymarket combining **three independent strategies**: Momentum Trading, Perfect-Hedge Arbitrage, and Time Decay (Theta). Each strategy operates autonomously with its own entry/exit logic, risk controls, and profit targets.

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

## Three-Strategy Architecture

The bot automatically detects market conditions and applies the most appropriate strategy:

### 🔥 **Momentum Trading** (High Risk / High Reward)
- **Entry**: One-sided speculative trades when Binance price moves sharply before Polymarket adjusts
- **Confirmation**: Requires multiple consecutive ticks (default: 2) to filter out false signals
- **Exit**: 
  - Take Profit: 3% gain
  - Stop Loss: 5% loss
  - Reversal: Momentum velocity drops below threshold
- **Position Size**: $5 per trade (configurable)
- **Best For**: Trending markets with sharp directional moves

### 📈 **Arbitrage Trading** (Medium Risk / Steady)
- **Entry**: Simultaneously buys YES and NO when combined price < $1.00
- **Exit**: 
  - Early Exit: When combined bid reaches safety ceiling (~99.5%)
  - Standard Exit: Hedged exit for profit
- **Position Size**: $10 per trade (configurable)
- **Best For**: Stable, range-bound markets

### 💰 **Time Decay Trading** (Low Risk / Passive) - *Now Enabled*
- **Entry**: Market-neutral YES+NO purchases when combined spread is attractive (< 1.01)
- **Exit**: Auto-exit when profit reaches 1.5% target, stop loss at 0.5%, or 30s before market close
- **No Manual Cleanup**: Completely automatic position management
- **Position Size**: $5 per side (configurable)
- **Best For**: Short-dated markets (hourly/5-minute expiry)
- **Status**: Fully integrated and active; enable/disable via config `ENABLE_TIME_DECAY_TRADING`

---

## Features

### Strategy Management
- **Autonomous Strategy Selection**: Bot automatically chooses the best strategy based on market conditions
- **Strategy Logging**: Startup logs clearly show which strategies are enabled:
  ```
  🎯 Strategies enabled: 🔥 Momentum Trading + 📈 Arbitrage Trading
  ```
- **Independent Configuration**: Each strategy has its own tunable parameters in `src/config.rs`
- **Modular Architecture**: Strategies live in `src/strategies/` as independent modules, making them easy to test, debug, and extend

### Advanced Safety Filters
- **Market Validator**: Professional-grade market filtering that validates crypto, time windows, expiry, and strike prices with 4+ pattern recognition types
- **Momentum Confirmation Ticks**: Requires multiple consecutive signal updates to filter out "fakeouts"
- **Minimum Liquidity Check**: Analyzes order book depth; only fires if sufficient liquidity exists
- **Strike Buffer**: Momentum trades only fire when price is safely away from the strike
- **Directional Lock**: Prevents "accidental arbitrage" at a loss
- **Price Cap**: Stops momentum buying if token price exceeds healthy risk/reward ratio
- **Binary Market Support**: Automatically detects and handles binary outcome markets (no explicit strike needed)

### Comprehensive Exit Management
- **Tight Take Profit**: Captures quick moves across all strategies
- **Stop Loss**: Limits downside on failed trades
- **Momentum Reversal**: Exits immediately if oracle trend reverses
- **Safety Ceiling**: Exits to ensure capital recycling
- **Automatic Cleanup**: No manual position management needed

### Advanced Infrastructure
- **Self-Healing Nonce Sync**: Automatically detects and recovers from "invalid nonce" errors
- **Telegram Monitoring**: Real-time alerts for trade failures and emergency circuit breakers
- **Ghost Mode Testing**: Simulate all trades without spending real capital
- **Binance Oracle Integration**: Real-time ticker data for oracle lag detection
- **Ultra-Parallel Execution**: Pre-signs and posts orders simultaneously using Rust's `tokio` runtime

---

## Geographic Deployment & Latency

Latency is critical for success, particularly for the "Oracle Lag" momentum strategy.

- **The Strategy**: High-frequency trades rely on submitting orders before exchange prices fully adjust to external moves.
- **Research Deployment**: Identify optimal geographic regions to minimize network round-trip time.
- **Compliance First**: Ensure your hosting jurisdiction complies with local trading regulations.

---

## Quick Start

### Prerequisites
- **Rust 1.91+** or **Docker**
- **Polygon Wallet**: An EOA with USDC and MATIC (for gas)
- **Telegram Bot** (Optional): For remote monitoring

### Configuration (`.env` & `src/config.rs`)

| Variable / Constant | Description | Default |
|----------|-------------|---------|
| `TRADE_SIZE_USDC` | Size for arbitrage trades | `10` |
| `MOMENTUM_TRADE_SIZE_USDC` | Size for momentum trades | `5` |
| `CRYPTO_FILTER` | Target asset (`btc`, `eth`, `sol`) | `btc` |
| `ENABLE_MOMENTUM_TRADING` | Toggle momentum strategy | `true` |
| `ENABLE_TIME_DECAY_TRADING` | Toggle time decay strategy | `false` |
| `ARBITRAGE_PROFIT_THRESHOLD` | Min margin for arbitrage entry | `0.05` (5%) |
| `MOMENTUM_CONFIRMATION_TICKS` | Signal confirmations before entry | `2` |
| `MOMENTUM_TARGET_PROFIT_PERCENT` | Momentum take profit target | `0.03` (3%) |
| `MOMENTUM_STOP_LOSS_PERCENT` | Momentum stop loss | `0.05` (5%) |
| `MIN_LIQUIDITY_FILL_RATIO` | Required depth at top of book | `0.80` (80%) |
| `POLYMARKET_PRIVATE_KEY` | Your Polygon EOA private key | `REQUIRED` |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot API token | `OPTIONAL` |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID | `OPTIONAL` |

### Deployment

1. **Test first in Ghost Mode**:
   Set `pub const GHOST_MODE: bool = true;` in `src/config.rs`. Run and watch logs for simulated trades.

2. **Go Live**:
   ```bash
   # Edit config.rs to set GHOST_MODE = false
   cargo build --release
   ./target/release/rustpolybot
   ```

3. **Docker Deployment** (Recommended):
   ```bash
   ./deploy-multi.sh
   ```
   This deploys BTC/ETH/SOL containers simultaneously with optimal resource allocation.
   - Includes market validator integration for professional market filtering
   - Modular strategy architecture (Momentum + Arbitrage + Time Decay)
   - Comprehensive helper functions for maintainability

---

## Strategy Configuration Examples

### Conservative (Low Risk)
```rust
ENABLE_MOMENTUM_TRADING = false              // Disable risky momentum trades
ARBITRAGE_PROFIT_THRESHOLD = 0.08            // Higher margin requirement
MIN_LIQUIDITY_FILL_RATIO = 0.90              // Only thick markets
TRADE_SIZE_USDC = 5                          // Small position size
```

### Aggressive (High Volatility)
```rust
ENABLE_MOMENTUM_TRADING = true               // Enable momentum
MOMENTUM_CONFIRMATION_TICKS = 1              // Faster entries
MOMENTUM_TARGET_PROFIT_PERCENT = 0.02        // 2% take profit (capture quicker)
MOMENTUM_STOP_LOSS_PERCENT = 0.08            // Wider stop loss
TRADE_SIZE_USDC = 20                         // Larger positions
```

### Balanced (Default)
```rust
ENABLE_MOMENTUM_TRADING = true
ENABLE_TIME_DECAY_TRADING = false
ARBITRAGE_PROFIT_THRESHOLD = 0.05
MOMENTUM_CONFIRMATION_TICKS = 2
MOMENTUM_TARGET_PROFIT_PERCENT = 0.03
TRADE_SIZE_USDC = 10
```

---

## FAQ & Troubleshooting

### Why are my orders being rejected?
Order rejections are usually due to **Price Slippage** caused by network latency. The bot uses Fill-or-Kill (FAK) orders. If the order takes too long to reach the exchange, the market price may have moved beyond your limit price. Deploy closer to Polymarket's infrastructure to reduce latency.

### EOA vs. Gnosis Safe
RustPolyBot defaults to **Gnosis Safe** (Maker address) for trading because:
1. Standard for API Trading on Polymarket
2. Many web-UI accounts use Gnosis Safe proxy under the hood
3. Simplified authentication and onboarding

The bot automatically derives your deterministic Gnosis Safe address from your EOA. This is usually the easiest path to success.

### Why is the bot scanning but not trading?
- **Spread/Fees**: Margin must exceed `ARBITRAGE_PROFIT_THRESHOLD` after fees
- **Momentum Thresholds**: Asset-specific price moves must meet configured buffers
- **Liquidity**: Order book may be too thin for your position size
- **Ghost Mode**: Check if `GHOST_MODE` is still enabled

### What does "Emergency Stopping" mean?
If the bot encounters **3 consecutive persistent failures**, it triggers a circuit breaker and shuts down (safety feature). You'll receive a Telegram notification. Check logs and re-enable manually.

### How do I enable/disable individual strategies?
Edit `src/config.rs`:
```rust
pub const ENABLE_MOMENTUM_TRADING = true;          // Toggle momentum
pub const ENABLE_TIME_DECAY_TRADING = false;       // Toggle time decay
pub const ARBITRAGE_PROFIT_THRESHOLD = dec!(0.05); // Set to 0 to disable arbitrage
```

Then rebuild and redeploy.

---

## Performance Tuning (HFT Mode)

For lowest possible latency (~15-30ms execution) on Linux:

### 1. Host Kernel Tuning
```bash
sudo sysctl -w net.ipv4.tcp_fastopen=3
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.ipv4.tcp_slow_start_after_idle=0
```

### 2. Docker Container
Run with high-priority resources: `--network host --cpus="1.0" --cpu-shares=1024`

---

## Architecture

```
src/
├── main.rs                    # Main orchestrator & market loop
├── lib.rs                     # Module exports
├── config.rs                  # All tunable parameters
├── risk.rs                    # Risk engine & exposure tracking
├── notifications.rs           # Telegram alerts
├── market_validator.rs        # Market selection & filtering logic
├── strategies/
│   ├── mod.rs                 # Strategy module registry
│   ├── momentum.rs            # Momentum trading logic
│   ├── arbitrage.rs           # Arbitrage trading logic
│   └── time_decay.rs          # Time decay (theta) trading
└── helpers/
    ├── mod.rs                 # Helper module registry
    ├── price.rs               # Price handling utilities
    ├── json.rs                # JSON parsing helpers
    ├── time.rs                # Time utilities
    ├── balance.rs             # Balance tracking
    └── nonce.rs               # Nonce management
```

Each strategy is independently testable and can be enabled/disabled via configuration. Helper functions are modularized for maintainability and code clarity.

---

## Future Enhancements

- [ ] **Time Decay Refinement**: Further testing and edge case handling
- [ ] **Maker Support**: Earn rebates by providing liquidity
- [ ] **Advanced Book Walking**: Calculate VWAP for larger orders
- [ ] **Multi-Outcome Arbitrage**: Support 3+ outcome markets
- [ ] **Mean Reversion**: Add counter-trend trading strategy
- [ ] **Grid Trading**: Automated grid-based position accumulation

---

**Happy (and safer) trading! 🚀**
