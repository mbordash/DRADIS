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
- **Start with minimal position sizes** (e.g., $10 per trade is the current recommended minimum) for testing only.

### Market Dynamics
- **Polymarket is dominated by sophisticated bots**, many far more advanced than this one.
- **Professional traders and algorithms are already operating** on these markets at scale.
- **The odds are stacked against retail traders.** This bot is a proof-of-concept, not a money-making machine.

### Disclaimer
By using this software, you agree that:
- You understand the financial risks involved.
- You are operating in a jurisdiction where this is legal.
- You take full responsibility for any losses.
- The authors bear no liability for your trading outcomes.

**Use at your own risk.** 🎲

---

## Features

- **Binance Oracle Integration**: Streams real-time BTC/ETH/SOL prices from Binance to detect "Oracle Lag" before Polymarket prices adjust.
- **Strike Price Discovery**: Automatically extracts price targets (Strike Price) from market metadata to calculate real-time "Distance to Strike" (Diff).
- **Response-Based Accounting**: Uses the exchange's direct API response for 100% accurate fill detection. No more "ghost" positions.
- **Ultra-Parallel Execution**: Uses Rust's `tokio::join!` to prepare, sign, and post YES and NO orders simultaneously, achieving latency as low as 100ms.
- **Industrial-Grade Safety**: Includes a 60-second failure cooldown, a 3-strike circuit breaker, and automatic unhedged position flattening.
- **Network Optimizations**: Implements DNS pinning and persistent connection pooling to minimize round-trip times to the exchange.
- **Dual WebSocket Feeds**: Maintains persistent, low-latency WebSocket connections for real-time order book data.

---

## Performance Tuning (HFT Mode)

To achieve the lowest possible latency, the following host and container optimizations are implemented:

### 1. Ubuntu Host (Kernel Tuning)
The Linux kernel is tuned for aggressive TCP performance by applying these `sysctl` settings:
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
- **AWS Server**: Recommended location: `ca-central-1` (Montreal) for ~15ms peering to the exchange.
- **Minimum $10 USDC**: Due to exchange minimum order sizes.

### Deployment
1. **Set up environment**
   ```bash
   cp .env.example .env
   # Edit .env with your POLYMARKET_PRIVATE_KEY and TRADE_SIZE_USDC
   ```

2. **Run with Optimizations**
   ```bash
   docker run -d \
       --network host \
       --cpus="1.0" \
       --cpu-shares=1024 \
       --restart unless-stopped \
       --name rustpolybot-btc \
       --env-file .env \
       -e CRYPTO_FILTER=btc \
       rustpolybot
   ```

3. **Monitor Latency**
   Look for the following logs to audit performance:
   - `📍 Network Pulse`: Baseline round-trip time to the exchange.
   - `📈 BOTH LEGS FILLED`: Real-world execution latency.

---

## Future Enhancements (TODO)

- [ ] **Oracle Momentum Snipe**: Automatically trade when Binance moves $>X\%$ before Polymarket reacts.
- [ ] **Maker Support**: Transition to earning rebates by placing limit orders at the best bid.
- [ ] **Book Walking**: Analyze order book depth up to 5 levels to prevent slippage.
- [ ] **Multi-Outcome Arbitrage**: Support markets with 3+ outcomes where the sum is < $1.00.

---

**Happy (and safer) trading! 🚀**
