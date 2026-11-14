# DRADIS

> **Direct Reaction And Dynamic Intelligence System** — Low-latency Rust prediction-market trading bot for Kalshi & Polymarket. 12 autonomous Viper strategies, a Raptor recon layer (Price, Funding, Derivatives, Tide "Institutional Pulse", Horizon "TradFi Velocity", a Sports book-consensus scout, and a venue-neutral Tennis event-state scout), a Squadron deployment framework, a CAG async dispatch layer with concurrent multi-asset support, a real-time Next.js Control Tower, and an LLM Advisor (Ollama local/remote, OpenAI-compatible, or Anthropic) that delivers optimization recommendations via Telegram & OpenClaw — and can propose or autonomously apply live config changes under a tiered, guard-railed autonomy policy.

![Rust](https://img.shields.io/badge/Rust-1.95+-orange?logo=rust&logoColor=white)
![Tokio](https://img.shields.io/badge/Tokio-async%20runtime-darkgreen?logo=rust&logoColor=white)
![axum](https://img.shields.io/badge/axum-REST%20API-blue?logo=rust&logoColor=white)
![Next.js](https://img.shields.io/badge/Next.js-15-black?logo=next.js&logoColor=white)
![Tailwind CSS](https://img.shields.io/badge/Tailwind-CSS-38bdf8?logo=tailwindcss&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-22-brightgreen?logo=node.js&logoColor=white)
![Ollama](https://img.shields.io/badge/Ollama-LLM%20Advisor-blueviolet?logo=ollama&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)
[![OpenClaw](https://img.shields.io/badge/OpenClaw-AI%20Integration-CC0000?logoColor=white)](https://openclaw.ai)
[![License](https://img.shields.io/badge/License-AGPL%20v3-blue)](LICENSE)
[![AWS Marketplace](https://img.shields.io/badge/AWS%20Marketplace-Available-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/marketplace/pp/prodview-dv5dats2bmm5q)

**WARNING**: You will probably lose money. Start in GHOST mode and tune before going live. Make sure to regularly pull updates as our own LLM advises on config and Viper strategy impls often.

Public Demo Site: https://demo.dradis.live/

## Two ways to run it

**From source — this repository**
Clone, pick a risk profile, run it with Docker. The full engine, all 12 strategies and the Control Tower are here; nothing is held back for the AMI. **AGPL v3** — see [LICENSE](LICENSE).

**Prebuilt AMI — [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-dv5dats2bmm5q)**
Launches a configured instance in your own AWS account. All three venues ship in the image, the Control Tower comes with it, and your API keys stay on your infrastructure. Marketplace subscribers receive a **commercial license**, not the AGPL, so the copyleft obligations below do not apply to that deployment. Install and upgrade guides: <https://dradis.live/docs>

The rest of this README covers the source route; [Getting started](docs/GETTING_STARTED.md) walks through it.

---

## Tactical Overview

DRADIS is a comprehensive trading automation platform for prediction markets. Built in Rust for maximum concurrency and memory safety, it evaluates selected markets every 50ms, coordinating multiple autonomous strategies to preserve capital and place orders where it sees inefficiencies.

The system is organized around four BSG-inspired tactical layers:

| Layer        | Folder          | Role                                                                           |
|--------------|-----------------|--------------------------------------------------------------------------------|
| **Raptors**  | `src/raptors/`  | Signal scouts — fetch, normalize, broadcast external data                      |
| **Vipers**   | `src/vipers/`   | Trading strategies — evaluate signals and place orders                         |
| **Squadron** | `src/squadron/` | Deployment unit — bundles Raptors + Vipers onto a battle location              |
| **CAG**      | `src/cag/`      | Commander Air Group — async dispatch, session state, multi-asset orchestration |

Details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Read this first

**This is experimental software. You will probably lose money. Start in GHOST mode and tune.**

- **Risk**: Momentum trades are directional and can get whiplashed. Arbitrage spreads are thin. None of this is guaranteed profit.
- **US Citizens**: Polymarket is rolling out US access under CFTC regulation via a waitlist.
- **Competition**: Polymarket is full of well-funded, low-latency bots. This project is a learning exercise, not an edge.

## Quick start

```bash
git clone https://github.com/mbordash/DRADIS.git && cd DRADIS
cp .env.example .env                               # fill in your venue credentials
cp src/config.balanced.rs.example src/config.rs    # or conservative / aggressive

./start-local.sh                  # Polymarket International (default)
VENUE=us ./start-local.sh         # or Polymarket US
VENUE=kalshi ./start-local.sh     # or Kalshi (start on demo.kalshi.co)
```

The Control Tower comes up on `http://localhost:3002` and the API on `http://localhost:9000/api/health`. Requirements, venue credentials, optional feeds and Docker deployment: [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md).

DRADIS compiles for **exactly one** venue, chosen at build time:

| Feature              | Venue                              | Auth                                   | Gateway                              |
|----------------------|------------------------------------|----------------------------------------|--------------------------------------|
| `intl_clob` *(default)* | Polymarket International (self-custody) | EOA wallet + EIP-712 over Polygon      | `clob.polymarket.com`                |
| `us_retail`          | Polymarket US (custodial, CFTC)    | Ed25519 challenge-response → JWT        | `api.prod.polymarketexchange.com`    |
| `kalshi`             | Kalshi (custodial, CFTC)           | RSA-PSS request signing                 | `external-api.kalshi.com` (demo: `external-api.demo.kalshi.co`) |

## Documentation

| Guide | What's in it |
|---|---|
| [Getting started](docs/GETTING_STARTED.md) | Requirements, venue setup, configuration profiles, optional feeds, running locally and in Docker |
| [Architecture](ARCHITECTURE.md) | Raptors, Vipers, Squadrons, CAG, design principles, safety systems |
| [Vipers](docs/VIPERS.md) | Every built-in strategy, including Bookline, Anchor and Helm |
| [Build a Viper](docs/SCRIPTED_VIPERS.md) | Writing your own strategies as scripts |
| [Control Tower](docs/CONTROL_TOWER.md) | The dashboard: live config, Setup, Squadron Builder, authentication |
| [LLM Advisor](docs/LLM_ADVISOR.md) | Providers and the autonomy tiers for AI-authored config changes |
| [REST API and integrations](docs/API.md) | Endpoints, OpenClaw, MCP server |
| [Troubleshooting and FAQ](docs/TROUBLESHOOTING.md) | Diagnosing a stall, common questions |
| [Upgrading](docs/UPGRADING.md) · [Performance tuning](docs/PERFORMANCE_TUNING.md) · [Custom strategies in Rust](docs/CUSTOM_STRATEGY.md) | |
| [Release notes](docs/RELEASES.md) · [Security](docs/SECURITY.md) | |

## License

DRADIS is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**.
The full text is in [LICENSE](LICENSE).

You are free to use, study, modify, and redistribute this software under the terms
of the AGPL. In return, the AGPL requires that anyone who receives the software —
**including users who interact with a modified version over a network** — is offered
the complete corresponding source code of that version under the same license.

That network clause (AGPL §13) is the practical difference from the GPL. If you run
a modified DRADIS as a hosted or SaaS product, you must make your modified source
available to its users, even though you never distribute a binary.

### Commercial licensing

The AGPL and the commercial license are meant to pull in different directions,
and which one suits you depends on whether your changes are your edge.

Under the AGPL, an improvement you make to a Viper is one you offer back to
anyone who uses it, including over a network. That is a good deal if you want
the core to improve around you, and it is how the project gets better for
everyone.

The commercial license is for the case where that is exactly what you do not
want. It gives you the right to modify DRADIS privately: fork a Viper, change
its formulas, run your version alongside the original, and keep every bit of it
to yourself. You owe nothing back, and that is totally OK. For a trading system that is the whole point,
because a strategy you are obliged to publish stops being an advantage the
moment you publish it.

It also covers the ordinary cases where the AGPL does not fit:

- running DRADIS or a derivative as a **hosted / SaaS product** without publishing your changes,
- **embedding** it in a proprietary trading system or closed-source product, or
- obtaining it under terms **other than the AGPL** for any other reason.

Contact **starbuck@dradis.live** with a short description of your intended use.

Dual licensing is possible because the project's contributions are covered by a
Contributor License Agreement — see [CONTRIBUTING.md](docs/CONTRIBUTING.md).

### Contributing

Pull requests are welcome. Contributors are asked to sign the
[Contributor License Agreement](docs/CLA.md) once; the CLA bot will prompt
automatically on your first pull request.

To share a strategy rather than change the engine, contribute a script viper
to [dradis-contrib](https://github.com/mbordash/dradis-contrib) instead: no
CLA, MIT licensed, and once merged it is published to the community registry
for every DRADIS operator to import.
