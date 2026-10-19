# Marketplace usage instructions

Source of truth for the "Usage information" field on the AWS Marketplace listing. The field is plain text with a 4,000 character limit and no bullet support, so the block below uses numbered steps and plain hyphens only. Copy everything between the two rule lines verbatim; update the version tag example and the Kalshi line when a release changes them.

Character count is printed by:

    sed -n '/^---$/,/^---$/p' docs/USAGE.md | sed '1d;$d' | wc -c

---
FULL DOCS: https://dradis.live/docs/install (install) and https://dradis.live/docs/upgrade (upgrade). Source: https://github.com/mbordash/DRADIS

1) LAUNCH AND SECURE. Launch on t3.medium or larger. Give the instance a Name tag carrying the version, such as dradis-v1.4.0; an upgrade briefly runs two instances you must tell apart. Before opening the dashboard, restrict the security group: ports 80 and 443 to your own address, port 22 only if you want SSH. The dashboard controls real money once venue credentials are entered, so treat it as an exchange login.

2) FIRST LOGIN. Open https://<instance-public-ip>/ in a browser. The instance generates a self-signed TLS certificate at first boot, so expect a browser warning. Sign in as 'admin' with your EC2 instance ID as the password. That is a one-time claim token: the setup page immediately requires a new admin password, which is hashed and becomes the real one.

3) VENUE AND CREDENTIALS. In the setup page select Kalshi, Polymarket US or Polymarket International. All three ship in the image, one instance trades one venue at a time, and a venue change applies on engine restart. Venue API credentials are stored only on your instance at /opt/dradis/data/secrets.env and are never transmitted anywhere else. Rotate them from the same page. Optional signal source keys are tagged required, recommended or optional; a source with no key idles and publishes a neutral snapshot while the other strategies run normally.

4) PICK A PROFILE. Conservative, balanced and aggressive apply in one click and can then be adjusted. Conservative is the default and the right starting point.

5) RUN IN GHOST MODE FIRST. A new install starts in ghost mode, which simulates fills against live order books without placing an order. Leave it on until you have watched the strategies act on real market data. The dashboard names the gate blocking every idle strategy, so inaction is always explained.

6) GOING LIVE. Turn ghost mode off from the dashboard and fund the venue account with an amount you are prepared to lose. Kalshi holds balances per exchange shard and rejects orders from an unfunded shard: the Setup tab shows your balance by shard, so fund the shard your squadron trades from before leaving ghost mode. Every strategy can be disabled individually and every risk parameter is tunable from the browser without a restart. If the engine behaves unexpectedly, switch ghost mode back on or stop the instance, then contact support.

7) HEALTH CHECKS. In the EC2 console confirm both status checks pass. In the dashboard, the footer shows engine-to-venue latency and the running version, and the strategy panel gives a live reason for every strategy. The Console tab exports a support bundle to attach to any support request. No AWS quota increases are needed beyond one EC2 instance and its EBS volume.

8) OPTIONAL LLM ADVISOR. Reviews session history and proposes configuration changes with written reasoning. Runs against Anthropic, any OpenAI-compatible endpoint, or a local Ollama model, and defaults to recommend-only; higher autonomy tiers are opt-in. A hosted provider bills you directly for tokens. t3.medium cannot host a model locally; see the install guide for sizing.

9) UPGRADING. DRADIS is an immutable image: launch a new instance from the new version, export the config bundle from the Instance Migration panel in the setup page, import it on the new instance, restart, then stop the old instance before the new one trades. The bundle carries your venue credentials, configuration, trade history and GBoost models, so two running engines would cancel each other's orders and claim the same positions. Kalshi operators: check shard balances after upgrading.

RISK: Trading involves substantial risk of loss. DRADIS executes strategies you choose and configure. It is not investment advice, and no outcome is promised or implied.
---
