# LumiBot growth: what actually moves stars, ranked by return

Research date: September 18, 2026. Star counts and versions pulled live from the
GitHub and PyPI APIs on that date. Where a claim is an interpretation rather
than a measurement, it says so.

## Corrections to earlier advice

Three things in the September 18 chat recommendation were asserted without
reading the current README. They were wrong:

- **A comparison table already exists.** Two, in fact: "Compared with AI trading
  agent projects" (9 projects) and "Compared with backtesting libraries" (9
  libraries), plus four dedicated `lumibot_vs_*` docs pages.
- **The README already leads with a visual.** There is a hero image at the top
  and an archived backtest screenshot further down.
- **Brokers are already shown as an image plus a list**, not a bare sentence.

The real README problems are different and are covered below.

## The competitive picture, measured

| Repo | Stars | Places real trades | LLM agents | Asset classes |
|---|---:|---|---|---|
| TauricResearch/TradingAgents | 107,432 | No | Yes | Research only |
| OpenBB-finance/OpenBB | 73,195 | No | Tooling for agents | Data platform |
| virattt/ai-hedge-fund | 63,460 | **No, stated in its README** | Yes | Simulated |
| freqtrade/freqtrade | 54,499 | Yes | No, FreqAI is classical ML | Crypto only |
| microsoft/qlib | 48,657 | No | Research | Research |
| ccxt/ccxt | 44,028 | Transport layer, not a strategy | No | Crypto |
| nautechsystems/nautilus_trader | 29,099 | Yes | No | Multi-asset |
| QuantConnect/Lean | 21,264 | Yes | No | Multi-asset |
| hummingbot/hummingbot | 20,048 | Yes | **Yes, "Condor" harness** | Crypto only |
| jesse-ai/jesse | 8,529 | Yes | No | Crypto only |
| **Lumiwealth/lumibot** | **2,073** | **Yes, 12 brokers** | **Yes** | **Stocks, options, futures, forex, crypto, prediction** |

ai-hedge-fund's README states: "Note: the system does not actually make any
trades." TradingAgents lists no broker integrations and its homepage is
arXiv 2412.20138.

### The one to actually watch

**Hummingbot.** It is the only project in the set that combines real execution
with an LLM agent layer. Its README describes **Condor**, "the AI harness for
building and running agentic strategies and bot instances," connecting LLM
decision-making to deterministic execution, controlled through Telegram or a web
dashboard. It also ships an `hbot` CLI designed for non-interactive scripted
control with stable exit codes. Its repo topics include `ai-agents`,
`hyperliquid`, `defi` and `solana`, and it claims over $34 billion of user
trading volume across 140+ venues.

For the MetaMask conversation specifically, Hummingbot is the realistic
alternative, not TradingAgents. What Hummingbot cannot do: stocks, options,
futures, US brokers, or backtesting of agent decisions.

Freqtrade is the other serious execution project. Crypto only, CEX plus
Hyperliquid, Telegram and a web UI, hyperopt parameter search, and a dry-run
mode that is the recommended first experience. FreqAI is classical machine
learning, not LLM agents.

## The uncomfortable finding

Rank the execution-capable projects by stars: freqtrade 54k, Hummingbot 20k,
Lean 21k, Nautilus 29k, Jesse 8.5k, LumiBot 2k.

Every project above LumiBot gives away the **operator experience** in open
source:

| Project | CLI | Dry run | Chat control | Web UI |
|---|---|---|---|---|
| freqtrade | `freqtrade` with new-config, download-data, backtesting, hyperopt, trade | Default recommended mode | Telegram | Yes |
| Hummingbot | `hbot` | Paper trade | Telegram | Condor dashboard |
| Jesse | Yes | Yes | No | Yes |
| LumiBot | **none** | Backtest only | No | BotSpot, paid |

LumiBot holds the operator experience behind BotSpot. That is a deliberate and
defensible business decision, and it is also the single clearest structural
reason the star count is an order of magnitude lower. Stars follow the thing a
developer can run and control for free.

This is a decision for Rob, not a bug to fix. The question is how much of the
operator surface moves into open source, and what BotSpot keeps. A defensible
split: open source gets local run, local control and a local dashboard; BotSpot
keeps hosted uptime, managed data, parallel backtests, alerting, kill switches
and the MCP.

## Ranked by return

### 1. Ship `lumibot` as a CLI

Highest return, and it is the one thing every higher-star project has.

The objection is that LumiBot needs a `Strategy` subclass. That is not actually
a blocker, because the CLI generates the file rather than replacing it:

```bash
pip install lumibot
lumibot init my-bot --template ai     # writes my_bot/strategy.py, an ordinary Strategy subclass
lumibot backtest my-bot --days 90     # runs it, prints a table, writes a tearsheet
lumibot run my-bot --paper            # same file, paper broker
```

Nothing about the framework changes. `lumibot init` writes the same code a user
would have written by hand, then runs it. The user gets a result in under a
minute and still ends up holding an editable, ordinary Python file, which is
better than what ai-hedge-fund gives them.

Add `lumibot demo`, which runs a bundled strategy on cached data with no API key
and no account, and ends at a tearsheet. That is the sixty-second first run.

The competitor research doc from September 13 already proposed exactly this at
`docs/research/2026-09-13_competitor-product-plan.md`. It has not been built.

### 2. Rewrite the top 30 lines of the README

Not the whole file. The part above the first scroll.

The README is 686 lines. ai-hedge-fund's is a fraction of that. Length is not
itself the problem; burying the claim is.

The current first line is "Turn trading ideas into working strategies." True,
but every project in the table could write it. The line that nobody else can
write is the one about execution. Put it in three places, because they are
three different audiences:

1. **The GitHub repo description** (the grey text under the repo name, edited in
   repo settings, not in any file). This is what shows in search results and in
   every "awesome-list". Current: "Backtestable AI trading agents and Python
   algorithmic trading strategies." Good, but it does not attack.
2. **The first bold line of README.md**, directly under the `# LumiBot AI Trading`
   heading, replacing "Turn trading ideas into working strategies."
3. **The social preview image**, set in repo settings, which is what renders when
   anyone shares the link.

Suggested line, adjust to taste: *AI agents that actually place the trade.
Twelve brokers, real backtests, stocks, options and crypto.*

Then move the existing comparison table much higher. It is currently at line 413
of 686, which is past where most readers stop. Its "places real trades" story is
the strongest asset in the file and almost nobody scrolls to it.

### 3. Translate the README

TradingAgents ships eight languages. Chinese first, then Spanish. A large share
of the algorithmic-trading developer population reads Chinese first, and no
English-only competitor serves them. Cheapest item on this list per unit of
reach.

### 4. Make every AI example runnable on BotSpot, and say so in the example

Each example page ends with: run this yourself in one click on BotSpot, and
follow it trading live. That converts a reader into a BotSpot signup at the
moment of highest intent, and it gives the open-source example a reason to stay
in sync with the product.

Prerequisite: the example code in `lumibot/example_strategies/` and the strategy
code inside BotSpot have drifted. They need to be reconciled, with BotSpot's
version as the source of truth where it is more current.

### 5. Publish the paper

See the section below. Slower than the rest, and the highest ceiling.

### 6. Add agent tools, browser control first

The harness is where AI trading quality actually comes from, and tools are the
harness. Browser control specifically unlocks any source without an API:
earnings call pages, filings, exchange notices, broker portals, niche data.

Constraints that matter: it has to survive bot detection, and it has to run
headless on Fargate. Candidates to evaluate rather than assume:
Playwright with stealth patches, Camoufox, nodriver, and Browserbase or a
similar hosted browser if self-hosting proves fragile. This needs a real
evaluation, not a pick from memory, and the evaluation should be written down.

There is a second reason to do this: Christy's engagement needs browser control.
A client paying for it is the cheapest possible way to fund a feature the open
source project also wants.

### 7. Upgrade google-adk

`setup.py` and `requirements.txt` pin `google-adk[extensions]>=2.1.0,<3.0.0`.
The latest release on PyPI is **2.9.1**. The version resolved in the local
environment reports as **1.19.0**, which is below the declared floor, so the
local environment and the declared requirement disagree. Worth reconciling
before assuming the harness is current.

Harness quality compounds across every agent example, so this is cheap leverage,
but verify against the agent test suite rather than bumping blind.

### 8. Show PyPI downloads next to stars

Downloads count people who ran it. Stars count people who scrolled past it.
If the download number is respectable relative to the star count, publishing it
reframes the whole comparison. Check the number before deciding to publish it.

## On publishing the paper without a PhD

A PhD is not required. Nothing in the process checks for one.

**arXiv** is a preprint server, not a journal. There is no peer review. The only
gate is an endorsement: a first-time submitter in a category needs one existing
arXiv author in that category to endorse them, which is a one-click favour and
is routinely given to people with real work. q-fin.TR (Trading and Market
Microstructure) and q-fin.CP (Computational Finance) are the relevant
categories. TradingAgents is a q-fin arXiv preprint and it has never been
through peer review either.

The realistic path:

1. Write it as a technical report. Eight to twelve pages, LaTeX, standard
   structure: abstract, introduction, related work, system description, method,
   results, limitations, conclusion.
2. Co-author with someone who has published. It makes endorsement trivial and
   it improves the paper. A finance or CS academic who wants access to a real
   execution dataset is a very easy person to interest.
3. Post to arXiv under q-fin.TR. Free, and live in about two days.
4. Optionally submit the same work afterwards to a workshop such as the ACM
   ICAIF conference on AI in finance. Workshop acceptance is a realistic first
   target and it adds the credibility a preprint alone does not have.

**What the paper should be about, and why only LumiBot can write it.**

Everyone else publishes on agents *deciding*. LumiBot is the only open-source
framework where LLM agents *execute* through real brokers across real asset
classes. So the paper is an evaluation of LLM agents placing real orders:
fill quality, slippage, latency between decision and execution, how often the
agent's stated intent matched the order it actually sent, how often a
deterministic risk gate had to override it, and what breaks in live markets that
never breaks in a backtest.

Nobody else has that dataset because nobody else places the trade. That is a
genuinely novel contribution, not a marketing exercise, and the honest negative
results are the most publishable part.

Note that BotSpot has the execution data to support this, which makes the paper
a company asset and not only a LumiBot one. Any use of customer data needs an
explicit privacy and consent review before a single number goes in a draft.

## What not to do

- Do not rename LumiBot. The name is `pip install lumibot`, 2,073 stars of
  accumulated search, 401 forks, and every existing tutorial link. Renaming
  resets that and breaks installs. "Agentic" belongs in the tagline, which is
  already there, and it will read as dated within two years.
- Do not chase the 100k number directly. TradingAgents and ai-hedge-fund both
  launched into the late-2024 agent hype wave with one viral concept each. That
  is a launch, not a strategy, and it cannot be replayed on demand.
- Do not close the eight stale pull requests in a hurry. See the September 18
  chat: they will conflict after the 61 commits that just merged, and two of
  them look like real fixes worth rebasing.

## One number worth keeping

Fork-to-star ratio measures what share of an audience actually clones the code.
LumiBot 401/2,073 = 19.4%. TradingAgents 19.1%. ai-hedge-fund 17.5%.
Proportionally, LumiBot converts attention into use slightly better than either
of them. The problem is the size of the audience, not the quality of it.
