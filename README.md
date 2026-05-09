[![CI Status](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml/badge.svg?branch=dev)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![Coverage](https://raw.githubusercontent.com/Lumiwealth/lumibot/badge/coverage.svg)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![PyPI](https://img.shields.io/pypi/v/lumibot)](https://pypi.org/project/lumibot/)
[![Python](https://img.shields.io/pypi/pyversions/lumibot)](https://pypi.org/project/lumibot/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

# Lumibot: Python Trading Strategies + AI Agents

**The same Python strategy can run in a backtest, paper account, or live broker account.** Lumibot is an open-source trading framework for deterministic strategies, AI-agent strategies, stocks, options, crypto, futures, forex, and real broker execution.

<p align="center">
  <img src="docs/assets/readme/lumibot_ai_trading_agents_overview.png" alt="Lumibot AI trading agents overview" width="100%">
</p>

## What You Can Build

- **Deterministic strategies:** normal Python logic, indicators, if statements, scheduled rules, position sizing, and risk controls.
- **AI-agent strategies:** one or more agents that reason through evidence, call tools, write memory, and optionally place orders.
- **Backtests:** replay historical data and simulated orders with artifacts you can inspect.
- **Paper or live trading:** reuse the same strategy code with real broker state and real order routing.

## AI Agents Are Optional, Not The Whole Product

Lumibot now includes a built-in AI agent runtime, but you can still write classic Python trading strategies exactly as before. AI agents are useful when the strategy needs reasoning: reading filings, comparing news, checking macro context, debating a thesis, or explaining why a trade should happen.

An investment committee is one example pattern:

<p align="center">
  <img src="docs/assets/readme/lumibot_investment_committee_architecture.png" alt="Lumibot AI investment committee architecture" width="100%">
</p>

In that pattern, read-only research agents gather evidence and a trading-enabled portfolio manager decides whether to place Lumibot orders. Other agent designs are possible: a single research agent, a pair of specialist agents, a strategy-refinement agent, or a risk-review agent layered on top of deterministic logic.

Built-in AI agent tools include market/account state, order inspection, DuckDB queries, documentation search, Alpaca news when credentials exist, technical indicators, SEC fundamentals and filings, FRED macro data, local memory, and Telegram notifications.

## Introducing BotSpot: No-Code AI Trading Bots

**[BotSpot](https://botspot.trade/?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme)** is our platform built on top of Lumibot that lets you build, backtest, and deploy trading strategies without writing any code. Just describe what you want in plain English and BotSpot's AI handles the rest.

- **Build** strategies using natural language -- the AI writes production-ready Lumibot code for you
- **Backtest** against years of historical data with a single click
- **Deploy** to live trading with real brokers (Alpaca, Interactive Brokers, and more)
- **Browse** a marketplace of proven, community-built strategies you can run immediately

<a href="https://botspot.trade/?utm_source=github&utm_medium=readme_badge&utm_campaign=lumibot">
  <img src="https://img.shields.io/badge/%F0%9F%9A%80_Try_BotSpot_Free-Build_AI_Trading_Bots_Without_Code-brightgreen?style=for-the-badge&labelColor=2e3440" alt="Try BotSpot Free" height="40">
</a>

## Quick Start

```bash
pip install lumibot
```

```python
from datetime import datetime
from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting

class MyStrategy(Strategy):
    def on_trading_iteration(self):
        if self.first_iteration:
            aapl = self.create_order("AAPL", 10, "buy")
            self.submit_order(aapl)

MyStrategy.backtest(
    YahooDataBacktesting,
    datetime(2023, 1, 1),
    datetime(2024, 1, 1),
)
```

```bash
python my_strategy.py
```

That same strategy code works with live brokers. Just swap the broker class.

> **Full documentation: [lumibot.lumiwealth.com](https://lumibot.lumiwealth.com/)**

## Why Lumibot?

| Feature | Lumibot | Backtrader | Freqtrade | Zipline | Backtesting.py | Jesse |
|---------|---------|------------|-----------|---------|----------------|-------|
| **Same code: backtest + live** | Yes | Yes | Yes (crypto) | No | No | Yes (paid) |
| **Stocks** | Yes | Yes | No | Yes | Yes | No |
| **Options** | **Yes** | No | No | No | No | No |
| **Crypto** | Yes | Limited | Yes | No | Yes | Yes |
| **Futures** | Yes | Limited | Crypto only | Partial | Yes | Crypto only |
| **Forex** | Yes | Outdated | No | No | Yes | No |
| **AI agent runtime** | Built-in | No | FreqAI (ML) | No | No | ML pipeline |
| **Brokers** | 5+ (Alpaca, IBKR, Tradier, Schwab, CCXT) | IB only (outdated) | 10+ crypto exchanges | None | None | 8+ crypto (paid) |
| **Actively maintained** | Yes (2026) | No (since 2023) | Yes | Minimal | Moderate | Yes |
| **License** | MIT | GPL-3.0 | GPL-3.0 | Apache-2.0 | AGPL-3.0 | MIT |

**Switching from Backtrader?** See our [migration guide](docs/MIGRATING_FROM_BACKTRADER.md) for a side-by-side comparison with code examples.

## Deploy Live

### Option A: BotSpot (managed cloud)

[BotSpot](https://botspot.trade/?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme) runs your Lumibot strategies on hosted infrastructure with scheduling, monitoring, and live execution. Build strategies with AI, no coding required.

- Create trading bots using natural language
- Backtest with historical data
- Deploy to trade automatically 24/7
- Join a community of algorithmic traders

**[Get started at BotSpot.trade](https://botspot.trade/?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme)**

### Option B: Self-hosted (full control)

Run Lumibot on your own machine with any supported broker:

```python
from lumibot.brokers import Alpaca
from lumibot.traders import Trader

ALPACA_CONFIG = {
    "API_KEY": "your-key",
    "API_SECRET": "your-secret",
    "PAPER": True,
}

broker = Alpaca(ALPACA_CONFIG)
strategy = MyStrategy(broker=broker)

trader = Trader()
trader.add_strategy(strategy)
trader.run_all()
```

## Supported Brokers & Data Sources

| Brokers (live trading) | Data Sources (backtesting) |
|------------------------|---------------------------|
| Alpaca | Yahoo Finance (free) |
| Interactive Brokers | ThetaData |
| Tradier | Polygon |
| Schwab | DataBento |
| CCXT (100+ crypto exchanges) | Interactive Brokers |
| Bitunix | CCXT |
| ProjectX | Alpaca |

### Recommended Data Provider

For the deepest historical coverage (stocks, options, futures, indexes), we recommend [ThetaData](https://www.thetadata.net/). Use promo code **`BotSpot10`** for 10% off your first order.

> *Affiliate disclosure: This is a referral link. Using this code supports the continued development of Lumibot as an open-source project.*

## AI Trading Agents

Lumibot includes a built-in AI trading agent runtime. Build agents that run identically in backtests and live trading.

- Create agents with `self.agents.create(...)`
- Use a different model per agent with `model="openai/gpt-5.5"` or any LiteLLM/ADK-supported provider string
- Make research agents read-only with `allow_trading=False`
- Give agents built-in SEC fundamentals, filings, FRED macro data, indicators, memory, and notifications
- Use **DuckDB** for time-series analysis instead of dumping raw bars into prompts
- Mount external **MCP servers** for news, macro data, filings, or any domain-specific tools
- Replay identical agent decisions in **backtests** without paying for another model call

Start here:
- [Agent Documentation](https://lumibot.lumiwealth.com/agents.html)
- [Agent Flow Design](https://lumibot.lumiwealth.com/agents_flows.html)
- [AI Investment Committee Example](https://github.com/Lumiwealth/lumibot/blob/version/4.5.11/lumibot/example_strategies/ai_investment_committee.py)
- [Standalone AI Committee Demo](https://github.com/Lumiwealth/lumibot-ai-investment-committee)
- [Stock Agent Example](https://github.com/Lumiwealth/lumibot/blob/version/4.5.11/lumibot/example_strategies/agent_stock_backtest.py)
- [Options Agent Example](https://github.com/Lumiwealth/lumibot/blob/version/4.5.11/lumibot/example_strategies/agent_option_backtest.py)
- [Full Guide](https://github.com/Lumiwealth/lumibot/blob/version/4.5.11/docs/AI_TRADING_AGENTS.md)

## Community Strategies

Browse and contribute trading strategies: **[lumibot-strategies](https://github.com/Lumiwealth/lumibot-strategies)** (fork, backtest, and share your own)

## Example Strategies

Lumibot includes 25+ example strategies covering stocks, options, crypto, futures, and forex:

```bash
# Run a simple buy-and-hold backtest
python -m lumibot.example_strategies.stock_buy_and_hold

# Or explore all examples
ls lumibot/example_strategies/
```

Browse all examples: [example_strategies/](https://github.com/Lumiwealth/lumibot/tree/version/4.5.11/lumibot/example_strategies)

**External example repo:** [stock_example_algo](https://github.com/Lumiwealth-Strategies/stock_example_algo) (deployable to Render or Repl.it)

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/Lumiwealth-Strategies/stock_example_algo)

## Backtesting Data Sources

Select a data source via environment variable (overrides code):

```bash
export BACKTESTING_DATA_SOURCE=thetadata   # or yahoo, ibkr, polygon
```

Multi-provider routing by asset type:

```bash
export BACKTESTING_DATA_SOURCE='{"default":"thetadata","crypto":"coinbase"}'
```

### Data source comparison

| Data Source | OHLCV | Split Adjusted | Dividends | Dividend Adjusted Returns |
|-------------|-------|----------------|-----------|---------------------------|
| Yahoo       | Yes   | Yes            | Yes       | Yes                       |
| Alpaca      | Yes   | Yes            | No        | No                        |
| Polygon     | Yes   | Yes            | No        | No                        |
| Tradier     | Yes   | Yes            | No        | No                        |
| Pandas*     | Yes   | Yes            | Yes       | Yes                       |

*Pandas loads CSV files in Yahoo dataframe format, which can contain dividends.

## Learn More

- **Documentation:** [lumibot.lumiwealth.com](https://lumibot.lumiwealth.com/)
- **Blog:** [lumiwealth.com/blog](https://lumiwealth.com/blog/)
- **AI Strategy Builder:** [BotSpot.trade](https://www.botspot.trade/?utm_source=github&utm_medium=referral&utm_campaign=lumibot_readme)
- **Discord:** [Join the community](https://discord.gg/TmMsJCKY3T)

## AI Bootcamp

Learn to build, backtest, and deploy trading strategies using AI. Join 2,400+ traders.

**[Learn more about the AI Bootcamp](https://www.botspot.trade/ai-bot-builder-bootcamp?utm_source=github&utm_medium=referral&utm_campaign=lumibot_readme)**

## DISCLAIMER

> **THIS SOFTWARE IS PROVIDED FOR EDUCATIONAL AND INFORMATIONAL PURPOSES ONLY. IT IS NOT FINANCIAL ADVICE AND DOES NOT CONSTITUTE A RECOMMENDATION TO BUY OR SELL ANY SECURITY. LUMIBOT AND BOTSPOT ARE NOT REGISTERED BROKER-DEALERS OR FINANCIAL ADVISORS. ALGORITHMIC TRADING INVOLVES SUBSTANTIAL RISK OF LOSS, INCLUDING THE POSSIBILITY OF LOSSES GREATER THAN YOUR INITIAL INVESTMENT. SOFTWARE BUGS AND ERRORS CAN LEAD TO RAPID FINANCIAL LOSSES. PAST BACKTEST PERFORMANCE DOES NOT GUARANTEE FUTURE RESULTS. USE THIS SOFTWARE AT YOUR OWN RISK. YOU ARE SOLELY RESPONSIBLE FOR COMPLIANCE WITH ALL APPLICABLE LAWS AND REGULATIONS REGARDING THE ASSETS YOU CHOOSE TO TRADE.**

---

## Contributing

We welcome contributions! Here's a video to help you get started: [Watch The Video](https://youtu.be/Huz6VxqafZs)

**Steps:**
1. Clone the repository
2. Create a new branch: `git switch -c my-feature`
3. Install dev dependencies: `pip install -r requirements_dev.txt && pip install -e .`
4. Make your changes
5. Run tests: `pytest`
6. Create a pull request

## Running Tests

```bash
pytest                          # Run all tests
pytest tests/test_asset.py      # Run a specific test file
coverage run; coverage report   # Show code coverage
```

## Remote Cache Configuration

Lumibot can mirror its local parquet caches to AWS S3. See `docs/remote_cache.md` for configuration.

## Architecture Documentation

- [Backtesting Architecture](docs/BACKTESTING_ARCHITECTURE.md) - Data flow diagrams for Yahoo, ThetaData, Polygon
- [Acceptance Backtests](docs/ACCEPTANCE_BACKTESTS.md) - End-to-end acceptance suite
- [Environment Variables](docsrc/environment_variables.rst) - All configurable env vars
- [Changelog](CHANGELOG.md) - Release notes
- [AI Assistant Guide](CLAUDE.md) - Instructions for AI coding assistants
- [Production Safety](AGENTS.md) - ThetaData and production rules

## License

MIT License - [View License](https://github.com/Lumiwealth/lumibot/blob/master/LICENSE)
