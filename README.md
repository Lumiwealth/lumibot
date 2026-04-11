[![CI Status](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml/badge.svg?branch=dev)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![Coverage](https://raw.githubusercontent.com/Lumiwealth/lumibot/badge/coverage.svg)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![PyPI](https://img.shields.io/pypi/v/lumibot)](https://pypi.org/project/lumibot/)
[![Python](https://img.shields.io/pypi/pyversions/lumibot)](https://pypi.org/project/lumibot/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Discord](https://img.shields.io/discord/1045669553608851456?label=Discord)](https://discord.gg/TmMsJCKY3T)

# Lumibot: Algorithmic Trading Made Easy

**The same code runs your backtest and your live trading.** Python framework for stocks, options, crypto, futures, and forex with built-in AI trading agent support.

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

## Recommended Data Provider

For the deepest historical coverage (stocks, options, futures, indexes), we recommend [ThetaData](https://www.thetadata.net/). Use promo code **`BotSpot10`** for 10% off your first order.

> **Affiliate disclosure:** This is a referral link. ThetaData directly supports the BotSpot ecosystem, and using this code helps fund continued development of Lumibot.

## AI Trading Agents

Lumibot includes a built-in AI trading agent runtime. Build agents that run identically in backtests and live trading.

- Create agents with `self.agents.create(...)`
- Use **DuckDB** for time-series analysis instead of dumping raw bars into prompts
- Mount external **MCP servers** for news, macro data, filings, or any domain-specific tools
- Replay identical agent decisions in **backtests** without paying for another model call

Start here:
- [Agent Documentation](https://lumibot.lumiwealth.com/agents.html)
- [Stock Agent Example](https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/agent_stock_backtest.py)
- [Options Agent Example](https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/agent_option_backtest.py)
- [Full Guide](https://github.com/Lumiwealth/lumibot/blob/dev/docs/AI_TRADING_AGENTS.md)

## Deploy with BotSpot

Want to build and deploy trading bots without code? [BotSpot](https://botspot.trade/sales?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme) lets you:

- Build trading bots using natural language and AI
- Backtest with historical data
- Deploy to trade automatically
- Join a community of algorithmic traders

**[Get started at BotSpot.trade](https://botspot.trade/sales?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme)**

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

Browse all examples: [example_strategies/](https://github.com/Lumiwealth/lumibot/tree/dev/lumibot/example_strategies)

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

> **THIS SOFTWARE IS PROVIDED FOR EDUCATIONAL AND INFORMATIONAL PURPOSES ONLY. IT IS NOT FINANCIAL ADVICE. THE AUTHORS AND CONTRIBUTORS ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS. ALGORITHMIC TRADING INVOLVES SUBSTANTIAL RISK OF LOSS. PAST BACKTEST PERFORMANCE DOES NOT GUARANTEE FUTURE RESULTS. USE THIS SOFTWARE AT YOUR OWN RISK.**

---

## Contributing

We welcome contributions! Here's a video to help you get started: [Watch The Video](https://youtu.be/Huz6VxqafZs)

**Steps:**
1. Clone the repository
2. Create a new branch: `git checkout -b my-feature`
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
