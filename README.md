[![CI Status](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml/badge.svg?branch=dev)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)
[![Coverage](https://raw.githubusercontent.com/Lumiwealth/lumibot/badge/coverage.svg)](https://github.com/Lumiwealth/lumibot/actions/workflows/cicd.yaml)

# Lumibot - A Backtesting and Trading Library for Stocks, Options, Crypto, Futures, FOREX and More!

Lumibot is a backtesting and trading library for stocks, options, crypto, futures and more. It is made so that the same code you use for backtesting can be used for live trading, making it easy to transition from backtesting to live trading. Lumibot is a highly flexible library that allows you to create your own strategies and indicators, and backtest them on historical data. It is also highly optimized for speed, so you can backtest your strategies quickly and efficiently.

**IMPORTANT: This library requires data for backtesting. Our recommended data source is [ThetaData](https://www.thetadata.net/) because they provide the deepest historical coverage we’ve found and directly support BotSpot. Use the promo code `BotSpot10` at checkout for 10% off the first order (the code also tells ThetaData you were referred by us).**

> **Contributor note:** Read `AGENTS.md` before running anything Theta-related. That file spells out the hard rules—never launch ThetaTerminal or the shared downloader locally, always point LumiBot at the AWS-hosted downloader, and wrap all long
> commands with `/Users/robertgrzesik/bin/safe-timeout`. Breaking these rules kills the only licensed Theta session.

## Introducing BotSpot: No-Code AI Trading Bots

**[BotSpot](https://botspot.trade/?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme)** is our platform built on top of Lumibot that lets you build, backtest, and deploy trading strategies without writing any code. Just describe what you want in plain English and BotSpot's AI handles the rest.

- **Build** strategies using natural language -- the AI writes production-ready Lumibot code for you
- **Backtest** against years of historical data with a single click
- **Deploy** to live trading with real brokers (Alpaca, Interactive Brokers, and more)
- **Browse** a marketplace of proven, community-built strategies you can run immediately

<a href="https://botspot.trade/?utm_source=github&utm_medium=readme_badge&utm_campaign=lumibot">
  <img src="https://img.shields.io/badge/%F0%9F%9A%80_Try_BotSpot_Free-Build_AI_Trading_Bots_Without_Code-brightgreen?style=for-the-badge&labelColor=2e3440" alt="Try BotSpot Free" height="40">
</a>

## Architecture Documentation

- `docs/BACKTESTING_ARCHITECTURE.md` - Detailed documentation of the backtesting data flow (Yahoo, ThetaData, Polygon data sources, caching, and data flow diagrams)
- `docs/ACCEPTANCE_BACKTESTS.md` - Manual end-to-end acceptance backtest suite + performance gate (ThetaData)
- `docsrc/environment_variables.rst` - Public documentation page for environment variables (update when env vars change)
- `CHANGELOG.md` - Deployment/release notes (keep this updated)
- `CLAUDE.md` - AI assistant instructions for working with the codebase
- `AGENTS.md` - Critical rules for ThetaData and production safety

## Releases / Deployments (internal)

For production deployments (BotSpot / BotManager), keep releases traceable:

- Update `CHANGELOG.md` for every deploy (include the deploy commit hash)
- Tag the deploy commit as `vX.Y.Z` and push the tag
- Create a GitHub Release from that tag using the `CHANGELOG.md` entry

## Documentation - 👇 Start Here 👇

To get started with Lumibot, you can check out our documentation below.

**Check out the documentation for the project here: 👉 <http://lumibot.lumiwealth.com/> 👈**

## AI Trading Agents and Agentic Backtesting

Lumibot now includes a built-in AI trading agent runtime for strategies. You can create an agent with `self.agents.create(...)`, run it from `initialize()`, `on_trading_iteration()`, `on_filled_order()`, or any other lifecycle method, and reuse the same strategy in both backtests and live trading.

- Build an **AI trading agent** directly inside a LumiBot strategy
- Use **DuckDB** as the query surface for time-series analysis instead of dumping raw bars into prompts
- Replay identical agent decisions in **backtests** without paying for another model call
- Mount external **MCP servers** for news, macro, filings, or any other domain-specific tools
- Use LumiBot's internal runtime prompt plus your own system prompt, with structured traces and default per-run summaries for debugging
- Keep the strategy in charge of timing, risk rules, and execution

Start here:

- Sphinx docs: <https://lumibot.lumiwealth.com/agents.html>
- Backtesting docs: <https://lumibot.lumiwealth.com/backtesting.html>
- Stock example: [lumibot/example_strategies/agent_stock_backtest.py](https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/agent_stock_backtest.py)
- Option example: [lumibot/example_strategies/agent_option_backtest.py](https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/agent_option_backtest.py)
- Repo guide: [docs/AI_TRADING_AGENTS.md](https://github.com/Lumiwealth/lumibot/blob/dev/docs/AI_TRADING_AGENTS.md)

## More About BotSpot

BotSpot is a platform built on Lumibot that lets anyone build, backtest, and deploy trading strategies using AI. **Visit [BotSpot.trade](https://botspot.trade/?utm_source=lumibot+docs&utm_medium=documentation&utm_campaign=GitHub+Readme) to get started.**

## Learn More

Check out example strategies and tutorials on our blog, or use our AI agent to build strategies for you:

**Blog:** https://lumiwealth.com/blog/
**AI Strategy Builder:** https://www.botspot.trade/?utm_source=github&utm_medium=referral&utm_campaign=lumibot_readme

## Run a backtest

To run a backtest, you can use the following code snippet:

```bash
python -m lumibot.example_strategies.stock_buy_and_hold
```

## Backtesting data sources (env override)

You can select a backtesting data source via the `BACKTESTING_DATA_SOURCE` environment variable (this overrides any explicit `datasource_class` in code). When your environment already chooses the provider, call `Strategy.backtest(...)` or `Strategy.run_backtest(...)` with `datasource_class=None`:

```python
MyStrategy.backtest(
    datasource_class=None,
    backtesting_start=None,
    backtesting_end=None,
)
```

Environment-driven routing still works the same way:

```bash
# Single-provider backtesting (examples)
export BACKTESTING_DATA_SOURCE=thetadata
export BACKTESTING_DATA_SOURCE=ibkr
```

Multi-provider routing (by asset type) is supported by setting a JSON mapping:

```bash
# Example: ThetaData for stocks/options/indexes, IBKR for futures/crypto
export BACKTESTING_DATA_SOURCE='{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","crypto":"ibkr"}'

# Example: route crypto to CCXT via a specific exchange id (if desired)
export BACKTESTING_DATA_SOURCE='{"default":"thetadata","crypto":"coinbase"}'
```

## Run an Example Strategy

We made a small example strategy to show you how to use Lumibot in this GitHub repository: [Example Algorithm GitHub](https://github.com/Lumiwealth-Strategies/stock_example_algo)

To run this example strategy, click on the `Deploy to Render` button below to deploy the strategy to Render (our recommendation). You can also run the strategy on Repl.it by clicking on the `Run on Repl.it` button below.

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/Lumiwealth-Strategies/stock_example_algo)

[![Run on Repl.it](https://replit.com/badge/github/Lumiwealth-Strategies/stock_example_algo)](https://replit.com/new/github/Lumiwealth-Strategies/stock_example_algo)

**For more information on this example strategy, you can check out the README in the example strategy repository here: [Example Algorithm](https://github.com/Lumiwealth-Strategies/stock_example_algo)**

## Contributors

If you want to contribute to Lumibot, you can check how to get started below. We are always looking for contributors to help us out!

Here's a video to help you get started with contributing to Lumibot: [Watch The Video](https://youtu.be/Huz6VxqafZs)

**Steps to contribute:**

0. Watch the video: [Watch The Video](https://youtu.be/Huz6VxqafZs)
1. Clone the repository to your local machine
2. Create a new branch for your feature
3. Run `pip install -r requirements_dev.txt` to install the developer dependencies
4. Install all the requirements from setup.py: `pip install -e .`
5. Make your changes
6. Run `pytest` to make sure all the tests pass
7. Create a pull request to merge your branch into master

## Running Tests

We use pytest for our testing framework. Some tests require API keys to be in a `.env` file in the root directory. To run the tests, you can run the following command:

```bash
pytest
```

To run an individual test file, you can run the following command:

```bash
pytest tests/test_asset.py
```

## Remote Cache Configuration

Lumibot can mirror its local parquet caches to AWS S3 when you enable the new
backtest cache manager. The feature is optional and defaults to local storage.
To configure the environment variables, understand the key naming convention,
and follow the manual validation checklist, review `docs/remote_cache.md`.

### Showing Code Coverage

To show code coverage, you can run the following command:

```bash
coverage run; coverage report; coverage html
```

#### Adding an Alias on Linux or MacOS

This will show you the code coverage in the terminal and also create a folder called "htmlcov" which will have a file called "index.html". You can open this file in your browser to see the code coverage in a more readable format.

If you don't want to keep typing out the command, you can add it as an alias in bash. To do this, you can run the following command:

```bash
alias cover='coverage run; coverage report; coverage html'
```

This will now allow you to run the command by just typing "cover" in the terminal.

```bash
cover
```

If you want to also add it to your .bashrc file. You can do this by running the following command:

```bash
echo "alias cover='coverage run; coverage report; coverage html'" >> ~/.bashrc
```

#### Adding an Alias on Windows

If you are on Windows, you can add an alias by running the following command:

Add to your PowerShell Profile: (profile.ps1)

```powershell
function cover { 
 coverage run
 coverage report
 coverage html
}
```

### Setting Up PyTest in VS Code

To set up in VS Code for debugging, you can add the following to your launch.json file under "configurations". This will allow you to go into "Run and Debug" and run the tests from there, with breakpoints and everything.

NOTE: You may need to change args to the path of your tests folder.

```json
{
    "name": "Python: Pytest",
    "type": "python",
    "request": "launch",
    "module": "pytest",
    "args": [
        "lumibot/tests"
    ],
    "console": "integratedTerminal",
}
```

Here's an example of an actual launch.json file:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Pytest",
            "type": "python",
            "request": "launch",
            "module": "pytest",
            "args": [
                "lumibot/tests"
            ],
            "console": "integratedTerminal",
        }
    ]
}
```

## Notes on data sources

This table points out some of the differences between the data sources we use in Lumibot. These refer to the data 
returned in a Bars entity that is returned from calls to get_historical_prices. 

| data_source | type  | OHLCV | split adjusted | dividends | returns | dividend adjusted returns |
|-------------|-------|-------|----------------|-----------|---------|---------------------------|
| yahoo       | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |
| alpaca      | stock | Yes   | Yes            | No        | Yes     | No                        |
| polygon     | stock | Yes   | Yes            | No        | Yes     | No                        |
| Tradier     | stock | Yes   | Yes            | No        | Yes     | No                        |
| Pandas*     | stock | Yes   | Yes            | Yes       | Yes     | Yes                       |

*Pandas is not a data source per se, but it can load csv files in the same format as Yahoo dataframes,
which can contain dividends.

## An assortment of git commands our contributors may find useful

Making a new branch and pulling from main:
```shell
git checkout -b my-feature
git fetch origin
git merge origin/dev
```
Committing work to you feature branch:
```shell
git add .
git commit -m "my changes"
git push -u origin my-feature
```

If work on main progressed while you were in another branch, this is how you rebase it into your branch. Note that
since you've rebased your local branch, you'll need to force push your changes to update the remote branch. 
The --force-with-lease option is a safer alternative to --force as it will abort the push if there are any new 
commits on the remote that you haven't incorporated into your local branch
```shell
git checkout dev
git fetch origin
git merge origin/dev
git checkout my-feature
git rebase dev
git checkout my-feature
git push --force-with-lease origin my-feature
```

When ready to merge the branch into main, go into github, create a pull request, and await review. When your PR is approved it will automatically be merged into the dev branch remotely. Now, you can delete your local branch and the remote branch.
```shell
git checkout dev
git fetch origin
git merge origin/dev
git branch -D my-feature
git push origin --delete my-feature
```

## Community

If you want to learn more about Lumibot or Algorithmic Trading then you will love out communities! You can join us on Discord.

**Join us on Discord: <https://discord.gg/TmMsJCKY3T>**

**Build AI-powered trading bots on [BotSpot.trade](https://botspot.trade/)** - Our platform for creating, testing, and deploying trading strategies with AI assistance!

## AI Bootcamp

Need hands-on help building your trading bots? Join our **AI Bootcamp** where you'll learn to build, backtest, and deploy trading strategies using AI—no coding required.

**What you'll learn:**
- Build stock, crypto, options, and futures bots using plain English prompts
- Master professional tools like VS Code with Copilot and Cursor
- Backtest and deploy your strategies to live brokers
- Join 2,400+ traders in our community

**[Learn more about the AI Bootcamp](https://www.botspot.trade/ai-bot-builder-bootcamp?utm_source=github&utm_medium=referral&utm_campaign=lumibot_readme)**

---

**Just want to try the AI?** [Start your free trial at BotSpot.trade](https://www.botspot.trade/?utm_source=github&utm_medium=referral&utm_campaign=lumibot_readme) — build strategies in minutes, no coding required.

## License

This library is covered by the MIT license for open sourced software which can be found here: <https://github.com/Lumiwealth/lumibot/blob/master/LICENSE>
