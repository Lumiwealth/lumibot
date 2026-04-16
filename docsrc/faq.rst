Frequently Asked Questions (FAQ)
=================================

This page answers common questions about LumiBot. If you're new, start with the :doc:`getting_started` guide.

.. contents:: On this page
   :local:
   :depth: 2

Getting Started
---------------

What is LumiBot?
~~~~~~~~~~~~~~~~

LumiBot is an open-source Python library for backtesting and algorithmic trading. It supports stocks, options, crypto, futures, and forex across multiple brokers including Alpaca, Interactive Brokers, Tradier, Schwab, Coinbase, Kraken, and more. You write your strategy once and run it in both backtesting and live trading with the same code.

How do I install LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~

Install with pip:

.. code-block:: bash

    pip install lumibot

To upgrade to the latest version:

.. code-block:: bash

    pip install lumibot --upgrade

What Python version does LumiBot require?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot requires **Python 3.10 or higher**. This is enforced in ``setup.py`` via ``python_requires=">=3.10"``. We recommend Python 3.10 or 3.11 for the best compatibility with all features, including AI agent trading.

What is the fastest way to get a strategy running?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fastest path is a Yahoo Finance backtest -- it requires no API keys or broker accounts:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.strategies import Strategy

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                price = self.get_last_price("AAPL")
                qty = self.portfolio_value // price
                order = self.create_order("AAPL", qty, "buy")
                self.submit_order(order)

    MyStrategy.backtest(
        YahooDataBacktesting,
        datetime(2023, 1, 1),
        datetime(2023, 12, 31),
    )

What broker should I start with?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We recommend **Alpaca** for beginners because they offer free paper trading accounts with API access, commission-free stock trading, and a simple signup process. Create a free account at `alpaca.markets <https://alpaca.markets/>`_, get your API keys, and you can be trading in minutes. For options-heavy strategies, consider **Tradier** which offers $10/month commission-free options.

Do I need a broker account to backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. For backtesting, you only need a data source. Yahoo Finance is free and requires no account. Polygon.io offers a free tier. ThetaData requires a subscription but provides the best options data. You only need a broker account when you're ready to paper trade or go live.

Can I use LumiBot without writing code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to create trading strategies without coding, check out `BotSpot <https://botspot.trade/>`_ -- a platform built on LumiBot that lets you build, test, and deploy strategies using AI and natural language. LumiBot itself is a Python library and does require coding.


AI Trading Agents
-----------------

What are AI trading agents in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

AI trading agents are LLM-powered decision makers that run inside your strategy. An agent can reason over market data, call external tools (REST APIs, MCP servers), analyze time-series data with DuckDB, and submit trading orders -- all within ``on_trading_iteration()``. The agent runs on every bar during a backtest and uses the exact same code in live trading.

Why is LumiBot's approach to AI agent trading unique?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot is the **only production framework** that puts an AI agent inside the backtest simulation loop. Most platforms either bolt an LLM onto the side (no bar-by-bar reasoning), provide agent frameworks with no backtesting capability (CrewAI, AutoGen, LangGraph), or are hobby scripts without infrastructure. LumiBot combines LLM-in-the-loop reasoning, external tool access, replay caching, DuckDB queries, and full observability -- all in one framework.

Can I really backtest an AI trading agent?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. This is LumiBot's most differentiating feature. During a backtest, the AI agent runs on every bar, receives point-in-time market state (positions, cash, prices), calls external tools, reasons over the data, and submits orders -- all within the historical simulation. The replay cache then makes subsequent backtests deterministic and nearly instant. No other production framework offers this capability.

How does the AI agent backtest loop work?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On each bar (e.g., each trading day), LumiBot calls ``on_trading_iteration()``. Inside that method, you call ``self.agents["my_agent"].run()``. The agent then:

1. Receives the current simulated datetime, positions, cash, and portfolio value
2. Gets your system prompt and any context you pass
3. Reasons over all available information
4. Calls tools (built-in market data, DuckDB queries, your custom ``@agent_tool`` functions)
5. Decides whether to trade and submits orders
6. Returns a structured result with summary, tool calls, and warnings

This happens on every single bar, just as it would in live trading.

What is replay caching and why does it matter?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Replay caching means LumiBot caches every AI agent run during backtesting. When a subsequent backtest hits the same combination of prompt, context, model, tools, and simulated timestamp, the cached result is returned instantly -- zero LLM calls, zero external API calls. This gives you:

- **Deterministic backtests** -- same inputs always produce same outputs
- **Fast warm reruns** -- a 30-minute backtest completes in seconds on rerun
- **Cost control** -- no duplicate LLM or API charges when iterating on strategy parameters

The replay cache is fully automatic. No configuration needed.

What LLM providers does LumiBot support for agents?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot's agent runtime is built on `Google ADK <https://google.github.io/adk-docs/>`_ (Agent Development Kit). The default and most tested model is **Google Gemini** (``gemini-3.1-flash-lite-preview``). The architecture supports routing to other providers (OpenAI, Anthropic) through Google ADK's model router, but Gemini is the primary, production-tested path. You need a ``GOOGLE_API_KEY`` environment variable set:

.. code-block:: python

    self.agents.create(
        name="research",
        default_model="gemini-3.1-flash-lite-preview",
        system_prompt="Your strategy prompt here.",
    )

How do I create my first AI trading agent?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the agent in ``initialize()`` and run it from ``on_trading_iteration()``. You need ``GOOGLE_API_KEY`` set in your environment:

.. code-block:: python

    from lumibot.strategies import Strategy

    class SimpleAgent(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="research",
                default_model="gemini-3.1-flash-lite-preview",
                system_prompt="Analyze the market and trade conservatively.",
            )

        def on_trading_iteration(self):
            result = self.agents["research"].run()
            self.log_message(f"[research] {result.summary}", color="yellow")

The agent automatically has access to all built-in tools (positions, prices, orders, DuckDB) without listing them.

What built-in tools do AI agents get automatically?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every agent gets these built-in tools by default -- no configuration needed:

- **Account:** ``account.positions``, ``account.portfolio`` -- current holdings and portfolio state
- **Market data:** ``market.last_price``, ``market.load_history_table`` -- quotes and historical bars
- **DuckDB:** ``duckdb.query`` -- SQL queries over time-series data
- **Orders:** ``orders.submit``, ``orders.cancel``, ``orders.modify``, ``orders.open_orders``
- **Documentation:** ``docs.search`` -- search LumiBot's own API docs

When you add custom tools via ``@agent_tool``, these built-in tools remain available.

What is ``@agent_tool`` and how do I use it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``@agent_tool`` is a decorator that wraps a Python method as a callable tool that the AI agent can invoke. It's the recommended way to give your agent access to external data. A key feature is **automatic source code inclusion** -- the function's source code and docstring are sent to the AI so it understands how to call the tool:

.. code-block:: python

    from lumibot.components.agents import agent_tool

    @agent_tool(
        name="get_fred_series",
        description="Fetch economic data from FRED.",
    )
    def get_fred_series(self, series_id: str) -> dict:
        """Fetch a FRED series.

        Args:
            series_id: FRED series identifier (e.g., M2SL)
        """
        resp = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}")
        return parse_response(resp)

What are MCP servers and how do they work with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MCP (Model Context Protocol) servers are external services that provide tools and data to AI agents. LumiBot can connect to any MCP-compatible server via URL -- there are over 20,000 available today covering news, economic data, filings, social sentiment, and more. While MCP servers work well for live trading, the ``@agent_tool`` pattern (wrapping REST APIs directly) is more reliable for backtesting:

.. code-block:: python

    from lumibot.components.agents import MCPServer

    self.agents.create(
        name="research",
        default_model="gpt-4.1-mini",
        system_prompt="Your strategy prompt.",
        mcp_servers=[
            MCPServer(name="my-server", url="https://my-mcp-server.example.com/mcp"),
        ],
    )

How long should my AI agent's system prompt be?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Keep it to **2-3 sentences** describing your strategy intent. LumiBot's base prompt already handles position sizing, DuckDB usage, backtesting safety, time management, and trading rules. Don't repeat those instructions. Example:

.. code-block:: python

    system_prompt=(
        "Use economic data to decide whether capital should be in TQQQ "
        "or SHV. Focus on M2 liquidity trends and interest rates."
    )

How does DuckDB work with AI agents?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the agent needs to analyze historical price data, LumiBot loads it into DuckDB tables automatically. The agent calls the built-in ``market.load_history_table`` tool to create a table, then queries it with ``duckdb.query`` using standard SQL for moving averages, volatility, or any other analysis. No DuckDB configuration is needed -- it's part of the default agent runtime.

What happens if my agent makes a bad trading decision?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The agent's decisions go through LumiBot's normal order execution pipeline. You can add guardrails in your strategy code (position limits, cash reserves, etc.) and review the agent's reasoning through the observability system. The structured trace records every tool call, every piece of evidence, and the agent's reasoning, so you can audit exactly why any decision was made.

How do I debug an AI agent's decisions?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot provides a full observability system:

1. **Summary log lines** -- every run emits agent name, model, cache status, tool count, and summary
2. **Structured JSON traces** -- the full record of prompts, tool calls, results, and reasoning
3. **Warning system** -- flags suspicious conditions (no tools called, future-dated data, unsupported orders)
4. Access the trace path via ``(result.payload or {}).get("trace_path")``

See :doc:`agents_observability` for the complete debugging workflow.

What observability warnings should I watch for?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The warning system flags conditions that deserve investigation:

- **No tools called** -- the agent decided without consulting any tools
- **Tool error** -- a tool returned an error
- **Future-dated data** -- a tool result references data after the simulated backtest time (look-ahead bias)
- **Unsupported order** -- an order was submitted without visible evidence in the trace

Warnings don't automatically invalidate a run, but they are a strong signal to review.

Does the same agent code work for backtesting and live trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, this is a core design principle. Your strategy code is identical for backtests and live trading. The only difference is how you launch it -- call ``.backtest()`` for backtesting or use a ``Trader`` with a broker for live. The agent, tools, prompts, and logic are the same.

What are the canonical demo strategies for AI agents?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot ships four reference demo strategies in ``lumibot/example_strategies/``:

1. **News Sentiment** (``agent_news_sentiment.py``) -- event-driven stock selection using Alpaca news
2. **Macro Risk** (``agent_macro_risk.py``) -- macro regime allocation using Alpaca market data
3. **Momentum Allocator** (``agent_momentum_allocator.py``) -- momentum + sentiment using price bars and news
4. **M2 Liquidity** (``agent_m2_liquidity.py``) -- liquidity-driven allocation using FRED money supply data

Start with the M2 Liquidity demo -- it only needs ``GOOGLE_API_KEY`` since FRED data is public.

How much does it cost to run AI agent backtests?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first run of an AI agent backtest incurs one LLM API call per bar. For example, a daily strategy over 5 years is ~1,260 Gemini API calls. With replay caching, all subsequent reruns are **free** -- zero LLM calls. This makes iterating on strategy parameters or re-running for reporting extremely cost-effective. The Gemini models used by default are among the most affordable LLM APIs available.

Can I use multiple AI agents in a single strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Create multiple agents in ``initialize()`` with different names, prompts, and tools. Run them from any lifecycle method:

.. code-block:: python

    def initialize(self):
        self.agents.create(name="macro", default_model="gemini-3.1-flash-lite-preview",
                           system_prompt="Analyze macro conditions.")
        self.agents.create(name="technicals", default_model="gemini-3.1-flash-lite-preview",
                           system_prompt="Analyze technical indicators.")

    def on_trading_iteration(self):
        macro = self.agents["macro"].run()
        technicals = self.agents["technicals"].run()
        # Combine insights and trade

Can agents call tools that fetch real-time external data during backtests?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. When you use ``@agent_tool`` to wrap a REST API, the agent calls it during the backtest just as it would in live trading. The data returned is real external data. The replay cache then stores those results so subsequent reruns are deterministic and fast. Note that you should be mindful of look-ahead bias -- the observability system will warn you if tool results contain future-dated data.

How does LumiBot prevent look-ahead bias in AI agent backtests?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot provides the agent with the simulated datetime (not the real current time) and warns if tool results contain data published after the simulated time. The ``@agent_tool`` pattern gives you control over what date parameters are passed to external APIs. Always use ``self.get_datetime()`` for the current date -- never ``datetime.now()``.

What is the ``AgentRunResult`` object?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you call ``self.agents["name"].run()``, you get back an ``AgentRunResult`` with:

- ``result.summary`` -- the agent's concluding summary
- ``result.text`` -- full text output
- ``result.cache_hit`` -- whether the result was replayed from cache
- ``result.warning_messages`` -- list of observability warnings
- ``result.tool_calls`` -- list of tool call events
- ``result.tool_results`` -- list of tool result events
- ``(result.payload or {}).get("trace_path")`` -- path to JSON trace file


Backtesting
-----------

What data sources are available for backtesting?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot supports multiple backtesting data sources:

- **Yahoo Finance** -- free, daily stock/ETF data. Best for quick prototyping.
- **ThetaData** -- premium stocks and options data with deep history. Recommended for options.
- **Polygon.io** -- stocks, options, crypto, forex. Free tier available.
- **DataBento** -- high-quality data for stocks, futures, and options. Only source for futures backtesting.
- **Interactive Brokers REST** -- via the LumiBot Data Downloader.
- **Pandas** -- bring your own CSV or custom data. Advanced users.

Which data source should I use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Quick prototyping (stocks):** Yahoo Finance -- free, no setup
- **Options backtesting:** ThetaData -- best coverage and quality. Use promo code ``BotSpot10`` for 10% off.
- **Futures backtesting:** DataBento -- only source that supports futures
- **Crypto backtesting:** Polygon.io or CCXT
- **Custom data:** Pandas backtesting with your own CSV files

How do I add trading fees to my backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``TradingFee`` class to model realistic costs:

.. code-block:: python

    from lumibot.entities import TradingFee

    # Stocks: percentage of order value
    stock_fee = TradingFee(percent_fee=0.001)  # 0.1%

    # Options: per-contract fee
    option_fee = TradingFee(per_contract_fee=0.65)  # $0.65/contract

    # Flat fee per order
    flat_fee = TradingFee(flat_fee=5.00)

    MyStrategy.backtest(
        datasource, start, end,
        buy_trading_fees=[stock_fee],
        sell_trading_fees=[stock_fee],
    )

What files does a backtest generate?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Backtests produce several output files, all prefixed with the strategy name and date:

- **Tearsheet HTML** -- interactive performance report with charts, metrics, and benchmark comparison
- **trades.csv** -- every trade with entry/exit prices, PnL, and timing
- **stats.csv** -- per-bar portfolio statistics
- **indicators.csv** -- custom indicator values from ``add_line()`` / ``add_marker()``
- **logs.csv** -- strategy log messages

See :doc:`backtesting` for details on each file.

Why does my minute-level backtest fail with "no data"?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most data sources limit minute-level data to about 2 years of history. Set your start date accordingly. For longer backtests, use daily data (``self.sleeptime = "1D"``).

Can I set backtest parameters via environment variables?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. LumiBot supports several environment variables for backtest configuration:

- ``IS_BACKTESTING`` -- ``True`` to enable backtesting mode
- ``BACKTESTING_START`` / ``BACKTESTING_END`` -- date range (``YYYY-MM-DD``)
- ``BACKTESTING_BUDGET`` -- starting cash (e.g., ``100000``)
- ``BACKTESTING_DATA_SOURCE`` -- data source (``yahoo``, ``polygon``, ``thetadata``, etc.)
- ``BACKTESTING_PARAMETERS`` -- JSON string of strategy parameters

See :doc:`environment_variables` for the full list.

How do I benchmark my strategy against an index?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass ``benchmark_asset`` to the backtest function:

.. code-block:: python

    MyStrategy.backtest(
        YahooDataBacktesting,
        start, end,
        benchmark_asset="SPY",
    )

The tearsheet will show your strategy's performance compared to the benchmark.

Can I backtest options strategies?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use ThetaData or Polygon as your data source (Yahoo does not support options). LumiBot provides ``get_chains()``, ``get_greeks()``, and the ``OptionsHelper`` class for reliable option selection. See :doc:`options_helper` for best practices.

Can I backtest futures strategies?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use DataBento as your data source and continuous futures (``Asset.AssetType.CONT_FUTURE``) for seamless multi-year backtesting without rollover complexity:

.. code-block:: python

    from lumibot.entities import Asset
    asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)


Strategy Development
--------------------

What are lifecycle methods?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lifecycle methods are the hooks where you implement your trading logic. They run in a specific order:

- ``initialize()`` -- called once at strategy start. Set up variables, create agents.
- ``before_market_opens()`` -- called before market opens each day
- ``on_trading_iteration()`` -- **main strategy logic**, runs on every bar/iteration
- ``before_market_closes()`` -- called before market close each day
- ``after_market_closes()`` -- called after market close each day
- ``on_filled_order()`` -- called when an order fills
- ``on_bot_crash()`` -- called if the strategy crashes
- ``trace_stats()`` -- log custom statistics each iteration

Why can't I use ``datetime.now()`` in my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

During backtesting, ``datetime.now()`` returns the real current time, not the simulated historical time. Your strategy will think it's 2026 when it should be simulating 2020. **Always use** ``self.get_datetime()``:

.. code-block:: python

    # WRONG
    current_time = datetime.now()

    # CORRECT
    current_time = self.get_datetime()

This is the single most common mistake in LumiBot strategies.

Why do my variables reset between iterations?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Local variables inside ``on_trading_iteration()`` are reset each time it runs. Use ``self.vars`` for persistent state:

.. code-block:: python

    def initialize(self):
        self.vars.trade_count = 0

    def on_trading_iteration(self):
        self.vars.trade_count += 1

``self.vars`` is backed up automatically and can survive bot restarts when a database connection is configured.

Why does ``from __future__ import annotations`` break my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This import breaks LumiBot's internal type checking system and causes backtests to crash. Simply remove it -- it's never needed in LumiBot strategies.

How do I store persistent variables that survive restarts?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``self.vars`` and set the ``DB_CONNECTION_STR`` environment variable to a database connection string. LumiBot automatically backs up ``self.vars`` after each trading iteration and loads it on startup.

What is ``self.sleeptime`` and how do I set it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.sleeptime`` controls how often ``on_trading_iteration()`` runs. Set it in ``initialize()``:

.. code-block:: python

    def initialize(self):
        self.sleeptime = "1D"    # Once per day
        self.sleeptime = "1H"    # Once per hour (for live trading)
        self.sleeptime = "180M"  # Every 180 minutes
        self.sleeptime = "15S"   # Every 15 seconds (crypto)

For live trading, this is the actual sleep interval. In backtesting, it determines the bar granularity.

How do I set custom parameters for my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Define a ``parameters`` dict on your class, then access them with ``self.parameters``:

.. code-block:: python

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "SPY",
            "quantity": 10,
            "sma_period": 20,
        }

        def on_trading_iteration(self):
            symbol = self.parameters["symbol"]
            period = self.parameters["sma_period"]

Parameters can be overridden at runtime via ``BACKTESTING_PARAMETERS`` environment variable.

What order types does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot supports: **market**, **limit**, **stop**, **stop-limit**, **trailing stop**, **bracket** (entry + take-profit + stop-loss), **OTO** (one-triggers-other), **OCO** (one-cancels-other), and **SMART_LIMIT** (timed ladder across bid/ask spread for options). Create orders with ``self.create_order()`` and submit with ``self.submit_order()``.

What is a SMART_LIMIT order?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SMART_LIMIT orders walk the bid/ask spread using a timed ladder, designed for realistic fills on wide-spread assets like options. In backtests, they fill at mid price plus/minus slippage:

.. code-block:: python

    from lumibot.entities import SmartLimitConfig, SmartLimitPreset

    config = SmartLimitConfig(preset=SmartLimitPreset.NORMAL, slippage=0.05)
    order = self.create_order(symbol, quantity, side, smart_limit=config)
    self.submit_order(order)


Options Trading
---------------

How do I trade options in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create an option asset with ``Asset`` specifying the underlying symbol, expiration, strike, and right:

.. code-block:: python

    from lumibot.entities import Asset
    import datetime

    option = Asset(
        symbol="SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.date(2025, 1, 17),
        strike=450,
        right="call"
    )
    order = self.create_order(option, 1, "buy")
    self.submit_order(order)

What is OptionsHelper and why should I use it?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``OptionsHelper`` is LumiBot's high-level helper for options selection (expirations, strikes, deltas) and multi-leg order building. It's faster and more reliable than manually scanning strikes because it uses bounded probing with caching instead of brute-force scanning:

.. code-block:: python

    def initialize(self):
        self.options_helper = OptionsHelper(self)

    # Find 20-delta put
    strike = self.options_helper.find_strike_for_delta(
        underlying_asset=spy, underlying_price=float(price),
        target_delta=-0.20, expiry=expiry, right="put",
    )

Why is my options backtest slow?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most common cause is brute-force delta or strike selection -- scanning many strikes and calling ``get_greeks()`` per strike. Each call can trigger additional data downloads. Use ``OptionsHelper.find_strike_for_delta()`` instead, which uses bounded probing and caching. Also call ``self.get_chains()`` once per iteration and reuse the result.

Why are my option position sizes off by 100x?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Options have a 100x multiplier. A $1.50 premium costs $150 per contract. Always account for this:

.. code-block:: python

    option_price = 1.50
    cost_per_contract = option_price * 100  # $150
    contracts = int(budget / cost_per_contract)

Should I use ``get_last_price()`` or ``get_quote()`` for options?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``get_quote()`` for options. ``get_last_price()`` returns the last trade, which can be very stale for illiquid options. ``get_quote()`` returns the current bid/ask, which is more reliable:

.. code-block:: python

    quote = self.get_quote(option_asset)
    if quote and quote.bid and quote.ask:
        fair_price = quote.mid_price


Crypto Trading
--------------

How do I set up crypto trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot uses CCXT to connect to crypto exchanges (Coinbase, Kraken, Binance, Kucoin, etc.). Set your exchange API keys as environment variables and call ``set_market("24/7")`` in ``initialize()``:

.. code-block:: python

    def initialize(self):
        self.set_market("24/7")  # Required for crypto
        self.sleeptime = "1M"

    # Create crypto asset
    btc = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)

Why does my crypto bot stop trading at 4pm?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot defaults to US stock market hours. You **must** call ``self.set_market("24/7")`` in ``initialize()`` for crypto strategies. Without it, your bot will stop trading when the stock market closes.

How do I close a crypto futures position?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For ``Asset.AssetType.CRYPTO_FUTURE``, use ``self.close_position()`` instead of creating a sell order. A sell order opens a new short position rather than closing the existing long:

.. code-block:: python

    # WRONG - opens new position
    self.submit_order(self.create_order(asset, qty, "sell"))

    # CORRECT - closes existing position
    self.close_position(asset)


Futures Trading
---------------

How do I backtest futures in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use continuous futures (``Asset.AssetType.CONT_FUTURE``) with DataBento as the data source. Continuous futures eliminate rollover complexity:

.. code-block:: python

    from lumibot.entities import Asset

    es = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
    mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

What types of futures contracts does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot supports three types:

1. **Continuous Futures** (``CONT_FUTURE``) -- recommended for backtesting, no expiration management
2. **Specific Expiry Futures** (``FUTURE``) -- for live trading with exact expiration dates
3. **Auto-Expiry Futures** -- automatically select front month or next quarter contracts

How should I handle futures fees in backtesting?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``TradingFee`` with ``per_contract_fee``:

.. code-block:: python

    from lumibot.entities import TradingFee

    # Standard futures: $0.85/contract, Micros: $0.50/contract
    fee = TradingFee(per_contract_fee=0.85)

    MyStrategy.backtest(datasource, start, end,
        buy_trading_fees=[fee], sell_trading_fees=[fee])


Brokers
-------

What brokers does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot supports the following brokers:

- **Alpaca** -- stocks, options, crypto (free paper trading)
- **Interactive Brokers** -- stocks, options, futures, forex (global)
- **Tradier** -- stocks, options ($10/month commission-free options)
- **Schwab** -- stocks, options (via Trader API)
- **Tradovate** -- futures (CME Group markets)
- **ProjectX** -- futures
- **CCXT-based exchanges** -- Coinbase, Kraken, Binance, Kucoin, Bitunix, and many more crypto exchanges

How do I configure my broker credentials?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The easiest method is a ``.env`` file in your project root. LumiBot auto-detects it:

.. code-block:: bash

    # Alpaca
    ALPACA_API_KEY=your_key
    ALPACA_API_SECRET=your_secret
    ALPACA_IS_PAPER=true

    # Or Tradier
    TRADIER_ACCESS_TOKEN=your_token
    TRADIER_ACCOUNT_NUMBER=your_account
    TRADIER_IS_PAPER=true

Then simply create your broker without any configuration -- LumiBot uses the environment variables automatically.

Can I use a different broker for data vs trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Set the ``TRADING_BROKER`` and ``DATA_SOURCE`` environment variables separately. For example, use Tradovate for futures execution and DataBento for market data:

.. code-block:: bash

    TRADING_BROKER=tradovate
    DATA_SOURCE=databento

Should I start with paper trading or live trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Always start with paper trading.** Every supported broker offers paper trading accounts. Paper trade for at least several days to verify your strategy behaves correctly before risking real money. Alpaca's free paper trading is the fastest way to start.

Are my strategies broker-agnostic?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, by design. Strategy code should contain only trading logic -- broker configuration is handled via environment variables at deployment time. The same strategy can run on Alpaca, Interactive Brokers, Tradier, or any other supported broker without code changes.


Data and Market Information
---------------------------

How do I get the current price of an asset?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``self.get_last_price(asset)`` for the latest tick price. Always check for ``None``:

.. code-block:: python

    price = self.get_last_price("AAPL")
    if price is None:
        self.log_message("No price data", color="red")
        return

For bid/ask data, use ``self.get_quote(asset)`` which returns bid, ask, and mid_price.

How do I get historical price data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``self.get_historical_prices(asset, length, timestep)``:

.. code-block:: python

    # Get 20 daily bars
    bars = self.get_historical_prices("AAPL", 20, "day")
    df = bars.df  # Pandas DataFrame with open, high, low, close, volume

    # Get 60 minute bars
    bars = self.get_historical_prices("AAPL", 60, "minute")

What is the difference between ``get_last_price()`` and ``get_historical_prices()``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``get_last_price()`` returns the most recent tick/trade price -- use it for current pricing. ``get_historical_prices()`` returns completed OHLCV bars and may be delayed by up to a minute. Use ``get_last_price()`` for real-time decisions and ``get_historical_prices()`` for technical analysis.


Deployment
----------

How do I deploy my strategy to run 24/7?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot strategies can be deployed to **Render** ($7/month) or **Replit** ($25/month). Both platforms run your strategy as a background worker. Render is recommended for most users. See :doc:`deployment` for step-by-step instructions.

What environment variables do I need for deployment?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At minimum, you need your broker's API credentials. Common variables:

- Broker credentials (varies by broker)
- ``TRADING_BROKER`` -- which broker to use
- ``DATA_SOURCE`` -- market data source (if different from broker)
- ``DB_CONNECTION_STR`` -- database for variable persistence (optional)
- ``DISCORD_WEBHOOK_URL`` -- for trade notifications (optional)

See :doc:`deployment` for broker-specific configuration tables.

Can I run my strategy locally?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Just run your Python script. For live trading, your computer needs to stay on and connected. For backtesting, local execution is the default. Many traders run strategies on a local machine or home server, though cloud deployment is more reliable for 24/7 operation.


Debugging and Troubleshooting
-----------------------------

Why does my strategy crash with a NoneType error?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most common cause is not checking if ``get_last_price()`` returns ``None``. Always check:

.. code-block:: python

    price = self.get_last_price(asset)
    if price is None:
        self.log_message("No price data", color="red")
        return

The same applies to ``get_chains()``, ``get_greeks()``, and ``get_quote()``.

Should I use ``print()`` or ``self.log_message()``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``self.log_message()`` -- it integrates with LumiBot's logging system and supports colored output:

.. code-block:: python

    self.log_message("Trade executed", color="green")
    self.log_message("Warning: low volume", color="yellow")
    self.log_message("Error: no data", color="red")

Why shouldn't I use ``time.sleep()`` in my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``time.sleep()`` blocks the entire bot, preventing important operations from running. Instead, use ``self.vars`` to track state across iterations and let LumiBot's ``sleeptime`` handle the timing between iterations.

Why can't I assign attributes directly on ``self``?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Assigning arbitrary attributes on ``self`` (like ``self.name``, ``self.asset``, ``self.symbol``) can collide with LumiBot framework internals and cause crashes. Always use ``self.vars`` for custom state:

.. code-block:: python

    # WRONG
    self.name = "MyBot"

    # CORRECT
    self.vars.strategy_label = "MyBot"

How do I use chart markers and lines for visual debugging?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``add_line()`` for continuous data (indicators) and ``add_marker()`` for infrequent events (signals). Always pass the ``asset`` parameter to overlay on the price chart, and use ``detail_text`` (not ``text``) for hover information:

.. code-block:: python

    self.add_line("SMA_20", sma_value, color="blue", asset=my_asset)

    if signal:
        self.add_marker("Buy Signal", price, color="green", asset=my_asset)

Never add markers every iteration -- it will crash the chart.


Advanced Topics
---------------

What is cash accounting in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cash accounting lets you track deposits, withdrawals, dividends, fees, interest, and other cash events separately from strategy performance. This keeps external cashflows out of your performance metrics. Use ``self.deposit_cash()``, ``self.withdraw_cash()``, and ``self.adjust_cash()`` in your strategy. See :doc:`cash_accounting` for the full guide.

How do I use ``trace_stats()`` for custom metrics?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Override ``trace_stats()`` to log custom statistics each iteration. Return a dict of values:

.. code-block:: python

    def trace_stats(self, context, snapshot_before):
        return {
            "sma_20": self.vars.get("sma_value", 0),
            "signal": self.vars.get("current_signal", "none"),
        }

These values appear in the stats CSV and can be plotted.

Can I run multiple strategies simultaneously?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use the ``Trader`` class to run multiple strategies:

.. code-block:: python

    trader = Trader()
    trader.add_strategy(strategy_1)
    trader.add_strategy(strategy_2)
    trader.run_all()

Each strategy operates independently with its own positions and cash allocation.

How do I handle the ``on_filled_order`` callback?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Override ``on_filled_order()`` to react when an order fills:

.. code-block:: python

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(
            f"Filled: {order.side} {quantity} {order.asset.symbol} @ {price}",
            color="green"
        )

This is useful for logging, adjusting state, or triggering follow-up orders.

How does LumiBot handle market hours?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, LumiBot follows US stock market hours (9:30 AM - 4:00 PM ET). Lifecycle methods like ``before_market_opens()`` and ``after_market_closes()`` run at the appropriate times. For crypto or forex, call ``self.set_market("24/7")`` in ``initialize()`` to enable 24/7 trading. The ``minutes_before_closing`` property tells you how many minutes remain before market close.

What is the ``first_iteration`` property?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.first_iteration`` is ``True`` only on the first call to ``on_trading_iteration()``. Use it to execute one-time setup logic like initial position entry:

.. code-block:: python

    def on_trading_iteration(self):
        if self.first_iteration:
            # Only runs once
            order = self.create_order("SPY", 100, "buy")
            self.submit_order(order)

What is the ``is_backtesting`` property?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.is_backtesting`` is ``True`` when running in backtest mode, ``False`` when live. You can use it to add backtest-specific logging or skip certain live-only operations, but your core strategy logic should be the same for both modes.
