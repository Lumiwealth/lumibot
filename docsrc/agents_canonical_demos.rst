Canonical AI Agent Demos
========================

LumiBot includes four canonical AI agent demo strategies that serve as both reference implementations and end-to-end acceptance tests for agentic backtesting. All four use the ``@agent_tool`` pattern with the ``requests`` library, the full built-in tool set, replay caching, and benchmarked tearsheet output.

These are complete, runnable strategies -- not snippets. They demonstrate how to backtest an AI trading agent with real external data sources, and they validate that LumiBot's AI-driven trading strategy backtest pipeline works end to end. All demo files are located in ``lumibot/example_strategies/``.

The Four Demos
---------------

- **News Sentiment Strategy** (``lumibot/example_strategies/agent_news_sentiment.py``) -- event-driven stock selection using Alpaca news data
- **Macro Risk Strategy** (``lumibot/example_strategies/agent_macro_risk.py``) -- macro regime allocation using Alpaca market data
- **Momentum Allocator Strategy** (``lumibot/example_strategies/agent_momentum_allocator.py``) -- momentum and sentiment allocation using Alpaca price bars and news
- **M2 Liquidity Strategy** (``lumibot/example_strategies/agent_m2_liquidity.py``) -- liquidity-driven allocation using FRED money supply data

News Sentiment Strategy
-----------------------

**File:** ``lumibot/example_strategies/agent_news_sentiment.py``

This strategy uses ``@agent_tool`` to call the Alpaca News API for recent stock market headlines, then lets the AI decide what to trade based on sentiment and catalysts.

**Tool:** ``search_news`` -- wraps the Alpaca News API via ``requests`` to fetch headlines, summaries, and associated stock symbols.

**What it demonstrates:**

- ``@agent_tool`` wrapping the Alpaca News REST API
- Agent-driven stock discovery from news flow
- Portfolio rotation between opportunities and a defensive parking asset (SHV)
- No-trade decisions when conviction is weak
- Replay caching of deterministic backtest runs
- Docstring with Args section for automatic source code inclusion

**What it is useful for:**

- Event-driven AI trading strategies
- Research agents that compare current holdings to new ideas
- Validating that the agent reacts to real point-in-time news, not hallucinated data

Macro Risk Strategy
-------------------

**File:** ``lumibot/example_strategies/agent_macro_risk.py``

This strategy uses ``@agent_tool`` to call the Alpaca market data API for historical price bars and market movers, then lets the AI allocate between TQQQ (risk-on) and SHV (risk-off) based on market trends.

**Tools:**

- ``get_stock_bars`` -- wraps the Alpaca bars API to fetch historical OHLCV data for any US stock
- ``get_market_movers`` -- wraps the Alpaca screener API to get top gainers and losers

**What it demonstrates:**

- Multiple ``@agent_tool`` functions in a single strategy
- Agent discovery of market trends from price data
- Binary allocation between a leveraged risk asset and a defensive asset
- De-risking during adverse market conditions
- Built-in DuckDB time-series analysis alongside custom tools
- Docstrings with Args sections for all tool parameters

**What it is useful for:**

- Macro regime AI trading strategies
- Concentrated AI strategies where concentration is intentional
- Validating entry and exit behavior across changing market conditions

Momentum Allocator Strategy
-----------------------------

**File:** ``lumibot/example_strategies/agent_momentum_allocator.py``

This strategy uses ``@agent_tool`` to call both the Alpaca bars API and the Alpaca news API, combining price momentum with news sentiment to decide between TQQQ and SHV.

**Tools:**

- ``get_stock_bars`` -- wraps the Alpaca bars API for historical price data
- ``search_news`` -- wraps the Alpaca News API for recent headlines

**What it demonstrates:**

- Combining multiple data sources (price bars + news) through ``@agent_tool``
- Momentum-based allocation with sentiment confirmation
- Agent reasoning over both quantitative and qualitative inputs
- Replay caching with multiple external tool calls per iteration

**What it is useful for:**

- Multi-factor AI trading strategies
- Strategies that combine technical and fundamental signals
- Testing how agents synthesize information from multiple tools

M2 Liquidity Strategy
----------------------

**File:** ``lumibot/example_strategies/agent_m2_liquidity.py``

This strategy uses ``@agent_tool`` to fetch real M2 money supply data from FRED (Federal Reserve Economic Data) and lets the AI allocate between TQQQ and SHV based on whether liquidity is expanding or contracting.

**Tool:** ``get_fred_series`` -- wraps the FRED public CSV endpoint via ``requests`` to fetch any of 800,000+ economic data series. No API key required.

**What it demonstrates:**

- ``@agent_tool`` wrapping a public government data API
- AI reasoning over macro and liquidity inputs
- Concentration in a single risk asset when the liquidity thesis is strong
- Defensive parking when the agent determines liquidity is contracting
- Benchmarked tearsheets and trade artifacts

**What it is useful for:**

- Long-horizon AI-guided allocation logic
- Validating defensive-asset behavior over multiple market cycles
- Checking cashflow accounting and observability in real artifacts

How to Use These Demos
----------------------

Use the demos for:

- ``@agent_tool`` patterns (wrapping REST APIs with ``requests``, docstrings with Args sections)
- Prompt design patterns (short system prompts, let LumiBot handle the rest)
- Strategy lifecycle placement (agent created in ``initialize()``, run in ``on_trading_iteration()``)
- Source code auto-inclusion (the AI sees the full function body and docstring)
- Observability and debugging (traces, summaries, warnings)
- Replay cache validation (warm reruns with zero model calls)
- Tearsheet interpretation (benchmarked against SPY)

Do not copy them blindly. Instead:

- Keep the shape that matches your use case
- Wrap your data source as an ``@agent_tool`` with proper docstrings
- Write a 2-3 sentence system prompt about your strategy
- Inspect the trace when the behavior surprises you

What to Inspect After a Run
----------------------------

For each demo, review:

- The tearsheet and benchmark comparison
- The trades chart
- ``trades.csv`` and ``trade_events.csv``
- The agent trace JSON
- The per-run summary log lines

These artifacts answer:

- Why did the agent trade (or not trade)?
- What tools did it call?
- What evidence did it use?
- Did the run replay from cache?
- Were there any observability warnings?

Related Pages
-------------

- :doc:`agents` -- main guide and architecture
- :doc:`agents_quickstart` -- code patterns and API reference
- :doc:`agents_observability` -- traces, replay cache, and debugging

Frequently Asked Questions
--------------------------

**Which demo should I start with?**

Start with ``agent_m2_liquidity.py`` if you want the simplest setup -- it only needs ``GOOGLE_API_KEY`` because FRED data is public. Start with ``agent_news_sentiment.py`` if you want a multi-stock news-driven strategy and have Alpaca API keys.

**Do these demos work out of the box?**

Yes. Set the required API keys (``GOOGLE_API_KEY`` for all demos, plus ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` for the Alpaca-based demos) and run the file directly with ``python3 agent_m2_liquidity.py``. Each demo is a complete, self-contained strategy file.

**Can I modify the demos?**

Yes. The demos are reference implementations meant to be adapted. Keep the structural pattern (agent created in ``initialize()``, run in ``on_trading_iteration()``, ``@agent_tool`` for external data) and modify the system prompt, tools, assets, and logic for your own strategy. Change the date range, add new tools, or swap the data source.

**What does each demo do?**

The News Sentiment demo discovers and trades stocks based on Alpaca news headlines. The Macro Risk demo allocates between TQQQ and SHV based on Alpaca price trends and market movers. The Momentum Allocator combines Alpaca price bars and news for momentum-plus-sentiment allocation. The M2 Liquidity demo allocates between TQQQ and SHV based on FRED money supply data.

**How do I run a demo?**

Set the required environment variables, then run the file directly: ``python3 lumibot/example_strategies/agent_news_sentiment.py``. Each demo has a ``if __name__ == "__main__"`` block that runs a backtest with default date ranges and benchmark asset (SPY).

**What external APIs do the demos use?**

The News Sentiment, Macro Risk, and Momentum Allocator demos use the Alpaca market data APIs (News API, Bars API, Screener API). The M2 Liquidity demo uses the FRED public CSV endpoint (no API key required). All demos use ``@agent_tool`` with the ``requests`` library to make HTTP calls.

**Do the demos use MCP servers?**

No. All four demos use the ``@agent_tool`` pattern exclusively. This is the recommended approach because it works reliably in both backtests and live trading. MCP servers are supported but not used in the canonical demos.

**How do the demos handle errors from external APIs?**

Each ``@agent_tool`` function wraps its HTTP call in a try/except block and returns a dictionary with an ``"error"`` key on failure. The agent sees the error result and can decide how to proceed -- for example, by making a conservative allocation instead of an aggressive one.

**What should I inspect after running a demo?**

Review the tearsheet and benchmark comparison, the trades chart, ``trades.csv`` and ``trade_events.csv``, the agent trace JSON (for tool calls and reasoning), and the per-run summary log lines. These artifacts show why the agent traded (or did not trade), what tools it called, and whether any observability warnings were raised.

**Can I change the backtest date range?**

Yes. Edit the ``backtesting_start`` and ``backtesting_end`` datetime values in the ``if __name__ == "__main__"`` block. Shorter date ranges run faster on cold runs. The replay cache stores results per simulated timestamp, so changing the date range means new cold runs for the new dates.

**Do the demos produce tearsheets?**

Yes. Every demo backtest produces a benchmarked tearsheet (compared against SPY by default), a trades chart, and CSV artifacts. These are standard LumiBot backtest outputs and are generated automatically.

**Why do some demos only trade TQQQ and SHV?**

TQQQ (3x leveraged Nasdaq) and SHV (short-term Treasury ETF) form a simple binary risk-on/risk-off pair. This makes it easy to evaluate whether the agent's macro, momentum, or liquidity thesis translates into meaningful allocation decisions. The News Sentiment demo trades a broader set of stocks discovered from news.

**Can I use a different model with the demos?**

Yes. Change the ``default_model`` parameter in the ``self.agents.create(...)`` call. The default is ``gemini-3.1-flash-lite-preview`` if not specified. You can use any model supported by the model router, though you may need to clear the replay cache when switching models since the cache key includes the model name.
