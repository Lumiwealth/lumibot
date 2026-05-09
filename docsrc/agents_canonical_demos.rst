Canonical AI Agent Demos
========================

LumiBot includes six canonical AI agent demo strategies that serve as both reference implementations and end-to-end acceptance tests for agentic backtesting. These examples cover both custom ``@agent_tool`` wrappers and built-in agent tools, the full built-in tool set, replay caching, and benchmarked tearsheet output.

These are complete, runnable strategies -- not snippets. They demonstrate how to backtest an AI trading agent with real external data sources, and they validate that LumiBot's AI-driven trading strategy backtest pipeline works end to end. All demo files are located in ``lumibot/example_strategies/``.

The Six Demos
---------------

- **Discretionary Trader** (``lumibot/example_strategies/agent_discretionary.py``) -- **maximum-discretion agent** with a one-sentence prompt, no asset whitelist, broad tool surface, and an ``AGENT_MODEL`` env var for multi-provider comparisons (Gemini, GPT, Grok, Claude)
- **Alpaca News Built-in Strategy** (``lumibot/example_strategies/agent_alpaca_news_builtin.py``) -- recommended built-in-tool pattern for Alpaca/Benzinga news: scan headlines/summaries first, fetch full article bodies on demand, and use pagination when needed
- **News Sentiment Strategy** (``lumibot/example_strategies/agent_news_sentiment.py``) -- event-driven stock selection using Alpaca news data
- **Macro Risk Strategy** (``lumibot/example_strategies/agent_macro_risk.py``) -- macro regime allocation using Alpaca market data
- **Momentum Allocator Strategy** (``lumibot/example_strategies/agent_momentum_allocator.py``) -- momentum and sentiment allocation using Alpaca price bars and news
- **M2 Liquidity Strategy** (``lumibot/example_strategies/agent_m2_liquidity.py``) -- liquidity-driven allocation using FRED money supply data

The first demo (Discretionary Trader) intentionally gives the AI maximum latitude so you can compare how different frontier models (Gemini 3.1 Pro, GPT-5.4, Grok 4.2) perform with minimal guidance. The Alpaca News Built-in Strategy is the recommended news-tool template for new code. The older News Sentiment Strategy intentionally remains as a custom ``@agent_tool`` example for users who need to wrap their own REST APIs.

Discretionary Trader
--------------------

**File:** ``lumibot/example_strategies/agent_discretionary.py``

Maximum-discretion AI trader. The user system prompt is literally one sentence: *"Make as much money as you possibly can."* Everything else -- risk discipline, drawdown protection, position sizing, look-ahead safety, tool-use guidance -- comes from LumiBot's base prompt. The agent picks its own universe (any US-listed stock or ETF via Yahoo), shorts if it wants, and decides when to park in cash-equivalents.

**Tools:**

- ``get_fred_series`` -- FRED macro data (M2SL, FEDFUNDS, CPIAUCSL, T10Y2Y, VIXCLS, DCOILWTICO, etc.)
- ``BuiltinTools.news.alpaca_news()`` -- Alpaca/Benzinga historical news with bring-your-own Alpaca credentials; scan headlines/summaries first, then fetch full article content with ``include_content=True`` when needed
- ``get_fundamentals`` -- yfinance fundamentals snapshot (P/E, forward P/E, market cap, profit margins, earnings date, analyst targets, short interest, 52W high/low, sector, industry)
- Plus all built-in tools (portfolio, positions, last_price, history, orders, DuckDB, docs)

**What it demonstrates:**

- The ``AGENT_MODEL`` env var pattern for parameterizing model choice without editing strategy code
- The standard ``BACKTESTING_START`` / ``BACKTESTING_END`` env vars for cheap short-window validation runs
- How multi-provider model comparison works in LumiBot (same strategy code, different LLM)
- Minimal user prompt + full base prompt delivering real discretionary behavior
- yfinance-based fundamentals wrapped as an ``@agent_tool`` with zero new dependencies
- Automatic token totals in the tearsheet plus a detailed ``*_agent_detail.parquet`` audit file beside the backtest artifacts

**What it is useful for:**

- Apples-to-apples multi-provider model benchmarking
- Testing how frontier LLMs reason over real market data with little guidance
- A template for building research-heavy AI strategies where the thesis is not pre-baked

**How to run it against different providers:**

.. code-block:: bash

    # Google Gemini 3.1 Pro (default)
    export GEMINI_API_KEY='your-key'
    export BACKTESTING_START='2026-03-01'
    export BACKTESTING_END='2026-03-31'
    AGENT_MODEL="gemini-3.1-pro-preview" python agent_discretionary.py

    # OpenAI GPT-5.4
    export OPENAI_API_KEY='your-key'
    AGENT_MODEL="openai/gpt-5.4" python agent_discretionary.py

    # xAI Grok 4.2 (reasoning)
    export XAI_API_KEY='your-key'
    AGENT_MODEL="xai/grok-4.20-0309-reasoning" python agent_discretionary.py

    # Anthropic Claude
    export ANTHROPIC_API_KEY='your-key'
    AGENT_MODEL="anthropic/claude-opus-4-7" python agent_discretionary.py

After the backtest finishes, the tearsheet will show the model id and cumulative token totals in **Parameters Used**, and the run directory will also contain ``*_agent_detail.parquet`` with one ``call_summary`` row per AI call plus event rows for thinking/text/tool calls/tool results/usage.

Use ``scripts/run_alpaca_news_ai_proof.py`` when you need a cheap real-provider proof that the built-in news tool retrieves relevant historical articles, fetches full content on demand, and records the calls in ``*_agent_detail.parquet``.

See ``scripts/run_discretionary_3way.py`` for a parallel runner that executes the same strategy against three providers concurrently with a memory watchdog and auto-retry on transient provider errors.

Alpaca News Built-in Strategy
-----------------------------

**File:** ``lumibot/example_strategies/agent_alpaca_news_builtin.py``

This strategy demonstrates the preferred way to give an AI agent Alpaca/Benzinga news access: pass ``BuiltinTools.news.alpaca_news()`` to ``self.agents.create(...)`` instead of writing a strategy-local Alpaca wrapper.

**What it demonstrates:**

- Built-in news-tool wiring with ``tools=[BuiltinTools.news.alpaca_news()]``
- Scan-first workflow with ``include_content=False`` and broad-market symbols like ``SPY,QQQ,DIA,IWM``
- Full article retrieval with ``include_content=True`` before trading on important stories
- Pagination via ``next_page_token`` / ``page_token`` when the first page does not provide enough evidence
- Backtest look-ahead protection through timestamp discipline and the tool's ``lookahead_clamped`` field

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

**Tool:** ``get_fred_series`` -- wraps the live public FRED graph CSV endpoint via ``requests`` for a simple revised-data macro demo. No API key is required for the demo endpoint, but strict point-in-time macro backtests should use the built-in FRED tools with ``FRED_API_KEY``.

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

Start with ``agent_m2_liquidity.py`` if you want the simplest setup -- it only needs ``GEMINI_API_KEY`` because FRED data is public. If you want to test another provider without changing strategy code, use ``agent_m2_liquidity_openai.py``, ``agent_m2_liquidity_grok.py``, or ``agent_m2_liquidity_anthropic.py``. Start with ``agent_news_sentiment.py`` if you want a multi-stock news-driven strategy and have Alpaca API keys.

**Do these demos work out of the box?**

Yes. Set the required model provider key for the demo you are running (for example ``GEMINI_API_KEY`` for Gemini, ``OPENAI_API_KEY`` for OpenAI, ``XAI_API_KEY`` or ``GROK_API_KEY`` for Grok, or ``ANTHROPIC_API_KEY`` for Claude), plus ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` for the Alpaca-based demos, and run the file directly with ``python3 agent_m2_liquidity.py``. Each demo is a complete, self-contained strategy file.

**Can I modify the demos?**

Yes. The demos are reference implementations meant to be adapted. Keep the structural pattern (agent created in ``initialize()``, run in ``on_trading_iteration()``, ``@agent_tool`` for external data) and modify the system prompt, tools, assets, and logic for your own strategy. Change the date range, add new tools, or swap the data source.

**What does each demo do?**

The News Sentiment demo discovers and trades stocks based on Alpaca news headlines. The Macro Risk demo allocates between TQQQ and SHV based on Alpaca price trends and market movers. The Momentum Allocator combines Alpaca price bars and news for momentum-plus-sentiment allocation. The M2 Liquidity demo allocates between TQQQ and SHV based on FRED money supply data.

**How do I run a demo?**

Set the required environment variables, then run the file directly: ``python3 lumibot/example_strategies/agent_news_sentiment.py``. Each demo has a ``if __name__ == "__main__"`` block that runs a backtest with default date ranges and benchmark asset (SPY).

**What external APIs do the demos use?**

The News Sentiment, Macro Risk, and Momentum Allocator demos use the Alpaca market data APIs (News API, Bars API, Screener API). The M2 Liquidity demo uses the public FRED graph CSV endpoint for a simple macro-data demo. That endpoint returns revised data, so use the built-in FRED tools with ``FRED_API_KEY`` for strict point-in-time macro backtests. All demos use ``@agent_tool`` with the ``requests`` library to make HTTP calls.

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
