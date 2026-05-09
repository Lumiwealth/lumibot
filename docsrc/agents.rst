AI Trading Agents and Agentic Backtesting
==========================================

LumiBot is the only production framework that lets an AI agent reason, call external tools, and execute trades **on every bar during a backtest** -- then run the exact same strategy code live. Whether you use ``@agent_tool`` to wrap any REST API as a callable tool or connect to one of 20,000+ external `MCP servers <https://modelcontextprotocol.io/>`_, LumiBot handles it in one unified codebase. A built-in replay cache makes warm reruns deterministic and fast. Whether you want to backtest an AI trading agent, build an agentic backtesting framework, or connect LLM-driven trading bots to live brokers, LumiBot handles it all.

.. toctree::
   :maxdepth: 1

   agents_quickstart
   agents_flows
   agents_builtin_tools
   agents_investment_committee
   agents_canonical_demos
   agents_observability
   agents_memory
   agents_notifications

Why This Is Different
---------------------

Most tools that combine LLMs and trading fall into one of three categories:

1. **LLM outside the loop.** Platforms like QuantConnect let you call an LLM externally, but the model is not part of the backtest simulation. It cannot reason over point-in-time data on each bar.
2. **Agent frameworks with no backtesting.** CrewAI, AutoGen, and LangGraph build multi-agent workflows, but none of them can simulate a trading backtest where the agent makes decisions bar by bar against historical data.
3. **Hobby scripts with no infrastructure.** Open-source experiments wire GPT to a broker, but they lack MCP support, replay caching, DuckDB time-series queries, and the observability needed for production.

LumiBot is different because it combines all of these in one framework:

- **LLM in the loop on every bar.** The AI agent runs inside ``on_trading_iteration()``, receives point-in-time market state, calls tools, reasons, and submits orders -- all within the backtest simulation.
- **@agent_tool for reliable external data.** Wrap any REST API as a callable tool using the ``@agent_tool`` decorator and the ``requests`` library. This is the primary and recommended pattern because it works reliably in both backtests and live trading.
- **MCP server support.** Connect to any MCP-compatible server with a URL for live trading or when you have a compatible server. There are over 20,000 MCP servers available today.
- **Replay caching for deterministic backtests.** Identical prompt + context + tools + timestamp = cached result. Warm reruns complete in seconds with zero model calls.
- **Any LLM provider.** Use OpenAI, Anthropic, Google Gemini, xAI Grok, or any provider supported by the underlying model router. Swap models with a single env var; ``@agent_tool`` functions and replay cache work unchanged across all providers.
- **Automatic retry on transient provider errors.** Rate limits (429), server errors (500/503/529), and transient network blips are retried automatically with exponential backoff. Production agents stay alive through normal cloud-provider hiccups without strategy-level error handling.
- **Same code for backtest and live.** No separate "backtest mode" strategy. Write once, backtest it, deploy it.

Quick Start
-----------

Here is a complete AI trading agent strategy that uses ``@agent_tool`` to fetch economic data from FRED and make trading decisions:

.. code-block:: python

    import csv
    import io
    import os
    import requests

    from lumibot.components.agents import agent_tool
    from lumibot.strategies import Strategy


    class M2LiquidityStrategy(Strategy):

        @agent_tool(
            name="get_fred_series",
            description=(
                "Fetch economic data from FRED (Federal Reserve Economic Data). "
                "Common series: M2SL (M2 money supply), FEDFUNDS (fed funds rate), "
                "CPIAUCSL (CPI), UNRATE (unemployment), GDP, T10Y2Y (yield spread). "
                "Returns date-value pairs."
            ),
        )
        def get_fred_series(
            self, series_id: str, start_date: str = "2020-01-01", end_date: str = ""
        ) -> dict:
            """Fetch a FRED series using the public CSV endpoint.

            Args:
                series_id: FRED series identifier (e.g., M2SL, FEDFUNDS, CPIAUCSL)
                start_date: Start date in YYYY-MM-DD format
                end_date: End date in YYYY-MM-DD format
            """
            params = {"id": series_id}
            if start_date:
                params["cosd"] = start_date
            if end_date:
                params["coed"] = end_date
            try:
                resp = requests.get(
                    "https://fred.stlouisfed.org/graph/fredgraph.csv",
                    params=params, timeout=15,
                )
                resp.raise_for_status()
                reader = csv.DictReader(io.StringIO(resp.text))
                observations = []
                for row in reader:
                    date = row.get("observation_date", "")
                    value = row.get(series_id, ".")
                    if value and value != ".":
                        observations.append({"date": date, "value": float(value)})
                return {"series_id": series_id, "count": len(observations), "observations": observations}
            except Exception as e:
                return {"error": str(e), "series_id": series_id}

        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="m2_analyst",
                default_model="gpt-4.1-mini",
                system_prompt=(
                    "Use money supply and liquidity data to decide between "
                    "TQQQ and SHV. Focus on whether M2 liquidity is expanding "
                    "or contracting."
                ),
                tools=[self.get_fred_series],
            )

        def on_trading_iteration(self):
            result = self.agents["m2_analyst"].run()
            self.log_message(f"[m2_analyst] {result.summary}", color="yellow")

    if __name__ == "__main__":
        IS_BACKTESTING = True
        if IS_BACKTESTING:
            from datetime import datetime
            M2LiquidityStrategy.backtest(
                datasource_class=None,
                backtesting_start=datetime(2020, 1, 1),
                backtesting_end=datetime(2026, 3, 1),
                benchmark_asset="SPY",
            )

That is the entire strategy file. No local MCP server scripts, no npm installs, no explicit built-in tool lists. The ``@agent_tool`` decorator wraps a standard REST API call using the ``requests`` library. LumiBot includes all built-in tools by default alongside your custom tools.

How ``@agent_tool`` Works
-------------------------

The ``@agent_tool`` decorator is the primary way to give your AI agent access to external data. It wraps a Python method as a callable tool that the agent can invoke during its reasoning loop.

**Key feature: automatic source code inclusion.** When you decorate a method with ``@agent_tool``, LumiBot automatically includes the function's source code in the tool description sent to the AI. This means the AI can see all parameters, default values, and implementation details without you having to describe them manually. Write a clear docstring with an ``Args`` section, and the AI will understand how to call your tool correctly.

The introductory FRED examples on this page use the public graph CSV endpoint because it keeps the sample short. That endpoint returns revised data. For strict point-in-time macro backtests, use the built-in FRED tools with ``FRED_API_KEY`` instead.

.. code-block:: python

    @agent_tool(
        name="search_news",
        description="Search recent stock market news from Alpaca.",
    )
    def search_news(
        self, start: str = "", end: str = "", symbols: str = "", limit: int = 10
    ) -> dict:
        """Call the Alpaca News API for historical news.

        Args:
            start: Start timestamp in ISO format
            end: End timestamp in ISO format
            symbols: Comma-separated stock symbols to filter by
            limit: Maximum number of articles to return
        """
        # The AI sees this entire function body automatically
        resp = requests.get("https://data.alpaca.markets/v1beta1/news", ...)
        return resp.json()

When you pass custom tools via ``tools=[self.my_tool]``, they are added **alongside** the default built-in tools. You only need to list your custom tools -- built-in tools are always included.

External Data Patterns
----------------------

**Pattern 1: @agent_tool wrapping a REST API (recommended)**

This is the primary and recommended approach. It works reliably in both backtests and live trading because you control the HTTP call directly.

.. code-block:: python

    import os
    import requests
    from lumibot.components.agents import agent_tool

    @agent_tool(
        name="get_stock_bars",
        description="Get historical daily price bars for a stock from Alpaca.",
    )
    def get_stock_bars(
        self, symbol: str, start: str = "", end: str = "", limit: int = 30
    ) -> dict:
        """Get historical OHLCV bars from the Alpaca market data API.

        Args:
            symbol: Stock ticker symbol (e.g., TQQQ, SPY, QQQ)
            start: Start date in YYYY-MM-DD or ISO format
            end: End date in YYYY-MM-DD or ISO format
            limit: Maximum number of bars to return
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        params = {"timeframe": "1Day", "limit": limit, "sort": "desc"}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        resp = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{symbol}/bars",
            headers=headers, params=params, timeout=15,
        )
        return resp.json()

This pattern works with any REST API -- Alpaca, FRED, Alpha Vantage, or your own internal services. All four demo strategies use this approach.

**Pattern 2: MCP server via URL (for live trading or compatible servers)**

If you have a compatible MCP server, you can connect it by URL. This is useful for live trading scenarios or when a third-party provides a dedicated MCP server.

.. code-block:: python

    from lumibot.components.agents import MCPServer

    MCPServer(
        name="my-data-server",
        url="https://my-mcp-server.example.com/mcp",
        timeout_seconds=120,
    )

Any MCP server that speaks the Model Context Protocol over HTTP or Streamable HTTP works with LumiBot. There are over 20,000 MCP servers available today covering news, economic data, filings, social sentiment, and more.

Built-in Tools
--------------

LumiBot includes a full set of built-in trading tools that are available to every agent **by default**. You do not need to list them explicitly. Even when you add custom tools via ``@agent_tool`` or MCP servers, the built-in tools remain available.

The built-in tools cover everything a trading agent needs:

- **Account:** ``account.positions``, ``account.portfolio`` -- current holdings and portfolio state
- **Market data:** ``market.last_price``, ``market.load_history_table`` -- real-time quotes and historical bars
- **DuckDB:** ``duckdb.query`` -- SQL queries over time-series data loaded into DuckDB tables
- **Orders:** ``orders.submit``, ``orders.cancel``, ``orders.modify``, ``orders.open_orders`` -- full order management
- **Documentation:** ``docs.search`` -- search LumiBot's own API docs for guidance

These tools give the agent access to positions, prices, history, and order execution without any setup. If you want to add external data on top of these, use ``@agent_tool`` or add MCP servers.

System Prompts
--------------

LumiBot handles all the common instructions internally through its base prompt. The base prompt tells the agent:

- Whether the run is a backtest or live trading
- The current datetime and timezone
- Current positions, cash, and portfolio values
- Rules about look-ahead bias and backtesting safety
- Default investor policy (conviction over activity, no overtrading)
- Position sizing, order execution, and limit order preferences
- DuckDB conventions and tool usage guidance

**Your system prompt should be 2-3 sentences about your strategy.** LumiBot handles the rest.

.. code-block:: python

    system_prompt=(
        "Use economic data to decide whether capital should be in TQQQ "
        "or a defensive asset like SHV. Check interest rates, inflation, "
        "and growth conditions. This is a binary allocator."
    )

Do not repeat instructions about position sizing, time safety, or tool usage. LumiBot already covers those in the base prompt.

DuckDB and Time-Series Data
----------------------------

When the agent needs to analyze historical price data, LumiBot loads it into DuckDB tables automatically. The agent can then query these tables with SQL instead of reading raw bar data in the prompt.

This is handled by the base prompt and the built-in ``market.load_history_table`` and ``duckdb.query`` tools. The agent loads a price history table by symbol and timeframe, then queries it with standard SQL for moving averages, volatility, or any other analysis. You do not need to configure DuckDB -- it is part of the default agent runtime.

Replay Cache
------------

In backtesting mode, LumiBot caches every agent run. When a subsequent backtest hits the same combination of prompt, context, model, tools, and simulated timestamp, the cached result is returned instantly without calling the LLM or any external tool.

This means:

- **Deterministic backtests.** The same inputs always produce the same outputs.
- **Fast warm reruns.** A cached backtest that took 30 minutes on the first run can complete in seconds.
- **Cost control.** No duplicate LLM API calls or external API calls on repeated runs.

The replay cache is automatic. No configuration needed.

Observability
-------------

Every agent run produces a structured trace that records:

- The full prompt surface (base prompt + system prompt + context)
- Every tool call and tool result
- Any observability warnings (e.g., future-dated data in a backtest)
- The agent's summary and reasoning
- Cache hit/miss status
- DuckDB query metrics

A compact summary log line is emitted for every run. For deeper debugging, inspect the full JSON trace file. See :doc:`agents_observability` for the complete debugging workflow.

Canonical Demos
---------------

LumiBot ships four canonical demo strategies that serve as end-to-end reference implementations for the AI agent runtime. All four use the ``@agent_tool`` pattern with the ``requests`` library and are located in ``lumibot/example_strategies/``:

1. **News Sentiment Strategy** (``lumibot/example_strategies/agent_news_sentiment.py``) -- Uses Alpaca News API to discover and trade on US stock news catalysts.
2. **Macro Risk Strategy** (``lumibot/example_strategies/agent_macro_risk.py``) -- Uses Alpaca market data API to allocate between TQQQ and SHV based on price trends and market conditions.
3. **Momentum Allocator Strategy** (``lumibot/example_strategies/agent_momentum_allocator.py``) -- Uses Alpaca price bars and news to allocate between TQQQ and SHV based on momentum and sentiment.
4. **M2 Liquidity Strategy** (``lumibot/example_strategies/agent_m2_liquidity.py``) -- Uses FRED public data to allocate between TQQQ and SHV based on money supply and liquidity trends.

Each demo validates tool usage, replay caching, trace quality, and benchmarked tearsheet output. See :doc:`agents_canonical_demos` for details on each strategy.

The demo files are located at ``lumibot/example_strategies/agent_*.py`` and can be run directly after setting the required environment variables.

Frequently Asked Questions
--------------------------

**Can I backtest an AI trading agent?**

Yes, LumiBot is the only production framework that lets an AI agent reason, call tools, and execute trades on every bar during a backtest. The agent runs inside ``on_trading_iteration()``, receives point-in-time market state, and uses tools to make decisions -- all within the backtest simulation. A built-in replay cache makes warm reruns deterministic and fast.

**What makes LumiBot different from other AI trading frameworks?**

Most alternatives either put the LLM outside the backtest loop (QuantConnect), have no backtesting at all (CrewAI, AutoGen, LangGraph), or are hobby scripts with no infrastructure. LumiBot is the only production framework that runs the AI agent inside the backtest simulation on every bar, with ``@agent_tool`` for reliable external data, MCP server support, replay caching, DuckDB time-series queries, and full observability -- all with the same code for backtest and live.

**What AI models are supported?**

LumiBot ships with first-class support for Gemini, OpenAI (GPT), xAI (Grok), Anthropic (Claude), and any other provider covered by LiteLLM (~100 providers). You pick the model per agent via the ``default_model`` parameter when creating your agent.

Gemini ids (e.g. ``"gemini-3.1-flash-lite-preview"``) take Google ADK's native fast path. Anything else is automatically routed through LiteLLM using the provider-prefixed id format:

- Gemini: ``"gemini-3.1-flash-lite-preview"`` (default) -- requires ``GEMINI_API_KEY``
- OpenAI: ``"openai/gpt-5.4-mini"`` (good default), ``"openai/gpt-5.4"``, ``"openai/gpt-5.4-pro"``, ``"openai/gpt-5.4-nano"`` -- requires ``OPENAI_API_KEY``
- xAI Grok: ``"xai/grok-4.20-0309-reasoning"`` (Grok 4.2, reasoning on, 2M ctx), ``"xai/grok-4-1-fast-reasoning-latest"`` (cheap/fast), or ``"xai/grok-4-latest"`` (older) -- requires ``XAI_API_KEY`` or ``GROK_API_KEY``
- Anthropic Claude: ``"anthropic/claude-opus-4-7"``, ``"anthropic/claude-sonnet-4-6"`` -- requires ``ANTHROPIC_API_KEY``

The replay cache keys on the model id, so swapping providers on the same backtest produces fresh runs rather than stale cross-model replays. Tool calling is normalized across providers by LiteLLM, so your ``@agent_tool`` functions work unchanged regardless of which model you pick.

**How do I get started?**

Install LumiBot, set ``GEMINI_API_KEY`` in your environment, copy the Quick Start example on this page, and run it. The M2 Liquidity Strategy example is a complete, runnable strategy file. Provider-specific variants are available for OpenAI, Grok, and Anthropic. See :doc:`agents_quickstart` for additional patterns and :doc:`agents_canonical_demos` for the reference demo strategies.

**What API keys do I need?**

At minimum, one model provider key matching the ``default_model`` you set: ``GEMINI_API_KEY`` for Gemini (the default), ``OPENAI_API_KEY`` for GPT models, ``XAI_API_KEY`` or ``GROK_API_KEY`` for Grok, or ``ANTHROPIC_API_KEY`` for Claude. If your ``@agent_tool`` functions call external APIs, you also need those keys -- for example ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` for Alpaca data APIs. The M2 Liquidity demo uses the public FRED graph CSV endpoint, which is useful for a simple demo but returns revised data. Use the built-in FRED tools with ``FRED_API_KEY`` for strict point-in-time macro backtests.

**How do I set up my environment?**

Create a ``.env`` file in your project directory with your API keys (e.g., ``GEMINI_API_KEY=your_key_here``). LumiBot reads environment variables at startup. You can also export them in your shell. For backtesting, set ``BACKTESTING_DATA_SOURCE`` in ``.env`` or use ``datasource_class=None`` to defer to the environment configuration.

**Can I use this for live trading?**

Yes. The same strategy code runs in both backtest and live modes. For live trading, connect to a supported broker (Alpaca, Interactive Brokers, Tradier, Schwab, and others). No code changes are required -- LumiBot handles the broker integration.

**Does it work with my broker?**

LumiBot supports Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, TopstepX futures (via ProjectX), Bitunix, and selected CCXT crypto paths. Documented CCXT examples include Coinbase, Kraken, KuCoin, Binance, BitMEX, WEEX, Bybit, and OKX across live/manual/backtesting paths; Lumibot does not claim support for every CCXT exchange. Any broker supported by LumiBot works with AI agents. The agent submits orders through the standard LumiBot order execution pipeline.

**What is @agent_tool?**

``@agent_tool`` is a decorator that wraps a Python method as a callable tool the AI agent can invoke during its reasoning loop. You provide a name and description, write a standard method with type hints and a docstring, and the decorator handles the rest. The function's source code is automatically included in the tool description so the AI can see parameters, defaults, and implementation details.

**How does the agent know what parameters my tool accepts?**

``@agent_tool`` automatically includes the function's entire source code in the tool description sent to the AI model. The AI sees your type hints, default values, and docstring. Write a clear docstring with a Google-style ``Args`` section and the AI will understand how to call your tool.

**Do I need to list built-in tools?**

No. All built-in tools (positions, portfolio, prices, orders, DuckDB, docs) are always included automatically. When you pass custom tools via ``tools=[self.my_tool]``, they are added alongside the built-in tools. You only need to list your custom ``@agent_tool`` functions.

**Can I use multiple custom tools?**

Yes. Pass a list of tools when creating the agent: ``tools=[self.tool_a, self.tool_b, self.tool_c]``. The Macro Risk and Momentum Allocator demos both use multiple ``@agent_tool`` functions in a single strategy. There is no hard limit on the number of custom tools.

**What REST APIs can I wrap with @agent_tool?**

Any REST API that returns JSON or text. The four canonical demos wrap Alpaca News API, Alpaca Bars API, Alpaca Screener API, and the FRED public CSV endpoint. You can wrap Alpha Vantage, your own internal services, SEC EDGAR, social sentiment APIs, or anything else accessible over HTTP.

**How do I add authentication to my tool?**

Read API keys from environment variables inside your ``@agent_tool`` function using ``os.environ.get("MY_API_KEY")``. Pass them as headers or query parameters in your ``requests`` call. See the Alpaca demos for examples that use ``APCA-API-KEY-ID`` and ``APCA-API-SECRET-KEY`` headers.

**What happens if my tool returns an error?**

Return a dictionary with an ``"error"`` key (e.g., ``return {"error": str(e)}``). The agent sees the error and can decide to retry, try a different approach, or proceed without that data. An observability warning is also recorded in the trace. Wrap your HTTP call in a try/except block to handle network failures gracefully.

**Can I use MCP servers instead of @agent_tool?**

Yes. Pass an ``MCPServer`` object with a URL when creating the agent. However, ``@agent_tool`` is the recommended primary pattern because you control the HTTP call directly, it works reliably in both backtests and live trading, and it does not require external server infrastructure.

**What is the difference between @agent_tool and MCP servers?**

``@agent_tool`` wraps a Python method that makes HTTP calls via ``requests`` -- you control the code, it runs in-process, and it works reliably in backtests. MCP servers are external services that speak the Model Context Protocol over HTTP. MCP servers are useful when a third party provides a dedicated server or you need access to one of the 20,000+ public MCP servers, but ``@agent_tool`` is more reliable for backtesting and gives you full control.

**How long should my system prompt be?**

Two to three sentences describing your strategy intent. For example: what data to use, what assets to trade, and what the allocation logic should be. LumiBot handles position sizing, DuckDB guidance, backtesting safety, time-awareness, and the default investor policy in its base prompt.

**What should I put in the system prompt?**

Describe your strategy's thesis and the assets it trades. Do not repeat instructions about position sizing, order execution, look-ahead bias, or tool usage -- LumiBot covers all of that in the base prompt. A good example: ``"Use economic data to decide between TQQQ and SHV. Check interest rates, inflation, and growth conditions."``

**What does LumiBot handle automatically in the base prompt?**

The base prompt tells the agent whether the run is a backtest or live, the current datetime and timezone, current positions and cash, rules about look-ahead bias, the default investor policy (conviction over activity, no overtrading), risk and drawdown discipline (risk-adjusted returns over raw returns, recovery math, cut losers, no chasing after drawdowns, Sharpe/Sortino/Calmar framing), position sizing and limit order preferences, and DuckDB conventions and tool usage guidance.

**Can I override the default investor policy?**

The base prompt includes a default policy favoring conviction over activity and discouraging overtrading. Your system prompt can direct the agent toward different behavior -- for example, telling it to rebalance daily or trade more aggressively. The system prompt is added on top of the base prompt, so your instructions take priority for strategy-specific guidance.

**How do I make the agent more aggressive or more conservative?**

Add explicit direction in your system prompt. For a more aggressive agent: ``"Trade actively. Rebalance into high-conviction positions quickly."`` For a more conservative agent: ``"Only trade when evidence is overwhelming. Prefer holding cash or SHV when uncertain."`` The agent follows your prompt guidance.

**How does backtesting work with AI agents?**

The agent runs inside ``on_trading_iteration()`` on every bar (e.g., every trading day if ``sleeptime="1D"``). On each bar, the agent receives point-in-time market state, calls tools (both built-in and custom), reasons over the data, and submits orders. The backtest simulation processes those orders at simulated market prices. The replay cache makes warm reruns deterministic.

**How does the agent avoid looking into the future during backtests?**

LumiBot injects the simulated datetime into the agent's context and the base prompt includes explicit rules about look-ahead bias. The observability system also flags future-dated data warnings if a tool result references data published after the simulated backtest time. Your ``@agent_tool`` functions should respect date parameters to avoid requesting future data.

**What is the replay cache?**

In backtesting mode, LumiBot caches every agent run keyed by a SHA-256 hash of the prompt, context, model, tool surface, and simulated timestamp. When a subsequent backtest hits the same combination, the cached result is returned instantly without calling the LLM or any external tool. This makes warm reruns deterministic, fast, and cost-free.

**How do I clear the cache for a fresh run?**

Delete the replay cache directory. On macOS the default location is ``~/Library/Caches/lumibot/agent_runtime/replay/``. You can also set the ``LUMIBOT_CACHE_FOLDER`` environment variable to control where caches are stored. After clearing, the next run will make fresh LLM and tool calls.

**How long does a backtest take?**

A cold run (no cache) depends on the number of bars, the number of tool calls per bar, and the LLM response time. A six-year daily backtest with one tool call per bar might take 20-40 minutes on the first run. A warm run (fully cached) completes the same backtest in seconds because no LLM or external API calls are made.

**Can I speed up backtests?**

Use the replay cache -- after the first cold run, all subsequent runs with the same inputs are near-instant. You can also reduce the date range, increase the ``sleeptime`` to trade less frequently, or use a faster model. Keeping your ``@agent_tool`` functions fast (short timeouts, efficient parsing) also helps.

**What data sources work for backtesting?**

Set ``datasource_class=None`` to use the data source from your ``.env`` file (via ``BACKTESTING_DATA_SOURCE``). For standalone examples, use ``YahooDataBacktesting``. LumiBot also supports ThetaData, Polygon, and other data sources for backtesting. The data source controls price bars and market data; your ``@agent_tool`` functions provide any additional external data.

**How do I see what the agent is doing?**

Every agent run emits a compact summary log line with the agent name, model, cache status, tool call count, warning count, and the agent's summary conclusion. For deeper inspection, open the structured JSON trace file. See :doc:`agents_observability` for the full debugging workflow.

**What are agent traces?**

Traces are structured JSON files that record everything the agent did during a single run: the full prompt surface, every tool call with arguments, every tool result, the agent's reasoning and summary, observability warnings, cache hit/miss status, and DuckDB query metrics. They are the source of truth for debugging.

**Where are trace files stored?**

Trace files are stored in the LumiBot cache directory under ``agent_runtime/``. The trace path is available on the result object via ``(result.payload or {}).get("trace_path")``. Machine-readable summaries are also written to ``agent_run_summaries.jsonl``.

**How do I debug a bad trade?**

Open the trace JSON for the run where the bad trade occurred. Check what tools the agent called, what data it received, and what reasoning it stated. Look for observability warnings (future-dated data, no tools called, unsupported orders). Compare the agent's summary to the actual trade. See :doc:`agents_observability` for the recommended debugging workflow.

**Why is my agent not trading?**

Check the agent's summary in the logs -- it may have decided not to trade because conviction was low. The default investor policy in the base prompt encourages conviction over activity. If you want more frequent trading, adjust your system prompt to be more directive. Also verify that your tools are returning valid data by inspecting the trace.

**Why is my agent only buying SHV?**

SHV is a common defensive parking asset used in the demo strategies. If the agent only buys SHV, it means the agent is not finding enough conviction to take risk. Check whether your tool is returning useful data (inspect the trace), whether the system prompt is clear about when to be risk-on, and whether the market data covers the right date range.

**How much does it cost to run?**

Cost depends on the LLM provider and model, the number of bars in your backtest, and how many tool calls the agent makes per bar. A six-year daily backtest might cost a few dollars on the first cold run with a fast model like Gemini Flash. Warm reruns cost nothing because the replay cache eliminates all LLM and external API calls.

**How can I reduce API costs?**

Use the replay cache -- once a backtest is cached, subsequent runs are free. Use cost-effective models (e.g., ``gemini-3.1-flash-lite-preview``). Keep your backtest date range focused during development. Reduce the number of tool calls by making your tools return comprehensive data in a single call rather than requiring multiple round trips.

**How does replay caching reduce costs?**

The replay cache stores every agent run result keyed by a hash of the inputs. When the same prompt, context, tools, model, and timestamp appear again, the cached result is returned with zero LLM calls, zero external API calls, and zero cost. A cold backtest that costs a few dollars becomes free on every subsequent warm run.

Error Handling and Reliability
------------------------------

.. note::

   This section describes error handling **specific to AI agent calls**. The rest of LumiBot's main-loop error handling (strategy executor, brokers, data sources) is unchanged. The behavior below is scoped to ``AgentHandle.run()`` and ``GoogleADKRuntime.run()``; it does not alter how non-agent code paths react to exceptions.

LumiBot's AI agent stack has three retry/safety layers that together keep live trading alive through provider outages and surface backtest-time bugs clearly:

1. **LiteLLM-level HTTP retries.** When using non-Gemini providers, LiteLLM retries each individual HTTP call 3 times with provider-aware backoff (429 Retry-After awareness, capped exponential). Configured automatically in ``_configure_litellm_quietly`` (``num_retries=3``, ``drop_params=True``, ``suppress_debug_info=True``).

2. **Runtime-level attempt retries.** ``GoogleADKRuntime.run()`` retries the full agent call up to **10 times** with capped exponential backoff (2s, 3s, 5s, 10s, 20s, 30s, 45s, 60s, 60s, 60s — total budget ~5 minutes). This covers session-setup errors, ADK runner glitches, and provider 5xx storms that LiteLLM's inner retry couldn't fix. Only transient and unknown errors retry; auth/config/billing errors surface immediately so we do not waste 5 minutes retrying a wrong API key.

3. **Strategy-level safety net with live-vs-backtest branch.** ``AgentHandle.run()`` wraps the runtime call in a final catch. Behavior depends on two things: the error category and whether the strategy is in backtest mode or live.

Error classifier buckets
~~~~~~~~~~~~~~~~~~~~~~~~

Every exception from an agent call is classified by ``_classify_agent_error`` into one of five buckets:

- ``auth`` -- missing or invalid API key, permission denied (401, 403)
- ``config`` -- bad model id, malformed prompt, context-window exceeded, invalid payload (400, 404, 422)
- ``billing`` -- out of credits, payment required, quota exhausted (402, 429 with ``insufficient_quota``, 403 with billing/credits keywords)
- ``transient`` -- 5xx, rate-limit bursts, timeouts, connection errors
- ``unknown`` -- anything not matched; treated as transient (safe default)

The classifier looks at the exception class name, HTTP status code (if the provider SDK attached one), and message substring keywords (``insufficient_quota``, ``credits``, ``billing``, ``payment``, ``no credits``) so that a 403 returned with a billing message is correctly classified as ``billing`` rather than ``auth``.

Backtest vs. live behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~

+--------------+------------------------------------------+----------------------------------+
| Category     | Backtest                                 | Live                             |
+==============+==========================================+==================================+
| ``auth``     | **Crash loud** with env-var guidance.    | Log + skip iteration.            |
+--------------+------------------------------------------+----------------------------------+
| ``config``   | **Crash loud** with model/prompt hint.   | Log + skip iteration.            |
+--------------+------------------------------------------+----------------------------------+
| ``billing``  | **Crash loud** with provider billing URL.| Log + skip iteration.            |
+--------------+------------------------------------------+----------------------------------+
| ``transient``| Log + skip iteration (silent).           | Log + skip iteration.            |
+--------------+------------------------------------------+----------------------------------+
| ``unknown``  | Log + skip iteration (safe default).     | Log + skip iteration.            |
+--------------+------------------------------------------+----------------------------------+

**Live trading invariant**: an AI agent call never stops a live trading bot. Ever. Even a completely missing API key will log an error and continue — the operator can fix the env var and the bot resumes on the next iteration without a process restart. This is intentional: shutting down a live bot with real money at risk because of a provider hiccup is unacceptable.

**Backtest philosophy**: surface bugs loudly. A silent +0% tearsheet caused by a wrong API key is worse than a clear error message — the user just started the run, can fix it, and re-run. Transient errors still skip silently because they are not bugs the user can act on.

Skipped iteration result shape
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the safety net returns a graceful skip, the ``AgentRunResult`` includes:

- ``summary`` starting with ``"RESULT: Skipped this iteration. Agent call failed (category=...)"``
- a text event with payload ``{"runtime_error": True, "error_category": "...", "error_class": "...", "error_message": "...", "traceback": "..."}``
- a warning in ``result.warnings`` with ``kind="agent_runtime_failure_skipped"`` and the category
- ``cache_key = None`` (failures are never cached — next iteration retries fresh)

Strategy authors can count skipped iterations with ``len([w for w in result.warnings if w.get("kind") == "agent_runtime_failure_skipped"])`` for post-run analysis.

Model id visibility in tearsheets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every time you call ``self.agents.create(name=..., default_model=...)``, the framework auto-populates ``self.parameters[f"agent_{name}_model"]`` with the resolved model id. This shows up automatically in the tearsheet's **Parameters Used** panel so every AI backtest self-identifies which model produced which tearsheet. Multi-agent strategies get one key per agent.

Token usage and audit trail
~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot also writes AI usage details for every agent run during a backtest:

- The tearsheet's **Parameters Used** panel shows running totals for each agent:

  - ``agent_<name>_calls``
  - ``agent_<name>_input_tokens``
  - ``agent_<name>_output_tokens``
  - ``agent_<name>_total_tokens``
  - ``agent_<name>_thinking_tokens``
  - ``agent_<name>_cached_input_tokens``
  - ``agent_<name>_uncached_input_tokens``
  - ``agent_<name>_latency_ms_avg``
  - ``agent_<name>_tool_calls``
  - ``agent_<name>_cache_hits``
  - ``agent_<name>_detail_parquet``

- A single detailed tabular artifact is written beside the normal backtest artifacts using the same base filename pattern:

  - ``<run>_agent_detail.parquet``

The Parquet file is the canonical machine-readable audit artifact used by BotSpot/MCP query tooling. LumiBot does not estimate provider pricing in this file because model prices change; it records raw token usage only.

Each agent call gets one ``call_summary`` row plus one row per model event inside the call. This avoids repeating call-level token totals on every tool row while still preserving the event timeline. The file includes:

- prompt/context fields (user system prompt, effective prompt, task prompt, runtime context)
- event kind (``call_summary``, ``thinking``, ``text``, ``tool_call``, ``tool_result``, ``usage`` when present)
- event text
- tool name
- flattened tool/event details in normal columns
- full event payload JSON for exact forensic inspection
- input/output/total token counts on the ``call_summary`` row
- cached/uncached input token counts when the provider reports them
- thinking token counts when the provider exposes them
- latency fields for the full call and first model event
- cache-hit flag and warnings

This file is meant to answer practical debugging questions after a backtest:

- What exactly did the agent say?
- What tools did it call?
- What came back from those tools?
- How many tokens did that call use?
- How many input tokens were cached vs. uncached?
- How long did the call take?

Thinking text is captured when the provider/SDK exposes it. Gemini thought summaries are requested automatically. Other providers may expose only thinking token counts and not the actual thought text.

Provider prompt caching
~~~~~~~~~~~~~~~~~~~~~~~

LumiBot has two separate cache layers:

- The LumiBot replay cache skips the entire agent call on identical warm backtests.
- Provider prompt caching reduces cost/latency during cold backtests and live trading when the static prompt prefix repeats.

The agent runtime keeps the large, stable instructions and tool definitions at the beginning of the request and moves dynamic fields such as current datetime, runtime mode, positions, orders, memory, task prompt, and user context into later request sections. This improves provider prefix-cache hit rates without changing strategy behavior.

Provider-specific routing:

- OpenAI models receive a stable ``prompt_cache_key`` plus ``prompt_cache_retention="24h"`` through LiteLLM.
- xAI/Grok models receive a stable ``x-grok-conv-id`` header through LiteLLM.
- Gemini native models use Gemini's implicit caching path. Explicit ADK context caching is a future optimization; the runtime already records Gemini ``cached_content_token_count`` when the provider reports it.

Use ``scripts/run_agent_prompt_cache_probe.py`` to verify provider-reported cache behavior with real calls:

.. code-block:: bash

    python scripts/run_agent_prompt_cache_probe.py --model gemini-3.1-flash-lite-preview
    python scripts/run_agent_prompt_cache_probe.py --model openai/gpt-5.4-mini

The probe bypasses LumiBot's replay cache, sends repeated calls with the same long static prefix, and prints input tokens, cached input tokens, uncached input tokens, output tokens, and latency for each call.

Built-in Alpaca news tool
~~~~~~~~~~~~~~~~~~~~~~~~~

Strategies can use ``BuiltinTools.news.alpaca_news()`` to give an agent access to the Alpaca News API without writing a custom wrapper:

.. code-block:: python

    from lumibot.components.agents import BuiltinTools

    self.agents.create(
        name="trader",
        system_prompt=(
            "Use Alpaca news and market tools to make trading decisions. "
            "Scan headlines and summaries first. If a story matters, fetch full article content before trading."
        ),
        tools=[BuiltinTools.news.alpaca_news()],
    )

The tool uses the active Alpaca broker credentials when the strategy is running on Alpaca, including OAuth connections. If the active broker is not Alpaca, set bring-your-own-key news credentials with ``ALPACA_NEWS_API_KEY`` and ``ALPACA_NEWS_API_SECRET``. If neither path is available, LumiBot logs a warning and does not expose ``alpaca_news`` to agents. It defaults ``end`` to the current simulated datetime in backtests and clamps future ``end`` values to avoid look-ahead. The response includes ``requested_end``, ``effective_end``, and ``lookahead_clamped`` so you can audit the exact window used.

Alpaca news is historical symbol/date-window retrieval, not keyword search. The API supports ``symbols``, ``start``, ``end``, ``limit`` (max 50), ``sort``, ``include_content``, ``exclude_contentless``, and ``page_token``. For broad market context, query market ETF proxies such as ``SPY,QQQ,DIA,IWM``; for sector context, query sector ETFs such as ``XLK,SMH`` (tech/semis), ``XLF,KRE`` (financials/banks), ``XLE,USO`` (energy), ``XLV,XBI`` (healthcare/biotech), ``TLT,IEF,SHY`` (rates/bonds), or ``GLD,SLV,DBC`` (gold/commodities).

Use a two-step workflow:

1. Scan with ``include_content=False``. Use ``limit=10`` to ``20`` for focused single-symbol checks and ``limit=30`` to ``50`` for broad market or sector scans. This returns headlines, summaries, URLs, sources, timestamps, symbols, and ``next_page_token`` without dumping long article bodies into the model context.
2. If a story looks important, call again for the same or narrower window with ``include_content=True`` and usually ``exclude_contentless=True``. Full article content is returned without truncation unless you explicitly pass ``content_max_chars``.
3. If ``next_page_token`` is present and the first page does not provide enough evidence, call again with ``page_token=next_page_token``.

Do not trade from one weak or noisy article. News can be sparse for single stocks, so broaden from the stock to its sector or market ETF when needed, compare article timestamps against the simulated datetime, and use ``page_token`` when the first page does not provide enough evidence.

Complete runnable example:

.. code-block:: python

    import os
    from lumibot.components.agents import BuiltinTools
    from lumibot.strategies.strategy import Strategy

    class AlpacaNewsBuiltinStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="news_trader",
                default_model=os.environ.get("AGENT_MODEL", "gemini-3.1-flash-lite-preview"),
                system_prompt=(
                    "Use Alpaca news and market tools to decide whether to hold SPY, QQQ, or a defensive ETF. "
                    "First call alpaca_news with symbols='SPY,QQQ,DIA,IWM', include_content=False, and limit=30. "
                    "If a story looks market-moving, call alpaca_news again with include_content=True and "
                    "exclude_contentless=True before trading. "
                    "Use page_token when next_page_token is returned."
                ),
                tools=[BuiltinTools.news.alpaca_news()],
            )

        def on_trading_iteration(self):
            self.agents["news_trader"].run(
                context={"current_datetime": self.get_datetime().isoformat()}
            )

See ``lumibot/example_strategies/agent_alpaca_news_builtin.py`` for the full example including the backtest runner.

To run the live proof that validates historical relevance, full-content retrieval, and the resulting ``*_agent_detail.parquet`` artifact:

.. code-block:: bash

    python scripts/run_alpaca_news_ai_proof.py --model gemini-3.1-pro-preview
