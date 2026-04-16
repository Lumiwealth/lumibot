AI Trading Agents and Agentic Backtesting
==========================================

LumiBot is the only production framework that lets an AI agent reason, call external tools, and execute trades **on every bar during a backtest** -- then run the exact same strategy code live. Whether you use ``@agent_tool`` to wrap any REST API as a callable tool or connect to one of 20,000+ external `MCP servers <https://modelcontextprotocol.io/>`_, LumiBot handles it in one unified codebase. A built-in replay cache makes warm reruns deterministic and fast. Whether you want to backtest an AI trading agent, build an agentic backtesting framework, or connect LLM-driven trading bots to live brokers, LumiBot handles it all.

.. toctree::
   :maxdepth: 1

   agents_quickstart
   agents_canonical_demos
   agents_observability

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
- **Any LLM provider.** Use OpenAI, Anthropic, Google Gemini, or any provider supported by the underlying model router.
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

The default model is Gemini (``gemini-3.1-flash-lite-preview``). You need a ``GOOGLE_API_KEY`` environment variable set. The architecture supports OpenAI, Anthropic, and other providers through the underlying model router. Pass the model name via the ``default_model`` parameter when creating your agent.

**How do I get started?**

Install LumiBot, set ``GOOGLE_API_KEY`` in your environment, copy the Quick Start example on this page, and run it. The M2 Liquidity Strategy example is a complete, runnable strategy file. See :doc:`agents_quickstart` for additional patterns and :doc:`agents_canonical_demos` for the four reference demo strategies.

**What API keys do I need?**

At minimum, ``GOOGLE_API_KEY`` for the Gemini model that powers the agent. If your ``@agent_tool`` functions call external APIs, you also need those keys -- for example ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` for Alpaca data APIs. The M2 Liquidity demo only needs ``GOOGLE_API_KEY`` because FRED data is public.

**How do I set up my environment?**

Create a ``.env`` file in your project directory with your API keys (e.g., ``GOOGLE_API_KEY=your_key_here``). LumiBot reads environment variables at startup. You can also export them in your shell. For backtesting, set ``BACKTESTING_DATA_SOURCE`` in ``.env`` or use ``datasource_class=None`` to defer to the environment configuration.

**Can I use this for live trading?**

Yes. The same strategy code runs in both backtest and live modes. For live trading, connect to a supported broker (Alpaca, Interactive Brokers, Tradier, Schwab, and others). No code changes are required -- LumiBot handles the broker integration.

**Does it work with my broker?**

LumiBot supports Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, CCXT (crypto), Bitunix, and more. Any broker supported by LumiBot works with AI agents. The agent submits orders through the standard LumiBot order execution pipeline.

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

The base prompt tells the agent whether the run is a backtest or live, the current datetime and timezone, current positions and cash, rules about look-ahead bias, the default investor policy (conviction over activity, no overtrading), position sizing and limit order preferences, and DuckDB conventions and tool usage guidance.

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
