AI Agents Quick Start
=====================

This page shows the core patterns for creating and running AI trading agents inside a LumiBot strategy. Whether you want to backtest an AI trading agent with external tools or build an agentic backtesting workflow, these examples get you started in minutes. For background on why LumiBot is the only framework that puts the LLM inside the backtest loop, see :doc:`agents`.

The pattern is simple:

- Create the agent once in ``initialize()``
- Run the agent from lifecycle methods like ``on_trading_iteration()``
- The agent reasons, calls tools, and executes trades on each bar
- The same code works in backtests and live trading

Imports
-------

.. code-block:: python

    from lumibot.components.agents import MCPServer, agent_tool
    from lumibot.strategies import Strategy

Built-in tools are included by default -- no import needed for those.

Minimal Example (Built-in Tools Only)
--------------------------------------

This strategy creates an agent that uses only the default built-in tools. No external APIs, no explicit tool list.

.. code-block:: python

    from lumibot.strategies import Strategy


    class SimpleAgentStrategy(Strategy):
        parameters = {"symbol": "SPY"}

        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="research",
                default_model="gpt-4.1-mini",
                system_prompt=(
                    "Analyze the current portfolio and market conditions. "
                    "Trade conservatively. If the evidence is weak, do nothing."
                ),
            )

        def on_trading_iteration(self):
            result = self.agents["research"].run(
                context={"symbol": self.parameters["symbol"]}
            )
            self.log_message(f"[research] {result.summary}", color="yellow")

The agent has access to all built-in tools (positions, portfolio, prices, history, DuckDB, orders, docs) without listing them.

``@agent_tool`` Example (Primary Pattern)
-------------------------------------------

The recommended way to give your agent access to external data is ``@agent_tool``. This wraps a Python method as a callable tool using the ``requests`` library. It works reliably in both backtests and live trading.

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
                name="liquidity_research",
                default_model="gpt-4.1-mini",
                system_prompt=(
                    "Use money supply and liquidity data to decide between "
                    "TQQQ and SHV. Focus on whether M2 liquidity is expanding "
                    "or contracting."
                ),
                tools=[self.get_fred_series],
            )

        def on_trading_iteration(self):
            result = self.agents["liquidity_research"].run()
            self.log_message(f"[liquidity_research] {result.summary}", color="yellow")

**Source code auto-inclusion:** ``@agent_tool`` automatically includes the function's source code in the tool description sent to the AI. The AI can see all parameters, default values, and implementation details. Write a clear docstring with an ``Args`` section, and the AI will understand how to call your tool correctly.

**Built-in tools stay included:** When you pass ``tools=[self.my_tool]``, those tools are added alongside the default built-in tools. You only need to list your custom tools.

MCP Server Example (Alternative Pattern)
------------------------------------------

If you have a compatible MCP server, you can connect it by URL. This is useful for live trading or when a third-party provides a dedicated MCP server.

.. code-block:: python

    import os
    from lumibot.components.agents import MCPServer
    from lumibot.strategies import Strategy


    class ExternalDataStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="research",
                default_model="gpt-4.1-mini",
                system_prompt=(
                    "Use the available data tools to make informed trading decisions. "
                    "This is a binary allocator between TQQQ and SHV."
                ),
                mcp_servers=[
                    MCPServer(
                        name="my-data-server",
                        url="https://my-mcp-server.example.com/mcp",
                        timeout_seconds=120,
                    ),
                ],
            )

        def on_trading_iteration(self):
            result = self.agents["research"].run()
            self.log_message(f"[research] {result.summary}", color="yellow")

Any MCP server that speaks the Model Context Protocol over HTTP or Streamable HTTP works. There are over 20,000 MCP servers available today. The ``@agent_tool`` pattern is recommended for most use cases because it gives you full control over the HTTP call and works reliably in backtests.

Custom Strategy Tools with ``@agent_tool``
-------------------------------------------

If your strategy needs a custom helper that the agent can call, decorate a method with ``@agent_tool``. Always include a docstring with an ``Args`` section so the AI knows how to use it:

.. code-block:: python

    from lumibot.components.agents import agent_tool
    from lumibot.strategies import Strategy


    class CustomToolStrategy(Strategy):

        @agent_tool(
            name="get_watchlist_bias",
            description="Return a structured bias payload for one symbol.",
        )
        def get_watchlist_bias(self, symbol: str) -> dict:
            """Look up the current bias for a symbol on the watchlist.

            Args:
                symbol: Stock ticker symbol to check (e.g., AAPL, MSFT)
            """
            return {"symbol": symbol, "bias": "neutral"}

        def initialize(self):
            self.sleeptime = "1D"
            self.agents.create(
                name="research",
                default_model="gpt-4.1-mini",
                system_prompt="Analyze watchlist bias before trading.",
                tools=[self.get_watchlist_bias],
            )

        def on_trading_iteration(self):
            result = self.agents["research"].run()
            self.log_message(f"[research] {result.summary}", color="yellow")

Passing Context
---------------

Pass point-in-time context into the agent run. LumiBot automatically injects positions, cash, and datetime, but you can add strategy-specific context:

.. code-block:: python

    result = self.agents["research"].run(
        context={
            "symbol": "SPY",
            "signal_state": self.vars.get("signal_state", "unknown"),
        }
    )

Working with the Result
-----------------------

``run(...)`` returns an ``AgentRunResult`` with these useful fields:

- ``result.summary`` -- the agent's concluding summary
- ``result.text`` -- full text output from the agent
- ``result.cache_hit`` -- whether the result was replayed from cache
- ``result.warning_messages`` -- list of observability warnings
- ``result.tool_calls`` -- list of tool call events
- ``result.tool_results`` -- list of tool result events
- ``(result.payload or {}).get("trace_path")`` -- path to the full JSON trace

.. code-block:: python

    result = self.agents["research"].run()
    self.log_message(f"summary={result.summary}", color="yellow")

    trace_path = (result.payload or {}).get("trace_path")
    if trace_path:
        self.log_message(f"trace={trace_path}", color="blue")

    if result.warning_messages:
        for warning in result.warning_messages:
            self.log_message(f"WARNING: {warning}", color="red")

Running a Backtest
------------------

Use the standard LumiBot backtest pattern. The agent runs on every bar just like it would in live trading:

.. code-block:: python

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

Set ``datasource_class=None`` to use the data source configured in your ``.env`` file via ``BACKTESTING_DATA_SOURCE``.

Best Practices
--------------

- **Create the agent once in ``initialize()``.** Do not recreate it on every iteration.
- **Keep system prompts short.** 2-3 sentences about your strategy intent. LumiBot handles the rest.
- **Do not list built-in tools.** They are included by default, even when you add custom tools.
- **Use ``@agent_tool`` for external data.** Wrap REST APIs with the ``requests`` library. This is the most reliable pattern.
- **Write docstrings with Args sections.** The AI sees the source code automatically and uses the docstring to understand parameters.
- **MCP servers are just URLs.** No local scripts, no npm installs. Use them for live trading or when you have a compatible server.
- **Log the summary.** Always log ``result.summary`` so you can understand agent decisions.
- **Inspect traces when surprised.** The JSON trace is the source of truth for debugging agent behavior.

Where to Go Next
-----------------

- :doc:`agents` -- main guide with competitive positioning and architecture
- :doc:`agents_canonical_demos` -- the four reference demo strategies
- :doc:`agents_observability` -- traces, replay cache, and debugging workflow

Frequently Asked Questions
--------------------------

**Do I need to list built-in tools?**

No. All built-in tools (positions, portfolio, prices, orders, DuckDB, docs) are included automatically, even when you add custom tools. When you pass ``tools=[self.my_tool]``, your custom tools are added alongside the defaults. You never need to list built-in tools explicitly.

**What API keys do I need?**

At minimum, ``GOOGLE_API_KEY`` for the Gemini model that powers the agent. If your ``@agent_tool`` functions call external APIs, you also need those keys -- for example ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` for Alpaca-based demos. The M2 Liquidity demo only needs ``GOOGLE_API_KEY`` because FRED data is public.

**How long should my system prompt be?**

Two to three sentences describing your strategy intent. For example: what data to look at, what assets to trade, and what the decision logic should be. LumiBot handles position sizing, DuckDB guidance, backtesting safety, time-awareness, and the default investor policy in its base prompt. Do not repeat those instructions.

**How do I get started with the minimal example?**

Copy the Minimal Example from this page, set ``GOOGLE_API_KEY`` in your environment, and run it. The agent will use only built-in tools (positions, prices, DuckDB, orders) to analyze the market and make decisions. No external APIs or custom tools are required for the minimal example.

**What does datasource_class=None mean?**

Setting ``datasource_class=None`` tells LumiBot to read the data source from your ``.env`` file via the ``BACKTESTING_DATA_SOURCE`` variable. This is the recommended approach for team projects. For standalone examples or quick experiments, use ``YahooDataBacktesting`` explicitly.

**How do I create an @agent_tool?**

Decorate a method on your Strategy class with ``@agent_tool(name="my_tool", description="What this tool does.")``. Add type hints to the parameters and write a docstring with a Google-style ``Args`` section. The decorator handles everything else -- the function's source code is automatically included in the tool description sent to the AI.

**How does source code auto-inclusion work?**

When you decorate a method with ``@agent_tool``, LumiBot reads the function's source code and appends it to the tool description. The AI model sees your parameter names, type hints, default values, docstring, and implementation logic. This means you do not need to manually describe every parameter -- just write clear code and a docstring.

**Can I combine @agent_tool and MCP servers?**

Yes. Pass custom tools via ``tools=[...]`` and MCP servers via ``mcp_servers=[...]`` when creating the agent. Both are added alongside the built-in tools. The ``@agent_tool`` pattern is recommended as the primary approach because it is more reliable for backtesting.

**What goes in the context parameter of run()?**

Pass any strategy-specific data the agent should see for this iteration. LumiBot automatically injects positions, cash, datetime, and timezone. You can add extra context like a target symbol, a signal state, or any other variable. The context is included in the prompt sent to the AI.

**What fields are available on the result object?**

``result.summary`` is the agent's concluding summary. ``result.text`` is the full output. ``result.cache_hit`` indicates whether the result came from the replay cache. ``result.warning_messages`` is a list of observability warnings. ``result.tool_calls`` and ``result.tool_results`` list tool interactions. ``(result.payload or {}).get("trace_path")`` gives the path to the full JSON trace file.

**Why should I create the agent in initialize()?**

Creating the agent once in ``initialize()`` is more efficient than recreating it on every iteration. The agent object holds configuration (model, system prompt, tools, MCP servers) that does not change between bars. Creating it once also ensures consistent configuration throughout the backtest.

**Can I run multiple agents in a single strategy?**

Yes. Call ``self.agents.create(...)`` multiple times with different names. Then run each agent separately in ``on_trading_iteration()`` using ``self.agents["agent_name"].run()``. Each agent can have its own system prompt, model, and tools.

**How do I log the agent's output?**

Use ``self.log_message(f"[agent_name] {result.summary}", color="yellow")``. Always log the summary so you can understand agent decisions in the backtest output. For debugging, also log the trace path: ``self.log_message(f"trace={trace_path}", color="blue")``.

**What happens during a backtest iteration?**

On each bar, ``on_trading_iteration()`` is called. You call ``self.agents["name"].run()`` inside it. The agent receives the current market state, reasons over it, optionally calls tools (both built-in and custom), and submits orders. The backtest simulation processes those orders at simulated prices. The result is cached for future warm reruns.

**What if my @agent_tool function raises an exception?**

Wrap your HTTP calls in try/except and return an error dictionary (e.g., ``return {"error": str(e)}``). The agent sees the error result and can adapt. If an unhandled exception propagates, the agent run will fail and an error will be logged. Always use try/except with a timeout for network calls.

**How do I pass secrets to my @agent_tool?**

Use ``os.environ.get("MY_SECRET_KEY")`` inside your tool function. Never hardcode secrets. Store them in your ``.env`` file or export them in your shell. The demos use this pattern for Alpaca API keys.

**Can I use @agent_tool without any external API?**

Yes. An ``@agent_tool`` function can do anything -- read a local file, compute a value, look up data from a dictionary, or run a calculation. It does not have to make HTTP calls. The decorator simply makes the function available as a tool the AI can call.
