# AI Trading Agents: Backtest AI Agents with Real External Tools

> LumiBot is the only production framework that backtests AI trading agents with real external tools, replay caching, and the same code for backtest and live.

**Last Updated:** 2026-03-30
**Status:** Active
**Audience:** Both

---

## Overview

LumiBot has a first-class AI agent runtime inside the `Strategy` lifecycle. An AI agent reasons, calls tools, and makes trading decisions **on every bar during a backtest**. The same strategy code runs live with zero changes. A built-in replay cache makes warm backtest reruns deterministic and fast.

The primary way to give your agent access to external data is the `@agent_tool` decorator, which wraps any REST API as a callable tool using the `requests` library. This pattern works reliably in both backtests and live trading. MCP servers via URL are also supported for live trading or when you have a compatible server.

If you are looking for an agentic backtesting framework, an LLM trading bot backtest solution, or a way to backtest AI-driven trading strategies with external data, LumiBot is the only production-ready option that puts the AI agent inside the simulation loop.

Related docs:

- `docs/AI_TRADING_AGENT_COMPONENT_GUIDE.md` -- internal component guide
- `docs/AI_TRADING_AGENT_CANONICAL_DEMOS.md` -- canonical demo strategies
- Public docs: `https://lumibot.lumiwealth.com/agents.html`

---

## Why LumiBot Is Different

Most tools that combine LLMs and trading fall into one of three categories:

1. **LLM outside the loop.** Platforms like QuantConnect let you call an LLM externally, but the model is not part of the backtest simulation. It cannot reason over point-in-time data on each bar.
2. **Agent frameworks with no backtesting.** CrewAI, AutoGen, and LangGraph build multi-agent workflows, but none of them simulate a trading backtest where the agent makes decisions bar by bar.
3. **Hobby scripts with no infrastructure.** Open-source experiments wire GPT to a broker but lack MCP support, replay caching, DuckDB, and production observability.

LumiBot combines:

- **LLM in the loop on every bar** -- the agent runs inside `on_trading_iteration()`, receives point-in-time state, calls tools, reasons, and submits orders
- **@agent_tool for reliable external data** -- wrap any REST API as a callable tool; works in both backtests and live trading
- **MCP server support** -- connect any MCP-compatible server with a URL for live trading or compatible servers
- **Replay caching** -- identical inputs = cached result, warm reruns in seconds
- **Any LLM provider** -- OpenAI, Anthropic, Google Gemini, and more
- **Same code for backtest and live** -- write once, backtest it, deploy it

---

## Quick Start

```python
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
```

No local MCP server scripts. No npm installs. No explicit built-in tool lists. The `@agent_tool` decorator wraps a standard REST API call. LumiBot includes all built-in tools by default alongside your custom tools.

---

## External Data Patterns

### Pattern 1: @agent_tool wrapping a REST API (recommended)

This is the primary and recommended approach. It works reliably in both backtests and live trading because you control the HTTP call directly.

```python
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
```

This pattern works with any REST API -- Alpaca, FRED, Alpha Vantage, or your own internal services. All four demo strategies use this approach.

**Source code auto-inclusion:** `@agent_tool` automatically includes the function's source code in the tool description sent to the AI. The AI can see all parameters, default values, and implementation details. Write a clear docstring with an `Args` section, and the AI will understand how to call your tool correctly.

### Pattern 2: MCP server via URL (for live trading or compatible servers)

If you have a compatible MCP server, connect it by URL. This is useful for live trading scenarios or when a third-party provides a dedicated MCP server.

```python
from lumibot.components.agents import MCPServer

MCPServer(
    name="my-data-server",
    url="https://my-mcp-server.example.com/mcp",
    timeout_seconds=120,
)
```

Any MCP server with a URL that speaks the Model Context Protocol over HTTP works. There are over 20,000 available today.

---

## Built-in Tools

All built-in tools are included by default -- even when you add custom tools via `@agent_tool` or MCP servers. No need to list them.

- **Account:** `account.positions`, `account.portfolio`
- **Market:** `market.last_price`, `market.load_history_table`
- **DuckDB:** `duckdb.query`
- **Orders:** `orders.submit`, `orders.cancel`, `orders.modify`, `orders.open_orders`
- **Docs:** `docs.search`

---

## System Prompts

LumiBot handles common instructions in its base prompt (backtesting safety, investor policy, position sizing, DuckDB guidance). Your system prompt should be 2-3 sentences about your strategy:

```python
system_prompt=(
    "Use economic data to decide whether capital should be in TQQQ "
    "or a defensive asset like SHV. Check interest rates, inflation, "
    "and growth conditions. This is a binary allocator."
)
```

---

## Replay Cache

In backtesting, identical prompt + context + tools + timestamp = cached result. Warm reruns make zero LLM calls and zero external API calls. This gives you deterministic backtests and cost control.

---

## Observability

Every run produces:

- A compact summary log line (agent name, model, cache status, warnings, summary)
- A structured JSON trace (full prompt, tool calls, results, warnings)
- Machine-readable artifacts (`agent_run_summaries.jsonl`, `agent_traces.zip`)

---

## Canonical Demos

Four demo strategies validate the full runtime. All use the `@agent_tool` pattern with the `requests` library and are located in `lumibot/example_strategies/`:

1. **News Sentiment Strategy** (`lumibot/example_strategies/agent_news_sentiment.py`) -- Alpaca News API, event-driven stock selection
2. **Macro Risk Strategy** (`lumibot/example_strategies/agent_macro_risk.py`) -- Alpaca market data API, macro regime allocation
3. **Momentum Allocator Strategy** (`lumibot/example_strategies/agent_momentum_allocator.py`) -- Alpaca price bars + news, momentum and sentiment allocation
4. **M2 Liquidity Strategy** (`lumibot/example_strategies/agent_m2_liquidity.py`) -- FRED public data, money supply allocation

Each demo produces benchmarked tearsheets, full traces, and validates replay caching.

---

## Architecture

Key code locations:

- Agent manager: `lumibot/components/agents/manager.py`
- Runtime wrapper: `lumibot/components/agents/runtime.py`
- DuckDB integration: `lumibot/components/agents/duckdb_tools.py`
- Schemas: `lumibot/components/agents/schemas.py`
- Strategy integration: `lumibot/strategies/_strategy.py`

---

## Frequently Asked Questions

**Can I backtest an AI trading agent?**

Yes, LumiBot is the only production framework that lets an AI agent reason, call tools, and execute trades on every bar during a backtest. The agent runs inside `on_trading_iteration()`, receives point-in-time market state, and uses tools to make decisions -- all within the backtest simulation. A built-in replay cache makes warm reruns deterministic and fast.

**What makes LumiBot different from other AI trading frameworks?**

Most alternatives either put the LLM outside the backtest loop (QuantConnect), have no backtesting at all (CrewAI, AutoGen, LangGraph), or are hobby scripts with no infrastructure. LumiBot is the only production framework that runs the AI agent inside the backtest simulation on every bar, with `@agent_tool` for reliable external data, MCP server support, replay caching, DuckDB time-series queries, and full observability -- all with the same code for backtest and live.

**What AI models are supported?**

The default model is Gemini (`gemini-3.1-flash-lite-preview`). You need a `GOOGLE_API_KEY` environment variable set. The architecture supports OpenAI, Anthropic, and other providers through the underlying model router. Pass the model name via the `default_model` parameter when creating your agent.

**How do I get started?**

Install LumiBot, set `GOOGLE_API_KEY` in your environment, copy the Quick Start example above, and run it. The M2 Liquidity Strategy example is a complete, runnable strategy file. See the public docs at `https://lumibot.lumiwealth.com/agents.html` for additional patterns and the four reference demo strategies.

**What API keys do I need?**

At minimum, `GOOGLE_API_KEY` for the Gemini model that powers the agent. If your `@agent_tool` functions call external APIs, you also need those keys -- for example `ALPACA_API_KEY` and `ALPACA_API_SECRET` for Alpaca data APIs. The M2 Liquidity demo only needs `GOOGLE_API_KEY` because FRED data is public.

**How do I set up my environment?**

Create a `.env` file in your project directory with your API keys (e.g., `GOOGLE_API_KEY=your_key_here`). LumiBot reads environment variables at startup. You can also export them in your shell. For backtesting, set `BACKTESTING_DATA_SOURCE` in `.env` or use `datasource_class=None` to defer to the environment configuration.

**Can I use this for live trading?**

Yes. The same strategy code runs in both backtest and live modes. For live trading, connect to a supported broker (Alpaca, Interactive Brokers, Tradier, Schwab, and others). No code changes are required -- LumiBot handles the broker integration.

**Does it work with my broker?**

LumiBot supports Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, CCXT (crypto), Bitunix, and more. Any broker supported by LumiBot works with AI agents. The agent submits orders through the standard LumiBot order execution pipeline.

**What is @agent_tool?**

`@agent_tool` is a decorator that wraps a Python method as a callable tool the AI agent can invoke during its reasoning loop. You provide a name and description, write a standard method with type hints and a docstring, and the decorator handles the rest. The function's source code is automatically included in the tool description so the AI can see parameters, defaults, and implementation details.

**How does the agent know what parameters my tool accepts?**

`@agent_tool` automatically includes the function's entire source code in the tool description sent to the AI model. The AI sees your type hints, default values, and docstring. Write a clear docstring with a Google-style `Args` section and the AI will understand how to call your tool.

**Do I need to list built-in tools?**

No. All built-in tools (positions, portfolio, prices, orders, DuckDB, docs) are always included automatically. When you pass custom tools via `tools=[self.my_tool]`, they are added alongside the built-in tools. You only need to list your custom `@agent_tool` functions.

**Can I use multiple custom tools?**

Yes. Pass a list of tools when creating the agent: `tools=[self.tool_a, self.tool_b, self.tool_c]`. The Macro Risk and Momentum Allocator demos both use multiple `@agent_tool` functions in a single strategy. There is no hard limit on the number of custom tools.

**What REST APIs can I wrap with @agent_tool?**

Any REST API that returns JSON or text. The four canonical demos wrap Alpaca News API, Alpaca Bars API, Alpaca Screener API, and the FRED public CSV endpoint. You can wrap Alpha Vantage, your own internal services, SEC EDGAR, social sentiment APIs, or anything else accessible over HTTP.

**How do I add authentication to my tool?**

Read API keys from environment variables inside your `@agent_tool` function using `os.environ.get("MY_API_KEY")`. Pass them as headers or query parameters in your `requests` call. See the Alpaca demos for examples that use `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` headers.

**What happens if my tool returns an error?**

Return a dictionary with an `"error"` key (e.g., `return {"error": str(e)}`). The agent sees the error and can decide to retry, try a different approach, or proceed without that data. An observability warning is also recorded in the trace. Wrap your HTTP call in a try/except block to handle network failures gracefully.

**Can I use MCP servers instead of @agent_tool?**

Yes. Pass an `MCPServer` object with a URL when creating the agent. However, `@agent_tool` is the recommended primary pattern because you control the HTTP call directly, it works reliably in both backtests and live trading, and it does not require external server infrastructure.

**What is the difference between @agent_tool and MCP servers?**

`@agent_tool` wraps a Python method that makes HTTP calls via `requests` -- you control the code, it runs in-process, and it works reliably in backtests. MCP servers are external services that speak the Model Context Protocol over HTTP. MCP servers are useful when a third party provides a dedicated server or you need access to one of the 20,000+ public MCP servers, but `@agent_tool` is more reliable for backtesting and gives you full control.

**How long should my system prompt be?**

Two to three sentences describing your strategy intent. For example: what data to use, what assets to trade, and what the allocation logic should be. LumiBot handles position sizing, DuckDB guidance, backtesting safety, time-awareness, and the default investor policy in its base prompt.

**What should I put in the system prompt?**

Describe your strategy's thesis and the assets it trades. Do not repeat instructions about position sizing, order execution, look-ahead bias, or tool usage -- LumiBot covers all of that in the base prompt. A good example: `"Use economic data to decide between TQQQ and SHV. Check interest rates, inflation, and growth conditions."`

**What does LumiBot handle automatically in the base prompt?**

The base prompt tells the agent whether the run is a backtest or live, the current datetime and timezone, current positions and cash, rules about look-ahead bias, the default investor policy (conviction over activity, no overtrading), position sizing and limit order preferences, and DuckDB conventions and tool usage guidance.

**Can I override the default investor policy?**

The base prompt includes a default policy favoring conviction over activity and discouraging overtrading. Your system prompt can direct the agent toward different behavior -- for example, telling it to rebalance daily or trade more aggressively. The system prompt is added on top of the base prompt, so your instructions take priority for strategy-specific guidance.

**How do I make the agent more aggressive or more conservative?**

Add explicit direction in your system prompt. For a more aggressive agent: `"Trade actively. Rebalance into high-conviction positions quickly."` For a more conservative agent: `"Only trade when evidence is overwhelming. Prefer holding cash or SHV when uncertain."` The agent follows your prompt guidance.

**How does backtesting work with AI agents?**

The agent runs inside `on_trading_iteration()` on every bar (e.g., every trading day if `sleeptime="1D"`). On each bar, the agent receives point-in-time market state, calls tools (both built-in and custom), reasons over the data, and submits orders. The backtest simulation processes those orders at simulated market prices. The replay cache makes warm reruns deterministic.

**How does the agent avoid looking into the future during backtests?**

LumiBot injects the simulated datetime into the agent's context and the base prompt includes explicit rules about look-ahead bias. The observability system also flags future-dated data warnings if a tool result references data published after the simulated backtest time. Your `@agent_tool` functions should respect date parameters to avoid requesting future data.

**What is the replay cache?**

In backtesting mode, LumiBot caches every agent run keyed by a SHA-256 hash of the prompt, context, model, tool surface, and simulated timestamp. When a subsequent backtest hits the same combination, the cached result is returned instantly without calling the LLM or any external tool. This makes warm reruns deterministic, fast, and cost-free.

**How do I clear the cache for a fresh run?**

Delete the replay cache directory. On macOS the default location is `~/Library/Caches/lumibot/agent_runtime/replay/`. You can also set the `LUMIBOT_CACHE_FOLDER` environment variable to control where caches are stored. After clearing, the next run will make fresh LLM and tool calls.

**How long does a backtest take?**

A cold run (no cache) depends on the number of bars, the number of tool calls per bar, and the LLM response time. A six-year daily backtest with one tool call per bar might take 20-40 minutes on the first run. A warm run (fully cached) completes the same backtest in seconds because no LLM or external API calls are made.

**Can I speed up backtests?**

Use the replay cache -- after the first cold run, all subsequent runs with the same inputs are near-instant. You can also reduce the date range, increase the `sleeptime` to trade less frequently, or use a faster model. Keeping your `@agent_tool` functions fast (short timeouts, efficient parsing) also helps.

**What data sources work for backtesting?**

Set `datasource_class=None` to use the data source from your `.env` file (via `BACKTESTING_DATA_SOURCE`). For standalone examples, use `YahooDataBacktesting`. LumiBot also supports ThetaData, Polygon, and other data sources for backtesting. The data source controls price bars and market data; your `@agent_tool` functions provide any additional external data.

**How do I see what the agent is doing?**

Every agent run emits a compact summary log line with the agent name, model, cache status, tool call count, warning count, and the agent's summary conclusion. For deeper inspection, open the structured JSON trace file. See `docs/AI_TRADING_AGENT_COMPONENT_GUIDE.md` for the full debugging workflow.

**What are agent traces?**

Traces are structured JSON files that record everything the agent did during a single run: the full prompt surface, every tool call with arguments, every tool result, the agent's reasoning and summary, observability warnings, cache hit/miss status, and DuckDB query metrics. They are the source of truth for debugging.

**Where are trace files stored?**

Trace files are stored in the LumiBot cache directory under `agent_runtime/`. The trace path is available on the result object via `(result.payload or {}).get("trace_path")`. Machine-readable summaries are also written to `agent_run_summaries.jsonl`.

**How do I debug a bad trade?**

Open the trace JSON for the run where the bad trade occurred. Check what tools the agent called, what data it received, and what reasoning it stated. Look for observability warnings (future-dated data, no tools called, unsupported orders). Compare the agent's summary to the actual trade.

**Why is my agent not trading?**

Check the agent's summary in the logs -- it may have decided not to trade because conviction was low. The default investor policy in the base prompt encourages conviction over activity. If you want more frequent trading, adjust your system prompt to be more directive. Also verify that your tools are returning valid data by inspecting the trace.

**Why is my agent only buying SHV?**

SHV is a common defensive parking asset used in the demo strategies. If the agent only buys SHV, it means the agent is not finding enough conviction to take risk. Check whether your tool is returning useful data (inspect the trace), whether the system prompt is clear about when to be risk-on, and whether the market data covers the right date range.

**How much does it cost to run?**

Cost depends on the LLM provider and model, the number of bars in your backtest, and how many tool calls the agent makes per bar. A six-year daily backtest might cost a few dollars on the first cold run with a fast model like Gemini Flash. Warm reruns cost nothing because the replay cache eliminates all LLM and external API calls.

**How can I reduce API costs?**

Use the replay cache -- once a backtest is cached, subsequent runs are free. Use cost-effective models (e.g., `gemini-3.1-flash-lite-preview`). Keep your backtest date range focused during development. Reduce the number of tool calls by making your tools return comprehensive data in a single call rather than requiring multiple round trips.

**How does replay caching reduce costs?**

The replay cache stores every agent run result keyed by a hash of the inputs. When the same prompt, context, tools, model, and timestamp appear again, the cached result is returned with zero LLM calls, zero external API calls, and zero cost. A cold backtest that costs a few dollars becomes free on every subsequent warm run.
