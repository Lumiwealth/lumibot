# AI Trading Agent Component Guide

> Internal companion to the public `docsrc/agents_quickstart.rst` page. Describes how the AI agent component works inside a LumiBot strategy, focused on BotSpot strategy generation.

**Last Updated:** 2026-03-30
**Status:** Active
**Audience:** Internal (BotSpot Agent, contributors)

---

## Purpose

LumiBot's AI runtime is a strategy component. It is not a separate orchestration product and it does not bypass the normal strategy lifecycle.

The core usage pattern is:

1. Create the agent in `initialize()`
2. Built-in tools are included by default (no explicit listing needed)
3. Add external data via `@agent_tool` (recommended) or MCP server URL
4. Run the agent from the lifecycle method that needs it
5. Inspect `result.summary`, warnings, traces, and cache behavior

---

## Public API Surface

Primary imports:

- `agent_tool` -- for wrapping REST APIs as callable tools (recommended pattern)
- `MCPServer` -- for connecting external MCP servers via URL
- `self.agents.create(...)` -- create an agent in `initialize()`
- `self.agents["name"].run(...)` -- run the agent from a lifecycle method

Result surface:

- `result.summary` -- the agent's concluding summary
- `result.text` -- full text output
- `result.cache_hit` -- whether result came from replay cache
- `result.warning_messages` -- list of observability warnings
- `result.tool_calls` -- list of tool call events
- `result.tool_results` -- list of tool result events
- `(result.payload or {}).get("trace_path")` -- path to JSON trace

---

## Design Rules

- The strategy owns timing, scheduling, and execution
- The agent is a component inside the strategy, not a separate platform
- Built-in tools (positions, portfolio, prices, history, DuckDB, orders, docs) are included by default -- even when custom tools are added
- External data comes from `@agent_tool` (primary) or MCP servers via URL (alternative)
- System prompts should be 2-3 sentences; LumiBot's base prompt handles everything else
- The strategy should pass point-in-time context explicitly in backtests
- The runtime supports arbitrary MCP servers without restriction

---

## External Data Patterns

### Pattern 1: @agent_tool wrapping a REST API (recommended)

This is the primary and recommended pattern. It works reliably in both backtests and live trading.

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

**Source code auto-inclusion:** `@agent_tool` automatically includes the function's source code in the tool description sent to the AI. The AI can see all parameters, default values, and implementation details without you having to describe them manually. Write a clear docstring with an `Args` section, and the AI will understand how to call your tool correctly.

### Pattern 2: MCP server via URL (for live trading or compatible servers)

If you have a compatible MCP server, connect it by URL:

```python
from lumibot.components.agents import MCPServer

MCPServer(
    name="my-data-server",
    url="https://my-mcp-server.example.com/mcp",
    timeout_seconds=120,
)
```

Any MCP server that speaks the Model Context Protocol over HTTP or Streamable HTTP works with LumiBot. There are over 20,000 MCP servers available today. The `@agent_tool` pattern is recommended for most use cases because it gives you full control and works reliably in backtests.

---

## Teaching This in BotSpot Agent

BotSpot Agent should teach this API through:

- **Shared components:** Show the `@agent_tool` pattern (primary) as the recommended approach for external data. Show `MCPServer(url=...)` as an alternative for live trading or compatible servers. Note that built-in tools are default.
- **Shared examples:** `@agent_tool` wrapping REST APIs with docstrings containing `Args` sections. Show that source code is auto-included in the tool description.
- **Shared reminders:** `@agent_tool` is the recommended pattern. MCP servers are URLs. Built-in tools are included automatically. System prompts are 2-3 sentences.

Key points for generated strategies:

- No `LUMIBOT_ROOT` path variables
- No `sys.executable` or script paths
- No explicit `BuiltinTools.xxx()` listing (they are default)
- `datasource_class=None` reads from `.env` config
- Flat `if __name__ == "__main__"` with `IS_BACKTESTING`
- `@agent_tool` functions should always have docstrings with `Args` sections
- Source code is automatically included in tool descriptions -- the AI can see parameters and defaults
