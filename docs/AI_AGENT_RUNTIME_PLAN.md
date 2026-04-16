# AI_AGENT_RUNTIME_PLAN

> Research summary, implementation notes, and forward plan for LumiBot agentic execution with backtest/live parity.

**Last Updated:** 2026-03-24
**Status:** Implemented on this branch, with follow-on roadmap items still open
**Audience:** Both

---

## Overview

LumiBot already has the core runtime pieces needed for agentic trading: repeated lifecycle hooks, market-aware scheduling, broker/data abstractions, order management, and a strong backtest/live parity philosophy. The missing piece is a first-class agent runtime that fits LumiBot's execution model instead of importing a broad computer-use agent into a trading engine.

This document recommends a backtest-first design:

- `Strategy` gets a built-in `self.agents` manager.
- Users create agents in `initialize()` and call `agent.run(...)` from any lifecycle method.
- ADK is the recommended orchestration layer, but it must be wrapped behind LumiBot-native tools, permissions, memory, and replay caching.
- Backtesting is the product constraint. If the same inputs are replayed, the same agent event stream and decision should be reusable without another model call.

---

## North Star

**North Star Metric:** agent-driven strategies that rerun with identical decisions in deterministic backtests and preserve trade behavior when moved to paper/live reference windows.

### Proposed OKRs

- `>= 95%` replay-cache hit rate on reruns of identical backtests.
- `0` unsafe live tool mutations without explicit permission configuration.
- `< 250 ms` median wall time for replay-cache hits.
- Fixed-window backtest vs paper/live trade-diff rate tracked weekly.

---

## What LumiBot Already Has

LumiBot is already a strong host runtime for this feature.

- Repeated lifecycle execution already exists via `initialize()`, `on_trading_iteration()`, order callbacks, and market-open/close hooks.
- The strategy API already exposes the right trading primitives: orders, positions, cash, history, quotes, portfolio state, and broker/session metadata.
- `self.vars` already exists as the strategy state container and is the right place to persist agent session snapshots.
- Backtesting already has a cache architecture and an S3 sync pattern that can be extended for agent replay.
- The `components/` folder already contains high-level helper objects, so an agent runtime can be implemented as a component internally while remaining ergonomic on `Strategy`.

This branch adds the first version of a core agent runtime with `self.agents.create(...)`, replay caching, DuckDB-backed history tools, custom tools, and external MCP server mounting. The remaining work is hardening, broader benchmarking, and follow-on capabilities.

---

## Recommended User API

### Principle

The user-facing API should feel native to LumiBot:

- create agents in `initialize()`
- call `agent.run(...)` anywhere
- keep scheduling under user control
- allow multiple agents with different prompts and permission sets
- support custom tools without making users learn ADK internals

### Recommended Shape

Expose a built-in `self.agents` manager on every `Strategy`.

Use a component package internally, but keep the strategy surface simple:

```python
from lumibot.components.agents import BuiltinTools, MCPServer, agent_tool
from lumibot.strategies import Strategy


class AgentMomentumStrategy(Strategy):
    parameters = {
        "symbol": "SPY",
        "run_every_n_iterations": 5,
    }

    @agent_tool(
        name="get_watchlist_bias",
        description="Return the current discretionary watchlist bias for a symbol.",
    )
    def get_watchlist_bias(self, symbol: str) -> dict:
        """Return a small structured bias payload for one symbol."""
        return {"symbol": symbol, "bias": "neutral"}

    def initialize(self):
        self.set_market("stock")
        self.sleeptime = "1M"
        self.vars.iteration_count = 0

        self.agents.create(
            name="research",
            default_model="gemini-2.5-flash",
            system_prompt=(
                "You are a conservative trading agent. "
                "Use DuckDB for time-series analysis. "
                "Do not overtrade. If uncertain, do nothing."
            ),
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.account.portfolio(),
                BuiltinTools.market.last_price(),
                BuiltinTools.market.load_history_table(),
                BuiltinTools.duckdb.query(),
                BuiltinTools.orders.submit(),
                BuiltinTools.orders.cancel(),
                self.get_watchlist_bias,
            ],
            mcp_servers=[
                MCPServer(
                    name="alpaca-news",
                    url="https://alpaca.example.com/mcp",
                    allowed_tools=["get_historical_news"],
                ),
            ],
        )

        self.agents.create(
            name="post_fill",
            default_model="gemini-2.5-flash",
            system_prompt=(
                "A fill just happened. Re-evaluate exposure and decide "
                "whether any follow-up orders or stop adjustments are needed."
            ),
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.orders.open_orders(),
                BuiltinTools.orders.modify(),
                BuiltinTools.orders.cancel(),
            ],
        )

    def on_trading_iteration(self):
        self.vars.iteration_count += 1
        if self.vars.iteration_count % self.parameters["run_every_n_iterations"] != 0:
            return

        symbol = self.parameters["symbol"]
        result = self.agents["research"].run(
            task_prompt=f"Review {symbol} and decide whether to trade.",
            context={
                "symbol": symbol,
                "max_risk_pct": 0.01,
            },
            model="gemini-2.5-flash",
        )

        if result.summary:
            self.log_message(result.summary, color="blue")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.agents["post_fill"].run(
            task_prompt="A fill occurred. Decide whether anything needs follow-up.",
            context={
                "symbol": order.asset.symbol,
                "price": price,
                "quantity": quantity,
            },
            model="gemini-2.5-pro",
        )
```

### Why this API

- It matches how LumiBot users already think: initialize state in `initialize()`, act in lifecycle hooks.
- It keeps control with the user. The agent runs when the strategy decides to run it.
- It supports multiple prompts cleanly by letting users create multiple agents.
- It leaves room for future multi-agent systems without forcing swarms on everyone on day one.

### Important API Rules

- `agent.run(...)` should be callable from any lifecycle method, including `on_trading_iteration`, `before_market_opens`, `on_filled_order`, and `after_market_closes`.
- The **system prompt** should live on agent creation. This is the agent's standing role, rules, and trading style.
- The per-call `task_prompt` should describe the current job or event. This is what the agent needs to do *right now*.
- `task_prompt` should be optional. Many runs can rely on the standing `system_prompt` plus structured `context`.
- `default_model` on creation should set the default model for that agent.
- The per-call `model` should be optional and should override the default only for that run.
- The public `tools` list should be the main allowlist. If a tool is not exposed there, the agent should not see it or call it.
- If users want different "system prompts" for different situations, the clean path is multiple named agents, not mutating a single agent's identity every run.
- Avoid storing the live agent handle in `self.vars`. `self.vars` is for serializable state, while the live handle should stay owned by `self.agents`.

### Public API Simplifications

The following are useful runtime concepts, but they do **not** need to be explicit day-one user-facing arguments:

- separate `permissions` map
- explicit `cache_mode`
- explicit `memory_scope`

Recommended v1 approach:

- `tools` is the public permission model
- cache behavior defaults from runtime mode:
  - backtest => replay cache on
  - live => trace-only
- memory defaults to per-agent continuity
- advanced policies remain internal or expert-only until the core surface is proven

### Custom Tool Definition

Custom tools need more than "just a Python method".

They need:

- a stable tool name
- a natural-language description
- typed inputs
- structured output expectations

Recommended v1 path:

- allow class-local tools through `@agent_tool(...)`
- infer schema from type hints
- allow explicit description text on the decorator
- also allow reusable standalone tool objects for teams that prefer not to place tool code on the strategy class

That keeps strategy code ergonomic while still preserving MCP-like tool metadata.

---

## Component Structure

This feature should be implemented as a component internally and as a strategy convenience API externally.

### Internal Structure

Recommended package shape:

```text
lumibot/
  components/
    agents/
      __init__.py
      manager.py
      runtime.py
      session_store.py
      tools.py
      builtins.py
      permissions.py
      replay_cache.py
      traces.py
      duckdb_tools.py
```

### External Structure

- `Strategy` gets `self.agents`.
- `self.agents.create(...)` returns an `AgentHandle`.
- `AgentHandle.run(...)` executes a single agent turn.
- Custom tool helpers live under `lumibot.components.agents`.

This gives us:

- component-level extensibility
- a natural strategy API
- future compatibility with sub-agents and swarms

---

## Tool Model

### Rule

The agent should only see **LumiBot-native tools** plus explicitly added custom tools.

Do **not** expose shell, filesystem, browser, or arbitrary HTTP as default tools.

### Built-in Tool Families

- `account.*`
  - positions
  - portfolio value
  - cash
  - open orders
- `market.*`
  - last price
  - quote
  - load history table
  - option chain helpers later
- `orders.*`
  - submit
  - cancel
  - modify
  - sell_all
- `cash.*`
  - deposit
  - withdraw
- `duckdb.*`
  - query
- `external.*`
  - news later
  - macro later
  - custom data providers later

### Permissions

Permissions should be explicit and user-configured.

Recommended model:

- default to read-only tools
- require explicit opt-in for:
  - `orders.submit`
  - `orders.modify`
  - `orders.cancel`
  - `cash.deposit`
  - `cash.withdraw`
  - `external.news`
  - any user-defined mutation tool

The simplest live-trading rule for v1 is:

- read-only by default
- mutating only when the user explicitly enables the relevant tool permission
- optional confirmation hooks for risky live mutations

For live trading, risky tools should support confirmation or policy hooks. For backtesting, permissions are still enforced, but there is no human approval loop.

### Public vs Internal Policy

For the public API, the user's mental model should be:

- if the tool is present, the agent can use it
- if the tool is absent, the agent cannot use it

That means the **public** API can collapse "tools" and "permissions" into one concept.

Keep a richer policy layer internally for:

- future confirmation flows
- live-vs-backtest tool gating
- mutating external MCP tools
- audit metadata

---

## DuckDB Is Mandatory

LLMs are bad at interpreting large time-series payloads directly. DuckDB should be a first-class agent tool from day one.

### Recommended Pattern

Provide built-in tools that support **load once, query many**:

- `market.load_history_table(...)`
- `duckdb.describe(table=...)`
- `duckdb.query(sql=...)`

Example agent flow:

1. load `SPY` minute bars into a temporary table
2. query moving averages, volatility, gaps, or breakout conditions with SQL
3. decide whether to trade

### Why this matters

- keeps prompts small
- keeps analysis structured
- makes replay caching easier
- aligns with current LumiBot/BotSpot use of DuckDB and Parquet

### Required Runtime Behavior

DuckDB is only useful if the data path is efficient.

Required rules:

- the first historical load should come from LumiBot's warm market-data path
- repeated queries in the same run must reuse an already-loaded table or relation
- `duckdb.query(...)` should return small structured results, not full raw time-series payloads
- loading the same frame into DuckDB on every tool call is not acceptable

Recommended internal design:

- keep a per-strategy or per-agent DuckDB session alive across iterations
- keep a warm table cache keyed by normalized data request
- register already-loaded pandas/polars frames once, then query many times
- expose only point-in-time filtered views to the agent to avoid lookahead

Recommended point-in-time pattern:

- LumiBot prefetches the underlying frame once through its existing warm data path
- DuckDB registers that in-memory frame once
- the agent queries a `visible_*` view filtered to `timestamp <= current strategy dt`
- each new iteration updates the visible cutoff, not the full table load

This matches two existing local design patterns:

- LumiBot backtesting docs already require "prefetch once -> slice forever" for repeated history access
- BotSpot Node's DuckDB path already proved that exact SQL over full tabular data is better than RAG/file-search chunking, especially for aggregations

### Pandas vs DuckDB

This does **not** mean LumiBot should stop using pandas or polars internally.

Recommended split:

- pandas/polars remain the in-process data container and historical price source
- DuckDB becomes the query surface exposed to the agent

That gives us the best of both:

- LumiBot keeps its current data-source/runtime ergonomics
- the agent gets an exact SQL tool instead of trying to reason over giant frames in prompt text

### Parquet and Analytics

Parquet remains important, but for a different reason.

- use Parquet for persisted backtest artifacts and trace analytics
- use in-memory frames / temporary DuckDB tables during live iteration and backtest execution

Do not force every runtime tool call through a Parquet serialization step.

### Future Extension

The same pattern works for:

- Alpaca historical news
- FRED/ALFRED macro series
- Quiver/congressional data
- other timestamped data marketplace feeds

The important design choice is that external data tools must be **point-in-time addressable** if they are allowed in backtests.

---

## Memory Model

Memory is required. Agents must not forget what happened on the previous cycle.

### Recommended Model

Use two layers in v1:

- **ADK session state / session memory** for the active agent conversation
- **`self.vars` snapshot persistence** for LumiBot lifecycle continuity
- **optional remote sync** only when a remote cache backend is already configured

### Why `self.vars`

`self.vars` is already the LumiBot state container:

- it persists through a strategy run
- it is the natural place for per-agent session snapshots
- in live trading, the existing vars backup/load path can later persist agent session state across restarts

This is one place LumiBot should **not** copy BotSpot Agent directly. BotSpot Agent is intentionally stateless per turn and relies on Node to resend conversation history. LumiBot has a persistent strategy object and should use that advantage.

### Recommended Internal Shape

```python
self.vars.agent_sessions = {
    "research": {
        "session_state": {...},
        "memory_notes": [
            "Opened SPY position because intraday momentum aligned with higher timeframe trend.",
            "Risk limit for this thesis is 1.0% of equity.",
        ],
        "open_theses": [
            {
                "symbol": "SPY",
                "entered_at": "2026-03-24T14:35:00Z",
                "reason": "Momentum continuation",
                "invalidates_if": "VWAP fails with expanding downside volume",
            }
        ],
        "last_cache_key": "...",
        "last_run_at": "...",
    },
    "post_fill": {
        "session_state": {...},
    },
}
```

### Local First, Remote Optional

S3 or any remote object store should **not** be required.

Recommended behavior:

- local LumiBot usage: persist memory and replay artifacts locally
- BotSpot / ephemeral workers: sync memory snapshots and replay artifacts through the existing remote cache path when configured
- one code path, two storage backends

The runtime should prefer:

- local filesystem by default
- remote cache/object storage only when credentials/config already exist

### Why V1 Should Stay Simple

For LumiBot v1, memory should be:

- auditable
- replayable
- cheap
- easy for users to inspect

That means the recommended first pass is:

- structured text summaries
- explicit notes / theses / reminders
- deterministic serialization into `self.vars`

This is enough to answer the trading-specific question that matters:

- why did the agent enter?
- what is it waiting for?
- what invalidates the thesis?

### Why Not Vector / Graph Memory in V1

Projects like Claude-Mem, Mem0, Hindsight, TradingGPT, and FinMem are useful references, but they solve a broader memory problem than LumiBot needs in the first release.

Vector or graph memory adds:

- more latency
- more storage complexity
- more difficult replay semantics
- another retrieval model whose behavior must be versioned and cached

Recommendation:

- v1: text-first structured memory backed by ADK session state + `self.vars`
- v2: optional pluggable long-term memory backend

Long-term vector memory can come later. It is not required for v1.

---

## Replay Cache and Trace Artifacts

This is the most important design area after the user API.

### Rule

Backtests should cache **the full agent run**, not only the final decision.

That means caching:

- normalized input payload
- system instruction hash
- model/provider settings
- tool call sequence
- tool outputs
- event stream
- final decision
- summary / result metadata

If ADK emits "thinking" events, those should be cached too. Do not assume every provider exposes the same reasoning data; cache exactly what the runtime receives.

The replay cache should store the **normalized LumiBot event stream**, not only provider-native event objects. BotSpot already uses this pattern for `thinking`, `tool_call`, `tool_result`, and text events, and that is the right abstraction to copy.

### Two Storage Targets

Use two different stores for two different jobs.

#### 1. Shared Replay Cache

Purpose:

- speed up reruns across backtests
- survive ephemeral backtest containers
- reuse identical agent runs across jobs

Recommendation:

- extend LumiBot's existing remote cache architecture
- store under a dedicated agent namespace in the same cache backend pattern
- use S3 for BotSpot/backtest workers
- scope entries to a user cache namespace for privacy
- do **not** store replay-cache entries only under a per-backtest results prefix; replay cache must be cross-run reusable

Suggested key prefix:

```text
<cache-prefix>/<version>/agents/replay/<user-scope>/<hash>.json.zst
```

### Why a Compressed Trace Blob Beats Parquet for Replay

Parquet is the right format for analytics tables, but it is not the best canonical format for exact event replay.

Recommended split:

- replay object: compressed canonical trace blob (`json.zst` or msgpack-equivalent)
- analytics views: Parquet artifacts (`agent_runs.parquet`, `agent_events.parquet`, `agent_tool_calls.parquet`)

Why:

- the replay object is nested and event-oriented
- exact rehydration is simpler from one canonical blob
- Parquet is still excellent for downstream analysis, filtering, dashboards, and DuckDB queries

#### 2. Per-Backtest Trace Artifacts

Purpose:

- debugging
- user inspection
- DuckDB analytics
- reproducibility audits

Recommendation:

Emit backtest artifacts such as:

- `agent_runs.parquet`
- `agent_events.parquet`
- `agent_tool_calls.parquet`
- optional `agent_trace.jsonl`

These should upload with the rest of the backtest artifacts.

### Cache Key Inputs

The replay key should include more than "same time of day".

Recommended key inputs should reflect the **agent-visible input surface**, not arbitrary strategy code changes.

Recommended key inputs:

- user-scope identifier
- agent identity
- model identifier
- model provider
- model settings
  - temperature
  - top_p if used
  - seed if supported
- base instruction hash
- run prompt hash
- tool registry version
- tool implementation fingerprint
- permission set hash
- normalized context payload hash
- normalized market timestamp
- backtest/live mode
- data provider identity

### Tool Implementation Fingerprint

This matters because the same tool name can behave differently after code changes.

Fingerprint sources should include:

- built-in tool version
- custom tool source hash where possible
- tool adapter/runtime version

Do **not** key the replay cache on the full strategy code hash by default. If unrelated strategy code changes do not change the agent-visible inputs or tool behavior, the cache should still be reusable.

### Cache Modes

These are useful runtime controls, but they do **not** need to be exposed in the default public API.

- `off`
- `record_replay`
- `strict_replay`
- `trace_only`

#### `record_replay`

- miss: execute model/tools and save result
- hit: replay cached event stream and decision

#### `strict_replay`

- miss: fail fast
- use for deterministic CI or parity reruns

#### `trace_only`

- live mode default candidate
- record full traces, but do not reuse them automatically

### Replay Behavior

On replay hit:

- do **not** call the model
- do **not** re-execute tools
- rehydrate the cached event stream
- return the cached `AgentRunResult`
- mark the run as `cache_hit=True`

### Existing LumiBot Patterns to Reuse

LumiBot already has:

- remote S3 cache configuration
- versioned cache prefixes
- local marker files
- backtest artifact uploads

This feature should extend those patterns rather than creating a second unrelated caching system.

---

## Live Trading vs Backtesting

### Backtesting

Backtesting is the stricter mode.

- only point-in-time tools should be allowed by default
- replay cache should be enabled
- trace artifacts should always be written
- external web/news tools should be disabled unless they are backed by timestamped, replayable data sources

### Live Trading

Live trading can be more flexible.

- replay cache should be off or `trace_only` by default
- external tools can be allowed if the user opts in
- permission hooks and confirmations matter more
- trace capture should be on by default for auditability

### Same Strategy Code

The same strategy should work in both modes.

The difference should come from:

- tool availability
- cache mode
- model selection
- provider configuration

not from requiring a separate strategy authoring style.

---

## Framework Evaluation

## Recommended Choice: ADK

ADK is the recommended orchestration layer for LumiBot.

### Why ADK Wins

- already used in BotSpot
- supports sessions, state, memory, tools, MCP, workflow agents, and multi-agent composition
- supports action confirmations and policy hooks
- officially supports non-Google model connectors:
  - LiteLLM
  - Ollama
  - vLLM
  - Claude
- has a clean enough abstraction boundary that LumiBot can wrap it

### Important Correction

Using ADK does **not** mean you are locked into Gemini.

Official ADK docs now document:

- LiteLLM integration
- Ollama integration
- vLLM integration
- direct model connectors

That means open-source or open-weight models are viable **if** they support reliable tool calling.

### Why ADK Still Fits V1 Best

ADK gives LumiBot the right primitives without forcing LumiBot to expose ADK directly:

- sessions/state for per-agent continuity
- memory hooks for future long-term extensions
- custom function tools for LumiBot-native trading tools
- MCP support for future external toolsets
- action confirmations for risky live mutations
- multi-agent composition later, without making it mandatory in v1

### Real Constraint

The real constraint is not ADK itself. It is model quality for:

- tool selection
- tool arguments
- multi-step loop stability
- refusal to over-call tools
- avoiding infinite tool loops

This is especially important for Ollama/vLLM-class deployments. ADK's own docs explicitly warn that model choice and tool support matter, and they show special handling for local model hosts.

### Alternatives Considered

#### OpenAI Agents SDK

Pros:

- very clean primitives
- good tracing
- sessions and MCP support
- strong ergonomics

Cons:

- not aligned with current BotSpot investment
- less attractive if we want one shared framework across BotSpot and LumiBot

#### PydanticAI

Pros:

- excellent Python ergonomics
- strong MCP support
- strong durable execution story with Temporal/Prefect

Cons:

- would introduce a second agent framework into the stack
- less compelling when ADK already satisfies the core runtime needs

#### AutoGen

Pros:

- strong multi-agent and team patterns
- good research reference for swarms and review loops

Cons:

- heavier abstraction surface
- more scaffolding
- not the simplest fit for a strategy-runtime feature that should start with one agent and deterministic replay

### Recommendation

Use ADK first, but wrap it behind LumiBot interfaces so we can replace the backend later if needed.

Do **not** let the public LumiBot API expose ADK-specific types directly.

For built-in LumiBot tools, prefer native Python tool wrappers in v1 even if the internal registry is MCP-shaped. Requiring a literal MCP transport boundary between the strategy and its own local tool implementations adds overhead without adding value.

---

## External Project Research

### Mature Trading Engines

These are useful references for execution architecture, not for agent design.

- **LEAN**
  - mature backtest/live engine
  - strong execution substrate
  - not built around LLM decision loops
- **NautilusTrader**
  - strong event-driven architecture
  - good reference for deterministic infrastructure
- **aat**
  - event-driven trading engine
  - useful execution reference

### Agentic Trading Projects

These are useful references for prompt structure, role decomposition, and model routing.

- **TradingAgents**
  - multi-agent trading framework
  - good reference for specialized analyst roles
  - good reference for "quick model" vs "deep model" separation
  - not a LumiBot-style backtest/live engine
- **FinRobot**
  - large AI-agent finance platform
  - useful for role libraries and financial analysis workflows
  - more finance-agent platform than deterministic strategy runtime
- **FinMem**
  - layered-memory trading agent
  - strong reference for explicit trading memory structure and "character" tuning
  - useful design inspiration, but too memory-heavy for LumiBot v1
- **TradingGPT**
  - multi-agent debate with layered memory and distinct trading personas
  - strong evidence that layered memory and role diversity are useful
  - not built around reproducible replay inside a broker/backtest runtime
- **FinAgent**
  - multimodal, tool-augmented trading agent with reflection and diversified memory retrieval
  - useful reference for future news/chart/multimodal extensions
  - broader research agenda than LumiBot needs in v1
- **virattt/ai-hedge-fund**
  - very popular proof-of-concept
  - clear portfolio-manager / analyst / risk-manager split
  - includes a backtester and local-model support
  - explicitly not intended for real trading
- **ai-hedge-fund-crypto**
  - useful crypto-native example
  - good reference for portfolio-manager style agent chains
- **Magents**
  - event-driven multi-agent hedge-fund simulation with shared risk controls
  - useful reference for pod/team structure inside a backtester
  - still separate from LumiBot's broker-aware runtime
- **Open-Finance-Lab/AgenticTrading**
  - explicit MCP/A2A orchestration with DAG planner + memory agent
  - useful future-facing reference for swarms and planner/orchestrator patterns
  - still research-oriented
- **QuantAgents**
  - simulated-trading analyst + risk analyst + news analyst + manager meeting loop
  - strong reference for "strategy meeting" / "risk alert" style multi-agent workflows
  - more of a research simulator than a broker-connected engine
- **Spartan / ATLAS**
  - useful references for autonomous agent ops, market-intelligence tooling, and self-improving prompt loops
  - better inspiration for optional future layers than for LumiBot core runtime

### Main Lesson

Most popular agentic trading projects are:

- research-heavy
- prompt-heavy
- multi-agent by default
- weak on strict replayable backtest/live parity

That is the opportunity for LumiBot.

---

## Future Data Tools

These are important, but not all are required for v1.

### Strong Candidates

- Alpaca historical news
- Alpaca realtime news
- FRED / ALFRED macro series
- other timestamped marketplace feeds

### Rule for Backtests

Backtest-safe external tools must support point-in-time retrieval.

Examples:

- Alpaca historical news by time window
- ALFRED using `realtime_start` / `realtime_end`

### Recommendation

Do **not** block custom tools.

Instead:

- ship a strong built-in tool registry
- let users register custom tools
- clearly mark whether each tool is:
  - backtest-safe
  - live-only
  - mutating
  - replay-cacheable

---

## Execution Plan

### Step 1: Freeze the Public API

- finalize `self.agents.create(...)`
- finalize `agent.run(...)`
- finalize tool/permission naming
- finalize how per-run model overrides work
- finalize what the returned `AgentRunResult` contains

Deliverable:

- one approved example strategy that looks like real LumiBot code

### Step 2: Build the Minimal Single-Agent Runtime

- add the internal `components/agents/` package
- wire `Strategy` to expose `self.agents`
- implement ADK runner/session glue
- add MCP server mounting for external toolsets
- let `self.agents` own the live handles and `self.vars` own only serializable agent state
- add read-only built-in tools:
  - positions
  - portfolio
  - last price
  - quote
  - historical price load
- add basic mutating tools:
  - submit order
  - modify order
  - cancel order
- add the permission gate

Deliverable:

- a strategy can create an agent in `initialize()` and call it from `on_trading_iteration()`

### Step 3: Add DuckDB the Right Way

- implement `market.load_history_table(...)`
- implement `duckdb.describe(...)`
- implement `duckdb.query(...)`
- keep a long-lived DuckDB session so one load can support many queries
- ensure history data is sourced from LumiBot's warm in-memory path, not reloaded repeatedly
- register in-memory pandas/polars frames once, not per tool call
- enforce point-in-time visible views so SQL cannot see future rows
- add instrumentation around table loads, query counts, and per-query latency

Deliverable:

- the agent can analyze time-series data with SQL without bloating prompts

### Step 4: Add Memory and Full Trace Capture

- persist ADK session state into `self.vars`
- persist structured memory notes / open theses into `self.vars`
- normalize the event stream into LumiBot event types
- capture text, thinking, tool calls, tool results, usage, final decision
- turn live traces on by default

Deliverable:

- every run has an inspectable audit trail

### Step 5: Add Replay Cache and Backtest Artifacts

- build an input-surface-based replay key
- isolate replay cache by user scope
- default replay storage to local disk
- reuse LumiBot's remote cache path when remote cache is configured
- store canonical replay blobs for exact rehydration
- emit Parquet trace artifacts for analysis
- extend LumiBot's existing remote cache pattern for cross-run reuse

Deliverable:

- rerunning the same backtest can skip the model call entirely

### Step 6: Test, Benchmark, and Harden

- add unit tests for:
  - tool wrappers
  - MCP allowlists
  - cache key generation
  - replay hit/miss behavior
  - memory serialization
  - local-vs-remote storage fallback
  - point-in-time gating for DuckDB tables/views
  - optional `task_prompt` handling
- add integration tests for:
  - one stock backtest agent strategy
  - one options backtest agent strategy
  - external MCP tool invocation with stubbed responses
- build two real demo-quality validation strategies:
  - one stock strategy
  - one options strategy
- run both over a multi-day or one-week window so the agent executes repeatedly
- run both with a real Gemini API key on the first pass
- rerun both at least twice to prove replay cache hits on later passes
- add deterministic parity tests:
  - cache miss vs replay hit must produce identical event streams and actions
  - same input surface across two strategies should reuse cache
- add acceptance coverage:
  - queue-free warm-cache invariant still holds
  - artifacts upload correctly
- add optional live/paper smoke coverage for mutating tools
- benchmark Gemini Flash vs Pro on realistic tool loops
- measure cache hit latency, miss latency, and tool count
- measure DuckDB load count, query count, load time, and query time
- verify DuckDB does not reload per iteration or per tool call
- run YAPPI on first-pass and replay-pass backtests
- compare first-run vs replay-run wall time and API/tool activity
- verify replay runs do not call the model again when the input surface matches
- test parity between cache miss and replay hit on fixed windows
- test live vs backtest behavior on identical historical windows

#### Required Validation Loop

This feature should not be considered real until this loop passes:

1. create a stock agent strategy that a real user could run
2. create an options agent strategy that a real user could run
3. run each strategy through a real backtest with Gemini enabled
4. capture traces, cache artifacts, and YAPPI profiling output
5. rerun the same backtests and confirm replay-cache hits
6. verify that repeated runs do not hit the model again when inputs match
7. inspect profiling output to ensure DuckDB is not the dominant bottleneck
8. fix bottlenecks and rerun until the profile looks acceptable

Deliverable:

- a benchmark table, test matrix, and parity report

### Step 7: Expand Carefully

- add optional higher-level external data docs and examples
- add optional parent-agent / sub-agent patterns
- add model routing beyond Gemini only after the single-model path is solid

Deliverable:

- broader capability without destabilizing v1

### Step 8: Document Aggressively

- add public Sphinx docs
- add cookbook examples
- add "backtest-safe vs live-only tools" docs
- add prompt patterns and anti-patterns
- add caching/replay docs
- add SEO-focused landing page(s) for AI trading agents + agentic backtesting
- add homepage/docs-site promotion once behavior is stable

---

## Open Questions

- For built-in LumiBot tools, do we only need MCP-shaped metadata, or do you want an actual local MCP adapter layer so internal and external tools are literally mounted the same way?
- In backtests, should non-point-in-time external MCP tools only emit warnings, or should there also be an optional strict mode later for CI and institutional users?
- Should replay entries cache raw MCP request/response payloads exactly, or a normalized JSON form when equivalent payloads can be serialized in multiple ways?
- How much live trace retention do we want by default for paper/live runs, and where should that retention policy live?

---

## Sources

### Local Codebase

- `lumibot/lumibot/strategies/strategy_executor.py`
- `lumibot/lumibot/strategies/strategy.py`
- `lumibot/lumibot/strategies/_strategy.py`
- `lumibot/lumibot/tools/backtest_cache.py`
- `lumibot/lumibot/components/options_helper.py`
- `lumibot/lumibot/components/perplexity_helper.py`
- `lumibot/docs/BACKTESTING_SECOND_LEVEL_ROADMAP.md`
- `lumibot/docs/BACKTESTING_ACCURACY_VALIDATION.md`
- `lumibot/docs/BACKTESTING_PERFORMANCE.md`
- `lumibot/docs/BACKTESTING_TESTS.md`
- `botspot_node/docs/mcp/duckdb-csv-analytics.md`
- `botspot_node/docs/handoffs/2026-02-10_parquet_backtest_artifacts_query_speed.md`
- `botspot_node/docs/ai-agent-chat-durability.md`
- `botspot_node/docs/handoffs/2026-01-13_conversationHistory-bug-fix.md`
- `botspot_node/src/Mcp/handlers/duckdb.ts`
- `botspot_agent/src/botspot_agent/runtime.py`
- `botspot_agent/src/botspot_agent/strategy/prompts/strategy/generate_prompt.py`
- `botspot_agent/docs/handoffs/2026-01-26_mcp-resilience-improvements.md`
- `botspot_agent/docs/chat-durability.md`
- `lumibot/docs/REMOTE_CACHE.md`
- `bot_manager/PROJECT_ARCHITECTURE_MERMAID.md`

### External Documentation and Repositories

- Google ADK models: https://google.github.io/adk-docs/agents/models/
- Google ADK LiteLLM: https://google.github.io/adk-docs/agents/models/litellm/
- Google ADK Ollama: https://google.github.io/adk-docs/agents/models/ollama/
- Google ADK vLLM: https://google.github.io/adk-docs/agents/models/vllm/
- Google ADK loop agents: https://google.github.io/adk-docs/agents/workflow-agents/loop-agents/
- Google ADK multi-agent systems: https://google.github.io/adk-docs/agents/multi-agents/
- Google ADK state: https://google.github.io/adk-docs/sessions/state/
- Google ADK memory: https://google.github.io/adk-docs/sessions/memory/
- Google ADK action confirmations: https://google.github.io/adk-docs/tools-custom/confirmation/
- Google ADK context caching: https://google.github.io/adk-docs/context/caching/
- Gemini API context caching: https://ai.google.dev/gemini-api/docs/caching/
- DuckDB Python relational API: https://duckdb.org/docs/stable/clients/python/relational_api.html
- OpenClaw trust model: https://trust.openclaw.ai/
- OpenAI Agents SDK: https://openai.github.io/openai-agents-python/
- PydanticAI durable execution: https://ai.pydantic.dev/durable_execution/overview/index.md
- PydanticAI MCP client: https://ai.pydantic.dev/mcp/client/
- AutoGen AgentChat: https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/index.html
- AutoGen memory: https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/memory.html
- AutoGen teams: https://microsoft.github.io/autogen/stable/user-guide/agentchat-user-guide/tutorial/teams.html
- TradingAgents: https://github.com/TauricResearch/TradingAgents
- FinRobot: https://github.com/AI4Finance-Foundation/FinRobot
- FinMem implementation: https://github.com/pipiku915/FinMem-LLM-StockTrading
- FinMem paper: https://arxiv.org/abs/2311.13743
- TradingGPT paper: https://arxiv.org/abs/2309.03736
- FinAgent paper: https://arxiv.org/abs/2402.18485
- virattt/ai-hedge-fund: https://github.com/virattt/ai-hedge-fund
- Magents: https://github.com/LLMQuant/magents
- AgenticTrading: https://github.com/Open-Finance-Lab/AgenticTrading
- QuantAgents: https://quantagents.github.io/
- Spartan: https://github.com/elizaOS/spartan
- ATLAS: https://github.com/chrisworsey55/atlas-gic
- Claude-Mem: https://github.com/thedotmack/claude-mem
- Mem0: https://github.com/mem0ai/mem0
- Hindsight: https://vectorize.io/hindsight
- ai-hedge-fund-crypto: https://github.com/51bitquant/ai-hedge-fund-crypto
- Alpaca historical news: https://docs.alpaca.markets/docs/historical-news-data
- Alpaca realtime news: https://docs.alpaca.markets/docs/streaming-real-time-news
- FRED real-time periods: https://fred.stlouisfed.org/docs/api/fred/realtime_period.html
- FRED versus ALFRED: https://fred.stlouisfed.org/docs/api/fred/fred_vs_alfred.html
