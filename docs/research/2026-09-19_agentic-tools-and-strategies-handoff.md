# Handoff: agent tools, browser control, and the Congress strategy

Written 2026-09-19 for the next agent (Codex) to implement. Everything below
was researched or measured on Rob's machine on 2026-09-18 and 2026-09-19.
Where something is measured it says so. Where it is a recommendation it says
that too.

Read these first, they are the evidence behind this plan:

- `docs/research/2026-09-18_browser-control-evaluation.md`
- `docs/research/2026-09-18_growth-roi-plan.md`
- `docs/research/2026-09-13_competitor-product-plan.md`

## The architecture rule this all serves

Rob's instruction, 2026-09-18, and it governs every item here.

A LumiBot AI strategy contains **two agents and almost no Python**. The only
Python allowed in a strategy file is:

- `self.sleeptime` in `initialize`
- creating the agents in `initialize`
- running the agents in `on_trading_iteration`

Two agents: a research agent with `allow_trading=False`, and a trading agent
with `allow_trading=True` that owns risk. Nothing else. No data fetching, no
parsing, no HTTP calls, no business logic in the strategy.

Rob: "It's pointless. If we have to add in fucking Python code that actually
does this stuff, that completely takes away from the whole agentic trading
thing."

**So when an agent lacks a capability, the fix is a new LumiBot tool, never
Python in the strategy.** The test he applies: "if I ask it to do a Pelosi
trading strategy, it could just do that."

### On MCP, because this has caused confusion

Rob believed ADK agents can only reach tools over MCP. They cannot only do
that, and LumiBot already proves it. `lumibot/components/agents/runtime.py`
line 1145 imports `google.adk.tools.function_tool`, line 1150 pulls out
`FunctionTool`, line 231 wraps each bound tool with it, and `manager.py` line
968 builds the list from `BuiltinTools.all()`. Every order tool the agents
call today is a plain Python function exposed as an ADK FunctionTool with no
MCP anywhere in the path.

LumiBot also supports remote MCP servers: `MCPServer`, `call_mcp_tool`,
`list_mcp_tools` and `_remote_mcp_tool_contracts` all exist in `manager.py`.

**Use builtin FunctionTools for LumiBot-native capabilities. Use MCP for
third-party servers we do not own.** The strategy author writes no Python
either way, which is the actual requirement. A Node MCP process next to the
Python agent inside a 0.5 vCPU / 1024 MiB Fargate task is a real cost for no
benefit when the library is already installable with pip.

## What already exists, so nobody rebuilds it

`BuiltinTools.all()` returns **51 tools** today. Verified by enumeration on
2026-09-18. Relevant ones:

- SEC EDGAR: `get_filings`, `get_filing_document`, `get_filing_section`,
  `list_filing_sections`, `search_filing`
- Macro: `get_fred_series`, `get_fred_latest`, `get_fred_snapshot`,
  `list_fred_series`
- Fundamentals: `get_income_statement`, `get_balance_sheet`, `get_cash_flow`,
  `get_company_facts`
- News: `alpaca_news` (Alpaca only, not general news)
- Market: `market_last_price`, `market_historical_prices`, `get_indicator`
- Orders: `orders_submit_order`, `orders_get_status`, `orders_wait_for_terminal`,
  `orders_cancel_order`, `orders_submit_multileg`
- Options: a full set including `options_get_chain`, `options_get_greeks`,
  `options_find_strike_for_delta`
- Memory and theses: `remember`, `search_memory`, `open_thesis`, `update_thesis`
- `duckdb_query`, `lumibot_docs_search`, `notify_user`

**There is no tool that fetches an arbitrary URL.** No HTTP, no JSON, no RSS,
no browser. That is the gap.

## Item 1: fix the SEC cache, before anything is built on it

**This is a live bug and it is the most important item in this document.**

`lumibot/fundamentals/sec.py` line 192:

```python
def _get_json(self, url: str, cache_path: Path) -> dict[str, Any]:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
```

The cache never expires. No TTL, no mtime check, no conditional request.
`_get_text` immediately below it has the identical bug.

Measured on Rob's machine 2026-09-18:

```
lumibot get_filings("NVDA", form="4")  -> newest filing 2026-03-24
EDGAR submissions API directly         -> newest Form 4 2026-09-18 (same day)

~/.lumibot/cache/sec/submissions/CIK0001045810.json   mtime May 12
~/.lumibot/cache/sec/submissions/CIK0000320193.json   mtime May  5
```

Four months stale and silent about it. Ten of the 51 builtin tools read
through that path, so any live strategy using filings or fundamentals is
trading on data frozen at first fetch, and it degrades the longer a deployment
runs.

**The fix is not a one-liner** because immutable caching is arguably correct
for a backtest, where it gives point-in-time behaviour, and clearly wrong for
live. The fix has to know which mode it is in. Suggested shape:

- Live: honour a TTL (an hour for submissions is sensible), and prefer a
  conditional request with `If-Modified-Since` so EDGAR's rate limits stay
  happy.
- Backtest: keep the immutable behaviour, and key the cache by the simulated
  as-of date so a replay is deterministic.
- Either way, never return a cache entry without knowing how old it is.

Write the failing test first: assert that a cache file older than the TTL
triggers a refetch in live mode and does not in backtest mode.

## Item 2: three missing tools

These are the unlock. Everything else in this document depends on them.

### `http_fetch`

A general GET and POST against any URL, returning text or parsed JSON. This
single tool is the answer to "some random API has the data."

In scope: custom headers, bearer tokens, basic auth, query parameters,
configurable timeout, a response size cap, and HTTPS obviously.
Out of scope: cookie-jar login flows, which belong in the browser tool.

Safety: this hands an LLM the ability to call arbitrary URLs. Consider an
allowlist or at minimum a block on private address ranges and link-local
addresses, because SSRF from a trading agent is a real concern. Do not skip
this part.

### `rss_fetch`

Parse a feed into structured items. Small, and a lot of financial sources are
still feeds.

### `browser_fetch`

For pages that genuinely need JavaScript. See item 3.

## Item 3: browser control, with the evidence

Full reasoning in `docs/research/2026-09-18_browser-control-evaluation.md`.
The short version and the two findings that decide it:

**Recommendation: Patchright with `channel=chrome`.**

- Drop-in Playwright replacement, so `from patchright.sync_api import ...` and
  nothing else changes.
- It closes the `Runtime.enable` CDP leak, which is the layer anti-bot systems
  actually gate on. A JavaScript stealth plugin runs too late to fix it.
- Chromium-based and headless-native, so no Xvfb on Fargate.
- Permissive licence.

**Do not use nodriver**, even though it won the 2026 benchmark outright with
28/31 and zero hard blocks. It is **AGPL-3.0**. The network clause reaches
software offered to users over a network, which is exactly what BotSpot is.
That is a question for counsel, not an engineering preference.

**Camoufox is the escalation, not the default.** MIT, strongest on pure JS
fingerprinting, but it is a Firefox fork wanting 200+ MiB per instance on top
of a ~150 MiB binary, and there is a direct practitioner report on X saying
"Camoufox in Docker fails Cloudflare checks." Docker is our case.

**Before any browser, try `curl_cffi`.** It cleared 26 of 31 Cloudflare targets
with no browser at all. Most targets need no browser and the container never
has to grow.

### The infrastructure constraint

Read out of `bot_manager/terraform/main.tf` on 2026-09-18:

- Base image `python:3.13-slim-trixie`, no browser in it
- Scheduled custom Fargate default: `cpu = 512` (0.5 vCPU),
  `memory = 1024` MiB
- Backtest tasks get `cpu = 2048`, `memory = 3700` MiB, so the bigger shape
  exists but is not what scheduled strategies get

Chrome with one page is roughly 300 to 500 MiB. It fits, barely. Measure the
image size increase and the task memory headroom before merging.

**Raising `scheduled_custom_fargate_memory` above 1024 MiB is a cost increase
across every scheduled task and needs Rob's explicit approval with a monthly
number attached. Do not do it unilaterally.**

### Which targets actually need a browser

Measured 2026-09-19 by fetching each with plain curl and checking whether the
content was in the raw HTML:

| Source | Plain HTTP | Needs a browser |
|---|---|---|
| fool.com earnings transcripts | 200, content present | No |
| nasdaqtrader.com trade halts | 200, content present | No |
| marketbeat.com insider trades | 200, content present | No |
| quiverquant.com congress trading | 200, content present | No |
| **seekingalpha.com transcripts** | **Cloudflare challenge** | **Yes** |

Almost everything works without a browser. Seeking Alpha is the one real case,
which makes it the honest demo for the browser tool: a page that provably
cannot be read otherwise.

## Item 4: the Congress strategy

Context so nobody mis-scopes this. Elisha Koh of MetaMask asked about it
unprompted during the 2026-09-18 call, at 22:03 in the transcript at
`/Users/robertgrzesik/Development/MarketingManager/docs/transcripts/2026-09-18_metamask-elisha-koh-call.txt`.
It is a **nice-to-have demo**, not the MetaMask partnership. Do not let it grow.

Build it as a LumiBot example and put it on the BotSpot marketplace. Two
agents, zero Python, per the rule at the top.

**Data sources, all public domain, no browser needed:**

- Senate: `efts.senate.gov` JSON API for Periodic Transaction Reports
- House: `disclosures-clerk.house.gov` year-to-date ZIP plus per-filing PDFs
- Free normalised APIs if the official ones are painful: QuantEngines (no key,
  20 req/min), Bargo (free key), capitol-api (open source, `?person=Pelosi`)

It needs `http_fetch` and nothing else. Build that first.

**The constraint that must appear in the strategy docstring and in any
marketplace listing:** STOCK Act disclosures lag the actual trade by **up to 45
days**, and amounts are disclosed as ranges such as $1,001 to $15,000. The
strategy mirrors positions as they are disclosed. It does not trade alongside
anybody. Saying otherwise is a false claim. NANC and KRUZ are real ETFs doing
exactly this, so the approach is sound, the honesty is just non-negotiable.

**A Trump strategy is a different and harder problem.** Rob's point, and he is
right: it is not about disclosed trades, it is about reacting to posts within
minutes. That is a low-latency streaming problem with a completely different
architecture. Do not conflate the two and do not scope it here.

## Item 5: already done, do not redo

All on branch `version/4.5.92`, pushed.

- **`lumibot` CLI** shipped: `init`, `backtest`, `run`, `demo`. `lumibot demo`
  runs a real backtest with no API key and no account in about 7 seconds.
  31/31 tests in `tests/test_cli.py`. Commits `9567a56c`, `8fa90a5e`.
- **README** rewritten at the top, comparison tables moved from line 413 to 48,
  nine languages. Commits `5f5a103d`, `28d307ed`, `6f64fde1`.
- **google-adk upgraded to 2.9.1**, with `google-genai>=2.24.0` and
  `litellm>=1.101.0`. Receipt with both test runs in
  `docs/research/2026-09-18_adk-291-upgrade-receipt.md`. Commits `35bad12f`,
  `0d7ae1b2`, `90f086f0`.

## Known pre-existing failures, not caused by the above

Measured on both the old and new dependency sets, so they predate this work:

```
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_backdoor_butterfly
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_backdoor_smartlimit
tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_spx_short_straddle
tests/test_alpaca.py::TestAlpacaBroker::test_initialize_broker_legacy
```

Also `lumibot/example_strategies/stock_buy_and_hold.py` line 43 crashes on a
2025 to 2026 window because `add_line` receives `None`.

## Suggested order

1. SEC cache TTL. It is a live correctness bug and it blocks item 4.
2. `http_fetch`, with the SSRF guard.
3. Congress strategy on `http_fetch`, two agents, zero Python.
4. `browser_fetch` with Patchright, plus the Seeking Alpha example.
5. `rss_fetch`.

Each step makes the next one smaller, and each is independently testable.
