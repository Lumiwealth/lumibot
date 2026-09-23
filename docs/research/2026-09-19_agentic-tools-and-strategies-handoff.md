# Handoff: agent tools, browser control, and the Congress strategy

> **SUPERSEDED (2026-09-20):** This is historical research, not the current
> implementation contract. LumiBot now provides the full ``http_request`` and
> ``rss_fetch`` built-ins plus a persistent ``BrowserSessionManager`` and
> browser tools. The architecture guidance is also corrected: fully agentic
> strategies are *recommended* to use two or more agents, with one dedicated
> trading/risk agent, but neither exactly two agents nor zero strategy Python is
> a framework requirement. Use the public agent documentation and tests as the
> authoritative source.

Written 2026-09-19 for the next agent (Codex) to implement. Everything below
was researched or measured on Rob's machine on 2026-09-18 and 2026-09-19.
Where something is measured it says so. Where it is a recommendation it says
that too.

Read these first, they are the evidence behind this plan:

- `docs/research/2026-09-18_browser-control-evaluation.md`
- `docs/research/2026-09-18_growth-roi-plan.md`
- `docs/research/2026-09-13_competitor-product-plan.md`

## Corrected architecture recommendation

LumiBot supports one agent or any size team. For a fully agentic team, the
recommended pattern is **two or more agents**, with at least one agent dedicated
to trading and risk. Any number of research, analysis, debate, or specialist
agents may support it. Deterministic Python execution and risk controls remain a
valid alternative, and normal Python wiring in a strategy remains supported.

This is guidance, not a framework restriction. Reusable capabilities such as
HTTP, RSS, SEC, and browser control belong in tools; strategy-specific wiring
and deterministic controls may remain in Python.

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

Four months stale and silent about it. Nine exposed SEC tools plus the shared
internal ticker/CIK lookup read through that path; the earlier total of ten was
counting that internal consumer as though it were another public tool. Any live
strategy using filings or fundamentals can therefore trade on data frozen at
first fetch, and the error degrades the longer a deployment runs.

**The fix is not a one-liner.** Immutable filing documents may remain cached,
but a backtest still needs explicit publication-time filtering; freezing a
company-facts response fetched today does not make it point-in-time data. The
fix has to know which mode and `as_of` boundary it is serving:

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

### `http_request`

A general HTTP transport supporting GET, HEAD, OPTIONS, POST, PUT, PATCH, and
DELETE against public URLs, returning text, bytes, or parsed JSON.

In scope: custom headers, bearer tokens, basic auth, API-key profiles, query
parameters, JSON/forms/multipart/raw bodies, cookie sessions, client
certificates, redirects, downloads, configurable timeout, and a response-size
cap. Secrets stay in host-scoped credential profiles and never enter model
prompts or tool output.

Safety does not remove methods or public internet access. It blocks loopback,
private/link-local/reserved networks, cloud metadata, and redirects into those
networks. Trusted internal targets require an explicitly configured profile.

### `rss_fetch`

Parse a feed into structured items. Small, and a lot of financial sources are
still feeds.

### Stateful browser sessions

For pages that need JavaScript, authentication, profiles, tabs, uploads,
downloads, or consequential actions. See item 3.

## Item 3: browser control, with the evidence

Full reasoning in `docs/research/2026-09-18_browser-control-evaluation.md`.
The short version and the two findings that decide it:

**Qualification result: no hosted default yet.**

- Patchright 1.62.3 is Apache-2.0, fast, and completed 100/100 local ARM64
  restart cycles, but its local headless fingerprint exposed `HeadlessChrome`
  and zero plugins.
- Camoufox 0.5.6 is MPL-2.0 and passed the same basic fingerprint probe and
  100/100 restart cycles, but peaked around 1.35 GiB, above the current 1 GiB
  Fargate task shape.
- Browser Use is an orchestration reference, not a third rendering engine; do
  not nest its LLM runtime inside LumiBot by default.
- The winner must pass in the exact Linux ARM64 Bot Manager image before hosted
  selection. See `docs/research/2026-09-20_browser-engine-qualification.md`.

**Do not use nodriver**, even though it won the 2026 benchmark outright with
28/31 and zero hard blocks. It is **AGPL-3.0**. The network clause reaches
software offered to users over a network, which is exactly what BotSpot is.
That is a question for counsel, not an engineering preference.

**Camoufox is the escalation, not the default.** MPL-2.0, strongest on pure JS
fingerprinting, but it is a Firefox fork wanting 200+ MiB per instance on top
of a ~150 MiB binary, and there is a direct practitioner report on X saying
"Camoufox in Docker fails Cloudflare checks." Docker is our case.

Use HTTP when it is sufficient, but do not treat that as a replacement for the
browser capability. Login flows, JavaScript-only applications, multi-tab work,
uploads/downloads, and authenticated consequential actions still require the
stateful browser surface.

### The infrastructure constraint

The hosted base image has no browser in it, and scheduled strategies run on a
small default task shape (exact sizes are in private BotSpot operations notes).
Chrome with one page is roughly 300 to 500 MiB, so it fits only barely. Measure
the image size increase and the task memory headroom before merging.

**Raising the hosted task memory is a cost increase across every scheduled task
and needs Rob's explicit approval with a monthly number attached. Do not do it
unilaterally.**

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
unprompted during the 2026-09-18 call, at 22:03 in the call transcript (kept in
private notes, not in this public repository).
It is a **nice-to-have demo**, not the MetaMask partnership. Do not let it grow.

Build it as a LumiBot example with a disclosure researcher and a dedicated
trading/risk agent. Additional agents and deterministic Python controls remain
valid. Public marketplace use waits for licensed commercial data rights.

**Candidate data sources, each requiring a rights review before commercial use:**

- Senate: `efts.senate.gov` JSON API for Periodic Transaction Reports
- House: `disclosures-clerk.house.gov` year-to-date ZIP plus per-filing PDFs
- Free normalised APIs if the official ones are painful: QuantEngines (no key,
  20 req/min), Bargo (free key), capitol-api (open source, `?person=Pelosi`)

It uses `http_request`; browser control is optional when a licensed source
genuinely requires it.

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

1. SEC point-in-time integrity plus live TTL/revalidation. It blocks valid
   disclosure backtests.
2. `http_request` plus `rss_fetch`, with the SSRF guard and credential profiles.
3. Congress strategy on `http_request`, with a dedicated trading/risk owner.
4. Stateful `BrowserSession` tools, with the engine selected by the ARM64
   reliability and anti-detection benchmark.
5. Browser showcase on the qualified stateful session layer.

Each step makes the next one smaller, and each is independently testable.
