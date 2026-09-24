# AI example strategies: status, evidence, and game plan (Sept 23, 2026)

Written for the next agent. Read this first, then
`/Users/robertgrzesik/Development/lumibot/docs/research/2026-09-23-ai-strategy-backtests/README.md`.

Branch: `version/4.5.92` in `/Users/robertgrzesik/Development/lumibot`
(LumiBot has no `main`). Everything below is pushed; last commit `0e285ac2`.
Model default everywhere: `openai/gpt-6-luna`, high reasoning. Total model
spend on Sept 23: about $24 (cap in the runner is now $35).

## 1. Where the evidence is

All paths are under
`/Users/robertgrzesik/Development/lumibot/docs/research/2026-09-23-ai-strategy-backtests/`:

- `<run>.html`: the real QuantStats tear sheet for that run.
- `<run>_<stamp>_trades.csv`: every order event. Filter `status == fill`.
- `<run>_<stamp>_settings.json`: window, universe, model, token counts.
- `<run>.log`: full LumiBot log, including every `[agents][tool_call]` line.
- `screenshots/`: tear sheet, docs, and GitHub screenshots.

Also in `/Users/robertgrzesik/Development/lumibot/logs/`:
- `<run>_<stamp>_stats.csv`: cash and portfolio value per bar.
- `<run>_<stamp>_agent_detail.parquet`: every AI call, including prompts,
  tool calls with arguments, tool results, the model's text, and its summary.

How to read a trace (DuckDB, read-only):

```sql
SELECT call_index, agent_name, tool_name, left(event_payload_json, 400)
FROM 'logs/<run>_agent_detail.parquet'
WHERE event_kind = 'tool_call' ORDER BY call_index, event_index;

SELECT call_index, agent_name, left(max(summary), 600)
FROM 'logs/<run>_agent_detail.parquet'
WHERE is_call_summary GROUP BY 1, 2 ORDER BY 1;
```

Runner: `/Users/robertgrzesik/Development/lumibot/scripts/run_ai_strategy_flash_backtests.py`
(`--wave N --already <dollars spent so far>`). Add a new `WAVEnn` tuple for
each rerun; never reuse a run name.

## 2. Status of every strategy

Code lives in `/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/`.
The shared trader rules are in `agent_cycle.py` (`trader_prompt`).

"BotSpot copy" means a strategy in Rob's production BotSpot account
(owner `87a1a372-272a-40a4-b72f-6f2704000b52`). Every BotSpot copy still runs
Gemini Flash Lite and none matches the repo code. See section 4 for why.

| Strategy (file) | Latest run | Result | Checked and OK | Open issue | BotSpot copy | Code current | Listed |
|---|---|---|---|---|---|---|---|
| Large-cap bull/bear (`ai_trading_team_bull_bear_large_cap_stocks.py`) | large-cap-luna-v3, Jan 5 to 15, Yahoo | -2.16% vs SPY +1% | Cash never negative (low $206); no same-day buy and sell; resizes to daily weights | Interpreter weights swing daily, so it trades most days | Yes (Sept 22) | No | No (withdrawn) |
| Leveraged ETF bull/bear (`ai_trading_team_bull_bear_leveraged_etf.py`) | leveraged-etf-luna-v7 | +1.47% | Never an ETF with its inverse; about 99% invested; cash low $189 | None found | Yes (Sept 22) | No | No (withdrawn) |
| Buffett value (`ai_trading_team_warren_buffett_value.py`) | buffett-luna-v3 | +3.44% | Real SEC work: filings searched and sections read, balance sheets and cash flows pulled, all dated to the backtest day. Stayed in cash 2 days, then 705 PG | Judge the quality of the analysis in the trace; not yet reviewed line by line | Yes (Sept 22) | No | No (withdrawn) |
| Ackman concentrated (`ai_trading_team_bill_ackman_concentrated.py`) | ackman-luna-v2 | +0.91% | 65% MSFT / 35% GOOGL, cash low $1,216 | Researcher used financial statements and indicators but read no filings; v2 also hit the NaT bug (fixed after) | No copy found | n/a | No (withdrawn) |
| Citadel sector pods (`ai_trading_team_citadel_sector_pods*.py`) | Not run on Sept 23 | n/a | n/a | Listing backtests (July, Gemini) went below zero cash: -$58,933 and -$24,775 | Yes (July) | No | No (withdrawn) |
| Ray Dalio meritocracy (`ai_trading_team_ray_dalio_idea_meritocracy*.py`) | Not run on Sept 23 | n/a | n/a | Listing backtests went to -$430,482 and -$17,462 cash. A third copy, "Macro Insight AI" (81af73b8), is still public; cash-clean but about 0% return | Yes (July) | No | 1 of 3 still public |
| Pelosi congress (`ai_congress_disclosures.py`) | congress-pelosi-luna-v4, Jan 22 to 27 | +8.7% | Read the official House Clerk PDFs; saw that the Jan 23 filing was not public at the Jan 22 and Jan 23 opens; bought AB, GOOGL, TEM, VST at the Jan 26 open. Weights 75 / 18.75 / 1.875 / 4.375% match the range midpoints exactly. Cash low $1,436 | Lookahead exposure: the strategy passes URLs of June and August filings too, so the agent downloads future documents and only the prompt stops it using them. Also used r.jina.ai and Google gview to read a PDF | "Pelosi Stock Disclosures" (Sept 22) | No | No |
| Public page fetch (`ai_public_web_fetch.py`) | public-fetch-luna-v3 | +7.3% | Used the browser and HTTP tools on the House PDF; read `published_at` 10:06 ET Jan 23; bought at the Jan 26 open | Same future-URL exposure as Pelosi | No | n/a | No |
| Iron condor (`ai_iron_condor.py`) | iron-condor-luna-v4, Alpaca options | ended $100,240 | One atomic multi-leg limit order: 40 SPY Feb 13 650/655P and 715/720C at a $1.31 credit | Days 1 to 3 it chose the Feb 9 weekly, which has no Alpaca data, and correctly refused. Fills land exactly at the mid (optimistic) | "SPY Iron Condor" (Sept 22) | No | No |
| Credit spread (`ai_credit_spread.py`) | credit-spread-luna-v4 | ended $100,627 | One atomic limit order: 33 SPY 655/650P at a $0.50 credit; checked the 50% profit, $1.00 loss, 21-DTE, and delta exits daily | Never exited inside the window; test a longer window | "SPY Credit Spread" (Sept 22) | No | No |
| Same-day bear call (`ai_spx_zero_dte_bear_call_team.py`) | spy-0dte-luna-v6 | ended $98,640 | Open and close each a single multi-leg limit order; 40 lots; closed on a strike breach | Lost on day 1 (closed at $0.61 after opening at $0.16) | "SPXW Zero Day Bear Call" (Sept 22) | No | No |
| Opening range breakout (`ai_opening_range_breakout.py`) | orb-luna-v4, Alpaca minute | -0.08% | One CMCSA breakout at 5% size | Two-hour cadence: the target was touched but the sale happened later at a worse price. Put a take-profit limit and a stop on at entry | "Opening Range Breakout" (Sept 22) | No | No |
| VWAP (`ai_vwap.py`) | vwap-luna-v5 | flat | No setup, no trade (correct) | Needs a window with a real setup to prove a trade | "VWAP Minute Example" (Sept 22) | No | No |
| SEC insider (`ai_sec_insider_filings.py`) | sec-insider-luna-v5 | -0.76% | Churn gone (37 fills down to 12); cash low $2,263 | Found no insider buying in the 4-day window, so it only held an equal-weight book; test a longer window | "SEC Form 4 Insider Buy Tracker" (May) | No | No |
| Quickstart researcher/trader (`ai_researcher_trader.py`) | researcher-trader-luna-v3 | small | 14 SPY at $687.93 (10% cap by design) | v2 refused all 10 days because of the NaT bug, fixed in `337c4995` | n/a | n/a | n/a |

Other public AI listings still up and cash-clean: AI Investment Committee,
AI-Powered Crypto Breakout, Macro Insight AI. Also public but with no backtest
attached: "SPY 1DTE AI Iron Condor" (1171b518) and "Diversified Leveraged ETF
ORB" (c7a7a179). Rob must decide on those two.

## 3. Fixes made on Sept 23 (all general, no strategy-specific hacks)

1. `515fc712` Backtest fills drain mid-iteration, so cash reflects earlier orders (the cause of every negative-cash listing).
2. `a14f930e` Trader plans from one account read, leaves holdings within 2 points of target, never buys and sells a symbol in one session.
3. `ee239cc1`, `989a9f88` Bull-bear books sell only what the weights dropped; the trade task no longer says to exit the whole book.
4. `989a9f88` Cash rule: new buys stay below cash plus sale proceeds with about 1% spare.
5. `989a9f88`, `2da15c25` Leveraged ETFs: one direction per index; sell the whole opposite side before switching.
6. `a239bf44` Rescale weights a rule netted away instead of skipping the rebalance.
7. `a66449e6` Congress researcher line carries the `[ST]`/`[AB]` code the trader checks.
8. `337c4995` Bar dates survive when the data source names its index `Date` (Yahoo). This regressed at 17:45 in `327acaaa` and broke every Yahoo daily date.
9. Docs: withdrawn listings and the old tear sheets are removed, and a test blocks them (`tests/test_public_docs_community_links.py`).
10. Ten marketplace listings were unpublished through the owner BotSpot MCP after their backing `stats.csv` showed negative cash.

## 4. Why nothing is relisted yet

The repo examples import `lumibot.example_strategies.agent_cycle`. BotSpot
runs whatever LumiBot version its bot image installs, so the new code only
works on BotSpot after LumiBot 4.5.92 is released and the runtime uses it.
Releasing LumiBot needs Rob's explicit authorization, and no agent may deploy
on its own. Until then, a BotSpot copy must either wait for the release or
inline the prompt helpers.

## 5. Data sources (answers to Rob)

- **Alpaca options: works.** Chains and bars load, fills use real Alpaca
  prices, and all three options strategies traded. Gaps: thin weekly
  expirations, such as Feb 9, have no bars, and fills are at the exact mid.
  Alpaca options history starts February 2024. For backtests after that date,
  Alpaca data is enough and ThetaData is not needed.
- **Alpaca news: easy fix.** In a backtest the news tool only reads
  `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET`. It does not fall back to
  `ALPACA_API_KEY` / `ALPACA_API_SECRET`, which are already in
  `/Users/robertgrzesik/Development/lumibot/.env`. Fix: add that fallback in
  `_resolve_alpaca_news_headers` in `lumibot/components/agents/builtins.py`,
  test first.
- **FRED: the key exists** (`FRED_API_KEY` in `lumibot/.env`, 32 characters).
  No example needs it today; Ray Dalio would use it for macro data.
- **ThetaData:** not needed for the examples. Recommend the broker-data path:
  tell users to connect Alpaca (free paper account gives stock, option, and
  news history) or Tradier.

## 6. Mingster / Willygee (Discord) logging question

Traces already exist for every AI call. In a backtest they are
`<run>_agent_detail.parquet` next to the logs. In live and paper they go to
the LumiBot cache folder: `~/Library/Caches/lumibot/1.0/agent_runtime/` on
macOS (set `LUMIBOT_CACHE_FOLDER` to move it), with
`agent_run_summaries.jsonl` alongside. Docs: `docsrc/agents.rst`,
"How do I see what the agent is doing?". Their 730-call live researcher loop
is worth asking for one parquet file. It may be an unsupported open model
looping on a tool.

Draft reply (Rob sends it; agents never post to Discord):

> Every agent call is already recorded, including prompts, every tool call and result, and the model's reasoning and summary. In a backtest it's the `*_agent_detail.parquet` file next to your logs. In live or paper it's in the LumiBot cache folder under `agent_runtime/` (on a Mac, `~/Library/Caches/lumibot/1.0/agent_runtime/`; set `LUMIBOT_CACHE_FOLDER` to move it). You can query it with DuckDB. If you send me the parquet from the run where the researcher made 730 calls, I'll find out why it looped. Which model were you using?

## 7. Game plan, in order

1. **Rob decides on the release.** Authorize the LumiBot 4.5.92 release so
   BotSpot can run the new code. Without it, nothing below reaches customers.
2. **Two lookahead fixes, tests first.** (a) Congress and public-fetch: give
   the agent a point-in-time disclosure tool that only returns filings whose
   report date is on or before the backtest clock, instead of passing future
   URLs. (b) Add an eval that hands the agent a future-dated filing and fails
   if it trades on it. It must start red.
3. **Options evals and skill.** Add an eval where the prompt does not say
   "limit" and the agent must still choose one atomic limit order priced
   between bid and ask. In `lumibot/components/agents/skills/options-trading/`,
   tell the agent to fall back to the nearest expiration with data. Consider a
   backtest fill model that does not always fill exactly at the mid.
4. **ORB exits.** Place the take-profit limit and the stop at entry so a
   two-hour cadence cannot miss the target. Rerun ORB.
5. **Alpaca news fallback** (section 5), then run the news proof in
   `docsrc/agents_canonical_demos.rst`.
6. **Run Citadel and Ray Dalio on Luna.** Neither ran today. Their July
   listing backtests overspent heavily, so they need fresh backtests before
   any relisting. Rob believes they are fine; the evidence says the old tear
   sheets are not.
7. **Longer windows** for credit spread, SEC insider, and VWAP so exits and
   real signals appear. Keep the $35 cap; each run costs about $0.10 to $0.50.
8. **After the release: update BotSpot copies.** Update each copy to the
   repo code through the owner BotSpot MCP (`replace_file`, new revision),
   run the backtest on BotSpot, check `stats.csv` for negative cash and the
   trades for sizing, then republish only passing strategies with
   `save_marketplace_publication`.
9. **Decide the two public listings with no backtest** ("SPY 1DTE AI Iron
   Condor", "Diversified Leveraged ETF ORB").
10. **Docs and screenshots** for anything that changes, then announce.

Rob's actions: step 1 (release authorization), step 9 (two listings), and
sending the Discord reply.
