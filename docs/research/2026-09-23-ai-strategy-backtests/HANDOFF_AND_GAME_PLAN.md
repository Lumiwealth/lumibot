# AI example strategies: status, evidence, and game plan (updated Sept 24, 2026)

Written for the next agent. Read this first, then
`docs/research/2026-09-23-ai-strategy-backtests/README.md`. Section 8 is the
Sept 24 work. Do not rerun it.

Branch: `version/4.6.0` in the repository root
(LumiBot has no `main`). Sept 24 product work is commit `488c2d52`
("Return only public filings and score options orders"). Later commits on
this branch belong to other agents. Model default everywhere:
`openai/gpt-6-luna`, high reasoning. Spend after the Sept 24 runs:
$28.1395 of the $35 cap in `spend.txt`. Do not start another wave unless
Rob asks. Each new run still needs its own `WAVEnn` name.

## 1. Where the evidence is

All paths are under
`docs/research/2026-09-23-ai-strategy-backtests/`:

- `<run>.html`: the real QuantStats tear sheet for that run.
- `<run>_<stamp>_trades.csv`: every order event. Filter `status == fill`.
- `<run>_<stamp>_settings.json`: window, universe, model, token counts.
- `<run>.log`: full LumiBot log, including every `[agents][tool_call]` line.
- `screenshots/`: tear sheet, docs, and GitHub screenshots.

Also in `logs/`:
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

Runner: `scripts/run_ai_strategy_flash_backtests.py`
(`--wave N --already <dollars spent so far>`). Add a new `WAVEnn` tuple for
each rerun; never reuse a run name.

## 2. Status of every strategy

Code lives in `lumibot/example_strategies/`.
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
| Citadel sector pods (`ai_trading_team_citadel_sector_pods*.py`) | citadel-luna, Jan 5 to 16, Yahoo | +2% vs SPY +1% | Did not repeat the July -$58,933 overspend. Cash dipped to -$78.47, then a 1-share sale brought it back. Last cash $33.21. Portfolio about $101,680 | Yahoo child had no Alpaca news and no FRED. See section 8 | Yes (July) | No | No (withdrawn) |
| Ray Dalio meritocracy (`ai_trading_team_ray_dalio_idea_meritocracy*.py`) | ray-dalio-luna, Jan 5 to 16, Yahoo | +1% vs SPY +1% | Cash never negative (low $261.97). Did not repeat the July -$24,775. Portfolio about $101,559 | FRED was not exposed on this Yahoo child. "Macro Insight AI" (81af73b8) is still public | Yes (July) | No | 1 of 3 still public |
| Pelosi congress (`ai_congress_disclosures.py`) | congress-pelosi-luna-v4, Jan 22 to 27 (not rerun) | +8.7% | `house_public_disclosures` returns only filings already public. Eval failed first on a future filing, then passed 3/3. See section 8 | The example file still lists clerk PDF URLs. The Jan 22 backtest was not rerun. v4 also used r.jina.ai and Google gview | "Pelosi Stock Disclosures" (Sept 22) | No | No |
| Public page fetch (`ai_public_web_fetch.py`) | public-fetch-luna-v3 | +7.3% | Used the browser and HTTP tools on the House PDF; read `published_at` 10:06 ET Jan 23; bought at the Jan 26 open | Same future-URL exposure as Pelosi | No | n/a | No |
| Iron condor (`ai_iron_condor.py`) | iron-condor-luna-v4, Alpaca options | ended $100,240 | One atomic multi-leg limit order: 40 SPY Feb 13 650/655P and 715/720C at a $1.31 credit | Days 1 to 3 it chose the Feb 9 weekly, which has no Alpaca data, and correctly refused. Fills land exactly at the mid (optimistic) | "SPY Iron Condor" (Sept 22) | No | No |
| Credit spread (`ai_credit_spread.py`) | credit-spread-luna-v5, Jan 5 to Feb 6 | ended about $100,088 vs SPY -2% | Two 50% profit closes and one 2x-credit loss close. Cash never negative (low $100,000). The 21-day time stop did not fire. See section 8 | Last loss close was submitted on the final bar. Alpaca priced from last trade when bid and ask were missing | "SPY Credit Spread" (Sept 22) | No | No |
| Same-day bear call (`ai_spx_zero_dte_bear_call_team.py`) | spy-0dte-luna-v6 | ended $98,640 | Open and close each a single multi-leg limit order; 40 lots; closed on a strike breach | Lost on day 1 (closed at $0.61 after opening at $0.16) | "SPXW Zero Day Bear Call" (Sept 22) | No | No |
| Opening range breakout (`ai_opening_range_breakout.py`) | orb-luna-v5, Jan 5 to 7, Alpaca | about flat | Same session as the DIS entry: market buy 200, limit sell 200 at $122.86, stop sell 200 at $111.38. Cash never negative (low $76,806) | Next session canceled the day-TIF stop, then the risk manager sold at market. Not a new prompt change | "Opening Range Breakout" (Sept 22) | No | No |
| VWAP (`ai_vwap.py`) | vwap-luna-v6, Jan 5 to 16, 1H | about flat vs SPY +1% | Two real SPY round trips of 143 shares. Cash never negative (low $1,008). A later bar correctly refused a long below VWAP | Prices on some trade-csv rows are blank; the log has them | "VWAP Minute Example" (Sept 22) | No | No |
| SEC insider (`ai_sec_insider_filings.py`) | sec-insider-luna-v6, Jan 5 to Feb 13 | +7% vs SPY -1% | Longer window still found no open-market Form 4 buy or discretionary sale through Feb 12. Held an equal-weight book | Cash dipped to -$220.39 (3 reads), then recovered. Last cash $2,657. Portfolio about $106,697 | "SEC Form 4 Insider Buy Tracker" (May) | No | No |
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
works on BotSpot after the current version branch (`version/4.6.0`) is
released and the runtime uses it. Releasing LumiBot needs Rob's explicit
authorization, and no agent may deploy on its own. Until then, a BotSpot
copy must either wait for the release or inline the prompt helpers.

## 5. Data sources (answers to Rob)

- **Alpaca options: works.** Chains and bars load, fills use real Alpaca
  prices, and all three options strategies traded. Gaps: thin weekly
  expirations, such as Feb 9, have no bars, and fills are at the exact mid.
  Alpaca options history starts February 2024. For backtests after that date,
  Alpaca data is enough and ThetaData is not needed.
- **Alpaca news: fallback is in.** `_resolve_alpaca_news_headers` now uses
  the regular `ALPACA_API_KEY` / `ALPACA_API_SECRET` when the news-specific
  keys and the broker OAuth path are empty. The saved proof used
  `alpaca_api_env` and kept one full article of 15,177 characters. Parquet:
  `logs/AlpacaNewsDeepReadProofStrategy_2026-09-24_12-03_kLnzKx_agent_detail.parquet`.
  Do not pay for another news proof.
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

1. **Rob decides on the release.** Authorize a LumiBot release from
   `version/4.6.0` so BotSpot can run the new code. Without it, nothing
   below reaches customers.
2. **Done, Sept 24. Public filings tool and red-first eval.**
   `house_public_disclosures` omits a filing until its publish time is on or
   before the strategy clock, and it does not download that PDF. The eval
   failed first (future ticker ZZZZ, doc 222), then passed 3/3. The Pelosi
   example file still lists clerk URLs, and that backtest was not rerun.
3. **Done, Sept 24. Options evals. Skill left unchanged.** The limit eval
   does not say "limit". Official 3/3: one atomic iron condor, limit price
   between bid and ask. The expiration eval's nearer date has no quotes.
   Official 3/3: the agent ordered 2026-08-28, the expiration that has data.
   The skill was not edited, because the agent already did that.
4. **Done, Sept 24. ORB exits.** Prompt places the take-profit and the stop
   in the same session as the entry. `orb-luna-v5` did that on DIS. The next
   session canceled the day stop. That cancel is an observation, not a new
   prompt rewrite.
5. **Done, Sept 24. Alpaca news fallback.** See section 5. Proof already saved.
6. **Done, Sept 24. Citadel and Ray Dalio Luna backtests.** Fresh Yahoo
   windows. Neither repeated the July cash overspend. See section 8.
7. **Done, Sept 24. Longer windows.** Credit spread produced real exits.
   VWAP produced real round trips. Insider still found no open-market Form 4
   buy through Feb 12. Spend stopped at $28.1395. Do not extend them again
   unless Rob asks.
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

## 8. Sept 24 results (do not rerun)

Product commit `488c2d52` is on `origin/version/4.6.0`. Backtest tear sheets
and logs stay untracked under this folder and `logs/`. `spend.txt` is a live
counter. Do not commit it.

### Public filings

`public_house_filings` filters before any PDF download. The recorded eval
path uses `visible_congress_disclosures`, so a future row never reaches the
model. Red artifact, saved before the filter was always on:
`agent_eval_baselines/2026-09-24_congress_public_filings_red.json`.
Clock `2026-08-11T14:35:00Z`. The unfiltered tool returned ZZZZ, published
`2026-09-15`, doc 222. Official green 3/3:
`artifacts/agent_evals/2026-09-24-congress-green/`. Cost $0.013692.
Fingerprint `c06b5f354223bcf271e2c506427b78cb5c767e816bdf13e48e1af6be14e20901`.

### Options

Both cases call the real model. Machine checks: limit price inside the
signed bid/ask, and every leg expiration exactly `2026-08-28`. Official 6/6:
`artifacts/agent_evals/2026-09-24-options-repeat3/summary.json`.
Cost $0.040187. Fingerprint
`5f75eaa4813f451f92ee9c43b652e7c4fb012322072a166b6c8d2bec678b537d`.
`options-trading/SKILL.md` was not edited. The judge rubric scores the
order, not a sentence about inspecting the empty expiration.

### Opening range, wave 28

`orb-luna-v5_2026-09-24_12-01_NB6wB6`. Jan 5, 13:30 ET: market buy 200 DIS,
then limit sell 200 at $122.86 and stop sell 200 at $111.38 (opening-range
low). Next session the day stop was canceled. Cash low $76,806, last
$99,694, never negative. Tearsheet return about 0. End period 2026-01-06.

### Citadel and Ray Dalio, wave 28

`citadel-luna_2026-09-24_12-01_Y9abv8`. Jan 4 to Jan 15. Return +2% vs SPY
+1%. Max drop 0.7%. Cash low -$78.47 (5 negative reads), last $33.21.
It bought, went slightly negative, and sold 1 share in that session.

`ray-dalio-luna_2026-09-24_12-01_LsKUBq`. Same window. Return +1% vs SPY
+1%. Max drop 0.32%. Cash low and last $261.97. Zero negative reads.
The old tearsheet figure of a 31% drop was an artifact. Cash never went
negative. Yahoo children do not expose Alpaca news or FRED.

### Longer windows

`vwap-luna-v6_2026-09-24_12-01_bXEC0i`. Through Jan 15. Return about 0 vs
SPY +1%. Max drop 0.3%. Cash low $1,008.25, last $99,905.62. Confirmed
orders: buy 143 SPY, sell 143, buy 143, sell 143. On Jan 13 at 11:30 ET it
refused a long because $693.48 was below VWAP $694.09.

`sec-insider-luna-v6_2026-09-24_12-01_TmDOYc`. Through Feb 12. Return +7%
vs SPY -1%. Max drop 2.32%. Cash low -$220.39 (3 reads), last $2,657.
Portfolio about $106,697. 28 researcher summaries, Jan 5 through Feb 12.
None found a qualifying open-market purchase or discretionary sale. Jan 5
bought an equal-weight book. That is the result. Do not run a longer
insider window for the same question.

`credit-spread-luna-v5_2026-09-24_13-53_jxgBhb`. Jan 4 to Feb 5. Return
about 0 vs SPY -2%. Max drop 0.25%. Cash low $100,000 (the start). Last
read on Feb 5 at 09:30 ET: cash $100,448, portfolio $100,088. Zero negative
reads. Three atomic 4-lot SPY put credit spreads, each closed:

- Feb 13 655/650 opened Jan 5 at a $0.50 credit. Closed Jan 22 at $0.22.
  50% profit rule (about 56% of the credit).
- Feb 27 655/650 opened at a $0.68 credit. Closed Jan 26 at $0.29.
  50% profit rule (about 57% of the credit).
- Feb 27 660/655 opened Jan 27 at a $0.45 credit. Loss close submitted
  Feb 5 at $0.90 (2x credit). Short delta was 0.263, so the 0.30 delta
  stop did not fire. The 21-day stop did not fire. The order was still
  new in that same agent turn. Twelve "Order was filled" lines match the
  six packages' legs. Treat the last close as submitted on the final bar,
  and the account as about flat at $100,088.

Wave 28 parent finished all five jobs at code 0. Spend then $27.6107.
Wave 29 (`credit-spread-luna-v5` only) finished at code 0. Spend $28.1395.
Headroom to the $35 cap is about $6.86. Do not start another parent.
