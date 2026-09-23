# Alpaca options backtesting and the IBKR 4.5.92 intraday window loop

One-line description: bring-your-own-key options backtests on Alpaca now work end to end, and the IBKR "same underfilled request every bar" loop seen on 4.5.92 is fixed (it is a latent bug, not a 4.5.92 change).

Last Updated: 2026-09-23
Status: Fixed on `version/4.5.92`, not released
Audience: LumiBot maintainers, BotSpot release captain, Bot Manager owners

## Overview

BotSpot stopped paying ThetaData, so hosted options backtests had no data source. The
product direction is "bring your own key": the customer's broker or data vendor supplies
option history. Alpaca is the first real path because a free Alpaca account includes option
history.

Two pieces of work on `version/4.5.92`:

- Task A: `AlpacaBacktesting` could not run an options strategy. `get_chains()` was a stub
  that returned `{}`, and option bars were reindexed and forward/back filled like stock bars.
  Both are fixed, with real-API tests and two real backtests (2026 and 2024).
- Task B: a production SPY 5-minute backtest on LumiBot 4.5.92 re-submitted the identical IBKR
  history request on every bar of the first simulated day (77 times). Root cause found, the same
  loop reproduced on 4.5.91 code, fixed with deterministic tests.
- Task C: the same options strategy also runs on a customer's own Polygon key (tiny window).

Nothing was released, tagged, published, merged to `dev`, or deployed.

## Task A: Alpaca options backtesting

### What was wrong

| Problem | Effect | Red test |
| --- | --- | --- |
| `get_chains()` returned `{}` | Strategies that discover strikes found nothing (an earlier agent run placed no condor for this reason) | `test_alpaca_backtesting_get_chains_lists_expired_and_active_contracts` |
| Option bars went through `_reindex_and_fill` | Every missing minute got an invented bar; `bfill` copied a later trade into the past (price 1.00 at 09:45 for a contract whose first trade was 10:00) | `test_alpaca_option_last_price_uses_real_bars_and_none_before_the_first_trade`, `test_alpaca_option_bars_keep_only_real_trade_prints` |
| Broker filled Alpaca orders on whatever bar came back | A market order at 09:35 filled on the stale 09:31 print | `test_alpaca_option_order_fills_on_the_next_real_print_in_a_full_backtest` |
| A contract with no bars raised `RuntimeError` | Crash in the fill path | `test_alpaca_option_without_any_bars_returns_none_with_a_clear_error` |

### What was built (`lumibot/backtesting/alpaca_backtesting.py`)

- `get_chains(asset, quote=None)` calls `TradingClient.get_option_contracts` for
  `status=inactive` (expired) and `status=active`, following `next_page_token`, limit 10000 per
  page. Expirations: simulated date through `OPTION_CHAIN_MAX_DAYS = 90` days, or the
  `min/max_expiration_date` hint that `OptionsHelper` sets. Only 100-share contracts whose OCC
  root is the underlying (adjusted `SPY1` style deliverables are dropped).
- Shape is the one the other backtesting sources return, wrapped in `Chains`:
  `{"Multiplier": 100, "Exchange": "SMART", "UnderlyingSymbol": "SPY", "Chains": {"CALL": {"YYYY-MM-DD": [sorted strikes]}, "PUT": {...}}}`.
- The trading client is created lazily with the same credentials as the data clients and the
  same precedence as the live broker (API key and secret first, then OAuth token), `paper=True`.
- Caching: each (underlying, simulated date, expiration window) chain is kept in memory and as
  JSON under `<LUMIBOT_CACHE_FOLDER>/alpaca/option_chains/`. One contract listing covers the
  window plus 30 more days and is reused by later simulated days, so a strategy calling
  `get_chains()` every minute costs about three requests per listing.
- Rate limits: a sliding window keeps this process under 180 requests per minute (free keys
  allow about 200). On HTTP 429 (after the SDK's own three 3-second retries) it waits for
  `Retry-After` when present, else 5, 10, 20 seconds, at most 5 waits and 120 seconds, then
  raises a clear error.
- Option prices: real trade prints only, no reindex, no forward fill, no back fill. Option cache
  keys end in `_TRADES` so older filled CSVs are never reused. `get_last_price` returns the open
  of a bar that printed in the current minute (or day), otherwise the close of the most recent
  earlier print, otherwise `None`. `get_historical_prices(option, n)` returns up to `n` real bars
  at or before the simulated time, or `None`.
- No data at all: one clear error ("Alpaca returned no option bars for SPY230616C00420000 ...
  Alpaca option history starts around February 2024 ...") and `None`, remembered for the run.
- `BacktestingBroker._requires_current_execution_bar` returns True for Alpaca options on any
  timestep, so an order fills only on a bar that printed in the current bucket and otherwise
  keeps working (the existing `[FILL][PENDING]` path).

Two legacy fixes in the same files, found by running the whole Alpaca apitest file:

- `AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET` was `None` since the 2026-07-02 lazy
  `AlpacaData` quote change; `_get_asset_key(quote_asset=None)` crashed in five legacy tests.
- Since 4.4.53, `StrategyExecutor` forced `data_source._timestep = "day"` for any daily
  sleeptime, silently overriding `AlpacaBacktesting(timestep="minute")` and failing four legacy
  tests. The priming now skips data sources whose bar size the caller set explicitly
  (`_timestep_explicit`, only `AlpacaBacktesting` sets it).

### Real API facts (checked 2026-09-23 with a paper key)

- SPY listing, expirations 2026-08-03 through +60 days: 16,430 contracts (13,482 inactive,
  2,948 active), 3 pages, 44 expirations, 0.8 s. Through +90 days: 18,492 contracts, 51
  expirations. 2024-03-04 through +60 days: 10,400 contracts, all inactive, 44 expirations.
  Only root `SPY`, size `100` came back in these windows.
- Option bars exist from about February 2024. A June 2023 SPY contract returns no bars.
- Stock feed on this free key: the default feed returns SIP history (SPY 2026-08-03 volume
  60,014,445) while `feed=iex` returns 1,927,439. The latest 15 minutes of SIP are refused
  ("subscription does not permit querying recent SIP data"). `AlpacaBacktesting` requests run to
  the end date plus one day, so end a backtest at least one full day before today.

### End-to-end proof runs

`scripts/alpaca_options_backtest_proof.py`: on the first session of each week at or after
09:35 ET, buy one SPY call from `get_chains()` (nearest expiration 7 to 21 days out, strike
nearest SPY); sell it on the last session of the week at or after 15:30 ET. Minute bars,
sleeptime one minute, $10,000 budget, one contract. Evidence:
`docs/research/2026-09-23-alpaca-options-backtests/`. These are engineering proof runs, not a
strategy result.

2026 window (2026-07-27 to 2026-08-14 traded):

| Contract | Buy | Sell | P/L |
| --- | --- | --- | --- |
| SPY 2026-08-03 745 C | 07-27 09:35 at 6.10 | 07-31 15:30 at 3.74 | -$236 |
| SPY 2026-08-10 751 C | 08-03 09:35 at 4.99 | 08-07 15:39 at 21.91 (order 15:30, no print until 15:39) | +$1,692 |
| SPY 2026-08-17 773 C | 08-10 09:37 at 4.97 (order 09:35, no print until 09:37) | 08-14 15:31 at 3.86 (order 15:30) | -$111 |

Total +$1,345 (tear sheet: +13% vs SPY +5%, max drawdown -5.26%). The chain held 55, 51 and 46
call expirations on the three entry days. Every fill matches the raw Alpaca option minute bar
open at that minute (`raw_bar_crosscheck_2026.txt`). The old code would have sold the 751 call
at 15:30 on an invented bar copied from 15:18.

2024 window (2024-03-04 to 2024-03-22 traded, across the DST change):

| Contract | Buy | Sell | P/L |
| --- | --- | --- | --- |
| SPY 2024-03-11 512 C | 03-04 09:35 at 3.47 | 03-08 15:30 at 1.56 | -$191 |
| SPY 2024-03-18 510 C | 03-11 09:35 at 4.07 | 03-15 15:30 at 1.76 | -$231 |
| SPY 2024-03-25 515 C | 03-18 09:35 at 4.11 | 03-22 15:30 at 6.83 | +$272 |

Total -$150 (tear sheet: -1% vs SPY +2%, max drawdown -6.97%). 63 call expirations each entry
day. Both runs finished in about 7 seconds cold; a rerun on the final committed code produced
identical trades.

### Task C: Polygon key

Same script with `--source polygon` and `LUMIBOT_OPTION_CHAIN_MAX_DAYS=21`, one week
(2026-08-03 to 2026-08-07) on the Polygon key in the local environment: it works end to end in
65 seconds (one 60-second free-tier rate-limit wait). It picked the same contract as Alpaca
(SPY 2026-08-10 751 C) and bought at 4.99 at 09:35. It sold at 15:30 at 22.11, which is the
15:18 print: Polygon's own minute aggregates for that contract also show only 15:18 (22.11) and
15:39 (21.91). The Pandas-based Polygon path fills sparse options on a carried-forward bar. That
is a pre-existing issue outside this change (see open items).

### Known limits (also in the class docstring and `docsrc/backtesting.alpaca.rst`)

- Option history starts around February 2024.
- The contract listing has no as-of date: a strike or expiration listed after the simulated
  date can appear in that day's chain (small lookahead). A contract with no trade yet has no
  price, so it cannot be traded before its first print.
- No historical option bid/ask or vendor greeks. Fills use trade bars (spread not modeled).
  `Strategy.get_greeks()` still works from the last trade and the underlying price.
- Daily option bars start with the day's first trade, which can print after 09:30.
- Free keys: about 200 requests per minute; recent 15 minutes of SIP stock data refused.
- Stock and crypto bars still use the legacy calendar fill in `_reindex_and_fill` (covered by
  legacy tests). That predates RULE #1 and is an open item, not something new code copies.

## Task B: IBKR "remained underfilled" loop on 4.5.92

### Production evidence (Bot Manager log group, read-only)

- A routed IBKR backtest (SPY, 250 bars of 5-minute history per 5-minute iteration, window
  2026-09-08 to 2026-09-16) ran on LumiBot 4.5.92 from a Bot Manager release bundle at 00:42 ET
  on 2026-09-15. `backtesting_end` was clamped to 2026-09-15 00:42:41 ET.
- It made 78 history requests: 77 identical
  `{"bar": "5min", "conid": "756733", "outsideRth": "true", "period": "5000min", "source": "Trades", "startTime": "20260908-08:00:00"}`
  and one `startTime=20260915-04:41:41`. Each took 4 to 5 seconds. All 77 repeats happened during
  the first simulated day (one per 5-minute bar), each followed by
  `IBKR cached history remained underfilled after refresh ... (placeholder_covered=False)`.
- Three other 4.5.92 runs of the same window, clamped at 21:29 to 21:38 ET on 2026-09-14 (after
  the session close), made only 2 requests each. A 4.5.91 run of 2026-08-03 to 2026-08-15 (not
  clamped, cold cache) paged backwards 5 times and was done.
- One live request through the production downloader on 2026-09-23
  (`startTime=20260908-08:00:00`, `period=5000min`): 40 bars, all Friday 2026-09-04 16:40 to
  19:55 ET, classification `complete`, zero bars inside the requested gap.

### Root cause

The routed IBKR adapter prefetches stock intraday series for
`[min(lookback start, backtest start), datetime_end]`. With the end clamped to 00:41 ET:

1. `frame_covers_requested_window` treated the end as covered only when it was after the last
   close of the last calendar day in the window. 00:41 ET on 2026-09-15 is before that day's
   session, so a cache holding every real bar through 2026-09-14 19:55 ET was reported as
   underfilled and the series was never marked fully loaded.
2. The lookback start for the 09:30 bar was 2026-09-07 12:40 ET (Labor Day). `get_price_data`
   saw the window start more than 15 minutes before the first cached bar (2026-09-08 04:00 ET)
   and fetched that edge. IBKR answers such a request with the bars that exist before
   `startTime` (the Friday after-hours bars above), which are outside the window, so coverage
   never moved.
3. The in-process cooldown only existed for exceptions and empty payloads. A successful but
   useless answer left no memory, so every iteration with a lookback start before the first
   bar asked again.

The 21:30 ET runs escaped because their end was after the last close. The 4.5.91 run escaped
because its end (23:59 ET) was after the last close and its cache was cold. The diff from
`v4.5.91` to 4.5.92 does not touch `frame_covers_requested_window`, the `needs_fetch` logic, or
the routed adapter (72755149 only resized daily periods). Replaying the production scenario
against the `v4.5.91` helper gives the same 13 requests for 12 iterations (12 identical
`20260908-08:00:00`). This is a latent bug that 4.5.91 production can hit too, not a 4.5.92
regression.

The strategy did get real bars (the cache was complete); the cost was about 6.5 minutes of
downloader time and 77 requests against the shared IBKR budget (about 50 history requests per
minute for everyone).

### Fix (`lumibot/tools/ibkr_helper.py`)

- `frame_covers_requested_window` (stock and index): the end is covered by the last session
  that opened before the requested end, and the start by the first session that had not closed
  by the requested start. A boundary inside a session still needs the raw check, so a truly
  missing final session is still reported.
- `get_price_data` (stock intraday only): a window edge that contains no NYSE session time
  (extended hours when `outsideRth`, early closes included, via `pandas_market_calendars`
  pre/post) is not fetched. Indexes and daily bars keep their existing behavior.
- `get_price_data` remembers every segment it requested per cache file
  (`_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS`) and does not request the same or a narrower segment
  again in the same process. It is in-memory only: no cache marker, a later process retries.
  This extends the documented invariant "a repeated partial or transient request in one process
  performs zero additional downloader calls" to successful but underfilled answers.
- The placeholder filter casts the `missing` flag to bool before negating (an object-dtype flag
  turned `~` into an integer NOT).
- Real bars are returned as before. Nothing is synthesized.

### Tests (red first, then green)

- `tests/backtest/test_routed_backtesting_ibkr_prefetch.py::test_router_ibkr_stock_minute_clamped_pre_open_end_does_not_refetch_every_bar`:
  the production path (real `get_price_data`, only the downloader faked, warm cache with the
  production coverage). Red: 13 requests, `20260908-08:00:00` repeated. Green: no repeats, at
  most 2 requests (0 in practice), the 09:30 bar is visible.
- `tests/test_ibkr_helper_unit.py`: pre-open end and after-close start coverage, closed-edge
  skip (red: 2 fetches), no repeat of an underfilled segment (red: 4 fetches), a guard that real
  missing sessions at both edges are still fetched, and the calendar helper (holiday, overnight,
  weekend, pre-market, early close).

## Test results

Run with `LUMIBOT_DISABLE_DOTENV_LOCAL=1 LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled`
(keeps the local `.env.local` broker settings and the shared remote cache out of tests).

| Command | Result |
| --- | --- |
| `pytest tests/test_ibkr_helper_unit.py tests/backtest/test_routed_backtesting_ibkr_prefetch.py` | 44/44 |
| `pytest tests -k "ibkr or IBKR or routed" -m "not apitest and not downloader"` | 255 passed, 2 skipped |
| `pytest tests/test_alpaca_backtesting_multitimeframe_unit.py tests/test_backtesting_broker.py` | 48/48 |
| `pytest tests/test_alpaca_backtesting.py` (apitest, paper key) | 38/38 |
| `pytest tests -k "alpaca or Alpaca" -m "not apitest and not downloader"` | 104 passed, 2 skipped |
| `pytest tests -k "backtesting_broker or order_lifecycle or options_helper or backtest_lookahead or lookahead" -m "not apitest and not downloader"` | 157 passed, 4 skipped |
| 16 daily-sleeptime related test files (executor priming change) | 190 passed, 2 skipped |

Environment notes: the local `ALPACA_TEST_API_KEY` is revoked (401 on data and trading), so
the Alpaca apitest file was run with a valid paper key exported as `ALPACA_TEST_API_KEY`.
`tests/test_alpaca.py::TestAlpacaBroker::test_initialize_broker_legacy` fails only when the
local `.env.local` Polymarket settings are loaded; it passes with
`LUMIBOT_DISABLE_DOTENV_LOCAL=1` and is unrelated to this change.

## What a release would need

Nothing here is released. When a release captain is authorized:

1. LumiBot: follow `docs/DEPLOYMENT.md` (merge `dev` into `version/4.5.92`, full CI including
   `pytest -m "not apitest and not downloader"`, changelog date, PR to `dev`, tag the `dev`
   merge commit, verify PyPI).
2. Bot Manager: the promoted 4.5.92 runtime bundle predates these commits and still has the
   IBKR loop. Rebuild the bundle (or set `LUMIBOT_VERSION` to the released version with
   `force_rebuild_images=true`), Dev then production, and check `settings.json.lumibot_version`.
3. BotSpot product wiring for "bring your own Alpaca key" is not in LumiBot yet. The router's
   Alpaca adapter reads `ALPACA_API_KEY`/`ALPACA_API_SECRET` (or the OAuth token) from the
   process environment and only serves bars, and `RoutedBacktestingPandas` inherits ThetaData's
   `get_chains()`, so routed chains still come from ThetaData whatever the option route says.
   Options backtests on BotSpot need either `BACKTESTING_DATA_SOURCE=alpaca` with the customer's
   Alpaca credentials injected into the backtest environment, or a router change that sends
   option bars and chains to Alpaca. Note that `AlpacaBacktesting` selected that way gets no
   `timestep` argument and uses daily bars (option fills on the day's bar), and it ends three
   trading days before `backtesting_end` (existing behavior).

## Open items

- Pandas-based option paths (Polygon, and any `Data.repair_times_and_fill` source) fill sparse
  options on a carried-forward bar (Task C: sold at 15:30 on the 15:18 print). Same class of
  problem fixed here for Alpaca; needs its own change and tests.
- `AlpacaBacktesting` stock/crypto calendar fill (`_reindex_and_fill`) predates RULE #1.
- `LUMIBOT_OPTION_CHAIN_MAX_DAYS` (Polygon chain bound) is not in `docsrc/environment_variables.rst`.
- The chain lookahead (strikes listed after the simulated date) could be tightened by dropping
  contracts whose first bar is after the simulated date, at the cost of one bar request each.
