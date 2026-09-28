# Alpaca options backtesting and the IBKR 4.5.92 intraday window loop

One-line description: bring-your-own-key options backtests on Alpaca now work end to end, and the IBKR "same underfilled request every bar" loop seen on 4.5.92 is fixed (it is a latent bug, not a 4.5.92 change).

Last Updated: 2026-09-23 (follow-ups: BACKTESTING_DATA_SOURCE=alpaca path; still-forming bar lookahead; history before backtesting_start)
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
- Follow-ups: `BACKTESTING_DATA_SOURCE=alpaca` (the BotSpot path) now works with minute bars
  over the full window, its history no longer returns the bar that is still forming at the
  simulated time (a lookahead of up to one bar), and a long lookback at the first bars gets real
  bars from before `backtesting_start`, like IBKR and ThetaData.

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
- The contract listing is not point-in-time. Alpaca's contracts API returns today's listing
  with no listing or first-trade date, so a strike or expiration listed after the simulated
  date can appear in that day's chain. That listing must not be read as historical
  availability. A true point-in-time chain needs an authoritative contract-availability date or
  a historical chain source, and Alpaca offers neither. The remaining guard: a contract with no
  trade yet has no price, so it cannot be traded before its first print.
- No historical option bid/ask or vendor greeks. Fills use trade bars (spread not modeled).
  `Strategy.get_greeks()` still works from the last trade and the underlying price.
- Daily option bars start with the day's first trade, which can print after 09:30.
- Free keys: about 200 requests per minute; recent 15 minutes of SIP stock data refused.
- Stock and crypto bars still use the legacy calendar fill in `_reindex_and_fill` (covered by
  legacy tests). That predates RULE #1 and is an open item, not something new code copies.

## Task B: IBKR "remained underfilled" loop on 4.5.92

### Hosted evidence (summary)

- A hosted routed IBKR backtest on 4.5.92 (SPY, 5-minute bars, 250 bars of history per
  iteration) whose end date was clamped to the current time re-sent the identical history
  request once per bar for the whole first simulated day (77 repeats), each followed by
  `IBKR cached history remained underfilled after refresh`.
- Runs of the same window clamped after the session close made only two requests each, and a
  4.5.91 run of an older window paged backwards a few times and finished.
- A single live request for the same window returned only bars from the previous trading day,
  classified `complete`, with zero bars inside the requested gap.
- Exact log evidence is kept in private BotSpot operations notes.

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

## Follow-up: Alpaca through `BACKTESTING_DATA_SOURCE=alpaca` (the BotSpot path)

### How BotSpot builds an Alpaca backtest

- BotSpot Node (`botspot_node/src/services/dataAccess.service.ts`, provider `alpaca`) sets
  `BACKTESTING_DATA_SOURCE=alpaca`, hard-codes `ALPACA_IS_PAPER=true`, and passes
  `ALPACA_OAUTH_TOKEN` and/or `ALPACA_API_KEY`/`ALPACA_API_SECRET` from the user's environment.
- Bot Manager (`flask_app.py` backtest start) copies that `bot_config` into the task environment
  with `BACKTESTING_START`/`BACKTESTING_END`; `bootstrap_backtest.py` only uses the data source
  name for cache prefixes and runs the strategy's `main.py`.
- The generated strategy template (`botspot_agent` `shared_strategy_structure.md`) calls
  `backtest(datasource_class=None, benchmark_asset=..., quote_asset=..., parameters=...)`: no
  config, no timestep, no dates.
- LumiBot then maps `alpaca` to `AlpacaBacktesting` and builds it through the generic branch of
  `run_backtest` with `config=None`.

### What was wrong on that path

1. `config=None` raised `ValueError("Config cannot be None")`, so every such backtest failed
   before its first bar.
2. With a config but no timestep it defaulted to daily bars: an intraday strategy would read the
   day's open all day and fill intraday orders at the day's open.
3. It stopped at the open of the third-to-last trading day of the window.
4. `log_backtest_progress_to_file` from `run_backtest` was not forwarded, so no progress.csv.
5. Found while checking the credentials path: `ALPACA_IS_PAPER` is always `true` from BotSpot, so
   a live-account key gets 401 from the paper Trading API (option contract list), and a window
   clamped to "now" asked Alpaca for bars up to tomorrow, which free keys refuse ("subscription
   does not permit querying recent SIP data").

### Fix (`lumibot/backtesting/alpaca_backtesting.py`)

- Environment mode: when no config is passed (this used to raise, so no caller depended on it),
  credentials come from `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `ALPACA_OAUTH_TOKEN` and
  `ALPACA_IS_PAPER`; bars default to minute (not marked explicit, so `StrategyExecutor` still
  switches a daily-cadence strategy to day bars and it downloads no minute history); and the
  backtest runs through `backtesting_end` like every other source (`full_window`, default True in
  environment mode). An explicit config keeps the legacy defaults, which the legacy tests pin.
- Progress-file settings are forwarded to the base class.
- The option contract list retries once on the other Trading API endpoint after a 401/403
  (read-only; backtests never send orders).
- Bar requests stop 16 minutes before now.
- ThetaData, IBKR and the BotSpot Auto router are untouched.

### Tests (red first, then green)

In `tests/test_alpaca_backtesting_multitimeframe_unit.py`, through the real env-var selection
(`run_backtest(datasource_class=None)` with `BACKTESTING_DATA_SOURCE=alpaca` and only
environment credentials; the SDK clients are faked at the module boundary):

- intraday 5-minute strategy: minute timestep, every session of the window (Aug 3 to Aug 7),
  last price at 10:00 is the 10:00 minute bar, the market order at 09:40 fills on the 09:40
  minute bar;
- daily strategy: day bars only (no minute requests), all five sessions;
- options: `get_chains()` plus a market order fills on the next real print (09:37);
- 401 on the paper endpoint lists contracts from the live endpoint;
- the bar request never reaches the latest 15 minutes.

Red before the fix: the three backtests raised "Config cannot be None", the 401 test raised
`APIError: unauthorized`, and the request end was tomorrow 03:59:59 UTC.

### Real runs selected only by the environment

`scripts/alpaca_env_selection_proof.py` with `BACKTESTING_DATA_SOURCE=alpaca`,
`ALPACA_IS_PAPER=true`, the paper key, and `BACKTESTING_START`/`BACKTESTING_END`, calling
`backtest(datasource_class=None)`. Evidence in `docs/research/2026-09-23-alpaca-options-backtests/`
(`alpaca_env_*`); `settings.json` records `backtesting_data_sources: alpaca`.

- SPY 5-minute opening range breakout, `BACKTESTING_START=2026-08-03`,
  `BACKTESTING_END=2026-08-08`: `sleeptime` 5M, simulated through 2026-08-07 23:59 (the legacy
  stop would have ended at the 2026-08-05 open). Ranges from the completed 09:30, 09:35 and 09:40
  bars; 4 round trips, no breakout on Aug 5; net +$125.65 on 10 shares. Fills match raw Alpaca
  minute bars (09:55 open 752.765, 10:30 open 773.16, 15:50 opens 758.15 and 772.85).
- Weekly SPY call, `BACKTESTING_START=2026-07-27`, `BACKTESTING_END=2026-08-15`: trades are
  identical to the explicit-class 2026 run (which had to pass an end date three sessions later).

## Follow-up 2: history returned the bar that was still forming (lookahead, fixed)

### What was wrong

Alpaca labels a bar with its start time: the 5-minute bar labeled 10:00 holds trades from
10:00 to 10:04:59, and the daily bar labeled 08-04 00:00 holds the whole 08-04 session.
`get_historical_prices()` picked the newest bar whose label was at or before the simulated time
(`searchsorted(now, side="right") - 1`), so at 10:00 it returned the 10:00 five-minute bar with
its 10:04:59 close, high, low and volume, and at 09:30 it returned today's daily bar with the
session's close. The documented `remove_incomplete_current_bar=True` option did not help for
multi-minute bars: it dropped the newest bar only when its label equaled the simulated time
exactly, so at 10:02 (or 10:50 for 20-minute bars) the forming bar still came back. The
BotSpot path used the default `False`.

Real SPY at 2026-08-04 10:00 (evidence file `forming_bar_fix_2026-09-23.txt`): the old history
showed the 10:00 five-minute close 761.98 and the daily close 769.42, up 1.4 percent from the
open, six hours before it happened.

### The contract the other sources follow

- IBKR, ThetaData and Polygon backtests go through `Data`. For intraday bars
  `Data._get_bars_row_bounds` ends the slice before `get_iter_count(dt)`, the bar at or just
  before the simulated time, so history holds finished bars only (the comment in
  `Data._get_bars_dict`: "the legacy behaviour is preserved to avoid lookahead"). Multi-minute
  requests drop a resampled bucket until its last base bar is in (`latest_complete_label`), and
  daily-from-minute requests drop today's partial day.
- Daily bars: IBKR and ThetaData stamp them at 16:00 ET so a day's bar is visible only after
  its session (`_align_day_index_to_market_close_utc` in `thetadata_helper.py`, and the day-bar
  reindex in `ibkr_helper.py`). Alpaca stamps daily bars at midnight, which is why its current
  day's bar leaked at 09:30.
- Fills read the current bar separately: the Pandas branch of `BacktestingBroker` asks for
  `timeshift=-1` intraday, which adds the bar that starts now, and fills market orders at its
  open. `Data.get_last_price` returns that bar's open when a bar starts at the simulated time.

### Fix (`lumibot/backtesting/alpaca_backtesting.py`, `lumibot/backtesting/backtesting_broker.py`)

- `_newest_bar_position()` is the one rule for stocks, crypto and options: with
  `remove_incomplete_current_bar` a bar qualifies only when its label plus its length is at or
  before the simulated time (daily bars: only earlier dates, multi-day buckets once their last
  day has passed). The length comes from the requested timestep, so native multi-minute,
  hourly and resampled bars are all covered.
- Environment mode (`config=None`, the BotSpot path) defaults `remove_incomplete_current_bar` to
  `True`. An explicit value passed to `backtest()` or the constructor wins in both modes.
- Update (post-review, 4.5.92): an explicit config now also defaults to `True`. `False` is an
  explicit opt-in because the forming bar is a lookahead of up to one bar. The February 2025
  apitests `test_amzn_day_1d_5`, `test_amzn_minute_1d_5` and `test_amzn_minute_30m_5`, which
  assert that the lookback at the 2025-01-15 09:30 iteration ends with the 2025-01-15 daily
  bar, now pass `remove_incomplete_current_bar=False` explicitly.
- When no bar has finished yet (the very start of the data window) history returns `None`, as
  `Data` does, instead of raising. The old "Datetime not found" error is kept for `False`.
- `get_last_price()` and fills are unchanged: they use the open of the bar that starts now.
  The broker's Alpaca branch now passes `remove_incomplete_current_bar=False` so it still reads
  that bar, the same bar the Pandas branch reads with `timeshift=-1`. The option check that a
  print happened in the current minute still applies.

### Tests (red first, then green)

In `tests/test_alpaca_backtesting_multitimeframe_unit.py`:

- `test_env_selected_alpaca_history_never_returns_a_bar_that_closes_after_now`: a full
  `backtest(datasource_class=None)` run selected by `BACKTESTING_DATA_SOURCE=alpaca`. At every
  iteration every 5-minute, 1-minute and daily bar returned must have closed. At 10:00 the
  newest 5-minute bar is 09:55 with the 09:59 close, the 10:04 price is absent, the daily bar
  is the previous session, and `get_last_price` and the fill are the 10:00 minute open. The fake
  5-minute bars now close at their last minute's price so a leaked close is visible. Red: "5minute
  bar still forming at 2026-08-03 09:30".
- `test_env_selected_alpaca_option_history_excludes_the_print_that_is_still_forming`: the 09:37
  option print is not in history at 09:37, is at 09:38, and `get_last_price` at 09:37 is still
  its open (5.40). Red: history at 09:37 held the 09:37 print.
- `test_alpaca_remove_incomplete_current_bar_drops_a_native_bar_that_is_still_forming`: the
  documented option at 10:50 with 20-minute bars. Red: returned the 10:40 bar that closes at 11:00.
- `test_env_selected_alpaca_runs_intraday_minute_bars_over_the_full_window` (added earlier the
  same day) asserted the forming 10:00 bar; it now asserts 09:55.

### Real runs after the fix

`docs/research/2026-09-23-alpaca-options-backtests/forming_bar_fix_2026-09-23.txt`.

- ORB proof: the strategy now counts bars the source returns while still forming. Reproducing
  the old history (`--legacy-forming-bar`): 33 of 33 five-minute history calls returned a
  forming bar. After the fix: 0 of 33. Trades are identical row for row, because this strategy
  already filtered to finished bars and asked for one extra bar. A strategy without that guard
  would have traded on a close up to 5 minutes in the future.
- Weekly SPY call (options): 12 trade rows identical, including the `[FILL][PENDING]` waits.
  It uses `get_last_price`, `get_chains` and market orders, none of which changed.

### Behavior notes

- Daily bars are treated as finished only on a later date, so a call after the close (16:00 to
  midnight) still gets the previous session. Conservative; `Data` sources whose daily bars are
  stamped at the close include today's bar from 16:00.
- The data window started at `backtesting_start` unless `warm_up_trading_days` was passed, so on
  the first bar there was nothing finished to return (`None`). Follow-up 3 fixes that.

## Follow-up 3: history before `backtesting_start` (fixed)

### What was wrong

`AlpacaBacktesting` downloads one series per asset and bar size for the data window, which
started at `backtesting_start` (midnight) unless `warm_up_trading_days` was passed, and BotSpot
does not pass it. The opening-range strategy that started this work asks at its first bars for
250 five-minute bars and for 15 daily bars (an ATR(14) filter). On real SPY at 2026-08-03 09:30
it got 66 of 250 five-minute bars (only that morning's pre-market bars) and the daily request
raised "Not enough historical data. Requested 15 bars but only 5 available." (the whole window
held 5 daily bars), which stops the backtest. IBKR and ThetaData backtests fetch the history
before the start for the request.

### Fix (`lumibot/backtesting/alpaca_backtesting.py`)

- New documented option `history_before_start`: `True` by default in environment mode, `False`
  with an explicit config (the old window), an explicit value wins in both modes.
- When a history request needs more finished bars than the loaded series holds at the simulated
  time, `_reach_back_for_history()` fetches the real bars before the series once and merges them
  into it. The closed-bar rule is applied after the merge, so nothing still forming is returned.
- Bounded: `_history_start_needed()` counts trading sessions on the market calendar for `length`
  bars (1-minute to hourly bars assume the 390-minute session, which over-counts for native
  multi-minute bars that include extended hours), adds a quarter plus two sessions, and caps a
  reach at 260 sessions intraday and 2520 daily.
- Cached: each reach is one segment `[needed start, loaded start)` saved as
  `<cache>/alpaca/<key>_HISTORY.csv`, an empty segment too. The same requests in a later run read
  the segments from disk. A new need that goes further back fetches only the missing earlier days.
- No request loop: a reach already covered, or already tried that day for the same request, never
  asks Alpaca again, so a new listing or an option before its first trade costs no request per
  bar. `get_last_price` and the broker's fill lookup never reach back (`_extend_history=False`).
- Real bars only (RULE #1): segments are not reindexed or filled. Stock and crypto 1-minute bars
  keep the regular-session minutes of the market calendar, the same minutes the window's minute
  series holds; daily, native multi-minute and option bars are kept as Alpaca returns them.
- With history before the start, a request that still cannot be met (a recent listing, a thinly
  traded option) returns the bars that exist instead of raising "Not enough historical data". An
  explicit config keeps that error.

### Tests (red first, then green)

In `tests/test_alpaca_backtesting_multitimeframe_unit.py` (the fake Alpaca now also serves five
weeks before the window, without the 2026-07-03 holiday):

- `test_env_selected_alpaca_history_reaches_back_before_backtesting_start`: a full
  `backtest(datasource_class=None)` run selected by `BACKTESTING_DATA_SOURCE=alpaca`, asking for
  250 five-minute and 15 daily bars on every bar. At 09:30 on the first day it gets exactly 250
  and 15 bars, all from before the start, each equal to a bar the fake served; every bar of the
  week gets the full history with nothing still forming; exactly one earlier request per series,
  starting 7 and 21 sessions back (within the margin). Red: "Not enough historical data.
  Requested 15 bars but only 5 available."
- `test_explicit_config_alpaca_history_keeps_the_window_unless_asked`: an explicit config still
  returns only the window's bars and still raises for 15 daily bars, with no earlier request;
  `history_before_start=True` reaches back. Red: the option was ignored (1 of 250 bars).
- `test_env_selected_alpaca_option_history_reaches_back_to_real_prints_before_the_start`: at
  09:35 the option history holds exactly the two real prints from before the start plus the
  09:31 print; asking again makes no request; a longer request reaches back once for the missing
  days only and invents nothing; a second run reads everything, the empty segment included, from
  the disk cache. Red: 1 of 3 prints.
- `test_env_selected_alpaca_history_never_returns_a_bar_that_closes_after_now` (follow-up 2)
  asserted `None` at the first bar; it now asserts the prior session's bars.

### Real runs after the fix

`docs/research/2026-09-23-alpaca-options-backtests/history_before_start_2026-09-23.txt`.

- ORB proof, first bar (08-03 09:30): 250 of 250 five-minute bars (from 07-31 04:40) and 15 of 15
  daily bars (07-13 to 07-31), ATR(14) = 8.40, 0 still forming. Without history before the start:
  66 of 250 and the "Not enough historical data" error. Three reaches in the run, one request
  each (5-minute from 07-23, daily from 07-02, and daily from 06-04 for the backtest's own 30-bar
  dividend and split checks).
- Every bar compared with raw Alpaca bars requested directly for the same span (250 five-minute,
  15 daily, 500 one-minute at 09:40): 0 bars missing from the raw data, largest OHLCV difference
  0.0, 0 still forming.
- Trades: the ORB proof is identical row for row with and without history before the start (its
  decisions use the latest few five-minute bars, which the window already held; the probe only
  logs). The weekly SPY call is identical, 12 trade rows and all 24 decision lines; it makes no
  history request.

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
| Follow-up: `pytest tests/test_alpaca_backtesting_multitimeframe_unit.py` | 18/18 |
| Follow-up: `pytest tests -k "alpaca or Alpaca" -m "not apitest and not downloader"` | 109 passed, 2 skipped |
| Follow-up: `pytest tests/test_strategy_backtest_env_override.py tests/test_backtesting_data_source_env.py tests/test_backtest_runtime_timings.py tests/test_backtesting_broker.py` | 60/60 |
| Follow-up: `pytest tests/test_alpaca_backtesting.py` (apitest, paper key) | 38/38 |
| Follow-up 2: `pytest tests/test_alpaca_backtesting_multitimeframe_unit.py` | 21/21 |
| Follow-up 2: `pytest tests -k "alpaca or Alpaca" -m "not apitest and not downloader"` | 112 passed, 2 skipped |
| Follow-up 2: `pytest tests/test_backtesting_broker.py tests/test_backtesting_broker_await_close.py tests/test_backtesting_broker_time_advance.py tests/backtest/test_backtesting_broker_processing.py tests/test_strategy_backtest_env_override.py tests/test_get_last_price_sim_time_safety.py tests/test_indicator_temporal_safety.py tests/test_ibkr_crypto_backtesting_smoke_stubbed.py` | 140/140 |
| Follow-up 2: `pytest tests -k "backtesting_broker or order_lifecycle or options_helper or backtest_lookahead or lookahead" -m "not apitest and not downloader"` | 157 passed, 4 skipped |
| Follow-up 2: `pytest tests/test_alpaca_backtesting.py` (apitest, paper key) | 38/38 |
| Follow-up 3: `pytest tests/test_alpaca_backtesting_multitimeframe_unit.py` | 24/24 |
| Follow-up 3: `pytest tests -k "alpaca or Alpaca" -m "not apitest and not downloader"` | 115 passed, 2 skipped |
| Follow-up 3: the eight broker, env override and sim-time safety files (as in follow-up 2) | 140/140 |
| Follow-up 3: `pytest tests -k "backtesting_broker or order_lifecycle or options_helper or backtest_lookahead or lookahead" -m "not apitest and not downloader"` | 157 passed, 4 skipped |
| Follow-up 3: `pytest tests/test_alpaca_backtesting.py` (apitest, paper key) | 38/38 |

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
3. BotSpot: the `alpaca` provider path (`BACKTESTING_DATA_SOURCE=alpaca` plus the customer's
   credentials) now works end to end for stocks and options once this LumiBot ships; nothing in
   BotSpot Node or Bot Manager has to change for it. The BotSpot Auto router is unchanged: it
   still sends options to ThetaData, its Alpaca adapter only serves bars, and
   `RoutedBacktestingPandas` inherits ThetaData's `get_chains()`. Customers who pick Auto do not
   get Alpaca options.

## Open items

- Pandas-based option paths (Polygon, and any `Data.repair_times_and_fill` source) fill sparse
  options on a carried-forward bar (Task C: sold at 15:30 on the 15:18 print). Same class of
  problem fixed here for Alpaca; needs its own change and tests.
- `AlpacaBacktesting` stock/crypto calendar fill (`_reindex_and_fill`) predates RULE #1: minute
  and daily stock bars are reindexed to the trading calendar, a minute with no trade becomes a
  flat bar at the previous close with volume 0, and a missing first open is back-filled from a
  later bar. Legacy tests (`test_reindex_and_fill_*`, February 2025) pin it, so removing it is a
  deliberate decision, not a drive-by edit.
- Fixed (follow-up 2): history on the BotSpot path no longer includes the bar that is still
  forming. With an explicit config the documented default `remove_incomplete_current_bar=False`
  still includes it (a lookahead of up to one bar; the 2025 apitests pin that default). Flipping
  the explicit default is a one-line change once those tests are updated deliberately.
- Fixed (follow-up 3): history before `backtesting_start` on the BotSpot path. With an explicit
  config the window still starts at `backtesting_start` unless `history_before_start=True` or
  `warm_up_trading_days` is passed.
- Seen in passing, not verified end to end: the multi-leg SMART_LIMIT fallback
  `BacktestingBroker._fill_multileg_children_at_market_open` passes an integer `timeshift` (bars)
  to `get_historical_prices`. `AlpacaBacktesting` treats `timeshift` as a `timedelta`, so that
  call raises inside a `try` and the fallback returns False. Check with a real Alpaca spread
  before relying on multi-leg option orders there.
- An explicit config still stops three sessions early (legacy tests pin it); `full_window=True`
  opts out.
- `LUMIBOT_OPTION_CHAIN_MAX_DAYS` (Polygon chain bound) is not in `docsrc/environment_variables.rst`.
- The chain lookahead (strikes listed after the simulated date) could be tightened by dropping
  contracts whose first bar is after the simulated date, at the cost of one bar request each.
