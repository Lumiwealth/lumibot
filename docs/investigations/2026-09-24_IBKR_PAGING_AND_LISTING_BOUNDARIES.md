# IBKR paging across closed markets and listing boundaries

One-line description: seven IBKR history defects that made routed stock and index backtests underfilled, empty or slow, fixed on `version/4.6.1`.

Last Updated: 2026-09-24
Status: Fixed on `version/4.6.1`, not released
Audience: LumiBot maintainers, BotSpot release captain

## Overview

Production BotSpot backtests routed to IBKR were found failing in five ways during a 30-day review of failed,
force-stopped and zero-trade runs. Customer-level detail lives in private BotSpot operations notes. This page
records the LumiBot mechanics, the live evidence (IBKR payload shapes only) and the fixes.

All fixes are in `lumibot/tools/ibkr_helper.py`, red-first tests are named below, and nothing fabricates bars
(RULE #1): missing history stays missing and is logged.

## 1. Minute paging stopped at the first closed gap

`_fetch_history_between_dates` pages backwards with `period=1000min` for 1-minute bars (16.7 hours). IBKR answers
`points=0` for a page with no bars (live: SPY `startTime=20260914-08:00:00`, Monday 04:00 ET). The loop treated the
first empty page after data as the start of history. Every weekend (about 56 closed hours) ended a stock series, and
every night (17.5 hours for indexes, which print 09:30 to 16:00 ET only) ended an index series. Multi-week minute
backtests saw the last few sessions and logged "IBKR cached history remained underfilled" on every bar.

Fix: `_cursor_before_closed_equity_page()`. When a page is empty and its whole window is closed-market time on the
NYSE calendar (extended hours per request for stocks, regular session for indexes), step the cursor over it and over
further closed pages without a request. An empty page during trading time keeps the old stop. Futures are unchanged.

Tests: `tests/backtest/test_routed_backtesting_ibkr_prefetch.py::test_router_ibkr_stock_minute_prefetch_pages_across_weekends`,
`::test_ibkr_index_minute_history_pages_across_overnight_and_weekend_gaps`.

## 2. `Chart data unavailable` discarded real daily history

A daily page reaching before the first bar IBKR holds for a contract gets HTTP 500 `{"error":"Chart data unavailable"}`.
Live, a 2022 listing: `5y` failed, `4y` returned 1,002 bars, `1000d` 684. The pager raised, so a recent listing got
no bars (first page) and a long backtest lost every page it had (later page).

Fix: `_smaller_daily_period_after_chart_unavailable()` halves the daily page (days) and retries the same cursor, down
to 5 days; after that the collected bars are kept. Test: `tests/test_ibkr_helper_unit.py::test_ibkr_daily_history_keeps_real_bars_when_a_page_reaches_before_the_first_bar`.

## 3. A failed older page discarded newer pages

Any exception on a later page (for example the downloader's "remained invalid after rebuild") re-raised and threw
away the pages already collected. Now the collected pages are kept, a warning names the reason and the oldest bar
kept, and nothing is negatively cached. A failure on the first page still raises.
Tests: `tests/test_ibkr_helper_unit.py::test_ibkr_later_page_failure_keeps_the_real_pages_already_collected`,
`::test_ibkr_first_page_failure_still_raises`.

## 4. Requests ending "now" hit the delayed feed

IBKR stock/index history on the shared account lagged 13 to 17 minutes. A request ending at the current time was
rejected by the downloader as `stale_tail`, and that newest page is the first page. Intraday stock/index requests now
end `IBKR_INTRADAY_HISTORY_DELAY` (20 minutes) before now (`_ibkr_history_now_utc()` is the test seam).
Test: `tests/backtest/test_routed_backtesting_ibkr_prefetch.py::test_ibkr_stock_minute_request_ending_now_stays_behind_the_delayed_feed`.

## 5. Daily windows just over a year used 5-year pages

`_history_period_for_request` sized daily pages exactly only up to 365 days. A one-year backtest plus an indicator
lookback (~390 days) asked for `5y`, which the downloader's validation rebuilt every time (~35 s per symbol). Exact
`<N>d` pages now cover spans up to `IBKR_DAILY_EXACT_PERIOD_MAX_DAYS` (993, so N <= 1000). Live: XLK and XSW
Aug 2025 to Sep 2026 are one `402d` request each, 11 to 14 s.
Test: `tests/test_ibkr_helper_unit.py::test_daily_fetch_sizes_windows_up_to_1000_days_exactly`.

## 6. Pages straddled the overnight gap

Paging continued from the oldest bar received, so every 1000-minute page after a session open reached back into
closed overnight time: about 1.8 requests per session in production (90 SPY requests for 50 sessions).
`_previous_equity_session_close_before()` anchors the next page at the previous session's close when only closed time
lies between. Live: 9 requests for 9 SPY sessions (pages end at 20:00 ET), no empty weekend pages.
Test: `tests/backtest/test_routed_backtesting_ibkr_prefetch.py::test_ibkr_stock_minute_paging_uses_one_request_per_session`
(red: 25 requests for 14 sessions).

## 7. A stopped walk lost all its pages

`get_price_data` wrote the cache only after `_fetch_history_between_dates` returned. A cold 8-month 1-minute walk
takes hours on the shared downloader, so a force-stopped or timed-out run kept nothing and every retry started over.
The pager now hands every `IBKR_PAGE_CHECKPOINT_EVERY` (10) pages to a checkpoint that merges them into the cache file.
Test: `tests/backtest/test_routed_backtesting_ibkr_prefetch.py::test_ibkr_minute_paging_checkpoints_pages_so_a_stopped_run_keeps_its_progress`
(red: no cache file after 25 served pages).

## 8. Holes inside a cached minute series were never fetched (2026-09-25)

Found by the 4.6.1 release gate. `get_price_data` compared only the edges of the requested window with the minute
cache. A cache holding June and September (two earlier backtests on the same symbol) served a June-to-September
backtest with July and August missing: zero requests, no error, one stale bar for weeks, zero trades. The same holes
come from sections 3 and 7 (kept newer pages, page checkpoints) and from LumiBot 4.6.0, which stopped at every weekend
and wrote those holes into the shared S3 cache. Daily and hourly series already had hole repair; minute did not.

`_repair_us_stock_index_minute_gaps` now runs after the edge checks for US stock and index minute series. It lists NYSE
sessions strictly between the first and last real bar of the window that have no bar, and fetches each one (one
request per session, the same cost as a cold walk). A session IBKR answers with no bars (a thin symbol with no prints)
gets a `minute_session_gap_empty` marker for `IBKR_GAP_RETRY_TTL_SECONDS`, so later backtests do not ask again. A failed
request writes nothing and is retried by the next process; each series and window is checked once per process.

Cost (CodeRabbit on PR #1180 asked for a time limit): at most one request per session in the window, once per process,
never more than a cold download of that window. A failed session is not asked again for 5 minutes in the same
process (a sliding-window caller used to ask it on every bar: 13 requests instead of 4 in the test), and repair pauses
for the series for 5 minutes after 3 failures in a row (30 requests instead of 3). The pause expires, so a long-lived
process (notebook, local script, multi-backtest service) repairs the hole once the downloader recovers; the release gate
found that a permanent give-up served a later backtest 34 of 77 sessions silently. Every serve of a series with known
unrepaired sessions logs a WARNING naming the symbol, timestep and missing-session count (at most once a minute per
series within one backtest; every new backtest warns). No wall-clock limit on purpose: results would depend on
downloader load. Live on the production downloader: a 10-session SPY hole took 10 requests, 52 s (median 5.1 s); the
next call made none. The session scan uses binary searches: 3 ms for a year of extended-hours minute bars (27 ms with
per-row date math).
Tests: `tests/test_ibkr_daily_gap_self_healing.py -k minute` (6 tests; red before the fix: 43 July/August sessions
missing, 24 and 19 sessions missing after an interrupted download, 10 SPX sessions missing).

Read-only scan of the shared cache on 2026-09-25 (`prod/cache/v44`, the namespace written this week): 1 of 29 intraday
stock files has a hole (SPY 5-minute, 2026-08-17 to 08-19). The older `prod/cache/v1` namespace (last written 2026-09-08)
has 5 of 78 files with holes (SPX, APP, QQQ, SPY, TQQQ minute; 233 sessions). With this fix those holes are fetched the
next time a backtest reads the file, so no manual cleanup is required.

## 9. Every page lost the bar just before its end (2026-09-25)

An IBKR page with `startTime=T` holds bars only up to `T - 2 bars`; the bar that starts one bar before T (and ends at
T) is left out. The downloader keeps every row it gets inside the window, so this is IBKR's answer. Recorded on the
production downloader: SPY 1-minute `startTime=20260918-00:00` (20:00 ET) ended at 19:58 ET; SPX 1-minute pages ending
at the 16:00 ET close held 389 bars ending 15:58; QQQ 5-minute `startTime=20260302-13:40` ended at 13:30 UTC.

Effect: section 6 anchors pages at a session close, so 4.6.1 lost every session's final bar (SPX 15:59, the closing
minute that close-of-day and 0DTE strategies read; stock extended-hours 19:59; NVDA hourly 19:00). A page continuing from
the previous page's earliest bar lost the bar just before it (both 4.6.0 and 4.6.1; QQQ 5-minute about every 3.5 days).

`_fetch_history_between_dates` now sends `startTime = cursor + one bar` for intraday bars (`_ibkr_page_request_end`), so
the page holds every bar that starts before the cursor. If IBKR ever includes the bar at T too, the merge drops the
duplicate. Daily requests are unchanged. The downloader's tail check (3-bar tolerance) still sees a 2-bar gap.
Tests: `tests/test_ibkr_helper_unit.py -k "closing_bar or last_extended_hours_bar or capped_5minute"` (red: SPX
15:58 in 4 of 5 sessions, SPY 19:59 missing in 4 sessions, 8 QQQ 5-minute bars missing). Live after the fix: 5 SPX
sessions, 390 bars each, every one ending 15:59.

## Test results

`LUMIBOT_DISABLE_DOTENV_LOCAL=1 LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled`:

| Command | Result |
| --- | --- |
| `pytest tests -k "ibkr or IBKR or routed" -m "not apitest and not downloader"` | 272 passed, 2 skipped |
| `pytest tests/test_ibkr_helper_unit.py` | 41 passed |
| `pytest tests/backtest/test_routed_backtesting_ibkr_prefetch.py` | 17 passed |

## Open items

- Each IBKR page costs the downloader one request plus three validation probes under a 48-per-minute limit, so a
  cold 8-month 1-minute series takes tens of minutes per symbol. The probes guard data integrity; reducing them is a
  downloader design decision.
- Stock minute history is always fetched with extended hours (`_ibkr_include_after_hours`), even when a strategy
  asks for regular hours only, which doubles the pages.
- A daily-cadence routed strategy asking for minute bars can get daily bars; tracked separately.
