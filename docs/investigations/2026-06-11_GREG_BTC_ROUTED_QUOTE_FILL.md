# Greg BTC Routed Quote Fill Investigation

One-line description: Production artifact diagnosis and LumiBot fix for Greg's BTC backtest drop caused by out-of-window IBKR crypto prices.
Last Updated: 2026-06-12
Status: Fixed in `v4.5.48`; production Bot Manager rerun of the April 15-19 window completed with no stale fills
Audience: LumiBot, BotSpot Node, and BotSpot support engineers

## Overview

Greg reported a large backtest result drop in a BotSpot email thread with subject `april 17`. The production backtest was:

- Backtest: `64d6495f-bfaf-46e8-b150-0110ed773b4a`
- Strategy: `72953aee-a7d0-45f3-b65c-877f1c3694ad`
- Revision: `9c8e12ce-7ffd-438e-907d-57a13660a1f8`
- AI strategy: `1e3d4506-3ebd-49a8-9d14-496c4e42ca66`
- Period: 2026-03-09 to 2026-06-05
- Production summary: total return about `-8.55%`, CAGR about `-30.99%`, max drawdown about `-64.18%`

An initial support copy of the saved revision files was exported from the
production database through the read-only support path into:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/main.py`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/requirements.txt`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/revision-9c8e12ce/manifest.json`

No broker deployment secrets or account credentials were copied.

Important correction from the 2026-06-12 audit: the executed production
`code.zip` pulled from the backtest runner artifact path is not byte-identical
to the older database-exported `main.py` in the 2026-06-11 support folder.
All future reproduction for this incident must use the executed code zip:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/code.zip`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/executed-code/main.py`

The executed `main.py` SHA-256 is
`91473f8fa813cd5fa818bdad6201053b936c999f3a2f725586c25d9e567e8b40`.

Production result artifacts were exported through the supported Bot Manager API
and CloudWatch, then parsed into:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/analysis/production_artifact_summary.md`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/api-artifacts/trades.csv`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/api-artifacts/indicators.csv`

The production artifacts prove the April drop was not just a visual/chart issue:

- 76 filled trades were present.
- 63 of 76 fills were outside the same-hour BTC OHLC candle.
- The stale price `70511.75` was reused across many fills.
- April 15-17 contained five fills at `70511.75`.
- The largest one-row portfolio drop was `-56.25%` at
  `2026-04-17 10:05:00-04:00`, immediately after a sell fill at `70511.75`.

## Reproduction

The copied strategy is `MultiMineralBot`, trading spot `BTC/USDT` in BotSpot while the local backtest harness maps it to BTC/USD quote data. It uses:

- `set_market("24/7")`
- `check_interval: "5M"`
- `timestep: "1h"`
- budget `1300`
- leverage `10`
- routed BotSpot Auto data provider config with crypto routed to IBKR

The local production-like replay used the copied revision and the same 2026-03-09 to 2026-06-05 window. Logs are under:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260611/local-run-v100-prodlike-remote-dd/subprocess_greg-v100-prodlike-remote-dd.log`

The run was intentionally stopped after it had already reproduced the incorrect early fill path. It did not need to finish the full multi-month backtest to prove the failing invariant.

## 2026-06-12 Audit Result

Rob correctly challenged the first pass as too narrow. The first draft only
guarded the direct IBKR REST quote-fill speed path. Greg's production backtest
used the BotSpot Auto JSON router:

```json
{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}
```

That routes BTC through `RoutedBacktestingPandas`, so the real fix needed to
cover routed IBKR crypto price and quote lookups, not only direct fill speed
helpers.

Correct production model for this audit:

- BotSpot Node starts the backtest from the saved revision and server-side data
  provider config. User-supplied raw env vars are blocked.
- BotSpot Auto routes crypto to IBKR. This is not a ThetaData investigation
  unless evidence later shows a non-crypto asset or routing bug.
- Bot Manager runs the backtest in an ephemeral ECS/container environment. The
  local disk starts effectively cold, so Rob's existing workstation cache is
  not production-equivalent.
- Bot Manager's `BACKTEST_CACHE_*` object-cache warm path is separate from
  LumiBot's `LUMIBOT_CACHE_*` market-data parquet cache.
- LumiBot's S3 cache key is based on the path relative to
  `LUMIBOT_CACHE_FOLDER`, and the cache env is read at import time. Local
  reproduction must set these variables before importing LumiBot.
- Data Downloader classifies IBKR history responses and has a write policy that
  should deny partial responses. If an underfilled parquet is present, the
  audit must identify whether it came from a stale S3 object, a too-narrow
  LumiBot request, a routed-data coverage bug, or a downloader classification
  bug.
- `LUMIBOT_BACKTEST_AUDIT=1` adds useful trade-event telemetry, but it can
  bypass the direct fast fill path. The replay matrix must include both audited
  and non-audited runs.

Do not bump the production cache version or delete production cache objects for
this incident. The bug is in LumiBot's acceptance of an out-of-window intraday
frame as a current price/quote. Missing or partial data must stay missing; it
must not be hidden by synthetic bars or cache deletion.

Additional verified facts from the 2026-06-12 audit:

- The production backtest row still points to revision
  `9c8e12ce-7ffd-438e-907d-57a13660a1f8`, version `100`, crypto data
  requirements, and a completed summary of about `-8.55%` total return and
  `-64.18%` max drawdown.
- The result manifest lists settings, logs, trades, trade events, indicators,
  and parquet artifacts for backtest
  `64d6495f-bfaf-46e8-b150-0110ed773b4a`. Settings, trades, stats, tearsheet,
  completion, and indicators were exported through the product-supported API.
  Direct S3 reads for some result objects failed KMS decrypt from the local CLI,
  so CloudWatch plus API artifacts were used for evidence.
- The owner-scoped production MCP artifact tools correctly rejected cross-user
  access to Greg's result artifacts from this session. That means exact
  production fill proof must come from a support/admin artifact export path, not
  from bypassing ownership checks.
- A fresh local replay from the executed code zip with production Data
  Downloader reached the early March 12 signal region and showed the corrected
  current price path (`71421.5000`, not `70511.75`). Full-window replay did not
  finish in-turn because a fresh local task had to hydrate many IBKR `1min`
  `Trades`, `Bid_Ask`, and `Midpoint` chunks through the downloader. Partial
  replay logs are preserved under
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/`.

## Finding From First Pass

The replay showed IBKR crypto history was underfilled for the early simulation window:

- Available BTC minute data started around 2026-03-22.
- The simulation was evaluating and submitting orders on 2026-03-12.
- LumiBot logged that requested BTC dates were outside the available data range.
- Despite that, market orders could still fill from quote/bid-ask data before the cached data object actually covered the simulated timestamp.

The clearest first-pass local evidence was around simulated
`2026-03-12 08:00:00-04:00`:

- the strategy submitted a BTC/USD market buy;
- the log repeatedly warned that `BTC/USD` minute `Trades` history remained
  underfilled and was returning available real bars;
- the order filled at `$62,718.00`;
- the same heartbeat region reported strategy price values around `$70,445.50`.

That was enough to justify the deeper fill-provenance audit. Later production
artifacts above confirmed the same class of problem in Greg's actual completed
backtest, especially the repeated `70511.75` fills around April 15-17.

The direct code path reproduced in the first pass was
`BacktestingBroker._fast_get_bid_ask_for_fill()` in
`lumibot/backtesting/backtesting_broker.py`.

That helper is an IBKR REST backtesting speed path. It reads bid/ask directly from the cached `Data` object's datalines and bypasses the normal guarded `Data.get_quote()` wrapper. Before the fix, it called `data_obj.get_iter_count(now)` directly. For timestamps before the first cached row, that can produce an invalid positional lookup instead of surfacing missing data.

This is a backtest engine bug, not a Greg strategy logic bug. The strategy submitted an order during a period where the data source did not cover the simulated timestamp; the engine should not fill that order from an out-of-window bid/ask row.

The reopened audit also found a separate routed-path hazard to verify. In
`lumibot/backtesting/routed_backtesting.py`, `_IbkrRoutingAdapter._fetch_df()`
can mark crypto/futures series as fully loaded after any non-empty prefetch in
some paths. Other routed prefetch paths already call
`ibkr_helper.frame_covers_requested_window()` before marking the series loaded.
If Greg's backtest received a partial non-empty routed IBKR frame, that
fully-loaded marker could prevent a later repair fetch. The fix now applies the
same `frame_covers_requested_window()` gate to those paths.

## Implemented Fix

The fix on `version/4.5.48` now covers all three unsafe surfaces found during
the audit:

- `BacktestingBroker._fast_get_bid_ask_for_fill()` rejects direct IBKR fast-fill
  data when the simulated timestamp is outside the cached object's
  `datetime_start`/`datetime_end`.
- `RoutedBacktestingPandas` and `InteractiveBrokersRESTBacktesting` mark
  intraday IBKR-backed `Data` objects with `strict_end_check=True`.
- `ThetaDataBacktestingPandas.get_last_price()` now respects strict intraday
  frame bounds before using its direct `get_iter_count()` last-trade path.
- `ThetaDataBacktestingPandas.get_quote()` now requires `frame_start <= dt <=
  frame_end` before taking its quote fast path, so a future-only frame cannot
  produce a current quote.
- `_IbkrRoutingAdapter._fetch_df()` no longer marks crypto/futures full-window
  prefetches as fully loaded unless `ibkr_helper.frame_covers_requested_window()`
  confirms coverage.
- `scripts/run_backtest_prodlike.py` has a `--cache-mode` flag so future
  replays can explicitly run production S3 cache in `readonly` mode.

If coverage is missing, LumiBot now returns `None`/empty `Quote` for the price
surface instead of carrying a stale or future intraday row into strategy sizing,
markers, or fills.

Regression coverage was added for direct and routed paths:

- direct fast-fill rejects a future BTC minute frame for a March 12 fill attempt;
- direct IBKR `get_last_price()` rejects both stale-after-end and
  future-before-start intraday frames;
- routed IBKR `get_last_price()` and `get_quote()` reject both stale-after-end
  and future-before-start intraday frames.

## Validation

Focused suite:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 240 python3 -m pytest tests/test_interactive_brokers_rest_backtesting_unit.py tests/test_routed_backtesting_ibkr_daily_prefetch.py tests/test_ibkr_crypto_backtesting_smoke_stubbed.py -q
```

Result on 2026-06-12:

- `24 passed`
- one third-party `websockets.legacy` deprecation warning

Production release and runtime validation:

- LumiBot `v4.5.48` was published and installed by Bot Manager production and
  development image builds. Production GitHub Actions run `27403208749` and
  development run `27403207949` both logged `LUMIBOT_VERSION=4.5.48` and
  `lumibot==4.5.48` in the dependency/backtest image builds.
- A production BotSpot/Bot Manager rerun used Greg's same revision
  `9c8e12ce-7ffd-438e-907d-57a13660a1f8` for `2026-04-15` through
  `2026-04-18 23:59` with budget `1300`. New manager bot/backtest id:
  `4aca383a-3688-4ca7-a058-cfd98586c7e7`.
- The completed rerun `settings.json` reported `lumibot_version: 4.5.48`,
  Data Downloader base URL `http://data-downloader.lumiwealth.com:8080`, and
  remote cache `backend=s3`, `mode=readwrite`, `bucket=lumibot-cache-prod`,
  `prefix=prod/cache`, `version=v1`.
- The completed rerun still logged underfilled IBKR BTC hourly data and explicit
  "data refresh required instead of using stale bars" messages, but those
  missing-data conditions no longer produced out-of-window fills.
- `trades.parquet` and `trade_events.parquet` each had 12 lifecycle rows:
  6 `new` BTC buy orders and 6 `canceled` BTC buy orders. They had
  0 priced rows, 0 filled-quantity rows, and 0 rows at stale price `70511.75`.
- `stats.parquet` had 1,154 rows with `portfolio_value` min/max exactly
  `1300`, `cash` min/max exactly `1300`, `return` min/max `0`, and
  0 rows with positions.

## Required Follow-Up

The April 15-19 production rerun proves the stale-fill failure no longer occurs
in the reported April drop window. Before promising an exact new result for the
entire March 9-June 5 customer backtest, run the full original window again and
compare final stats.

Remaining follow-up:

1. Run the full `2026-03-09` to `2026-06-05` window after `v4.5.48` if Greg or
   support needs exact replacement metrics for the full reported run.
2. Investigate why fresh local replay needs to hydrate many BTC minute chunks
   despite production S3 cache configuration; that is a performance/cache
   coverage issue, not the root data-integrity bug fixed here.

Do not add fake trades, synthetic bars, carry-forward bid/ask rows, or strategy-specific patches to make this backtest look better. Missing data must stay missing.
