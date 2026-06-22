# Greg BTC Routed Quote Fill Investigation

One-line description: Production artifact diagnosis and LumiBot fix for Greg's BTC backtest drop caused by out-of-window IBKR crypto prices.
Last Updated: 2026-06-18
Status: Production stale-fill fix released in `v4.5.49` plus Data Downloader commit `0fb287a`; local `version/4.5.52` is adding broker-like pending-order/TIF semantics before any new release
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

The fix series covers all unsafe surfaces found during the audit.

`v4.5.48` covered the direct and routed LumiBot stale-fill surfaces:

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

`v4.5.49` added the deeper stale-bar guard Rob asked for after reviewing the
first pass:

- `Data` now rejects strict intraday minute/hour lookups when the nearest row is
  older than the allowed bar-age tolerance, even if the broader cached frame
  technically overlaps the requested window.
- IBKR malformed/underfilled rebuild failures are classified as terminal no-data
  failures instead of being silently reinterpreted as usable history.

The Data Downloader was also fixed and deployed at commit
`0fb287a15e17086edb25a5e5cc2329228484ad11`:

- 24/7 crypto history no longer preemptively rebuilds one-week minute requests
  before validating the top-level provider response.
- Stale-tail BTC responses are returned as `classification=partial` with
  `cache_write_policy=deny`, so they are not written back into the S3 cache.
- IBKR Crypto Basic weekend closures are treated as valid closed-market tails
  only through the documented Friday 16:00 ET to Sunday 03:00 ET closure. After
  Sunday 03:00 ET, a Friday tail is still stale and must rebuild or return a
  partial/no-cache response.

`v4.5.51` fixes a separate backtest progress stall found while rerunning Greg's
full window after the data-integrity fixes:

- Production full-window rerun
  `bench-default-f6f00d6f-7348-42eb-ab70-9dc00d05fa16` used LumiBot `4.5.49`,
  production Data Downloader, and `lumibot-cache-prod/prod/cache/v1`.
- It was force-stopped after staying pinned at simulation
  `2026-03-12 05:15:00`, `3.70%`, with cash/portfolio still `1300.00` and no
  positions/orders. The visible warnings were warmup/history-shortage warnings,
  not stale fills.
- Targeted rerun `bench-default-f297a164-f219-40d6-8562-6df9e9877653` covered
  the old `70511.75` Mar 25/Mar 26 fill window. It advanced to
  `2026-03-26 01:35:00`, `76.66%`, still with no positions/orders and no
  `70511.75` or order/fill logs, then pinned on Greg strategy's intrabar
  cooldown path.
- Root cause found in the framework path: `StrategyExecutor.safe_sleep()` builds
  the progress payload before advancing simulated time, and the payload called
  `strategy.get_portfolio_value()`, which forces a fresh valuation. Progress
  logging should never be able to block `_update_datetime()`.
- `v4.5.51` changes progress payloads to use cached `strategy.portfolio_value`
  with cash fallback. This does not change order fills, stats valuation, or
  strategy trading logic; it only prevents UI progress logging from blocking
  simulated time advancement.
- The earlier `v4.5.50` tag failed release tests before PyPI/GitHub publication
  because existing tests still expected progress payloads to force a fresh
  `get_portfolio_value()` call. It is superseded by `v4.5.51`.

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

Final `v4.5.49` production validation:

- LumiBot `v4.5.49` release workflow `27446507001` completed successfully.
- Bot Manager production workflow `27447738117` and development workflow
  `27447738160` both completed successfully. Production logs showed
  `LUMIBOT_VERSION=4.5.49`, `uv pip install ... "lumibot==4.5.49"`, and
  `LumiBot v4.5.49 starting` during the image build.
- Data Downloader production workflow `27450318925` completed successfully and
  live `/version` later reported
  `0fb287a15e17086edb25a5e5cc2329228484ad11`, including the IBKR Crypto Basic
  weekend-tail fix.
- A production-routed BTC one-week/minute probe for end time
  `2026-04-19 00:00:00 UTC` returned
  `classification=partial`, `cache_write_policy=deny`, `rebuild_count=0`,
  `first_timestamp=2026-04-17T03:20:00+00:00`, and
  `last_timestamp=2026-04-17T19:59:00+00:00`. That proves stale-tail BTC data is
  no longer allowed to warm cache.
- Final production Bot Manager rerun:
  `bench-default-44b87f29-04b9-4570-a61a-95760d4c89a8`, window
  `2026-04-15` through `2026-04-19 23:59`, executed Greg's production
  `main.py` with the original routed data-source JSON and prod cache
  `lumibot-cache-prod/prod/cache/v1`.
- The rerun settings reported `lumibot_version: 4.5.49`, the original
  `backtesting_data_sources` JSON, production Data Downloader, and remote cache
  `backend=s3`, `mode=readwrite`, `bucket=lumibot-cache-prod`,
  `prefix=prod/cache`, `version=v1`.
- `trades.csv` had 0 rows, 0 priced fills, and no `70511.75` value.
- `stats.csv` had 1,442 rows with `portfolio_value` min/max exactly `1300`,
  `cash` min/max exactly `1300`, `return` min/max `0`, and all positions empty.
- `indicators.csv` had 52 rows, no `70511.75` value, and only emitted indicator
  rows near the available BTC history window.
- Logs explicitly surfaced missing/stale history instead of using stale bars,
  including `data refresh required instead of using stale bars` after the BTC
  data ended at `2026-04-19 22:00:00-04:00`.

`v4.5.51` local validation before release:

- `pytest -q tests/backtest/test_strategy_executor_progress_payload.py`
  returned `2 passed`.
- `pytest -q tests/backtest/test_strategy_executor_progress_payload.py tests/backtest/test_backtesting_broker_processing.py tests/backtest/test_strategy_executor_backtest_end_clamp.py`
  returned `45 passed`.
- The new regression asserts that backtest progress payload generation does not
  call `get_portfolio_value()`, which would force fresh valuation before the
  simulated clock advances.

The bad production S3 BTC minute cache objects remain identified but not
quarantined from this machine because the available AWS profiles were denied
`s3:PutObject`/versioning access to `lumibot-cache-prod`. Do not bump the whole
production cache version to work around this. If direct cleanup is needed, use a
principal with write/delete permissions and surgically quarantine only:

- `s3://lumibot-cache-prod/prod/cache/v1/ibkr/crypto/minute/bars/crypto_BTC_USD_minute_ZEROHASH_TRADES_AHR.parquet`
- `s3://lumibot-cache-prod/prod/cache/v1/ibkr/crypto/minute/bars/crypto_BTC_USDT_minute_ZEROHASH_TRADES_AHR.parquet`

## Required Follow-Up

The April 15-19 production rerun proves the stale-fill failure no longer occurs
in the reported April drop window. The post-`0fb287a` full and targeted reruns
then exposed the separate progress-payload stall fixed in `v4.5.51`. Before
promising an exact new result for the entire March 9-June 5 customer backtest,
deploy `v4.5.51`, rerun the full original window again, and compare final stats.

Remaining follow-up:

1. Release and deploy LumiBot `v4.5.51` through Bot Manager.
2. Rerun the full `2026-03-09` to `2026-06-05` window after `v4.5.51` and
   verify completion, no `70511.75` fills, and final replacement metrics.
3. Investigate why fresh local replay needs to hydrate many BTC minute chunks
   despite production S3 cache configuration; that is a performance/cache
   coverage issue, not the root data-integrity bug fixed here.

Do not add fake trades, synthetic bars, carry-forward bid/ask rows, or strategy-specific patches to make this backtest look better. Missing data must stay missing.

## 2026-06-12 Deeper Cache And Downloader Audit

Follow-up audit found that the `70511.75` stale fill was not random. A read-only
copy of the production S3 object
`s3://lumibot-cache-prod/prod/cache/v1/ibkr/crypto/minute/bars/crypto_BTC_USD_minute_ZEROHASH_TRADES_AHR.parquet`
showed:

- 72,806 total rows, timestamp range `2025-04-21 04:00 UTC` to
  `2026-06-05 03:57 UTC`;
- 0 rows for `2026-04-01` through `2026-04-08`;
- only 2 rows for `2026-04-15` through `2026-04-19`, both placeholder rows with
  `missing=True` and all OHLC/volume values `NaN`;
- the last real BTC minute before that gap was `2026-03-24 03:58 UTC`, close
  `70511.75`, followed by placeholder rows at `2026-03-24 04:00 UTC`,
  `2026-04-15 04:00 UTC`, and `2026-04-18 04:00 UTC`.

That explains the observed April stale fills: older LumiBot paths could ask for
an April timestamp, skip over placeholder rows, and reuse the last real minute
from March 24.

A bounded production Data Downloader probe against the queued IBKR path also
showed a downloader validation issue. Request:

```text
symbol=BTC, conid=541686651, source=Trades, bar=1min, period=1w,
startTime=20260419-00:00:00, outsideRth=true
```

The queued response returned 7,960 rows from `2026-04-12 07:00 UTC` through
`2026-04-17 19:59 UTC` and annotated the payload as
`classification=complete`, `cache_write_policy=allow`, `requested_end=2026-04-19
00:00 UTC`. That should not be cacheable as complete for 24/7 crypto data.

The likely downloader bug is in
`botspot_data_downloader/src/botspot_data_downloader/queue_worker.py`:
`_ibkr_history_tail_coverage_error()` only checks intraday tail coverage when
`requested_end` falls during US regular trading hours. That exception makes
sense for US stock/index history after market close, but it lets BTC/crypto
weekend or overnight tail gaps bypass stale-tail detection.

The Greg rerun task definition also did not include `LUMIBOT_CACHE_BACKEND`,
`LUMIBOT_CACHE_S3_BUCKET`, `LUMIBOT_CACHE_S3_PREFIX`,
`LUMIBOT_CACHE_S3_REGION`, or `LUMIBOT_CACHE_S3_VERSION` as task-definition
environment variables. Those values may have been passed as ECS run-task
overrides or loaded from another runtime source, but `describe-task-definition`
alone is insufficient proof. The observed artifact reported cache version `v1`,
which matches LumiBot's default when `LUMIBOT_CACHE_S3_VERSION` is not injected.

Next fix should be root-cause oriented:

1. Fix Data Downloader crypto/24-7 tail coverage so stale tails become
   `partial`/non-cacheable instead of `complete`/`allow`.
2. Add LumiBot gap/staleness guards for intraday price/quote/fill reads so a
   sparse in-window object cannot walk back days or weeks to the previous real
   bar.
3. Trace the BotSpot Node -> Bot Manager -> ECS run-task environment to explain
   why the Greg production backtest used cache `v1` despite Node source
   defaulting BotSpot Auto cache version to `v44`.
4. After code fixes, surgically invalidate only the affected production BTC
   cache objects proven bad. Do not bump the global production cache version.

## 2026-06-13 Local Execution-Path Guard In v4.5.52

The next local fix is intentionally independent of Interactive Brokers Crypto
Plus approval. Crypto Plus should improve 24/7 data availability, but LumiBot
must still fail honestly when the downloader/cache returns sparse intraday data.

Local v4.5.52 changes add a current execution-bar invariant for IBKR and
routed-IBKR intraday fills:

- `Data.get_quote()` now includes `bar_timestamp` and `bar_timestep` in
  `raw_data` so a quote fill can prove which source row supplied bid/ask.
- Direct IBKR fast bid/ask fills reject the selected row when the row timestamp
  is not in the current simulated minute/hour bucket.
- Quote-based market, marketable-limit, and fallback fills reject IBKR/routed
  IBKR quotes whose source `bar_timestamp` does not match the current simulated
  execution bucket.
- OHLC fill selection now applies the same current-bucket check for
  IBKR/routed-IBKR intraday data before reading `open/high/low/close`.

This is stricter than the earlier `Data.strict_end_check` tolerance. Generic
historical data reads may still use as-of semantics where that is expected, but
an execution fill cannot turn March 24 BTC data into an April 17 fill just
because both rows live inside the same cache object. If the current minute/hour
is missing, the order remains unfilled or the quote path returns `None`.

Local test evidence:

- `python3 -m pytest -q tests/test_data_entity.py tests/test_backtesting_broker.py`
  returned `39 passed`.
- `python3 -m pytest -q tests/test_ibkr_crypto_backtesting_smoke_stubbed.py tests/test_interactive_brokers_rest_backtesting_unit.py`
  returned `15 passed`.
- `python3 -m pytest -q tests/backtest/test_backtesting_broker_processing.py tests/backtest/test_quote_fill_fallback.py`
  returned `46 passed`.
- Combined targeted run across those six files returned `100 passed`.

One existing IBKR crypto OCO/OTO smoke fixture had only bars through `00:01`
while the test clock advanced to `00:02`; it was updated to include the real
`00:02` bar. The old fixture was implicitly depending on stale `00:01` data.

## 2026-06-13 Full Greg Window Replay And Market-Order Lifecycle Fix

Rob asked for a local rerun of the original Greg window to prove the current
v4.5.52 execution guard does not grab wrong-date BTC bars. Two local full-window
replays were run against Greg's executed production code:

- strategy code:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/executed-code/main.py`
- original window: `2026-03-09` through `2026-06-05`
- data source: router
- cache mode: read-only
- cache bucket/prefix/version: `lumibot-cache-dev`, `dev/cache`, `v1`
- production Data Downloader credentials were loaded from the approved local
  env files, but raw credential values were not logged or documented.

First v4.5.52 replay after the current-bar guard:

- artifact root:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260613/local-v4552-full-mar09-jun05`
- result: exit code `0`, elapsed `972.2s`
- proof the wrong-date fill path was blocked:
  - `strict start check rejected future frame`: `4,058`
  - `resolved to stale hour data`: `6,204`
  - `[FILL][REJECT]`: `1,534,550`
  - every parsed `[FILL][REJECT]` selected the same stale minute bar,
    `2026-03-27 15:59:00-04:00`, while the simulation clock was between
    `2026-04-01 04:00:00-04:00` and `2026-05-31 02:55:00-04:00`
  - old stale price strings `70511.75`, `70510.75`, and `70512.75` did not
    appear in the new trade output.

That proved the original wrong-date price selection was blocked, but it exposed
a second bug: market orders rejected for missing current data were left open.
Seventy market orders submitted between April 1 and May 25 later filled together
on `2026-05-31 03:00:00-04:00` once a valid current BTC minute bar existed.
Those fills used a matching May 31 bar, not March data, but the lifecycle was
still wrong because market orders must not wait days or weeks for future data.

The local follow-up fix changes strict IBKR/routed-IBKR execution paths so a
market order is canceled when current execution data is unavailable. Non-market
orders remain open because limit/stop orders can legitimately wait for future
trigger conditions.

Second v4.5.52 replay after the market-order lifecycle fix:

- artifact root:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260613/local-v4552-full-mar09-jun05-market-cancel`
- result: exit code `0`, elapsed `617.7s`
- trade events: `176` rows total
  - `new`: `88`
  - `canceled`: `84`
  - `fill`: `4`
- filled rows:
  - `2026-03-23 12:00:00-04:00`, buy BTC at `70100.75`
  - `2026-03-23 17:00:00-04:00`, sell BTC at `70888.00`
  - `2026-03-25 11:00:00-04:00`, buy BTC at `71219.75`
  - `2026-03-26 07:00:00-04:00`, sell BTC at `69481.00`
- all filled rows had `fill time == audit.bar.datetime` with max absolute
  mismatch `0.0` seconds.
- inferred submit-to-fill delay for filled rows had max `0.0` days.
- `[FILL][CANCEL]`: `280` log lines, representing `70` unique market orders
  canceled when the selected execution bar was stale.
- `[FILL][REJECT]`: `0`
- stale May 31 mass-fill timestamp `2026-05-31 03:00:00-04:00`: `0`
- old stale price strings `70511.75`, `70510.75`, and `70512.75`: `0`
- final stats:
  - starting portfolio value: `1300.0`
  - ending portfolio value: `1195.363451301118`
  - minimum portfolio value: `1126.1195532480024`
  - tearsheet total return: `-8%`

Additional local test evidence after the lifecycle fix:

```bash
python3 -m pytest -q tests/test_backtesting_broker.py tests/test_data_entity.py tests/test_ibkr_crypto_backtesting_smoke_stubbed.py tests/test_interactive_brokers_rest_backtesting_unit.py tests/backtest/test_backtesting_broker_processing.py tests/backtest/test_quote_fill_fallback.py
```

Result: `101 passed, 1 warning`.

Current conclusion: local v4.5.52 now prevents the stale/wrong-date BTC price
from being used for execution fills, and it also prevents market orders from
surviving missing-data gaps until a future bar appears. Missing data is still
present and still needs the separate Data Downloader/cache fix, but the backtest
no longer turns missing BTC minutes into either March-price April fills or
weeks-late market fills.

## 2026-06-14 Review: Market-Order Lifecycle Scope And BTC Availability

Rob challenged the local market-order lifecycle change because thinly traded
assets can legitimately have no fresh last-trade bar for a while. That challenge
is valid. The current local `version/4.5.52` follow-up cancellation is too broad
to call release-ready:

- The safe invariant is universal: execution fills must not use a bar from the
  wrong minute/hour/day bucket. A March BTC row must never fill an April order.
- The risky part is the follow-up market-order cancellation behavior. As written,
  `_requires_current_execution_bar()` applies to intraday direct IBKR and routed
  IBKR paths generally, not only BTC/crypto. That can affect sparse options or
  thin equities where the correct model is usually quote/NBBO-led execution or a
  bounded pending order, not immediate cancellation just because a fresh trade
  print is missing.
- Before release, the lifecycle fix should be narrowed/redesigned. Keep the
  current-bar guard, but do not treat "no current trade bar" as an unconditional
  cancellation rule for all intraday IBKR assets. Add regression coverage for
  options and thin equities before using this as a generic order lifecycle rule.

Live production Data Downloader probe on 2026-06-14 used the queued `/ibkr/*`
path, not a local IBKR login. Evidence artifact:

- `/Users/robertgrzesik/Development/support-artifacts/greg-btc-data-check-20260614/downloader_btc_probe_summary.json`

Results:

- `/healthz` returned HTTP 200 with IBKR enabled/authenticated.
- `/version` reported `0fb287a15e17086edb25a5e5cc2329228484ad11`.
- Original April closed-weekend request
  (`BTC`, conid `541686651`, `Trades`, `1min`, period `1w`, end
  `2026-04-19T00:00:00Z`) returned `classification=complete`,
  `cache_write_policy=allow`, `rebuild_count=1`, 7,960 rows, first row
  `2026-04-12T07:00:00Z`, last row `2026-04-17T19:59:00Z`. That matches the
  Crypto Basic Friday close handling.
- April after-Sunday-reopen request ending `2026-04-19T08:00:00Z` returned
  `classification=complete`, `cache_write_policy=allow`, `rebuild_count=1`,
  7,963 rows, first row `2026-04-12T08:00:00Z`, last row
  `2026-04-19T07:58:00Z`.
- Recent Saturday request ending `2026-06-13T16:00:00Z` returned only 240 rows,
  `2026-06-12T16:00:00Z` through `2026-06-12T19:59:00Z`, with
  `classification=complete` / `cache_write_policy=allow`. That still looks like
  Crypto Basic behavior, not Crypto Plus 24/7 Saturday trading.
- Current one-week request ending `2026-06-14T16:30:41Z` returned
  `classification=complete`, `cache_write_policy=allow`, `rebuild_count=1`,
  7,961 rows, first row `2026-06-07T16:31:00Z`, last row
  `2026-06-14T16:29:00Z`. Current BTC minute data is available through the
  production downloader, but the account still appears to have Basic weekend
  behavior.

Read-only S3 object checks for the known BTC cache objects were attempted but
the current AWS identity received `AccessDenied`/`Forbidden` for list/head/get
operations on `lumibot-cache-prod/prod/cache/v1/ibkr/crypto/minute/bars/...`.
The live downloader probe is therefore the current evidence source for BTC data
availability.

## 2026-06-14 Broker/Backtester Semantics Review

Rob challenged the "cancel every market order when the current execution bar is
missing" approach. The broker/platform research supports that challenge.

Current Crypto Plus signal from the production-like queued Data Downloader path
on 2026-06-14 at `17:28Z`:

- `sat_midday_1d` (`BTC`, `ZEROHASH`, `Trades`, `1min`, end
  `2026-06-13T16:00:00Z`) returned only 240 payload rows from
  `2026-06-12T16:00:00Z` through `2026-06-12T19:59:00Z`.
- `sat_late_6h` ending `2026-06-14T03:00:00Z` returned 0 payload rows.
- `sun_after_reopen_6h` ending `2026-06-14T16:00:00Z` returned 359 rows from
  `2026-06-14T10:00:00Z` through `2026-06-14T15:58:00Z`.

That still looks like Basic Crypto weekend behavior on the production downloader
account, not full Crypto Plus 24/7 Saturday access.

Broker/platform references:

- IBKR defines a market order as buying or selling at the bid/offer currently
  available, without a guaranteed execution price:
  https://www.ibkrguides.com/traderworkstation/order-types.htm
- SEC/Investor.gov says market orders generally execute near the current bid
  for sells or ask for buys, and warns that the last-traded price is not
  necessarily the execution price:
  https://www.investor.gov/introduction-investing/investing-basics/how-stock-markets-work/types-orders
- FINRA says market orders generally execute near the current bid/ask during
  regular hours, and FINRA Rule 5310 requires firms to make every effort to
  execute marketable customer orders fully and promptly:
  https://www.finra.org/investors/investing/investment-products/stocks/order-types
  and https://www.finra.org/rules-guidance/rulebooks/finra-rules/5310
- IBKR Time in Force docs say day orders are canceled at the market close if
  unexecuted, while IOC cancels any portion that does not fill immediately:
  https://www.ibkrguides.com/traderworkstation/time-in-force-columns.htm
  and https://www.interactivebrokers.com/campus/glossary-terms/immediate-or-cancel-order-ioc/
- IBKR historical data supports `TRADES`, `MIDPOINT`, `BID`, `ASK`, and
  `BID_ASK` by product type, including stocks, options, futures, ETFs, and
  cryptocurrency. This proves IBKR can provide historical quote-like data, but
  our Client Portal history payload is still candlestick-shaped, not a true
  tick-by-tick NBBO feed:
  https://interactivebrokers.github.io/tws-api/historical_bars.html
- QuantConnect documents that market orders can take a few minutes to fill for
  illiquid assets such as out-of-the-money options and penny stocks, and that
  stale fills are a realism problem:
  https://www.quantconnect.com/docs/v2/writing-algorithms/trading-and-orders/order-types/market-orders
  and https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/trade-fills/key-concepts
- Backtrader models market orders as filling at the next available price; in
  bar backtests, that is the next bar open:
  https://www.backtrader.com/docu/order/

Recommended model:

1. Keep the universal invariant: a fill must never use a timestamp that does not
   belong to the current or next executable market event. A March/May BTC row can
   never price an April order.
2. Prefer current quote data over last-trade data for marketable execution. Buy
   market orders should use ask-side data; sell market orders should use bid-side
   data. Last trade is fallback evidence, not the primary broker-like market
   price for thin symbols.
3. If no executable quote/trade exists at the order time but the order is still
   valid, keep it open/pending and evaluate it against future data events instead
   of immediately canceling it.
4. Enforce time-in-force and session boundaries:
   - `ioc`: fill immediately in whole/part, then cancel unfilled quantity.
   - `day`: keep pending only until that asset/session's close, then expire.
   - `gtc`/`gtd`: may remain pending across sessions, but must still fill only
     from a future executable event and should emit data-gap warnings for long
     waits.
5. For known liquid/continuous assets such as BTC, large missing periods are a
   data/entitlement/cache failure, not normal illiquidity. The backtest should
   surface that as data-quality evidence instead of fabricating a fill.

Conclusion: the current v4.5.52 broad market-order cancellation is too blunt.
The correct fix is a pending-order lifecycle with strict timestamp guards,
quote-first fills, TIF/session expiration, and data-quality reporting for gaps.

## 2026-06-14 Feasibility Review: Broker-Like Pending Orders Without Lookahead

Status: research-only; no deployment approval. This section records the
recommended direction after Rob challenged whether the market-order cancellation
patch would break thinly traded assets.

The proposed broker-like model is feasible in LumiBot, but the local v4.5.52
implementation is not the right shape yet.

What the current code already supports:

- `Order` has the required lifecycle fields: `time_in_force` defaults to `day`
  and documents `day`, `gtc`, `gtd`, and `ioc`; `EXPIRED` is a terminal status.
- `BacktestingBroker.process_pending_orders()` is already the central place
  where active orders are retried each simulated bar/event.
- Quote-first fills already exist: market buys can fill from ask, market sells
  from bid, and marketable limits can fill from bid/ask before falling back to
  OHLC.
- Quote and fast bid/ask fill paths now have current-execution-bar checks via
  `bar_timestamp` / `bar_timestep` and `_execution_bar_matches_datetime(...)`.
- `docs/BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md` already states the correct
  high-level policy: no synthetic intraday bars; fills require actionable OHLC
  or bid/ask; session gaps mean no fills and pending orders wait for the next
  available bar/quote event.

What is still wrong or incomplete:

- `_cancel_market_order_with_unavailable_execution_data(...)` is too broad. A
  missing current trade bar is not the same thing as a canceled market order for
  options, penny stocks, futures gaps, or other sparse products.
- The Pandas/IBKR fill path still has a legacy `df_original.iloc[-1:]` fallback
  after an empty selected frame. Any execution path that reaches this fallback
  risks reintroducing arbitrary stale/future-row fills.
- Current quote timestamp validation protects quote fills, but order lifecycle
  semantics are still implicit. There is no central "not executable yet, but
  still valid until TIF/session expiry" decision point.
- Data continuity and execution eligibility are mixed together. Forward-filled
  series can be useful for indicators or mark-to-market, but execution fills
  need source-row timestamp proof.

Broker/backtester semantics from external references:

- Investor.gov and FINRA describe market orders as generally executing near the
  current bid/ask, while warning that last trade is not necessarily the
  execution price.
- FINRA Rule 5310 explicitly discusses limited-quotation securities and says
  firms still need reasonable diligence when pricing information is limited.
- IBKR historical data can expose `TRADES`, `BID`, `ASK`, `BID_ASK`, and
  `MIDPOINT` as candlestick-style bars by product type. This supports a
  quote-first model, but it is not a promise that every current backtest path has
  true tick-level NBBO.
- IBKR documents IOC as canceling any portion not filled immediately, while day
  orders expire at the close of the trading day.
- QuantConnect documents that market orders can take minutes for illiquid
  options or penny stocks, and also treats stale fills as a realism problem.
- Backtrader models market orders in bar backtests as the next available price,
  usually the next bar open.
- IBKR documents Crypto Basic as Sunday 3 AM ET through Friday 4 PM ET, while
  Crypto Plus provides 24/7 crypto trading. The production downloader still
  looked like Basic Crypto on the 2026-06-14 checks.

Recommended implementation direction:

1. Remove or redesign the broad market-order cancellation helper. Keep strict
   timestamp validation, but change "no executable data now" into "leave pending
   if the order is still valid."
2. Add one central backtesting order-lifecycle helper that decides:
   - current executable price exists -> fill;
   - `ioc` and no executable price -> cancel unfilled quantity;
   - `day` and before asset/session close -> keep pending;
   - `day` at/after asset/session close -> expire;
   - `gtc`/`gtd` -> keep pending until canceled or GTD expiry;
   - option orders/positions cannot survive option expiration.
3. Preserve broker-like price priority:
   - market buy -> current actionable ask when valid;
   - market sell -> current actionable bid when valid;
   - marketable limit -> bid/ask if current and valid;
   - OHLC open fallback only when the selected source bar belongs to the current
     or next executable simulated event.
4. Delete or quarantine arbitrary "best data we have" execution fallbacks. A
   source row whose timestamp cannot be mapped to the simulated event is not
   executable data.
5. Add data-quality diagnostics for continuous/liquid assets. For BTC, missing
   Saturday/overnight windows should be surfaced as entitlement/data/cache
   coverage evidence, not treated as normal thin-market behavior.
6. Verify with a matrix before release:
   - stale March/May BTC row cannot fill an April order;
   - same-minute BTC quote can fill when bid/ask are valid;
   - Basic Crypto weekend gap does not fabricate a fill;
   - Crypto Plus, once active, returns Saturday BTC rows through the production
     downloader and the backtest fills only from matching source timestamps;
   - thin stock with no current print can fill at the next valid same-session
     trade/quote event;
   - thin option with quote but no trade fills from bid/ask;
   - thin option with neither quote nor trade expires at day/session close;
   - IOC with no executable data cancels immediately;
   - GTC/GTD can carry forward but only fills from a future event after the
     simulation clock reaches that event.

Bottom line: yes, broker-like delayed fills are possible here. The fix must be a
real pending-order/TIF lifecycle plus timestamp-proven price selection, not an
asset-specific band-aid and not blanket cancellation.

## 2026-06-15 Read-Only Follow-Up

Rob asked whether the proposed TIF/order-lifecycle pieces already exist and
whether quote-first execution would create new IBKR performance risk.

Findings:

- Time-in-force is already part of the public order model and live broker
  adapters. `Order` stores `time_in_force` and `good_till_date`, documents
  `day`, `gtc`, `gtd`, and `ioc`, and has an `EXPIRED` status. IBKR, Alpaca,
  Schwab, and Tradier adapters pass TIF/duration through for live orders. Do not
  add parallel TIF fields.
- Backtesting still does not appear to centrally enforce TIF expiry. The
  `BacktestingBroker.process_pending_orders()` loop has a `todo valid date`
  comment at the validity check, and `time_in_force` is currently visible in
  audit fields rather than lifecycle enforcement.
- The target is not a global ban on `iloc[-1]` or forward-fill behavior. Those
  patterns can be valid for indicators, historical lookbacks, and mark-to-market.
  The dangerous path is execution pricing when the selected source row timestamp
  cannot be proven current or otherwise valid for the simulated executable event.
- Quote-first execution already exists when bid/ask is available and current.
  It should not be expanded into unconditional extra IBKR quote/history fetches
  without a benchmark. Current IBKR futures docs intentionally disable
  `Bid_Ask`/`Midpoint` derivation by default because it multiplies request volume
  and reintroduces Client Portal history flakiness.
- Current Crypto Plus proof is blocked by Data Downloader session health. On
  2026-06-15 around 20:53 UTC, production `/healthz` reported IBKR
  `connected=true` but `authenticated=false` with `auth_status_http=401`. Three
  fresh BTC Saturday/Sunday production Data Downloader probes timed out after
  45 seconds each. Those timeouts are not valid entitlement proof; the latest
  clean evidence remains the 2026-06-14 probe, which looked like Crypto Basic
  weekend behavior.

## 2026-06-15 Account-Path Follow-Up

Rob's later IBKR screenshots showed Crypto Plus active on live account
`U6750594`, with paper account `DU4299039` and paper username `rgrze4067`.
Production Data Downloader checks then showed:

- Client Portal/IBeam is authenticated as `rgrze4067`.
- The only Client Portal account exposed to the downloader is `DU4299039`.
- The account is reported by CPAPI as `type=DEMO`.
- A BTC/USD ZEROHASH Trades 1-day/1-minute request ending
  `2026-06-13 16:00:00 UTC` returned only 240 rows from
  `2026-06-12 16:00:00 UTC` through `2026-06-12 19:59:00 UTC`, with no Saturday
  rows.

That proves the deployed paper Client Portal session still behaves like Crypto
Basic for history, even though the live account UI shows Crypto Plus. It does
not prove Crypto Plus is unavailable on the live account. It means the account
path and entitlement behavior must be resolved in Data Downloader before using
weekend BTC data as a LumiBot correctness signal.

Independent LumiBot conclusion: missing current execution data should not
cancel a market order by itself. The current helper
`_cancel_market_order_with_unavailable_execution_data()` is too aggressive for
thin assets, closed sessions, and temporary no-print/no-quote windows. The
correct model is:

- reject stale/out-of-window rows as executable prices;
- keep eligible orders pending when no current actionable data exists;
- expire/cancel only because the order lifecycle says so, e.g. IOC immediately,
  DAY at the relevant session/day boundary, GTD at `good_till_date`, or explicit
  strategy/user cancellation;
- prove every fill with a source timestamp that belongs to the simulated
  execution event, not merely the surrounding requested history window.

Local `version/4.5.52` update after this review:

- Replaced the local cancellation helper with
  `_defer_market_order_with_unavailable_execution_data()`.
- Missing current execution data now logs `[FILL][PENDING]` and leaves active
  market orders working instead of calling `cancel_order()`.
- Focused validation:
  - `python3 -m pytest tests/test_backtesting_broker.py -q` -> `32 passed`
  - `python3 -m pytest tests/test_interactive_brokers_rest_backtesting_unit.py tests/test_routed_backtesting_ibkr_daily_prefetch.py tests/test_ibkr_crypto_backtesting_smoke_stubbed.py -q` -> `24 passed`

This is still not release-ready as a full order-lifecycle solution until central
TIF/session expiry is added and tested separately.

## 2026-06-18 Local v4.5.52 Order-Lifecycle Matrix

Rob asked for the fix to be proven with durable LumiBot tests before changing
more logic. The local `version/4.5.52` implementation now keeps the strict
execution timestamp invariant while moving missing-data handling into a central
order-lifecycle helper.

Code-level change:

- Removed the old "best data we have" execution fallback where an empty
  execution frame could fall back to `df_original.iloc[-1:]`.
- Replaced the broad market-order cancellation helper with
  `_handle_unavailable_execution_data(...)`.
- `IOC` and `FOK` orders cancel immediately when no current executable data is
  available.
- `DAY` / `GFD` orders may remain pending while the same trading date/session is
  still valid, and cancel when the order reaches the next trading date or the
  session is closed.
- `GTD` orders cancel once the simulated time reaches `good_till_date`.
- A hard pre-fill expiry check cancels `GTD` orders whose `good_till_date` has
  elapsed and `DAY` / `GFD` orders that have crossed into a later trading date
  before any quote/OHLC price lookup can fill them.
- `GTC` orders can remain pending when there is no executable data, but still
  cannot fill from stale or future source rows.
- Existing quote-first behavior is preserved: if a thin option has a current
  actionable quote but no current trade bar, the market order can still fill
  from the quote.

New durable regression file:

- `/Users/robertgrzesik/Development/lumibot/tests/test_backtesting_order_lifecycle.py`

The test matrix currently covers:

- DAY market order waits when the current execution bar is missing but the same
  trading session/date is still active.
- IOC/FOK orders cancel when no current executable data exists.
- DAY orders cancel after session close without fabricating a fill.
- DAY orders cancel on the next trading date when they never received current
  executable data.
- DAY orders that crossed into a later trading date are canceled before any
  next-date valid bar can fill them.
- Same-date DAY orders are still allowed to evaluate a final session bar when
  valid data exists.
- GTD orders cancel at `good_till_date` and remain pending before that time.
- GTD orders that reached `good_till_date` are canceled before any later valid
  bar can fill them.
- Thin option market orders can fill from current quote data even when no trade
  bar exists.
- Stale quote data does not fill an intraday IBKR market order.

Focused validation on 2026-06-18:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 300s python3 -m pytest tests/test_backtesting_order_lifecycle.py tests/test_backtesting_broker.py tests/test_ibkr_crypto_backtesting_smoke_stubbed.py -q
```

Result:

- `50 passed`
- one third-party `websockets.legacy` deprecation warning

Broader broker/final-bar/quote fallback validation:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 300s python3 -m pytest tests/backtest/test_backtesting_broker_processing.py tests/backtest/test_strategy_executor_backtest_end_clamp.py tests/backtest/test_quote_fill_fallback.py -q
```

Result:

- `47 passed`
- one third-party `websockets.legacy` deprecation warning

Important scope note: this is still local proof only. No LumiBot release or
BotManager/BotSpot deployment was performed from this work. The purpose of this
change is to prove the behavior locally before any release decision.

## 2026-06-18 Greg Full-Window Replay, Production Cache Read-Only

A full original-window replay was run locally against Greg's executed
production `main.py` using production-like settings:

- strategy code:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/executed-code/main.py`
- original window: `2026-03-09` through `2026-06-05`
- data source router:
  `{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}`
- production Data Downloader env loaded from the approved local prod-like env
  file, without documenting raw secrets
- production S3 cache target: `lumibot-cache-prod/prod/cache/v1`
- cache mode: `readonly`
- local artifact root:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_full_prodcache_readonly_20260618`

Terminal result:

- The guarded command timed out after 40 minutes with exit code `124`.
- The process did not enter the simulation/progress loop. `progress.csv`
  contains only the header and the initial `0.00%` row.
- No terminal stats, trades CSV, trade-events artifact, or completed backtest
  result was produced.
- The local cache folder remained effectively empty (`4.0K`), so the replay did
  not get a useful warm load from `lumibot-cache-prod/prod/cache/v1`.
- The run submitted `342` production Data Downloader queue requests and
  received `340` completed results before timeout.
- The log includes two `classification=partial cache_write_policy=deny`
  responses for BTC history, confirming that partial/stale BTC history was not
  cacheable in this path.
- The last logged data-hydration request before timeout was still only around
  `startTime=20260501-11:20:00` for
  `BTC` / `ZEROHASH` / `Trades` / `1min`, short of the June 5 end of Greg's
  original window.
- Search of the timed-out replay log found no `70511`, `70510`, or `70512`
  fills, but this is not end-to-end proof because the replay never reached
  order execution.

Evidence:

- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_full_prodcache_readonly_20260618/subprocess.log`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_full_prodcache_readonly_20260618/logs/progress.csv`
- `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/cache/greg_full_prodcache_readonly_20260618`

Conclusion: the local `v4.5.52` order-lifecycle tests prove the stale/future
fill behavior in isolation, but the full original Greg replay with production
cache read-only is currently blocked by BTC data hydration/cache coverage before
it can prove the final customer backtest result end-to-end. The next production-
like proof should either use a verified complete BTC cache prefix for the full
window or run a narrower Greg window that reaches the previously bad April 15-17
fill region without spending the entire timeout on cold BTC hydration.
