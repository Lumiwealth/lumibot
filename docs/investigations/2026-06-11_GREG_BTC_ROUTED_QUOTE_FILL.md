# Greg BTC Routed Quote Fill Investigation

One-line description: Production artifact diagnosis and LumiBot fix for Greg's BTC backtest drop caused by out-of-window IBKR crypto prices.
Last Updated: 2026-06-12
Status: Fixed in `v4.5.49` plus Data Downloader commit `0fb287a`; `v4.5.51` fixes a separate progress payload stall found during full-window reruns
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
