# TQQQ Stock Split Position Accounting RCA

One-line description: Root cause, fix, and validation for the TQQQ 2025-11-20 split causing a false local/dev backtest equity drop.
Created: 2026-07-01
Last Updated: 2026-07-02
Status: Fixed locally on branch `version/4.5.64`; production release still required
Audience: Engineering (Backtesting, BotSpot Auto, Data Routing)

## Overview

Backtest `22f6c3fd-90e0-4c03-a9d5-0a9a6cc994c3` for strategy `TQQQ 200-Day Trend Bot` dropped roughly in half around the real TQQQ 2:1 split effective before market open on 2025-11-20. The bad run was local/dev, used BotSpot Auto routing with stock/index data routed to IBKR/Data Downloader, and used S3 cache hydration.

The final root cause was not a React/chart problem. It was also not simply "LumiBot never handled splits." Past TQQQ 200-day style backtests did work. The failing local/dev cache contained a mixed-adjustment IBKR daily TQQQ series:

- Older split rows, including 2021-01-21 and 2022-01-13, were already continuous/split-adjusted.
- The local/dev cache segment from about 2024-01-31 through 2025-11-19 was raw by a factor of 2.
- The 2025-11-20 split row and later rows were back in the post-split price level.

That mixed cache made the 2025 split look like a raw price halving while the held position was not doubled. A first ledger-only fix removed that 2025 crash but wrongly doubled positions on the already-adjusted 2021 and 2022 split rows, creating a false 60% CAGR result. The durable fix normalizes IBKR stock/day bars into split-adjusted price space before backtesting, marks those frames as `_split_adjusted`, and keeps the generic position-ledger split path only for genuinely raw/unadjusted frames.

The remaining dev/prod result gap is a separate cache-quality issue. The prod IBKR TQQQ object is continuous and matches Yahoo closely. The dev/local TQQQ object was not a mirror of prod: it had fewer rows, missing split events, a 2016-03-31 to 2019-02-04 hole, and non-split level spikes. That explains why the fixed dev replay landed around 29.5% CAGR while prod IBKR and Yahoo landed around 38% CAGR.

## What Failed

Bad local/dev replay before the fix:

- Replay folder: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-prodlike-10k/run/`
- 2025-11-19: held `3012` TQQQ, close about `100.05`
- 2025-11-20: still held `3012` TQQQ, close about `46.45`
- 2025-11-20 one-step equity move: about `-52.83%`
- 2025-11-21: sold only `3012` shares at about `46.85`
- Final value: about `$145,217.79`
- Independent CAGR: about `30.69%`
- Independent max drawdown: about `-60.78%`

The `30.69%` CAGR was not comforting because the max drawdown and split-window rows proved the path was mathematically wrong.

## Why The 60% Result Was Wrong

The first local fix added broker-ledger stock split accounting. That was directionally useful for raw data, but it was incomplete for IBKR cache reality.

After that first fix, the same strategy replay produced:

- Replay folder: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-after-fix6-10k/run/`
- Final value: about `$1,150,153.82`
- Independent CAGR: about `60.75%`
- Max drawdown: about `-47.89%`

That result was false. It double-counted earlier TQQQ splits:

- 2021-01-21: position doubled even though the price series was already continuous (`24.75` to `25.36` around the split).
- 2022-01-13: position doubled even though the price series was already continuous (`38.17` to `35.40` around the split).
- 2025-11-20: position doubled correctly for the local raw tail, but by then the earlier false doublings had already inflated the account.

This is why a 60% CAGR was out of range compared with past TQQQ 200-day results.

## Cache Evidence

The relevant IBKR daily TQQQ cache objects were copied and inspected locally:

- Dev cache copy: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-comparison/dev_stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet`
- Prod cache copy: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-comparison/prod_stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet`

Observed dev/prod difference:

| Date | Dev close | Prod close | Interpretation |
| --- | ---: | ---: | --- |
| 2024-01-30 | `27.97` | `27.97` | Same adjusted level |
| 2024-01-31 | `52.64` | `26.32` | Dev cache raw segment begins |
| 2025-01-02 | `78.63` | `39.32` | Dev remains roughly 2x prod |
| 2025-11-19 | `100.05` | `50.03` | Dev remains raw pre-split |
| 2025-11-20 | `46.45` | `46.45` | Dev returns to post-split level |

Both dev and prod were already continuous through older splits:

| Date | Dev close | Prod close | Split |
| --- | ---: | ---: | ---: |
| 2021-01-20 | `24.75` | `24.75` | `0` |
| 2021-01-21 | `25.36` | `25.36` | `2` |
| 2022-01-12 | `38.17` | `38.17` | `0` |
| 2022-01-13 | `35.40` | `35.40` | `2` |

This proves the regression was a mixed-adjustment cache shape, not a simple "all IBKR bars are raw" rule.

The full dev/prod S3 stock/day cache scan is stored here:

- JSON scan: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-scan/ibkr_stock_day_cache_scan.json`
- Interesting subset: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-scan/ibkr_stock_day_cache_scan_interesting.json`
- Summary CSV: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-scan/ibkr_stock_day_cache_scan_summary.csv`
- Dev/prod comparison CSV: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-scan/ibkr_stock_day_cache_dev_prod_comparison.csv`

Cache namespace inventory at scan time:

| Namespace | Stock/day objects | Stock/day bytes | Index/day objects | Index/day bytes |
| --- | ---: | ---: | ---: | ---: |
| Dev S3 prefix `dev/cache/v1/ibkr/...` | `126` | `8,772,815` | `2` | `116,177` |
| Prod S3 prefix `prod/cache/v1/ibkr/...` | `340` | `23,894,640` | `4` | `335,058` |

The TQQQ cache comparison was especially clear:

| Field | Dev | Prod |
| --- | ---: | ---: |
| Rows | `3114` | `4115` |
| First row | `2011-04-04` | `2010-02-22` |
| Last row | `2026-06-29` | `2026-06-29` |
| Split events | `5` | `8` |
| Gaps > 7 days | `1` | `0` |
| Large non-split jumps | `3` | `0` |

The largest dev TQQQ gap was `2016-03-31` to `2019-02-04` (`1040` days). Dev also missed TQQQ split events that existed in prod (`2011-02-25`, `2017-01-12`, and `2018-05-24`) and had a non-split `2022-11-01` close of `20.30` where prod had `10.15`. Those are not cash-accounting bugs. They are cache data quality and completeness differences.

Direct historical artifact inspection from production S3 was attempted but blocked by permissions: the Bot Manager profile could list the target object names but could not decrypt/copy those backtest artifacts due missing KMS decrypt permission, and the BotSpot artifact read path returned `Forbidden`. Production DB metrics and local cache/replay artifacts were used instead.

## Historical BotSpot Evidence

Production BotSpot database queries found recent TQQQ 200-day style backtests in the expected range. This supports the user report that these strategies were working recently.

Representative completed rows:

| Backtest ID | Created | Strategy | Window | CAGR | Max DD | Total Return |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `9000f410-8316-4882-b9cd-64c876808f83` | 2026-06-29 | `TQQQ SMA 200 (Copy 2)` | 2025-01-01 to 2026-06-28 | `22.91%` | `-29.02%` | `35.68%` |
| `2891a903-3a77-4585-9198-6734d1398508` | 2026-05-22 | `TQQQ 200-Day Trend Follower` | 2025-04-01 to 2026-04-30 | `-5.62%` | `-23.96%` | `-6.04%` |

The reported local/dev backtest id and revision id were not present in production `backtest`, matching the report that the bad run was local/dev.

## Why Earlier Fixes Did Not Permanently Solve This

Prior split/corporate-action fixes were real but on adjacent surfaces:

- `acb83ca1` (`2025-12-17`): stabilized ThetaData split/dividend normalization and day-mode quotes.
- `b73ec373` (`2026-01-04`): added ThetaData intraday corporate action support.
- `47f73693` (`2026-03-02`): hardened IBKR stock/index routing and corporate-action enrichment so daily frames can contain `dividend` and `stock_splits`.
- `tests/test_ibkr_daily_split_spike_repair.py`: covered isolated split-like price spikes, but intentionally did not rewrite persistent level shifts.
- `86dc17e6` (`2026-07-01`): added generic backtest position-ledger split accounting, but by itself double-counted already-adjusted split rows in the mixed IBKR cache.

The missing permanent invariant was provider/data-shape aware:

1. If daily bars are raw/unadjusted, held stock quantity must be multiplied by the split ratio and average fill price divided by the split ratio.
2. If daily bars are already split-adjusted, the strategy fills and marks are already in current share units, so the ledger must not multiply shares again.
3. If an IBKR cache is mixed, LumiBot must first normalize the affected raw pre-split segment into split-adjusted price space, then mark the frame `_split_adjusted`.

The current fix adds that third invariant for IBKR stock/day cache data.

There was also an operational reason this surfaced in local/dev first. `BacktestCacheManager.on_local_update()` only uploads to S3 in `S3_READWRITE` mode, and several local reproduction runs used dev readwrite cache settings. The original local replay log shows `remote_cache.mode=readwrite`, bucket `lumibot-cache-dev`, prefix `dev/cache`, version `v1`, and `uploads=2`. A later normalization replay also wrote a normalized TQQQ object back into the dev namespace. For incident diagnosis, use S3 readonly first; dev readwrite can mutate the evidence while debugging.

## Fix

Implemented in `lumibot/tools/ibkr_helper.py`:

- After IBKR daily stock corporate-action enrichment, normalize equity daily prices for split boundaries before returning/cache-writing the frame.
- Detect whether each split row still has a raw split-level jump by comparing local close continuity against the split ratio.
- For mixed caches, scan backward to find the splice where the raw segment begins instead of adjusting the entire historical series.
- Adjust only the raw pre-split segment:
  - price columns (`open`, `high`, `low`, `close`, `bid`, `ask`, `last`, `vwap`) are divided by the split ratio;
  - `dividend` is divided by the split ratio;
  - `volume` is multiplied by the split ratio.
- Mark the resulting frame `_split_adjusted=True`.
- Keep the existing generic stock-position split ledger for raw/unadjusted providers.
- Preserve Yahoo and other already-adjusted providers because `DataSource.should_apply_stock_splits_to_positions()` skips ledger split multiplication when frames are `_split_adjusted` or the provider declares `AUTO_ADJUST_IMPLIES_SPLIT_ADJUSTED_PRICES`.

## Regression Tests

Existing split accounting coverage from `tests/test_stock_split_accounting.py` covers:

- Regular splits: 2:1, 3:1, 7:1, 10:1.
- Reverse splits: 1:2 (`0.5`) and 1:10 (`0.1`).
- Short positions.
- Basis adjustment.
- Idempotent repeated calls on the same split date.
- Invalid ratios: zero, one, negative, NaN, infinity, `None`, and non-numeric strings.
- Options are not adjusted by the stock-position path.
- `stock_splits` survives `Data` repair/slicing and is not forward-filled.
- Same-date/pre-open lookup for close-timestamped daily bars.
- Pre-close valuation uses split-adjusted previous close instead of stale pre-split price.
- Daily backtest hold-through-split sells the adjusted quantity and does not create a false 50% equity drop.
- Split-adjusted frames and provider-declared auto-adjusted data sources do not apply a second ledger split.

New IBKR normalization coverage in `tests/backtest/test_ibkr_equity_actions.py` covers:

- Raw 2:1 forward split normalization.
- Raw reverse split normalization.
- Already-adjusted split rows are marked `_split_adjusted` without double-adjusting prices.
- The exact mixed TQQQ cache shape: 2021/2022 already continuous, 2025 raw tail. The test asserts 2021 and 2022 are not halved again, while 2025-11-19 is normalized from `100.05` to `50.025`.
- The old-dev-cache class where a non-split spike remains visible for cache-quality audits while only the persistent raw pre-split tail is adjusted.
- The actual IBKR `get_price_data()` cached daily stock path: cached parquet in, normalized frame out, and persisted `_split_adjusted` marker.
- A deterministic daily TQQQ backtest that buys before the 2025-11-20 split, holds through it, sells after it, emits no split double-count event on adjusted data, and asserts the split-day portfolio move is a normal price move rather than a false 50% cliff.

These are ordinary pytest tests, not live downloader/S3 acceptance tests, so they should run in the normal LumiBot deployment gate. A live S3/read-only cache audit can be added on top, but the core split regression does not need network access to catch this class of bug.

Focused command run:

```bash
LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 300s python3 -m pytest tests/test_stock_split_accounting.py tests/test_ibkr_daily_split_spike_repair.py tests/backtest/test_ibkr_equity_actions.py tests/test_strategy_dividend_cash_batch.py tests/test_split_adjustment.py tests/test_backtesting_broker.py tests/backtest/test_pandas_backtest.py tests/backtest/test_yahoo.py tests/backtest/test_yahoo_helper_actions.py -q
# 111 passed, 1 warning
```

## Replay Validation

All replay commands used the saved revision file:

- Strategy file: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/revision-files/main.py`
- Strategy hash: `ea07dbb637c922c8aa6c4a3e3e1479b7ef3c0f282dffdfc4490fbd5171e66a6b`
- Window: 2016-03-04 to 2026-03-04
- Budget: `$10,000`

The IBKR reruns used BotSpot Auto style routing, Data Downloader settings loaded from the local BotSpot dotenv file, explicit `AWS_PROFILE=BotManager`, and S3 cache settings supplied via environment/script flags. The real downloader URL and credentials are intentionally not written into this document.

| Scenario | Folder | Final PV | Independent CAGR | Independent Max DD | Worst one-step EOD move | Split-window result |
| --- | --- | ---: | ---: | ---: | --- | --- |
| Bad local/dev before fix | `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-prodlike-10k/run/` | `$145,217.79` | `30.69%` | `-60.78%` | `-52.83%` on 2025-11-20 | Broken |
| Ledger-only intermediate | `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-after-fix6-10k/run/` | `$1,150,153.82` | `60.75%` | `-47.89%` | normal 2025 date, but false 2021/2022 jumps | Broken high |
| Dev IBKR cache after normalization | `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-after-normalize-dev-readonly-clean-10k/run/` | `$132,872.63` | `29.53%` | `-47.89%` | `-20.22%` on 2020-03-09 | Fixed |
| Prod IBKR cache after normalization | `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-after-normalize-prod-readonly-10k/run/` | `$267,230.90` | `38.90%` | `-48.20%` | `-20.42%` on 2020-03-09 | Fixed |
| Yahoo current code | `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-yahoo-current2-10k/run/` | `$253,478.59` | `38.17%` | `-48.57%` | `-16.90%` on 2020-03-02 | Fixed / unchanged |

Important split-window checks after the final fix:

| Scenario | 2021-01-21 | 2022-01-13 | 2025-11-20 |
| --- | --- | --- | --- |
| Dev IBKR fixed | Quantity stayed `1481`; return about `+2.49%` | Quantity stayed `1481`; return about `-7.30%` | Quantity stayed `2713`; return about `-7.04%` |
| Prod IBKR fixed | Quantity stayed `2978`; return about `+2.49%` | Quantity stayed `2978`; return about `-7.30%` | Quantity stayed `5457`; return about `-7.05%` |
| Yahoo current | Quantity stayed `5272` through 2025 split; no split/corporate ledger events | No split/corporate ledger events | 2025-11-20 move about `-1.30%`; no false halving |

The final IBKR and Yahoo runs emitted zero stock-split/corporate-action ledger events because their data frames were treated as split-adjusted. That is expected for these adjusted-price backtests and prevents double counting.

## Other Symbols And Cache Quality

The dev cache result (`29.53%`) is below prod IBKR (`38.90%`) and Yahoo (`38.17%`) because the inspected dev S3 TQQQ cache had broader completeness and quality differences. That is separate from the 2025 split crash:

- The split crash is fixed by normalization and verified by the 2021/2022/2025 split windows.
- The dev/prod result gap should be tracked as a dev-cache completeness issue, not as a split accounting or cash-ledger issue.

The scan also found that dev and prod are not mirrors for other common stock/day objects:

| Symbol | Dev rows | Prod rows | Row delta | Notable scan finding |
| --- | ---: | ---: | ---: | --- |
| `QQQ` | `1554` | `3990` | `-2436` | Dev starts in 2020; prod starts in 1982 |
| `SPY` | `4135` | `6009` | `-1874` | Dev starts in 2009; prod starts in 1981 |
| `AMZN` | `1263` | `2594` | `-1331` | Dev starts in 2021; prod starts in 2016 |
| `GOOG` | `1255` | `2509` | `-1254` | Dev starts in 2021; prod starts in 2016 |
| `SGOV` | `3` | `1298` | `-1295` | Dev object is effectively unusable for long windows |
| `TNA` | `1252` | `2929` | `-1677` | Dev is missing a split event that prod has |
| `TECL` | `1561` | `2929` | `-1368` | Prod scan flags one raw split-like boundary for follow-up |
| `UPRO` | `1734` | `2928` | `-1194` | Prod scan flags one large non-split jump for follow-up |
| `SQQQ` | `2540` | `1795` | `745` | Dev scan flags one large non-split jump |
| `O` | `1257` | `1257` | `0` | The split ratio is `1.032`; likely a special corporate-action style row, not a TQQQ-style 2:1 cliff |

Do not over-interpret every scan flag as a trading bug. Leveraged ETFs can have very large legitimate moves, small split ratios can represent spinoff/special-action data, and dev/prod row-count differences can be expected when dev cache is warmed by ad hoc tests. But dev cache should not be used as the canonical truth for long-horizon parity. For future incidents:

1. Reproduce with prod S3 readonly before mutating any cache.
2. Keep dev/local readwrite cache off until the evidence object has been copied or versioned.
3. Add a small S3 read-only cache audit gate for high-usage daily-stock symbols if the deployment pipeline can tolerate networked checks.
4. Prefer rebuilding or replacing bad dev IBKR stock/day objects from a known-good source over broad cache deletion.

If a future provider/cache omits `stock_splits` entirely, LumiBot still cannot safely infer true corporate actions from price moves alone without risking false positives. Provider enrichment and cache quality remain required.

## Cache Audit Results And Revised Game Plan

Additional read-only cache audit on 2026-07-02 changed the recommended next step. Do not add a per-request internal-gap scan to LumiBot's hot `get_price_data()` path unless a separate performance design proves it is safe. The next step is offline cache audit plus targeted cache deletion/refill for confirmed bad IBKR objects. Do not do row-level surgical repair unless delete/refill fails and a separate RCA proves the provider still returns bad data.

Audit artifacts:

- `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-audit-2026-07-02/ibkr_cache_audit_all.json`
- `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-audit-2026-07-02/ibkr_daily_stock_index_realrow_audit.json`
- `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-audit-2026-07-02/prod_v1_daily_flagged_yahoo_comparison.json`

Read-only namespace facts:

- Production BotSpot Node task definition `botspot-prod:130` explicitly injects `LUMIBOT_CACHE_S3_VERSION=v1`, `bucket=lumibot-cache-prod`, `prefix=prod/cache`, `mode=readwrite`, and Data Downloader URL `http://data-downloader.lumiwealth.com:8080`.
- BotSpot Node code defaults provider environments to `v44` when the env var is absent, but production overrides that default to `v1`.
- `prod/cache/v44` was empty at scan time; it is not the production market-data namespace for this incident.
- Active production `prod/cache/v1/ibkr/**/bars` contained `488` parquet bar objects.
- Dev `dev/cache/v1/ibkr/**/bars` contained `320` parquet bar objects.
- Dev `dev/cache/v44/ibkr/**/bars` contained `137` parquet bar objects.

Production findings:

- Non-daily production IBKR bar objects had no real-row structural flags in this audit: no real-row close NaNs, nonpositive closes, duplicate indexes, or non-monotonic indexes.
- Production TQQQ RTH (`prod/cache/v1/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet`) was clean and matched Yahoo closely. It is not the bad local/dev TQQQ object.
- Production daily stock/index audit found `45` placeholder-only objects. These are persisted no-data markers, not corrupt price rows. They should not be treated as repair targets unless a strategy actually needs those symbols/windows.
- Production daily stock/index audit produced `42` split-like or large-jump event rows for Yahoo comparison. `30` matched Yahoo close levels and are likely real market moves, not cache problems.
- Confirmed production delete/refill candidates:
  - `TECL`: `244` real rows from `2020-03-12` through `2021-03-01` are exactly `10x` Yahoo close, followed by the real `2021-03-02` 10:1 split row. This is a mixed split-adjustment cache segment.
  - `UPRO`: `270` real rows from `2020-03-12` through `2021-04-07` are exactly `2x` Yahoo close, then return to Yahoo level on `2021-04-08`. This is a mixed adjustment splice without a same-day split marker.
  - `SPXU`: one bad reverse-split date, `2021-01-21`; IBKR close `116.20`, Yahoo close `541.80`, Yahoo split `0.2`.
  - `OUST`: one bad reverse-split date, `2023-04-21`; IBKR close `0.40`, Yahoo close `3.72`, Yahoo split `0.1`.
- Needs manual review before any delete/refill:
  - `AMC`: `330` real rows from `2021-04-30` through `2022-08-19` sit at a constant `0.6198x` Yahoo close. This may be an AMC/APE special-action adjustment difference rather than a simple split bug.
  - `VIX9D`: differs from Yahoo on two jump dates; this may be index-close/vendor methodology rather than a cache splice.
  - `AMR` and one `APLD` action event could not be verified cleanly through Yahoo in the audit window.

Dev findings:

- `dev/cache/v1` has confirmed bad long-window objects:
  - `TQQQ` RTH: real internal gap `2016-03-31` to `2019-02-04`, plus non-split jumps around `2019-02-04` and `2022-11-01/02`.
  - `SQQQ` RTH: real internal gap `2020-04-01` to `2021-04-29`, with a corresponding level jump.
  - `SPY` RTH: real internal gap `2019-03-27` to `2020-04-16`.
- `dev/cache/v44` has clean TQQQ RTH around the 2025 split, but non-RTH TQQQ source variants (`AUTO_TRADES`, `AUTO_BID_ASK`, `AUTO_MIDPOINT`) contain duplicate dates and raw split-like jumps. Do not assume all source variants are clean because the RTH object is clean.

Revised plan:

1. Do not change the hot cache-read path for internal gap scanning right now.
   - The backtest cache is performance-sensitive.
   - Treat this as an offline audit and targeted invalidation problem unless a future design proves near-zero overhead.

2. Keep the LumiBot split-normalization fix.
   - The TQQQ local/dev chart cliff is still a real split-normalization bug.
   - The existing unit/backtest coverage should stay because it prevents double-counting adjusted providers like Yahoo and mixed IBKR split rows like TQQQ/TECL.

3. Build a reusable offline audit script.
   - Default mode: read-only audit, exactly like this pass.
   - Scope first: `ibkr/stock/day/bars` and `ibkr/index/day/bars`.
   - Output JSON/CSV with symbol, source variant, real-row spans, split markers, Yahoo comparison for flagged events, and delete/refill recommendation.
   - This can run before deployments or as an operations task without slowing every backtest.
   - Do not build a row-level repair script as the primary path. It is more complex than needed and risks preserving stale provider mistakes.

4. Delete/refill production confirmed targets.
   - Back up each target object first by copying it to a dated S3 backup prefix or local durable artifact path.
   - Start with `TECL`, `UPRO`, `SPXU`, and `OUST`.
   - Delete only those exact S3 objects, not broad `prod/cache/v1`.
   - Refill them by running a fixed LumiBot `4.5.64+` backtest or data-hydration job that requests those symbols through the normal IBKR/Data Downloader/S3 path.
   - Do not delete/refill `AMC`, `VIX9D`, `AMR`, or `APLD` until manually reviewed.
   - Do not delete or reset broad `prod/cache/v1`.

5. Delete/refill or retire bad dev objects.
   - Dev can be fixed more aggressively because it is lower-stakes, but still back up objects first if they are useful evidence.
   - Delete/refill `TQQQ`, `SQQQ`, and `SPY` in `dev/cache/v1`.
   - Delete/refill or remove dirty non-RTH TQQQ variants in `dev/cache/v44`.

6. Validate after refill.
   - Rerun the offline audit and require zero confirmed repair candidates for active production symbols.
   - Rerun the TQQQ 2016-03-04 to 2026-03-04 matrix: prod IBKR should remain close to Yahoo (`38.90%` CAGR / `-48.20%` max DD versus Yahoo `38.17%` / `-48.57%` in the local replay).
   - Add symbol-level validation for refilled objects: TECL, UPRO, SPXU, and OUST should match Yahoo close levels around the bad spans.

7. Release and deploy after cache plan is clear.
   - Follow `docs/DEPLOYMENT.md`: release LumiBot `4.5.64`, verify installability, then update Bot Manager.
   - Bot Manager currently pins `LUMIBOT_VERSION=4.5.63`; update after the LumiBot release is published.
   - Deploy Bot Manager dev first, run version/backtest canaries, then production.

## 2026-07-02 Release, Deploy, And Cache-Refill Execution

LumiBot `4.5.64` was released and verified.

- Release PR: `https://github.com/Lumiwealth/lumibot/pull/1102`
- Fix commit: `32ff0e14 Fix release-gate backtest regressions`
- Dev merge/tag commit: `b6367dad2a69b5bdf940003bf984d0d66cce1367`
- Tag: `v4.5.64`
- GitHub release: `https://github.com/Lumiwealth/lumibot/releases/tag/v4.5.64`
- PyPI install verification:
  - `python3 -m pip install --no-deps --target /tmp/lumibot-pip-verify-4.5.64 lumibot==4.5.64`
  - `PYTHONPATH=/tmp/lumibot-pip-verify-4.5.64 python3 -c "import lumibot; print(lumibot.__version__)"`
  - Output: `4.5.64`
- Local checkout was moved to `version/4.5.65` and `python3 scripts/verify_release_checkout_state.py --post-release-of 4.5.64` passed.

Bot Manager dev and production were both deployed with a forced image rebuild.

- Dev workflow run: `https://github.com/Lumiwealth/bot_manager/actions/runs/28577122393`
  - Logs showed `LUMIBOT_VERSION: 4.5.64`.
  - Build logs showed `uv pip install "lumibot==4.5.64"`.
  - Runtime logs showed `LumiBot v4.5.64 starting`.
- Production workflow run: `https://github.com/Lumiwealth/bot_manager/actions/runs/28580027661`
  - Logs showed `FORCE_REBUILD_IMAGES: true`.
  - Tests were enabled: `SKIP_UNIT_TESTS: false`, `SKIP_INTEGRATION_TESTS: false`, `RUN_INTEGRATION_TESTS: 1`.
  - Backtest image build logs showed `uv pip install ... "lumibot==4.5.64"` and `+ lumibot==4.5.64`.
  - Runtime logs showed `LumiBot v4.5.64 starting`.

The production TQQQ canary on fixed `4.5.64` was sane and matched the prod/Yahoo range.

- Bot ID: `direct-prod-tqqq-10k-4-5-64-s3-3e128f7c-ca20-425a-9a2a-2122cb9d9880`
- Artifacts: `/Users/robertgrzesik/Development/bot_manager/logs/tqqq-split-release-2026-07-02/prod-canary-10k/`
- `settings_json.lumibot_version`: `4.5.64`
- Budget: `$10,000`
- Remote cache: `s3://lumibot-cache-prod/prod/cache/v1`
- Data route: `{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"coinbase","crypto_future":"coinbase","future":"ibkr","cont_future":"ibkr"}`
- Final PV: `$285,205.63`
- Independent CAGR: `39.82%`
- Independent max drawdown: `-48.22%`
- Worst one-step EOD move: `-20.42%` on 2020-03-09
- 2025 split window was sane. Quantity stayed `5622` through 2025-11-19/20/21; there was no false 50% portfolio cliff.

Cache backup/delete/refill status is mixed and should not be represented as complete.

Backups were made before deletion:

- Local backup manifest: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/cache-remediation-2026-07-02/local-backups/manifest.tsv`
- S3 backup manifest: `/Users/robertgrzesik/Development/bot_manager/logs/tqqq-split-release-2026-07-02/cache-backup/s3-backup-allowed-prefix.tsv`
- Live-delete manifest: `/Users/robertgrzesik/Development/bot_manager/logs/tqqq-split-release-2026-07-02/cache-delete/deleted-live-objects.tsv`

Exact deleted targets were:

- Production `prod/cache/v1`: `TECL`, `UPRO`, `SPXU`, `OUST` RTH daily stock bars.
- Dev `dev/cache/v1`: `TQQQ`, `SQQQ`, `SPY` RTH daily stock bars.
- Dev `dev/cache/v44`: non-RTH `TQQQ` daily `AUTO_BID_ASK`, `AUTO_MIDPOINT`, and `AUTO_TRADES`.

Refill attempts did not successfully recreate the canonical cache objects:

- Exact-window prod refill:
  - Bot ID: `cache-refill-prod-v1-redo-4-5-64-0c9aec89-3f83-44e5-bb8a-93274fef055d`
  - Result: failed on `TECL`.
  - Relevant error: `IBKR history rebuild failed ... Chart data unavailable`, then `No daily bars returned for TECL`.
  - Evidence showed 2026/2021/2016 TECL chunks completed, then the older `2011-03-09` chunk failed. This indicates a brittle exact-window rebuild path around older/inception-era IBKR data, not inability to fetch TECL generally.
- Bounded prod refill:
  - Bot ID: `cache-refill-prod-v1-bounded-4-5-64-787a712e-0aa2-4268-a8a0-be8a7a0226df`
  - Window: `2026-03-03` to `2026-03-04`
  - Lookback: `2700` daily bars.
  - Result: failed on `OUST` with `No daily bars returned for OUST`.
  - Logs showed `TECL`, `UPRO`, and `SPXU` chunks completed. SPXU also exercised the `IBKR daily split-spike repair adjusted 1 row(s)` path under `4.5.64`.
  - Despite completed chunks, the four canonical production S3 objects were still missing after the failed run. That means this refill strategy path is not a reliable way to recreate the canonical S3 cache objects.

Because refill failed, the live objects were restored from S3 backups. Verification after restore:

- `prod/cache/v1/ibkr/stock/day/bars/stock_TECL_USD_day_AUTO_TRADES_RTH.parquet`: present, `142031` bytes.
- `prod/cache/v1/ibkr/stock/day/bars/stock_UPRO_USD_day_AUTO_TRADES_RTH.parquet`: present, `144068` bytes.
- `prod/cache/v1/ibkr/stock/day/bars/stock_SPXU_USD_day_AUTO_TRADES_RTH.parquet`: present, `85834` bytes.
- `prod/cache/v1/ibkr/stock/day/bars/stock_OUST_USD_day_AUTO_TRADES_RTH.parquet`: present, `60597` bytes.
- `dev/cache/v1/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet`: present, `130802` bytes.
- `dev/cache/v1/ibkr/stock/day/bars/stock_SQQQ_USD_day_AUTO_TRADES_RTH.parquet`: present, `139224` bytes.
- `dev/cache/v1/ibkr/stock/day/bars/stock_SPY_USD_day_AUTO_TRADES_RTH.parquet`: present, `151994` bytes.
- `dev/cache/v44/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_BID_ASK.parquet`: present, `34755` bytes.
- `dev/cache/v44/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_MIDPOINT.parquet`: present, `35008` bytes.
- `dev/cache/v44/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_TRADES.parquet`: present, `203090` bytes.

After cleanup:

- No production refill ECS tasks remained running.
- Data Downloader queue was clear: `pending_count=0`, `processing_count=0`, `failed_count=0`, `active_workers=0`.

Updated cache conclusion:

1. The LumiBot split normalization release and Bot Manager deployment are complete.
2. The prod TQQQ canary proves the original chart cliff is fixed on the deployed `4.5.64` runtime.
3. Cache deletion/refill is not complete. The deleted objects were restored because the refill path failed and did not recreate canonical S3 cache objects.
4. The next cache fix should be a dedicated Data Downloader/LumiBot cache-hydration fix, not manual row repair and not broad cache deletion.
5. The refill/hydration path needs tests proving:
   - a symbol with limited/inception-era history does not collapse a partially valid multi-window rebuild into an empty frame;
   - `Chart data unavailable` on an older no-data chunk is handled as an end-of-history condition when newer chunks succeeded;
   - successful chunk fetches write or merge into the exact canonical S3 cache key expected by managed backtests;
   - OUST-style short-history symbols produce usable recent-window daily bars instead of `No daily bars returned`.
