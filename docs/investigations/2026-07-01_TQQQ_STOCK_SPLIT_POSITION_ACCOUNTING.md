# TQQQ Stock Split Position Accounting RCA

One-line description: Root cause, fix, and validation for the TQQQ 2025-11-20 split causing a false local/dev backtest equity drop.
Created: 2026-07-01
Last Updated: 2026-07-01
Status: Fixed locally on branch `version/4.5.64`; production release still required
Audience: Engineering (Backtesting, BotSpot Auto, Data Routing)

## Overview

Backtest `22f6c3fd-90e0-4c03-a9d5-0a9a6cc994c3` for strategy `TQQQ 200-Day Trend Bot` dropped roughly in half around the real TQQQ 2:1 split effective before market open on 2025-11-20. The bad run was local/dev, used BotSpot Auto routing with stock/index data routed to IBKR/Data Downloader, and used S3 cache hydration.

The final root cause was not a React/chart problem. It was also not simply "LumiBot never handled splits." Past TQQQ 200-day style backtests did work. The failing local/dev cache contained a mixed-adjustment IBKR daily TQQQ series:

- Older split rows, including 2021-01-21 and 2022-01-13, were already continuous/split-adjusted.
- The local/dev cache segment from about 2024-01-31 through 2025-11-19 was raw by a factor of 2.
- The 2025-11-20 split row and later rows were back in the post-split price level.

That mixed cache made the 2025 split look like a raw price halving while the held position was not doubled. A first ledger-only fix removed that 2025 crash but wrongly doubled positions on the already-adjusted 2021 and 2022 split rows, creating a false 60% CAGR result. The durable fix normalizes IBKR stock/day bars into split-adjusted price space before backtesting, marks those frames as `_split_adjusted`, and keeps the generic position-ledger split path only for genuinely raw/unadjusted frames.

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

Focused command run:

```bash
LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 300s python3 -m pytest tests/test_stock_split_accounting.py tests/test_ibkr_daily_split_spike_repair.py tests/backtest/test_ibkr_equity_actions.py tests/test_strategy_dividend_cash_batch.py tests/test_split_adjustment.py tests/test_backtesting_broker.py tests/backtest/test_pandas_backtest.py tests/backtest/test_yahoo.py tests/backtest/test_yahoo_helper_actions.py -q
# 108 passed, 1 warning
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

## Remaining Non-Split Follow-Up

The dev cache result (`29.53%`) is below prod IBKR (`38.90%`) and Yahoo (`38.17%`) because the inspected dev S3 TQQQ cache also had completeness/coverage differences around early 2016. That is separate from the 2025 split crash:

- The split crash is fixed by normalization and verified by the 2021/2022/2025 split windows.
- The dev/prod result gap should be tracked as a dev-cache completeness issue, not as a split accounting issue.

If a future provider/cache omits `stock_splits` entirely, LumiBot still cannot safely infer true corporate actions from price moves alone without risking false positives. Provider enrichment and cache quality remain required.
