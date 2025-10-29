# Weekly Momentum Options – ThetaData vs Polygon (September 2025)

## Overview

Two saved runs of `WeeklyMomentumOptionsStrategy` (September 2025 window) were reviewed to compare ThetaData-backed backtests against the Polygon feed. Artefacts live under `Strategy Library/logs/` and capture trades, stats, and full strategy logs. The focus is to understand why ThetaData produced materially higher returns and additional fills, and to document the option-data coverage behind those results.

## Trade Outcomes

| Datasource | Timestamp (ET)        | Symbol | Side  | Status        | Price | Quantity |
|------------|-----------------------|--------|-------|---------------|-------|----------|
| ThetaData  | 2025-09-04 00:00      | WDC    | buy   | fill          | 2.64  | 25       |
| ThetaData  | 2025-09-11 00:00      | HOOD   | buy   | fill          | 5.60  | 11       |
| ThetaData  | 2025-09-20 00:00      | WDC    | sell  | cash_settled  | 20.86 | 25       |
| ThetaData  | 2025-09-26 00:00      | APP    | buy   | fill          | 25.00 | 3        |
| ThetaData  | 2025-09-27 00:00      | HOOD   | sell  | cash_settled  | 3.79  | 11       |
| Polygon    | 2025-09-25 00:00      | APP    | buy   | fill          | 21.18 | 2        |

*Source files: `WeeklyMomentumOptionsStrategy_2025-10-28_21-07_2phbgt_trades.csv` (ThetaData) and `WeeklyMomentumOptionsStrategy_2025-10-28_21-10_Hfglnn_trades.csv` (Polygon).*

ThetaData achieved three fills and two cash-settled exits; Polygon filled only the final APP contract, leaving WDC and HOOD orders in `new` status for the entire run.

## ThetaData Coverage Evidence

ThetaData cache instrumentation (e.g., `[THETA][DEBUG][CACHE][UPDATE_WRITE]`) shows complete minute-level coverage for all requested options:

- **WDC 2025-09-19 86C**: minute quotes span `2025-08-29T13:31:00+00:00` through `2025-09-20T04:00:00+00:00`, with only six OHLC placeholder rows (`option_WDC_250919_86.0_CALL_minute_{ohlc,quote}.parquet`).
- **HOOD 2025-09-26 118C**: minute quotes cover `2025-09-05T13:31:00+00:00` through `2025-09-27T04:00:00+00:00`; no placeholders injected.
- **APP 2025-10-03 610C** and **APP 2025-10-10 645C**: minute quotes reach `2025-09-30T04:00:00+00:00`, confirming data availability through the final trading day.

Each cache write also records the number of placeholders inserted (zero for all but WDC minute OHLC, which carries six placeholder entries prior to the first liquid prints). These diagnostics demonstrate that ThetaData delivered fresh quotes up to expiration, enabling the broker to value the positions and trigger the cash-settled exits on 20 Sep and 27 Sep.

## Polygon Observations

Polygon logs show repeated quote failures for the same contracts:

- Data warning: `Data object WDC 2025-09-19 86.0 CALL is missing quote columns [...] returning None`, immediately after order submission.
- Subsequent messages: `No current price for APP`, repeated four times on 25 Sep.

Because the backtesting broker never receives a valid quote, the limit orders remain in `new` status. There is no evidence of bid/ask coverage beyond the sparse OHLC bars, which is expected for Polygon’s options feed during illiquid periods.

## Timeline Comparison

1. **4 Sep** – Both runs submit a WDC 86C buy. ThetaData hydrates both OHLC and quote caches, fills immediately. Polygon returns a missing-quote warning; order stays open.
2. **11 Sep** – ThetaData fills HOOD 118C, while Polygon again reports missing quotes for the same order.
3. **20 Sep** – ThetaData logs the first cash settlement as the WDC contract expires, increasing cash in the portfolio. Polygon still shows the original order `new`.
4. **25–26 Sep** – Both feeds pursue APP exposure. ThetaData fills on 26 Sep; Polygon fills a smaller 2-lot on 25 Sep (likely due to OHLC-only pricing).
5. **27 Sep** – ThetaData cash-settles HOOD; Polygon retains the open order.

Portfolio equity jumps on ThetaData align with these settlement events. The Polygon tear sheet lacks corresponding sell markers because the orders never left the `new` status.

## Coverage Gap Summary

| Contract (Expiry) | ThetaData quote max timestamp | Placeholders | Polygon behaviour |
|-------------------|-------------------------------|--------------|-------------------|
| WDC 86C (19 Sep)  | 2025-09-20 04:00 UTC          | 0 (quotes)   | Missing bid/ask; order never fills |
| HOOD 118C (26 Sep)| 2025-09-27 04:00 UTC          | 0            | Missing bid/ask; order never fills |
| APP 610C (03 Oct) | 2025-09-30 04:00 UTC          | 0            | Only OHLC available; earlier orders remain `new` |

*All max timestamps pulled from `WeeklyMomentumOptionsStrategy_2025-10-28_21-07_2phbgt_logs.csv` cache diagnostics.*

## Key Takeaways

- ThetaData reliably cached the entire minute-level quote history required for the three target contracts, enabling the broker to calculate expirations and recognise cash settlements.
- Polygon’s feed missed bid/ask data for WDC and HOOD, leaving the strategy with unfilled orders and a muted return profile.
- The divergence is therefore attributable to data availability, not to caching faults within the ThetaData helper.

## Trades Visualization Fix

- `_build_trade_marker_tooltip` now falls back to the computed notional when `trade_cost` is absent, so cash-settled exits produce Plotly tooltips/markers.
- Regression coverage lives in `tests/test_indicator_subplots.py::test_cash_settled_tooltip_generated_without_trade_cost`, alongside a sanity check that `plot_returns` preserves the `cash_settled` status in the exported CSV.
- A fresh ThetaData run (`logs/WeeklyMomentumOptionsStrategy_2025-10-28_22-25_gYiGRT_trades.html`) now shows red sell triangles on 20 Sep and 27 Sep, matching the CSV rows.

## Liquid Instrument Parity

- `pytest -s tests/backtest/test_thetadata_vs_polygon.py -k stock_price_comparison` reports `ThetaData=$189.285, Polygon=$189.285, diff=$0.0000` for AMZN equity (July/Aug 2024 window).  
- `pytest -s tests/backtest/test_thetadata_vs_polygon.py -k option_price_comparison` confirms ATM AMZN weekly calls match to the cent: `ThetaData=$6.10, Polygon=$6.10, diff=$0.0000`, and both vendors expose ~85 expirations.  
- The coverage script (`python scripts/compare_option_data_coverage.py`) documents why the September 2025 contracts diverge: ThetaData cached 20–31k minute rows with full placeholder counts, while Polygon returned zero rows for the same symbols/expirations.

## Cache Confidence

- `pytest -s tests/test_thetadata_pandas_verification.py::test_pandas_cold_warm` (also called via `run_cache_validation.sh`) shows the cold pass creating nine cache files, the warm pass creating zero, and identical portfolio values/data-fetch counts (54). Results are archived in `logs/pandas_verification_results.json`.
- Weekly Momentum cold run (`logs/WeeklyMomentumOptionsStrategy_2025-10-28_22-25_gYiGRT_logs.csv`) contains `[THETA][DEBUG][CACHE][MISS]` lines for every asset; the immediate warm replay (`logs/WeeklyMomentumOptionsStrategy_2025-10-28_22-27_lhcW2o_logs.csv`) contains only `[THETA][DEBUG][CACHE][HIT]` records—no `Downloading` entries.

## Remaining Checks

- Retain the `[THETA][DEBUG]` logging tag until parity investigations conclude; once all strategies show cache hits on warm runs, we can cull the diagnostics in a follow-up branch.
- Consider extending the automated parity harness with a stored SPY option fixture so CI can enforce penny-level comparisons without live network calls.

These findings will guide the next steps: expanding logging so the behaviour is visible without manual log scraping, scripting automated data coverage comparisons, and running A/B tests with ThetaData forced into OHLC-only mode.
