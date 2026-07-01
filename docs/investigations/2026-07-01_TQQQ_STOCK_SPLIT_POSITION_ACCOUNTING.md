# TQQQ Stock Split Position Accounting RCA

One-line description: Root cause, fix, and validation for the TQQQ 2025-11-20 split causing a false 50 percent backtest equity drop.
Created: 2026-07-01
Status: Fixed locally on branch `version/4.5.64`
Audience: Engineering (Backtesting, BotSpot Auto, Data Routing)

## Incident

Backtest `22f6c3fd-90e0-4c03-a9d5-0a9a6cc994c3` for strategy `TQQQ 200-Day Trend Bot` dropped roughly in half around the real TQQQ 2:1 split effective before market open on 2025-11-20. The dev S3 IBKR daily cache already contained the split:

- S3 cache: `s3://lumibot-cache-dev/dev/cache/v1/ibkr/stock/day/bars/stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet`
- 2025-11-19 close: `100.05`, `stock_splits=0`
- 2025-11-20 close: `46.45`, `stock_splits=2.0`

The bad local replay held 3012 shares on 2025-11-19, still held 3012 shares on 2025-11-20, then sold 3012 shares on 2025-11-21 at 46.85. The price series was post-split, but the position ledger was not split-adjusted.

## Root Cause

This was a LumiBot accounting bug, not a React chart bug and not a Data Downloader/S3 cache bug.

LumiBot had several split-related data fixes, but no broker-ledger path that consumed `stock_splits` and mutated open stock positions during backtests. Dividends had a cash-event path (`_update_cash_with_dividends`), but splits had no equivalent position-event path. As a result, a real split row could be present in data while held quantity stayed unchanged.

There was also a second chart artifact: once quantity adjustment was added, routed daily valuation could still mark the new doubled share count against the previous pre-split close before the split-date daily bar timestamp. That created a synthetic 2x spike followed by a synthetic -50 percent raw-stat move. Daily split-date snapshots now adjust stale previous prices by the split ratio until the split-date bar is available.

The fix must be provider-aware. Raw/unadjusted daily bars (the IBKR/Data Downloader path in this incident) need a position-ledger split. Split-adjusted data sources already express historical fills in current share units and must not multiply held quantities again. The implementation now skips position-ledger split application when a data frame carries LumiBot's `_split_adjusted` marker or when a provider explicitly declares that `auto_adjust=True` means split-adjusted prices.

## Why Earlier Fixes Did Not Cover This

The prior fixes were adjacent, not equivalent:

- `docs/investigations/2026-03-03_IBKR_STOCK_INDEX_PARITY_AND_CORPORATE_ACTIONS.md` fixed IBKR corporate-action enrichment so `dividend` and `stock_splits` columns exist.
- `tests/test_ibkr_daily_split_spike_repair.py` covers isolated bad data spikes and explicitly leaves persistent level shifts alone. A real split is a persistent level shift plus a corporate-action event, not a bar-rewrite problem.
- `tests/test_split_adjustment.py` covers ThetaData historical price/dividend/option normalization. It does not simulate a broker account holding shares through a split.

The missing regression surface was position accounting: buy before a split, hold through the split effective date, verify share quantity, basis, valuation, and later sell quantity.

Production backtest history also explains why this looked fine recently. The high-return, long-window TQQQ/200-day-style examples that worked in April used `theta_data`, whose historical stock path normalizes prices to current share units. Example rows from production Postgres:

- `10fcefe2-d4fc-4144-b978-883f12893c43`, created `2026-04-22`, window `2016-01-21` to `2026-04-16`, provider `theta_data`, CAGR `42.56%`, max drawdown `-70.36%`.
- `c4b0c46a-54ca-4a29-9aef-d502245caa73`, created `2026-04-22`, same window family, provider `theta_data`, CAGR `55.34%`, max drawdown `-28.92%`.

The bad replay was different: `botspot_auto` routed stocks to IBKR/Data Downloader raw daily bars, over a window that held TQQQ through the real 2025-11-20 split. Recent `botspot_auto` TQQQ rows found in production either used shorter windows that did not hold through that split, or were different strategies. This is why earlier user-visible TQQQ results could be correct while this local/dev V6 run failed.

## Fix

Implemented split handling in the backtest accounting path:

- Preserve `stock_splits` in `Data`/`Bars` slices and avoid forward-filling corporate-action event columns.
- Add `DataSource.get_yesterday_stock_splits()` with a full preloaded daily-frame lookup before normal historical slices. This matters for IBKR daily bars timestamped at 16:00 when the split is effective before market open.
- Add `Strategy._update_positions_with_splits()` and call it before dividend handling and strategy iteration/lifecycle work.
- Multiply open stock position quantity by the split ratio and divide average fill price by the same ratio.
- Keep split application idempotent by `(date, symbol, ratio)`.
- Emit `corporate_action` rows in trade events so split applications are auditable.
- Adjust stale daily mark-to-market prices across split boundaries for pandas, Theta/routed snapshots, Theta/routed last-trade lookup, and standalone IBKR daily stock/index valuation.
- Skip ledger split accounting for already split-adjusted frames (`_split_adjusted`) and provider-declared auto-adjusted data, preventing double counting on Theta/Yahoo adjusted paths.

## Regression Tests

New test file: `tests/test_stock_split_accounting.py`

Covered cases:

- Regular splits: 2:1, 3:1, 7:1, 10:1.
- Reverse splits: 1:2 (`0.5`) and 1:10 (`0.1`).
- Short positions.
- Basis adjustment.
- Idempotent repeated calls on the same split date.
- Invalid ratios: zero, one, negative, NaN, infinity, `None`, and non-numeric strings.
- Options are not adjusted by the stock-position path.
- `stock_splits` survives `Data` repair/slicing and is not forward-filled.
- Same-date/pre-open lookup for close-timestamped daily bars.
- Pre-close valuation uses split-adjusted previous close instead of the stale pre-split price.
- Daily backtest hold-through-split sells the adjusted quantity and does not create a false 50 percent equity drop.
- Split-adjusted frames do not apply a second ledger split.
- Provider-declared auto-adjusted data sources do not apply ledger splits.

Commands run:

```bash
LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 180s python3 -m pytest tests/test_stock_split_accounting.py -q
# 33 passed, 1 warning

LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 240s python3 -m pytest tests/test_stock_split_accounting.py tests/test_ibkr_daily_split_spike_repair.py tests/backtest/test_ibkr_equity_actions.py tests/test_strategy_dividend_cash_batch.py tests/test_split_adjustment.py -q
# 58 passed, 1 warning

LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 300s python3 -m pytest tests/test_backtesting_broker.py tests/backtest/test_pandas_backtest.py -q
# 38 passed, 1 warning

LUMIBOT_DISABLE_DOTENV=1 /Users/robertgrzesik/bin/safe-timeout 300s python3 -m pytest tests/test_stock_split_accounting.py tests/test_ibkr_daily_split_spike_repair.py tests/backtest/test_ibkr_equity_actions.py tests/test_strategy_dividend_cash_batch.py tests/test_split_adjustment.py tests/test_backtesting_broker.py tests/backtest/test_pandas_backtest.py tests/backtest/test_yahoo.py tests/backtest/test_yahoo_helper_actions.py -q
# 103 passed, 1 warning
```

## Exact Replay Validation

Replay command used the saved revision files, BotSpot Auto routing, IBKR stock data, Data Downloader URL, and dev S3 cache:

- Strategy hash: `ea07dbb637c922c8aa6c4a3e3e1479b7ef3c0f282dffdfc4490fbd5171e66a6b`
- Window: 2016-03-04 to 2026-03-04
- Budget: 10000
- Data source router: `{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"coinbase","crypto_future":"coinbase","future":"ibkr","cont_future":"ibkr"}`
- `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
- `AWS_PROFILE=BotManager`
- S3 cache bucket/prefix/version: `lumibot-cache-dev` / `dev/cache` / `v1`
- Final replay folder after provider-safety tightening: `/Users/robertgrzesik/Development/support-artifacts/tqqq-split-backtest-2026-07-01/local-replay-after-fix6-10k/run/`

Final replay results:

- 2025-11-19 16:00: portfolio value `1,228,154.55`, cash `17,049.56`, position `12,105` TQQQ.
- 2025-11-20 08:30 and 09:30: portfolio value stays `1,228,154.55`, position is `24,210` TQQQ after the split, no synthetic 2x spike.
- 2025-11-20 16:00: portfolio value `1,141,604.06`, position `24,210` TQQQ, return `-7.0472%`.
- 2025-11-21 09:30: sell fill for `24,210` TQQQ at `46.85`.
- 2025-11-21 16:00 onward: flat cash about `1,150,154`.
- Tearsheet CAGR: `60.73%`.
- Independently computed CAGR from stats CSV: `60.7469%`.
- Tearsheet max drawdown: `-47.89%`.
- Independently computed max drawdown from stats CSV: `-47.8937%`.
- Worst one-step raw-stat move is now 2020-03-09 at about `-20.2159%`, not the split date.

Corporate-action events emitted:

- 2021-01-21: TQQQ split ratio 2, quantity `1481 -> 2962`.
- 2022-01-13: TQQQ split ratio 2, quantity `2962 -> 5924`.
- 2025-11-20: TQQQ split ratio 2, quantity `12105 -> 24210`.

## Follow-Up Risk

The fix consumes split rows already present in the daily data. If a provider/cache omits `stock_splits`, LumiBot still cannot infer true corporate actions from price moves alone without risking false positives. Data Downloader/provider enrichment should remain monitored separately, but this incident was fixed in LumiBot because the split data was already available.
