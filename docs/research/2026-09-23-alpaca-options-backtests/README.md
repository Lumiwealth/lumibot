# Alpaca options backtest evidence, 2026-09-23

Real local backtests of `scripts/alpaca_options_backtest_proof.py` with `AlpacaBacktesting`
(minute bars, a paper Alpaca key, no other data vendor). Summary, numbers and limits are in
`docs/investigations/2026-09-23_alpaca-options-backtesting-and-ibkr-4592-window-regression.md`.

Strategy: on the first session of each week at or after 09:35 ET, buy one SPY call from
`get_chains()` (nearest expiration 7 to 21 days out, strike nearest to SPY), sell it on the
last session of the week at or after 15:30 ET. Market orders fill only on a real option
trade bar in the current minute.

Reproduce (credentials from your own environment):

```bash
LUMIBOT_DISABLE_DOTENV_LOCAL=1 BACKTESTING_DATA_SOURCE=none \
LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled PYTHONPATH=. \
python scripts/alpaca_options_backtest_proof.py --start 2026-07-27 --end 2026-08-19 --name alpaca_spy_weekly_call_2026

LUMIBOT_DISABLE_DOTENV_LOCAL=1 BACKTESTING_DATA_SOURCE=none \
LUMIBOT_CACHE_BACKEND=local LUMIBOT_CACHE_MODE=disabled PYTHONPATH=. \
python scripts/alpaca_options_backtest_proof.py --start 2024-03-04 --end 2024-03-27 --name alpaca_spy_weekly_call_2024
```

Files per window (`2026` = 2026-07-27 to 2026-08-14 trading, `2024` = 2024-03-04 to 2024-03-22):

- `*_trades.csv`, `*_trade_events.csv`: every order and fill.
- `*_decisions.txt`: the strategy's chain pick, orders, pending waits and fills from the run log.
- `*_tearsheet.html`, `*_tearsheet.csv`, `*_tearsheet_metrics.json`: QuantStats tear sheet.
- `*_settings.json`: run settings (downloader and remote cache fields removed; they were unused).
- `raw_bar_crosscheck_2026.txt`: the 2026 fills checked against raw Alpaca option minute bars.

These are one-contract engineering proof runs, not a strategy result.
