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
- `polygon_spy_weekly_call_2026_*`: Task C, the same strategy for one week on a customer's own
  Polygon key (`--source polygon`, `LUMIBOT_OPTION_CHAIN_MAX_DAYS=21`). It works end to end; the
  cross-check file shows its 15:30 exit filled on the stale 15:18 print.

- `alpaca_env_orb_2026_*`, `alpaca_env_weekly_call_2026_*`: the same data source selected ONLY through
  `BACKTESTING_DATA_SOURCE=alpaca` (the BotSpot path: `backtest(datasource_class=None)`, no config, no
  timestep, credentials and `BACKTESTING_START`/`BACKTESTING_END` from the environment), with
  `scripts/alpaca_env_selection_proof.py`. `orb` is a SPY 5-minute opening range breakout
  (2026-08-03 to 2026-08-07); `weekly_call` reproduces the 2026 options trades exactly.
- `forming_bar_fix_2026-09-23.txt`: the lookahead fix (history returned the bar that was still
  forming). Real SPY bars at 10:00 and 10:02 before and after, the ORB proof's count of forming
  bars returned (33 of 33 before, 0 of 33 after) and the rerun of both environment proofs after
  the fix (trades identical). `alpaca_env_orb_2026_decisions.txt` now ends with that count.
- `history_before_start_2026-09-23.txt`: history before `BACKTESTING_START`. The ORB proof's first-bar
  request for 250 five-minute and 15 daily bars before and after (66 of 250 and an error, then 250 and
  15 with ATR(14) = 8.40), every returned bar checked against raw Alpaca bars (0 differences), and
  the rerun of both environment proofs (trades identical). The decisions file now starts with that
  first-bar line.

These are one-contract engineering proof runs, not a strategy result.
