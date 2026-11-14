# BROKER_HISTORY_BACKTESTING — Backtest on your own broker's price history (Schwab, Tradier)

> One generic backtesting source, `BrokerHistoryBacktesting`, reads daily bars from the user's own broker connection. A broker plugs in as a small adapter. Limits fail loudly; nothing is switched or filled.

**Last Updated:** 2026-11-11
**Status:** Daily bars for stocks and ETFs (intraday is designed, not built)
**Audience:** Developers and AI agents

---

## Overview

| Piece | File |
|---|---|
| Generic source | `lumibot/backtesting/broker_history_backtesting.py`: `BrokerHistoryBacktesting(PandasData)`, plus `SchwabHistoryBacktesting` and `TradierHistoryBacktesting` (they only set `BROKER`) |
| Broker adapters | `lumibot/tools/broker_history.py`: `BrokerHistoryAdapter`, `TradierHistory`, `SchwabHistory`, `RateLimiter`, `make_adapter` |
| Selection | `BACKTESTING_DATA_SOURCE=schwab` or `tradier`, or pass the class to `backtest()` |
| Tests | `tests/test_broker_history_backtesting.py` (recorded response shapes, real broker clients), `tests/test_broker_history_yahoo_parity.py` (live, `apitest`) |

### How it works

1. On the first request for a stock, the source downloads its daily bars for the whole backtest window. The window runs from the backtest start, minus the lookback and a 7-day buffer, to the backtest end. That is one request per stock.
   - A longer lookback later in the run downloads only the earlier stretch it is missing.
2. Reads go through the shared bar cache (`shared_bar_cache.read_bars`), shared by every connection of the broker (`shared_scope`; Tradier sandbox and live are separate because sandbox data is delayed). The live data sources use the same scope, so any backtest or chart on that broker reuses the bars. Bars cached per connection before this are still read.
   - The series is regular-session daily bars. Charts default to extended hours, so a backtest series is a separate series from the default chart series.
3. Daily bars are stamped at the 16:00 New York close, as the Yahoo daily source does.
   - A strategy running during day D sees bars through D−1. The day's bar appears after its close.
   - Market-order fills use the backtesting broker's existing daily path, the same as Yahoo and Polygon daily bars.
   - Bars dated after "now" are dropped.
4. `PandasData` serves every slice from memory: the same point-in-time slicing and no-lookahead rules as the other pandas-backed sources.

### Limits (fail loudly; never switch source; never fill)

| Limit | Behavior |
|---|---|
| Asset types | Stocks and ETFs only. Options, futures, crypto, indexes and forex raise `BrokerHistoryUnavailable` ("... support stocks and ETFs only ..."). `get_chains` raises. |
| Bar size | Daily only for now. A minute or hour request raises with the broker's depth for that bar size, for example "Tradier keeps 1-minute bars for about 20 days back from today". |
| Depth | Schwab daily history starts 1985-01-01; an earlier backtest start raises with that date. Tradier daily goes back the full listing. |
| No bars | A symbol with no bars in the window raises ("returned no daily bars for ..."). A day the broker did not return stays missing. |
| Rate limit | 120 requests per minute for each broker (sliding one-minute window). When the broker refuses for rate (Tradier HTTP 400 "Quota Violation", HTTP 429), the source waits 15, 30, then 60 seconds. After that it raises `BrokerHistoryRateLimited`. Waits are counted in the run telemetry. |

Broker facts used (from each broker's documentation, checked 2026-11-11):

- **Schwab:** daily and weekly back to 1985; 5 to 30 minute bars about 9 months; 1 minute about 48 days; 120 requests per minute; prices split-adjusted only. The refresh token expires after 7 days, so a Schwab backtest needs a fresh token.
- **Tradier:** daily and weekly for the full listing; 1 minute 20 days; 5 and 15 minutes 40 days; 120 requests per minute (over the limit: HTTP 400 "Quota Violation"); no expired options.

### Dividends and adjustment

- Prices are what the broker returns. Schwab and Tradier daily history is split-adjusted, not adjusted for cash dividends.
- The adapters return no dividend column. LumiBot credits cash dividends from that column, so **cash dividends are not credited** in broker-history backtests.
- Total return of dividend-paying stocks is therefore understated by their dividends. `tests/test_broker_history_backtesting.py::test_no_cash_dividends_are_credited` pins this.

### Parity with Yahoo (live)

`tests/test_broker_history_yahoo_parity.py` covers NVDA across its 10-for-1 split on 2024-06-10, using a Tradier sandbox account, run 2026-11-11:
- Same 25 trading dates from 2024-05-22 to 2024-06-27.
- Closes within 0.39% on every day. The largest gap was 2024-06-21: 127.06 against 126.57.
- Both sources are split-adjusted on each side of the split (2024-06-07: 120.87 against 120.89).

The small differences fit the two sources using different closing prints (for example last trade versus official close); this was not verified.

### Credentials

- **Tradier:** `TRADIER_ACCESS_TOKEN`, `TRADIER_ACCOUNT_NUMBER`, `TRADIER_IS_PAPER` (`TRADIER_CONFIG`).
- **Schwab:** `SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`, `SCHWAB_ACCOUNT_NUMBER` and a token file (`SCHWAB_TOKEN_PATH`), read through `SchwabData.create_schwab_client`. You can also pass a ready client as `config={"client": client, "SCHWAB_ACCOUNT_NUMBER": ...}`.
- An `adapter=` argument replaces both. Tests use it.

## Adding a broker

Subclass `BrokerHistoryAdapter`:
- set `name` and `limits` (requests per minute, earliest daily date, intraday depth per bar size),
- implement `_daily(symbol, start, end)` to return real OHLCV rows indexed by the broker's daily stamps (`daily_labels` converts them),
- implement `is_rate_limited(exc)`.

Then add a `make_adapter` branch, a one-line `XHistoryBacktesting` subclass, the class to `lumibot/backtesting/__init__.py` and `_BACKTESTING_CLASS_MODULES`, and the routing name to the `datasource_map` in `run_backtest`. Test with recorded response shapes copied from the broker's client code.

## Intraday (designed, not built)

Intraday bars use the same adapter contract plus `intraday(symbol, minutes, start, end)`. The depth check compares the backtest start with "today minus the broker's depth for that bar size". Most backtests then fail with a message naming that depth (Tradier 1-minute: 20 days). Only recent windows fit, so the daily path came first.
