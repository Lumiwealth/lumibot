# SHARED BAR CACHE

> One historical price-bar cache for every broker, shared by live reads (charts, bots) and broker-backed backtests.

**Last Updated:** 2026-10-22
**Status:** Active
**Audience:** Developers + AI Agents

---

## Overview

`lumibot/tools/shared_bar_cache.py` stores real broker bars in Parquet partitions and serves
repeated or overlapping history requests from them. Each broker adapter only supplies:

- a `BarSeries` identity (provider, provider symbol, bar size, data contract, connection scope,
  whether prices are adjusted), and
- a `fetch(start, end_exclusive)` function that reads one range from the broker and raises on errors.

Wired today: `AlpacaData` and `AlpacaBacktesting` (through `tools/alpaca_history.py`),
`SchwabData`, `TradierData`, `InteractiveBrokersRESTData` (Client Portal history) and
`CcxtData`. The BotSpot Auto backtest providers (IBKR downloader, ThetaData, Yahoo, Polygon,
DataBento) keep their own caches: they read from a different source than a customer's broker
connection, so their bars are never mixed with broker-connection bars.

## How a read works

1. Partitions: one file per calendar month for intraday bars, one per calendar year for daily
   and longer bars, under `<LUMIBOT_CACHE_FOLDER>/bars/v2/<provider>/<digest>/`. With the S3
   backend the same path sits under `<prefix>/<version>/bars/v2/...`.
2. Each partition records the exact ranges the broker answered (`coverage`). The request range
   minus coverage gives the gaps. Remote partitions load in parallel.
3. Each gap is read from the broker, widened by one bar on both sides so callers whose start or
   end differ by milliseconds or a time-zone offset still land inside coverage next time.
4. Settled rows are kept in memory for this process at once. With a remote store the local
   file write (merge under a file lock) and the S3 upload both run on the background
   uploader, because on a one-CPU Lambda even the Parquet encode delayed answers by seconds;
   without a remote store the local write stays synchronous. The S3
   upload runs on a background thread; `flush_shared_bar_writes()` waits for it and also runs at
   interpreter exit.

## Invariants (do not break)

- **Never fabricate.** Only provider rows are stored or returned. An empty provider answer is
  never recorded as coverage (it can be an outage or entitlement gap).
- **Settled bars never expire.** A bar is stored only once it ended at least
  `LUMIBOT_SHARED_BARS_SETTLE_MINUTES` (default 15) before the provider clock. Delayed feeds pass
  `now - delay` as the clock. The still-open tail is read every time and never stored.
- **Adjusted history can change.** For `adjusted=True` series every gap next to cached bars also
  re-reads the last three cached bars. If the broker's prices differ, the series epoch is bumped in
  `series.json`, old partitions are ignored and the whole request is read again. A fully cached
  adjusted series is re-checked once a day.
- **Truncating providers.** Providers that can drop the start of an answer (IBKR Client Portal
  `period` reads) use `coverage_from_rows=True`, so coverage starts at the first returned bar.
- **Connection scope.** By default bars are shared only within one broker connection (scope is a
  one-way hash of the account/key, never the raw credential). `LUMIBOT_SHARED_BARS_SCOPE=provider`
  shares across connections of one broker. A series without a scope is never cached.
- **Errors propagate.** A provider error raises and writes nothing.
- **Provider bars are returned as the provider sent them.** Brokers sometimes publish internally
  inconsistent bars (Tradier SPY 2024-12-19: close below low); they are cached and returned like
  any other real observation. Only adapters passing `strict_ohlcv=True` (Alpaca) reject invalid
  OHLCV and write nothing. Stored partitions remember which rule wrote them.
- **Warm-up.** One-request worker processes may call `prewarm()` at startup so the Parquet
  engine and S3 client load while the process starts.
- **Look-ahead.** The cache only answers the range the caller asked for, so backtest no-look-ahead
  behavior is unchanged.

## Measurements

Every returned frame carries `frame.attrs["historyCache"]`: `hit`, `providerReads`, `partitions`,
`lookupMs`, `providerMs`, `writeMs` (local writes only), `totalMs`, `pendingUploads`, `remote`.
Bot Manager copies these into the broker-data job timing.

## Tests

- `tests/test_shared_bar_cache.py`: core contract (expiry, tail, partial ranges, empty answers,
  splits, daily re-check, scope, non-blocking uploads, cross-process S3 reuse, parallel loads).
- `tests/test_shared_bar_cache_brokers.py`: Schwab, Tradier, IBKR Client Portal and CCXT through
  their real data-source methods with fake broker clients.
- `tests/test_alpaca_shared_history.py`: Alpaca chart and backtest sharing.
- Unit tests never touch a real bucket: `tests/conftest.py` sets `LUMIBOT_SHARED_BARS=false`, and
  cache tests re-enable it with an isolated folder and a disabled or in-memory remote store.
