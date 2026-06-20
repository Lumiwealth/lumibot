# Crypto Provider And Execution Gap Research

Date: 2026-06-19

Status: investigation note, not a release sign-off.

Artifacts:

- `docs/investigations/data/2026-06-19_crypto_provider_probe.csv`
- `docs/investigations/data/2026-06-19_crypto_provider_probe_summary.json`

## Summary

This note captures the research pass for crypto backtesting data providers and the
remaining LumiBot execution-safety gaps. As of the 2026-06-19 follow-up decision,
the intended implementation direction is **Coinbase only** for crypto historical
backtesting data. This means no Kraken phase two, no backup provider, and no
silent quote-asset fallback.

The critical execution invariant is:

> An order may wait for the next actionable bar/quote, but the fill must be
> recorded at that actual executable source timestamp. It must not fill at the
> original simulated timestamp using a price from a later bar, a stale bar, or a
> synthetic forward-filled row.

## Implementation Update - 2026-06-20

Local LumiBot changes now cover the first correctness layer for Coinbase/CCXT
crypto backtesting:

- `lumibot/tools/ccxt_data_store.py` supports `1m`, `1h`, and `1d` cache
  requests, records provider-request coverage separately from executable bars,
  and no longer forward-fills missing OHLCV buckets into strategy-visible rows.
- `lumibot/backtesting/routed_backtesting.py` defaults the generic CCXT crypto
  route to Coinbase when no explicit exchange credential/config is present, and
  preserves the exact requested quote asset for crypto-future spot proxies.
  `BTCUSDT` now routes to `BTC/USDT`, not `BTC/USD`.
- `lumibot/data_sources/ccxt_backtesting_data.py` accepts hour timesteps and
  requests native `1h` candles instead of forcing hour history through minute
  assumptions.
- `lumibot/backtesting/backtesting_broker.py` applies strict intraday
  current-bar execution checks to CCXT paths, prevents CCXT lookahead fills, uses
  UTC-day expiry for crypto `DAY` orders, and cancels unfilled IOC/FOK orders
  when the current executable quote/bar does not satisfy the order.

Durable regression tests were added or updated in:

- `tests/test_ccxt_store.py`
- `tests/test_backtesting_ccxt_execution_semantics.py`
- `tests/test_hour_timestep_support.py`
- `tests/test_routed_backtesting_unit.py`

Passing local proof:

```bash
python3 -m pytest \
  tests/test_routed_backtesting_unit.py \
  tests/test_hour_timestep_support.py \
  tests/test_backtesting_crypto_cash_unit.py \
  tests/test_ccxt_store.py \
  tests/test_backtesting_ccxt_execution_semantics.py -q
```

Result on 2026-06-20 after adding sparse GTC/GTD regression coverage and the
CCXT UTC cache-boundary regression:
`39 passed, 2 skipped`.

### Greg Local Replay - 2026-06-20

Greg's saved production strategy was replayed locally with production-like
routing and Coinbase for crypto:

```bash
python3 scripts/run_backtest_prodlike.py \
  --main /Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/prod-artifacts/executed-code/main.py \
  --start 2026-03-15 \
  --end 2026-04-20 \
  --data-source '{"default":"thetadata","stock":"thetadata","index":"thetadata","option":"thetadata","future":"ibkr","cont_future":"ibkr","crypto":"coinbase","crypto_future":"coinbase"}' \
  --cache-mode disabled \
  --label greg_coinbase_mar15_apr20_20260620_utcfix2 \
  --audit
```

Artifact folder:
`/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2`

Result:

- Backtest completed successfully in 454 seconds.
- Trades CSV: 21 lines, meaning 10 filled trade rows plus new-order rows and
  header.
- Trade events CSV: 21 lines.
- The filled rows all used `audit.timestep=minute`.
- `audit.bar.datetime` matched the trade event time exactly for every fill.
  Maximum absolute fill timestamp gap was `0.0` seconds.
- Grep over the subprocess log and strategy log found no stale/future timestamp
  errors, no traceback, and no "after available data" cache-boundary failures.

Primary evidence:

- Trades CSV:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/logs/MultiMineralBot_2026-06-19_22-50_0dPozs_trades.csv`
- Trade events CSV:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/logs/MultiMineralBot_2026-06-19_22-50_0dPozs_trade_events.csv`
- Metrics JSON:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/metrics.json`
- Subprocess log:
  `/Users/robertgrzesik/Development/support-artifacts/greg-backtest-april17-20260612/local-runs/greg_coinbase_mar15_apr20_20260620_utcfix2/subprocess.log`

The replay also exposed and confirmed the CCXT cache timezone bug. Before the
cache-boundary fix, aware New York datetimes were stripped to naive values before
DuckDB cache queries, so `2026-03-15 00:00 America/New_York` was treated as
`2026-03-15 00:00 UTC` instead of `2026-03-15 04:00 UTC`. That caused the local
Coinbase cache to look four hours short and produced repeated "after available
data" errors. `CcxtCacheDB` now converts aware request bounds to UTC before
dropping timezone metadata.

One related note: Greg's saved strategy parameters say `BTC/USDT`, but the
production entrypoint in the captured artifact passes `quote_asset=USD` to
`run_backtest`. The replay therefore proves the captured production code path as
saved, not a rewritten `BTC/USDT` strategy.

This is still not a production deployment sign-off. It is local proof that the
Coinbase-routed replay completes and that this run's actual fills are recorded
at the executable minute-bar timestamps rather than at stale or future prices.

## Provider Findings

Live public API probes were run for BTC/USD, BTC/USDT, and BTC/EUR using
1-minute, 1-hour, and 1-day sampled windows in 2016, 2020, 2024, and 2026.
No private API keys were used for these probes; they hit public market-data
endpoints only.

Provider scorecard from the probe:

| Provider | Active/usable symbols from public metadata | BTC quote coverage observed | Historical 1m REST fit | Legal/commercial fit | Near-term recommendation |
| --- | ---: | --- | --- | --- | --- |
| Coinbase Exchange | 523 active/online products | BTC-USD, BTC-USDT, BTC-EUR, BTC-GBP, BTC-USDC, BTC-INR | Best direct REST fit for BTC/USD; 300-candle chunks required | Restrictive market-data terms; no redistribution without care | Recommended single-provider implementation target |
| Kraken REST | 1459 active/online pairs | 18 BTC-like pairs in metadata | REST OHLC is recent-tail only, not arbitrary old history | CSV/bulk path may be useful; REST alone not enough | Do not implement for this workstream if Coinbase is selected |
| Binance.US | 264 trading symbols | BTCUSD, BTCUSDT, BTCUSDC, BTCDAI, BTCBUSD, etc. | Good recent data; no 2016 BTC/USD or BTC/USDT in probe | Explicit commercial-use restrictions | Backup only after legal review |
| Gemini | 347 symbols | BTCUSD, BTCUSDT, BTCEUR, etc. | Public candles endpoint did not prove arbitrary old start/end windows | Explicit market-data agreement; commercial/redistribution restrictions | Backup if we pursue legal agreement/API terms |
| KuCoin | blocked from this environment | not usable in probe | Docs look capable, live probe blocked | Requires separate connectivity/compliance work | Not near-term |

### Coinbase

- Public products probe returned 826 products, 523 active/online products, and
  six BTC quote pairs: BTC-EUR, BTC-GBP, BTC-INR, BTC-USD, BTC-USDC, BTC-USDT.
- BTC/USD returned strong sampled 1-minute, 1-hour, and 1-day results:
  - 2016-06-18: 1m 1425/1440 bars, 1h 24/24, 1d 1/1.
  - 2020-01-01: 1m 1440/1440, 1h 24/24, 1d 1/1.
  - 2024-01-01: 1m 1440/1440, 1h 24/24, 1d 1/1.
  - 2026-06-18: 1m 1440/1440, 1h 24/24, 1d 1/1.
  - Full daily 2016-01-01 through 2026-06-18: 3822/3822 bars.
- BTC/USDT did not return 2016 sampled bars and returned missing 1-minute
  buckets in 2024 and 2026 samples. This appears to be a Coinbase pair-liquidity
  or listing-history issue, not a downloader issue.
- BTC/EUR returned sparse historical minute data but strong 1-hour and daily
  data:
  - 2016-06-18: 1m 743/1440 bars, 1h 24/24, 1d 1/1.
  - 2026-06-18: 1m 1403/1440 bars, 1h 24/24, 1d 1/1.
  - Full daily 2016-01-01 through 2026-06-18: 3821/3822 bars.
- Coinbase Exchange candles are limited to 300 candles per request and the docs
  state historical rate data may be incomplete; no data is published for
  intervals with no ticks.
- Legal note: Coinbase Market Data Terms are restrictive for end-user
  applications. Broad commercial BotSpot use likely needs counsel or a
  commercial data agreement/vendor.

### Kraken

- Public asset-pair probe returned 1552 pairs and 1459 active/online pairs.
- Kraken has broader BTC pair coverage than Coinbase in the public AssetPairs
  response.
- Normal Kraken REST OHLC is not enough for old history. The official endpoint
  returns up to 720 recent entries regardless of `since`; 2016 and many 2026
  minute requests returned current tail data, not the requested old window.
- Kraken downloadable OHLCVT/trade datasets may still be useful, but that is a
  separate ingestion path and not a quick CCXT REST switch.

### Binance.US

- Public exchange info probe returned 625 symbols, 264 trading symbols, and
  seven BTC quote pairs.
- BTC/USD and BTC/USDT worked for sampled 2026 1-minute and 1-hour windows.
- 2016 sampled windows returned no data, so Binance.US does not satisfy the
  desired BTC/USD 2016+ history requirement.
- Binance.US does not support BTC/EUR in the probe.
- Binance.US market-history terms flag commercial-use restrictions; legal
  review is required before using it as a customer-facing data source.

### Gemini

- Public symbols probe returned 347 symbols and BTC/USD, BTC/USDT, BTC/EUR.
- The public candles endpoint returns recent candles without a simple start/end
  query path in the endpoint used here, so it did not prove 2016 minute history.
- Gemini has explicit API and Market Data Agreements. This may be legally
  cleaner than some exchange public endpoints, but it still needs review before
  customer-facing redistribution or paid backtest use.

### KuCoin

- The public API request from this environment returned a restricted-area error.
- Official docs support spot klines up to 1500 records per request and warn that
  kline data may be incomplete when there are no ticks.
- Because the API is restricted from this environment, KuCoin is not a reliable
  near-term default without separate compliance and connectivity work.

### Commercial data vendors

CoinAPI, Tardis.dev, and Kaiko were considered as alternatives if legal or
commercial rights become the overriding constraint. They are not part of the
current implementation plan because the current direction is one crypto provider,
Coinbase, with no fallback stack.

## Pair Fidelity Decision

Silent quote mapping is not allowed. If strategy code requests `BTC/USDT`, then
the data request must be for Coinbase `BTC-USDT`. If Coinbase has no historical
bar for that product and timeframe, the result is missing data, not a `BTC-USD`
proxy. The user can explicitly change strategy code to `BTC/USD` if that is what
they want.

This decision invalidated the older routed-backtesting behavior that mapped
USDT crypto futures/perps to USD spot history. The local 2026-06-20 LumiBot
change removes that silent mapping: exact pair data is required, and missing
exact-pair data must remain missing.

## LumiBot Findings

The existing order-lifecycle tests cover useful cases: DAY/IOC/FOK behavior,
GTD expiry, stale quote rejection for IBKR, and future intraday bar rejection.
They do not yet prove the CCXT/Coinbase/Kraken path is safe.

Important current code risks:

- `lumibot/tools/ccxt_data_store.py` expands requested downloads to whole days.
- `_fill_missing_data` forward-fills missing rows and marks them with
  `missing=1`.
- `_fill_missing_data` currently treats any timeframe except `1d` as minute
  frequency. A live `1h` probe through `CcxtCacheDB.download_ohlcv(..., "1h")`
  raised `TypeError("'>' not supported between instances of 'datetime.timedelta' and 'int'")`
  before data returned, so hourly CCXT cache support is not proven.
- The strict current-bar guard in `backtesting_broker.py` is currently scoped to
  IBKR/routed-IBKR paths. CCXT rows with a current timestamp but `missing=1`
  need explicit execution rejection.
- The current `tests/test_ccxt_store.py` tests are skipped integration tests and
  do not prove minute, hour, day, warm cache, partial overlap, missing data,
  provider errors, or execution behavior.

Additional live proof on 2026-06-19:

```text
CcxtCacheDB("coinbase").download_ohlcv("BTC/USD", "1h", ...)
TypeError "'>' not supported between instances of 'datetime.timedelta' and 'int'"
```

This means the CCXT cache cannot be called product-ready for hourly data. Since
the same cache path is responsible for minute/day behavior too, the correct
bar is not "fix one-hour only"; it is to build a real cache test matrix across
1m, 1h, and 1d before any provider is trusted.

## Crypto Order Semantics Research

The current public exchange docs do not support treating crypto DAY orders as a
universal native exchange order type:

- Coinbase Advanced Trade exposes market IOC/FOK and limit/stop/bracket GTC/GTD
  style configurations; no plain DAY order was found in the order schema.
- Kraken Add Order documents GTC, IOC, GTD, and FOK behavior; no DAY equivalent
  was found in the public API docs.
- Gemini documents market, limit, and stop-limit. Time-in-force is documented
  for limit orders as GTC, MOC, IOC, and FOK.
- Binance.US trading rules define market orders as taker orders at the best
  available order book price and limit-order TIF as GTC, IOC, and FOK.

Backtesting implication:

- `DAY` for crypto is a LumiBot abstraction, not a universal exchange-native
  behavior. If we support it, use UTC-day expiry for crypto because exchange
  OHLCV buckets and daily candles are UTC-based in the providers tested here.
- `GTC` is realistic for resting limit orders. A "GTC market order" is not a
  normal crypto exchange primitive; if LumiBot supports it as an abstraction, it
  must never fill using a future price at the original timestamp. It can only
  fill when an executable bar/quote actually exists, and the recorded fill time
  must be that executable source timestamp.
- For OHLC-only backtesting, if there is no bar and no quote/book snapshot, the
  honest result is "not executable yet." We can let eligible DAY/GTC/GTD orders
  remain open, but IOC/FOK must expire immediately.

## Required Regression Tests

Add durable tests under `tests/` before any BotSpot default switch:

1. Minute BTC/USD exact real bar fills at the real bar timestamp.
2. Minute BTC/USDT missing row with `missing=1` does not fill.
3. Minute BTC/USDT missing row leaves DAY/GTC/GTD orders working until the next
   real row, then fills at the next real row timestamp if the TIF is still valid.
4. IOC/FOK cancel immediately when only a `missing=1` row exists.
5. One-hour CCXT requests do not expand into minute synthetic execution rows.
6. One-hour missing bucket waits until the next real hourly bucket and records
   the fill at that real bucket timestamp.
7. Daily execution keeps existing daily semantics without permitting far-future
   prices.
8. Rare/illiquid pair that trades once per day:
   - DAY expires according to the configured crypto day/session rule.
   - GTC can remain open and fill only at the next real bar if conditions match.
   - Fill timestamp equals the actual next real bar timestamp.
9. Rare/illiquid pair that trades once per week:
   - Same as once-per-day, plus GTD expiry before next real print.
10. Limit/stop/trailing/stop-limit/bracket/OCO paths reject synthetic rows, not
    just market orders.
11. Quote path rejects stale/future/synthetic quote timestamps for intraday
    execution.
12. Fill audit metadata records simulated timestamp, fill timestamp,
    source timestamp, provider, exchange, symbol/pair, timeframe, and missing
    flag.

## Remaining Order

Completed local groundwork:

- Silent `BTCUSDT`/`USDT` to `BTC/USD` proxy behavior removed.
- CCXT cache timeframe handling and no-synthetic-row filtering covered by
  local regression tests.
- CCXT intraday fill timestamp/current-bar invariants covered by local
  regression tests.

Remaining work before customer-facing confidence:

1. Build Coinbase-only proof inside LumiBot with isolated cache for BTC/USD,
   BTC/USDT, and BTC/EUR across 1m, 1h, and 1d.
2. Reproduce Greg with exact strategy code and exact requested pair. If the pair
   is missing data, fail honestly or leave eligible orders open until a real
   executable Coinbase bar arrives. Do not map symbols.
3. Wire BotSpot to the proven Coinbase-only LumiBot path after LumiBot tests and
   local/prod-like proof pass.
