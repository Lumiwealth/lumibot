# Schwab history depth, quotes, and crypto

Date: 2026-09-22. Branch: `version/4.5.92`. Read-only, plus one order preview that was not sent.

Raw counts: `/Users/robertgrzesik/Development/lumibot/docs/investigations/2026-09-22_schwab-capability-probe-result.json`.

The account match error for placeholder `000` is expected. Schwab returned 4 margin accounts. None was saved. Market data does not need the account hash.

## How far back, and which bar size

Measured with the live token on 2026-09-22.

| Series | Daily | 1-minute | 30-minute | 60-minute | Second |
| --- | --- | --- | --- | --- | --- |
| SPY | 20-year request returned 2006-09-27 to 2026-09-22 (5027 bars). 10, 5, 2, and 1 year requests also filled their windows. | 90-day request capped at 2026-08-07 (39454 bars). Same cap at 48 days. | 365-day request started 2026-01-05 (8601 bars), not a full year. | Rejected. Schwab frequencies are 1, 5, 10, 15, 30 minutes. | Rejected. |
| SPY 775 call expiring 2026-09-22 | 11 bars from 2026-09-08, even on a 20-year request. That is the contract life, not years of history. | 1369 bars from 2026-09-08. | 145 bars from 2026-09-08. | Rejected. | Rejected. |
| SPY 775 call expiring 2027-09-17 | 30-day request: 20 bars from 2026-08-25. 1-year, 2-year, and 5-year requests all returned 95 bars from 2026-03-26. Quote 68.31, bid 67.75, ask 69.63. | 227 bars back to 2026-08-06 on a 48-day request. Sparse versus the stock. | Not remeasured on this contract. | Not remeasured. | Not remeasured. |
| SPY 775 call expiring 2029-01-19 | 3 bars from 2026-09-18 on every daily window through 5 years. Quote 126.11. | 5 bars from 2026-09-18. | Not remeasured. | Not remeasured. | Not remeasured. |
| Ticker BTC | 538 bars from 2024-07-31 on every request of 5 years or more. | Capped at 2026-08-06. | 365-day request started 2026-01-05. | Rejected. | Rejected. |

LumiBot `get_historical_prices` matched those windows for day and minute on SPY, the option, and ticker BTC. An hour request is not a 60-minute bar. Schwab has no 60-minute frequency, so LumiBot asks for 30-minute candles and logs that. A second request used to fall through into a daily request that Schwab rejected. It now returns no bars and does not call Schwab.

## Crypto is not on this API

Ticker `BTC` is an equity. Quote asset type EQUITY, description GRAYSCALE BITCOIN MINI TR ETF, last price 38.265. Ticker `ETH` is GRAYSCALE ETHEREUM STAKING MINI ETF, last price 26.355.

Instrument search: `DOGE` and `SOL` empty. `LTC` is LTC Properties, a REIT. `BCH` is Banco de Chile.

Description search for bitcoin and ethereum returned other equities and two indexes, `$BLX` and `$ELX`. Those indexes have a last price, and their daily history stops on 2023-10-30. `$ETH/USD` returned zero candles. None of that is a spot crypto pair, and none of it is a current bitcoin price.

LumiBot was rewriting crypto `BTC` and `BTC/USD` onto ticker `BTC`, which returned the ETF. That rewrite is removed. Crypto history, quote, and last price now return nothing instead of the ETF.

## Trading

`preview_order` for a market buy of 1 share of ticker `BTC` returned HTTP 200 with order value 38.28. That is one share of the ETF. The preview was not placed. `placed` is false.

LumiBot's order builder has stock, option, and future branches. Crypto hits "asset type crypto is not supported" and never reaches Schwab. Futures candle history is still skipped. Futures trading stays unsupported.

No live order was sent. The four linked accounts are margin accounts.

## What was actually tested

SchwabData methods that talk to Schwab, and what the live read showed:

| Method | Live result |
| --- | --- |
| `get_historical_prices` | SPY daily, minute, and hour (30-minute candles). Option daily and minute via the OCC symbol. Future history still skipped. Crypto and second return no bars. |
| `get_quote` | SPY about 774.21 bid / 774.23 ask. The 775 call about 0.10 / 0.11. Crypto is not requested. |
| `get_last_price` | Same path as `get_quote`. SPY 774.23. The call 0.10. |
| `get_chains` | SPY chain returned. The probe used the first listed call expiration, 2026-09-22, strike 775. |

Other `DataSource` methods (dividends, splits, and the rest) are inherited defaults. They were not part of this Schwab proof.

Unit coverage: `/Users/robertgrzesik/Development/lumibot/tests/test_schwab_price_history_symbols.py`. Option OCC symbol, future skip, crypto must not request the equity ticker, second bars must not call Schwab. 5 passed after the crypto and second fixes. Those two tests failed first on the old behavior.
