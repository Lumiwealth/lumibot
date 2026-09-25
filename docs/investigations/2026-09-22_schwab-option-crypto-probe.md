# Schwab option candles, BTC, and token refresh

Date: 2026-09-22. Branch: `version/4.5.92`. Read-only. No orders.

## What was saved

The login JSON was wrapped into LumiBot's token file format and stored at:

- `/Users/robertgrzesik/Development/lumibot/schwab_token.json`
- `/Users/robertgrzesik/Development/lumibot/schwab_token_probe.json`

Both are mode 600. `.gitignore` already ignores `schwab_token*.json` and `.env.local`. `id_token` was removed because schwab-py rejects it.

`/Users/robertgrzesik/Development/lumibot/.env.local` now has `SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`, `SCHWAB_BACKEND_CALLBACK_URL` (`https://api.botspot.trade/broker_oauth/schwab`), and `SCHWAB_TOKEN_PATH` pointing at `schwab_token.json`. The access token is not stored as `SCHWAB_TOKEN`. Schwab's broker init rewrites the token file from that variable on every start, which would throw away a later refresh.

Schwab returned 4 account numbers. None was written down, because picking one would be a guess. Market data does not need the account hash. The placeholder account `000` does not match, and that error is expected.

## Live results

Counts only. The raw status file is `/Users/robertgrzesik/Development/lumibot/docs/investigations/2026-09-22_schwab-option-crypto-probe-result.json`.

| Call | Result |
| --- | --- |
| SPY daily bars | HTTP-level success, 6 bars through LumiBot |
| SPY option chain | 32 expirations |
| One SPY call, direct price history with the OCC symbol | 200, 7 candles |
| Same option through LumiBot before the fix | stopped inside LumiBot, 0 bars |
| Same option through LumiBot after the fix | 6 bars |
| Instrument search and price history for `BTC` | 200, 538 daily candles on the direct history call; 6 bars through LumiBot for a 5-day request |
| `BTC/USD` and `BTCUSD` direct history | 200 with 0 candles |
| `BTC/USD` through LumiBot after the fix | 6 bars, because the request is sent as `BTC` |
| ES futures history | still not requested |

Schwab does return option candle history. The old early return in `SchwabData.get_historical_prices` was wrong for options. It is still correct for futures. Option history has to use the OCC symbol. Sending the root (`SPY`) would return the stock.

The `BTC` candles in this note are not bitcoin. A later read the same day showed ticker `BTC` is the Grayscale Bitcoin Mini Trust ETF, last price about $38, asset type EQUITY. See `/Users/robertgrzesik/Development/lumibot/docs/investigations/2026-09-22_schwab-capability-probe.md`. LumiBot no longer rewrites a crypto `BTC` or `BTC/USD` request onto that ETF ticker.

## Refresh

With the app secret loaded and the token kept in the file, the background refresher ran. The log said the token was refreshed and written back, and the refresh token was still in the file. That happened in the same sitting by marking the access token already expired. No second login.

The 30-minute re-login shows up when either of these is true:

- `SCHWAB_APP_SECRET` is missing, so Schwab rejects the refresh exchange.
- `SCHWAB_TOKEN` is set, so the next start overwrites the refreshed file with the original payload.

The refresher only runs while the Python process is alive. The next start can still refresh an expired access token as long as the secret and the refresh token are both present.

## Code

`lumibot/data_sources/schwab_data.py` now requests option history with the OCC symbol and sends `BTC` when the symbol is `BTC/USD` or `BTC-USD`. Futures still return before any history call.

Test: `tests/test_schwab_price_history_symbols.py`. It failed first on the option early return and on `BTC/USD` being sent unchanged. After the change: 3 passed.
