# Kalshi account connection

Connect a Kalshi account through LumiBot's existing broker and data-source interfaces.

Last Updated: 2026-10-26
Status: Account-read foundation; authenticated demo qualification pending
Audience: Developers implementing or testing the Kalshi adapter

## Overview

This increment adds `Kalshi(Broker)`, `KalshiData(DataSource)`, the shared native
`KalshiClient`, lazy exports, and normal credential selection. It implements
signed account balance reads and public market metadata. Strategy account reads
use `update_broker_balances()`, `get_cash()` and `get_portfolio_value()`.
Construction performs no network read and starts no order or stream workers.

Order execution, outcome-aware position/order normalization, quotes/history,
stream reconciliation, settlement and backtesting are unfinished increments.
Their normal hooks raise `NotImplementedError`; they do not return fabricated
empty accounts or simulated fills. The existing prediction-contract asset type
is the instrument owner. Trading work must resolve persistent selected-outcome
identity before adopting order code.

## Configure and run the account check

Use a separate demo account and demo API key. API keys support Ed25519 and
RSA-PSS/SHA-256; the parsed private key determines the algorithm. Configure the
following through your own secret manager or environment:

- `TRADING_BROKER=kalshi`.
- `KALSHI_API_KEY_ID`.
- `KALSHI_PRIVATE_KEY` containing PEM text, or `KALSHI_PRIVATE_KEY_PATH` pointing
  to your own protected PEM file. Inline text takes precedence.
- `KALSHI_IS_DEMO=true` (the default). Production uses a different account/key.
- Optional `KALSHI_SUBACCOUNT` and `KALSHI_EXCHANGE_INDEX`. Omitting subaccount
  preserves a restricted key's provider binding; explicit `0` remains `0`.

Run:

```shell
python -m lumibot.example_strategies.kalshi_account_check
```

The check submits no orders. It prints cash, derived holdings value, total value
and the provider's update timestamp. Successful construction or public market
access alone does not prove authentication.

`DATA_SOURCE=kalshi` is an optional override. When selected with the Kalshi
broker, both share its client. An explicitly supplied unrelated data source
remains selected; the broker does not replace it.

## Balance contract and precision

The standard Broker hook returns `(cash, holdings_value, total_value)` as floats
for compatibility. Parsing and arithmetic use `Decimal` first:

- Cash uses `balance_dollars`, with legacy `balance / 100` fallback.
- Total uses `portfolio_value / 100`; the official October 7, 2025 changelog
  defines this field as cash plus positions. Do not add cash again.
- Derived holdings value is total minus cash. Its accuracy inherits the
  legacy total's cent precision, whereas direct-member cash can have sub-cent
  precision. A small residual is not an independently measured position mark.
- Missing, invalid or non-finite fields raise a sanitized `KalshiAPIError`.
  They never become a zero-valued account.

Consecutive cash/equity requests share a snapshot for one second per broker
instance because balance has its own Basic/Advanced one-request-per-second
bucket. This is not a cross-process rate limiter. HTTP GET retries are bounded;
uncertain writes are never automatically replayed. Trading integration must
invalidate or refresh balance evidence on actual account events.

Before enabling trading, reconcile current funded-flat and open-position
examples independently, including reserved collateral, source precision,
selected subaccount and shard funds. A parser unit test or a single algebraic
identity cannot establish those account semantics.

## Caller-owned clients and resource ownership

An explicit `client=` wins over config `CLIENT`/`KALSHI_CLIENT`. A compatible
client provides `request(method, path, *, params=None, json=None,
authenticated=True)`, returning a response mapping. The provider-native client
also provides pagination and signing helpers for later adapter work. The caller
owns injected credentials and authorization. LumiBot requires no particular
hosted service, secret store or deployment architecture.

`KalshiClient` accepts an injected `http_client`. Explicit `BASE_URL` and `WS_URL`
configuration supports standard endpoints; configured URL query parameters are
preserved. Request signing covers the actual path and excludes query parameters.
The default hosts are Kalshi's documented demo/production endpoints. A custom
endpoint remains the caller's responsibility.

`broker.close()` shuts down owned resources once. Injected clients and supplied
data sources remain caller-owned. The client also supports a context manager.
No credentials, signed headers or provider error bodies are logged.

## Verification at this increment

The affected offline inventory passed 216 tests, including signing, pure balance
arithmetic, Strategy cash/equity wiring, eager/lazy credential selection,
Polymarket compatibility and CCXT derivative/startup regressions. The public demo
metadata API check passed; the signed balance check remains pending dedicated
demo credentials. No order, fill, settlement or backtest is claimed by this
account-read increment.

## Tests and remaining proof

Run affected offline contracts and the read-only API checks separately:

```shell
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_client.py tests/test_kalshi_account.py
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_apitest.py
```

The API file includes a public demo metadata check and a signed demo balance
check. The latter skips visibly when dedicated demo credentials are absent and
does not use production mode. Neither check places trades. Useful public GET
success is distinct from signed account-read proof, and both remain distinct
from fills, settlement, populated historical prices and backtests.

This foundation adapts transport, scaffolding and offline contract tests from
[the public Kalshi draft](https://github.com/Lumiwealth/lumibot/pull/1167), pinned
at `1ed4d0f3dc361de96b04c795355f2e4e7bb6b670`. Its signed-YES order model and old
balance arithmetic were not imported as working trading behavior. Its historical
test claims do not replace current qualification.

Sources: [API keys](https://docs.kalshi.com/getting_started/api_keys),
[balance](https://docs.kalshi.com/api-reference/portfolio/get-balance),
[changelog](https://docs.kalshi.com/changelog),
[rate limits](https://docs.kalshi.com/getting_started/rate_limits),
[demo](https://docs.kalshi.com/getting_started/demo_env).
