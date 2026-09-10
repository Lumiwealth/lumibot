# Kalshi live broker architecture and qualification

Standard LumiBot live trading and data contracts, with staged Demo qualification.

Last Updated: 2026-09-10
Status: Implemented; authenticated Demo qualification pending
Audience: Contributors, maintainers, and integration testers

## Overview

The integration implements the existing `Broker`, `DataSource` and `CustomStream`
contracts. It adds no asset type or public strategy method. `prediction_contract`
assets use Kalshi market tickers; buy/sell and every order/quote/bar price refer to
YES. Signed positions represent net YES (positive) and NO (negative). Polymarket
uses the same asset type but distinct outcome tokens, so its token identifiers
and wallet logic must not be copied into Kalshi.

## Boundaries

- `lumibot/tools/kalshi_client.py`: private RSA-PSS/SHA-256 signing, fixed-point
  validation, HTTP requests, pagination, bounded read retries and sanitized errors.
- `lumibot/data_sources/kalshi_data.py`: asset/quote validation, market prices,
  quotes, OHLCV normalization, history chunking and archived-market fallback.
- `lumibot/brokers/kalshi.py`: balances, signed positions, order validation,
  state parsing, submit/cancel/amend and lifecycle reconciliation. `KalshiStream`
  derives from the existing `CustomStream`.
- `lumibot/credentials.py` and lazy package exports: standard selection through
  `TRADING_BROKER=KALSHI`; exported object is `BROKER`.

The official generated Python SDK is not a dependency: its release inspected at
implementation time requires Python 3.13, while LumiBot supports 3.10 and later.
The adapter uses existing `httpx`, with direct `cryptography` and `websockets`
dependencies. Provider wire models stay as dictionaries at the private boundary;
strategies receive LumiBot objects.

## Method and endpoint map

| Existing LumiBot contract | Kalshi path (under `/trade-api/v2`) |
|---|---|
| `_get_balances_at_broker` → `get_cash`, `get_portfolio_value` | GET `/portfolio/balance` |
| `_pull_position(s)` → `get_position(s)` | GET `/portfolio/positions`, paginated |
| `_pull_broker_all_orders` → `get_orders` | GET `/portfolio/orders`, paginated |
| `_pull_broker_order` → `get_order` | GET `/portfolio/orders/{id}`, historical order pagination after 404 |
| `_submit_order` → `submit_order` | POST `/portfolio/events/orders` |
| `cancel_order`, inherited `cancel_orders` | DELETE `/portfolio/events/orders/{id}` |
| `_modify_order` → `modify_order` | POST `/portfolio/events/orders/{id}/amend` |
| Internal fill reconciliation | GET `/portfolio/fills`, historical fills when needed |
| `get_last_price`, `get_last_prices` | GET `/markets/{ticker}`; plural reads preserve failed assets as `None` |
| `get_quote` | GET `/markets/{ticker}` → LumiBot `Quote` |
| `get_historical_prices` | GET `/markets/candlesticks`, then `/historical/markets/{ticker}/candlesticks` for archived markets |
| `get_historical_prices_for_assets` | Existing `DataSource.get_bars` dispatch and per-asset errors |
| `get_chains`, `get_historical_account_value` | Empty result; unsupported provider features |

The preferred strategy `submit_order` accepts one order or a list. The broker's
inherited plural path submits independent orders concurrently. It is not atomic.
`submit_orders` and strategy `get_bars` remain existing deprecated aliases.
There is no `create_orders` or `get_quotes` addition.

## Authentication and values

Inline `KALSHI_PRIVATE_KEY` takes precedence over `KALSHI_PRIVATE_KEY_PATH`.
The timestamp is milliseconds; sign `timestamp + METHOD + full path`, excluding
query parameters. The signer uses RSA-PSS with SHA-256 and a 32-byte salt. No
private key, raw response body, signed header or unsanitized WebSocket exception
is logged. Demo is the default; explicit `KALSHI_IS_DEMO=false` selects production.
Subaccount 0 is the default; reads/writes that support account scope receive it.

Prefer `balance_dollars` when provided, otherwise divide `balance` cents by 100.
Divide `portfolio_value` cents by 100 for positions value. Return the ordinary
broker tuple `(cash, positions_value, cash + positions_value)`.

Prices use dollar `Decimal` values at the wire boundary. Quantities use 0.01
contract precision. A market's published price ranges determine valid tick
increments. Trade candles contain actual OHLCV; null-trade periods are omitted,
not synthesized from bids/asks. Bars use timezone-aware period-end timestamps
and exclude incomplete periods. The requested time window can contain fewer
traded bars than `length` on sparse markets.

## Order support and limits

Simple LIMIT orders support GTC, IOC, FOK and GTD. GTD maps to provider GTC with
a future expiration. `day` and unpriced MARKET requests fail explicitly. Stops,
trailing/smart limits, bracket/OCO/OTO/multileg, custom provider parameters,
scalar/MVE markets, and non-USD quotes are unsupported. Price modification uses
the broker's current order quantity; this API does not change quantity.

A non-marketable limit can become marketable if the market changes. An IOC can
partially fill and then cancel. A cancel/amend can race an execution. Tests and
callers must inspect fills and positions, not assume cancellation means no fill.

## Lifecycle invariants

1. A successful submit attaches the provider ID to the original LumiBot `Order`
   and emits NEW through `_process_trade_event`.
2. A transport/5xx/409 ambiguity retains the original client ID. Pending
   submissions are matched back to that original object during reconciliation.
3. Order and fill WebSocket messages enqueue an order-ID update on `CustomStream`.
   The dispatcher reads authoritative order/fill state and applies only the
   cumulative quantity/notional delta. This deliberately tolerates duplicates,
   out-of-order delivery and a lagging order view. The order and deduplicated
   fill quantities must agree before a delta is applied; either endpoint can lag.
4. REST fills are deduplicated by provider fill ID. Partial and final events carry
   incremental quantity, not cumulative quantity. Fees and sub-cent average
   prices are retained on the normal Order.
5. A canceled order with partial fills emits the fill delta before cancellation.
   A missing broad-list order is looked up directly; disappearance is not a fill
   or cancellation. Unknown states stay unknown.
6. Historical account orders are imported without callbacks for old executions.
   Use a dedicated account/subaccount for a strategy; no cross-strategy ownership
   scheme or external platform identity is added.
7. Reconnect, subscription recovery and queue overflow request REST repair.
   Both `user_orders` and `fill` acknowledgements are required before the stream
   reports subscribed, and reconnect does not reuse earlier acknowledgements.
   Periodic reconciliation continues during a WebSocket outage. API errors are
   surfaced/logged without private payloads; no mutation is blindly retried.
8. Live cash/portfolio value use the broker refresh contract. Prediction-market
   fills already bypass equity short-sale cash arithmetic in StrategyExecutor.

## Qualification layers

### Offline suite (normal deployment tests)

```text
python -m pytest tests/test_kalshi_client.py tests/test_kalshi_data.py tests/test_kalshi_broker.py tests/test_kalshi_stream.py tests/test_kalshi_credentials.py tests/test_kalshi_demo_safety.py -m "not apitest" --cov=lumibot.brokers.kalshi --cov=lumibot.data_sources.kalshi_data --cov=lumibot.tools.kalshi_client --cov-report=term-missing
```

Tests verify signing independently with the RSA public key, credential precedence,
request routing, retry boundaries, redaction, numeric validation, pagination,
historical range/archival rules, order capability matrices, signed positions,
cash arithmetic, errors, imported history, partial/final/canceled states, duplicate
fills, uncertain submit recovery, stream shutdown/reconnect, and actual Strategy
singular/plural accessors. The target is at least 90% line coverage on new modules,
with meaningful error/lifecycle branches covered. Existing broker/lifecycle tests
must remain unchanged in behavior. Only the central provider marker gate is
extended so Kalshi API tests do not require Polygon/Theta credentials.

### Demo read-only suite

Public Demo price/quote/history checks can run first without any credentials:

```text
python -m pytest tests/test_kalshi_public_apitest.py -m kalshi -v
```

Provide **separate test secrets**: `KALSHI_TEST_API_KEY_ID`,
`KALSHI_TEST_PRIVATE_KEY` or `KALSHI_TEST_PRIVATE_KEY_PATH`, and mandatory
`KALSHI_TEST_IS_DEMO=true`. There is no production-credential fallback.

```text
python -m pytest tests/test_kalshi_apitest.py -m "kalshi and not kalshi_demo_mutation and not kalshi_demo_fill" -v
```

### Demo submit/modify/cancel suite

Additionally enable `KALSHI_TEST_ENABLE_ORDER_MUTATIONS=true`:

```text
python -m pytest tests/test_kalshi_apitest.py -m kalshi_demo_mutation -v
```

The test-only market selector finds an open binary market with enough time before
close, liquidity and valid non-crossing test prices. It does not export a new
LumiBot method. `KALSHI_TEST_TICKER` is an optional troubleshooting override.
The test submits a one-contract GTC, reads it, changes price, cancels it, and
checks non-crossing IOC/FOK behavior. Cleanup is mandatory and failures are visible.
Mutation tests require the selected market to have no initial position. They
reconcile uncertain submissions, cancel outstanding test orders, and attempt to
close accidental fills with at most three IOC requests. No closing order exceeds
the current position or the total contracts opened by that test. An unresolved
submission/cancellation stops automated closing and fails with a manual-inspection
warning. These separate test environment flags are explicit safety authorization
for account mutations, not a workaround for a failing offline suite.

### Demo fill suite

Also enable `KALSHI_TEST_ENABLE_FILL=true`:

```text
python -m pytest tests/test_kalshi_apitest.py -m kalshi_demo_fill -v
```

This deliberately buys at most one Demo contract, checks fills/positions/cash and
stream subscription, then closes the resulting position. It never uses production
hosts. It skips an already-held market to preserve existing positions. An inability
to close is a test failure that requires inspecting the Demo account.

### CI and release

The existing main unit jobs exclude `apitest`, so offline coverage runs there.
The dedicated Kalshi workflow runs offline tests for Kalshi changes and offers a
manual Demo read-only/mutation/fill selection using repository environment secrets.
Secrets are not exposed to forked PRs. Demo tests cannot be called passed when
credentials are missing or the tests skip. PR/release evidence must state separately
which offline, existing-broker and Demo checks actually ran.

## Scope after this PR

Backtesting, public market discovery/books, provider order groups/batches,
advanced order emulation, OAuth and hosted platform onboarding remain separate
work. The transport, data-source and broker boundaries leave backtesting possible
later without claiming that it already works.

## Implementation verification (2026-09-10)

- Windows Python 3.12 offline contracts including the Demo safety harness:
  171 passed with 93.39% combined
  statement/branch coverage across the three new runtime modules.
- Demo-harness safety tests: 8 passed, including unknown submissions, incomplete
  cancellation, partial cleanup, unexpected inventory, and bounded retries.
- Existing Alpaca, Tradier, Polymarket, credentials and broker/strategy lifecycle
  regression selection: 184 passed, 3 skipped, 7 deselected.
- Real public Demo market-data test: 1 passed. The selected open market returned
  zero traded hourly candles; populated OHLCV normalization is verified offline,
  not yet against a populated real Demo response.
- Authenticated Demo tests: 4 skipped because separate test credentials were
  unavailable. Authentication, account values, positions, orders, authenticated
  streaming and trading are **not yet certified against a real Demo account**.
- Scoped Ruff checks passed. Sphinx generated the Kalshi page with no Kalshi-page
  warnings; the full docs build still reports pre-existing autodoc/indentation
  warnings and errors in other providers' documentation.
- Full repository CI and the Python 3.10/3.11 matrix require hosted validation.
  Do not treat this evidence as a production-trading or release qualification.

## Official sources

- [REST/OpenAPI specification](https://docs.kalshi.com/openapi.yaml)
- [WebSocket/AsyncAPI specification](https://docs.kalshi.com/asyncapi.yaml)
- [Authentication](https://docs.kalshi.com/getting_started/quick_start_authenticated_requests)
- [API environments](https://docs.kalshi.com/getting_started/api_environments)
- [Create order V2](https://docs.kalshi.com/api-reference/orders/create-order-v2)
- [Historical data](https://docs.kalshi.com/getting_started/historical_data)
- [SDK policy](https://docs.kalshi.com/sdks/overview)
