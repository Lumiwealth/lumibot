# Kalshi connection, trading and backtesting

Connect a Kalshi account through LumiBot's existing broker and data-source interfaces.

Last Updated: 2026-10-26
Status: Native demo trading, current/archive history and candle backtesting implemented
Audience: Developers implementing or testing the Kalshi adapter

## Overview

`Kalshi(Broker)`, `KalshiData(DataSource)` and the shared `KalshiClient` use the
existing credential factory, entity serializers, order trackers, position sync,
Strategy methods and fill callbacks. Construction makes no network request;
normal construction starts the existing `PollingStream` (five-second interval).
Set `connect_stream=False` for deterministic one-shot use; ordinary order/position
reads still reconcile through the existing hooks. No separate trading engine,
strategy functions or persistence service is introduced.

Implemented: signed balances, YES/NO orders and net positions, last prices,
quotes, limit orders, marketable IOC execution for market orders, order reads,
amend/cancel, fill deltas/fees, REST polling, historical Bars and completed-candle
backtesting. Full discovery, WebSocket acceleration, live settlement reconciliation
and other product paths remain later increments.

## Selected outcome identity

The existing `prediction_contract` type now accepts an optional `outcome`:

```python
yes = Asset("<market-ticker>", "prediction_contract", outcome="yes")
no = Asset("<market-ticker>", "prediction_contract", outcome="no")
```

These are distinct assets for equality, dictionary keys, orders, positions,
full/minimal JSON and the existing strategy backup codec. Outcome labels are
case-normalized and may contain generic named outcomes. No new asset class,
order method or strategy persistence system is introduced. Existing assets
without an outcome retain their previous hash, display and serialized shape;
Polymarket token identifiers continue to work unchanged.

Prediction fill-price fallback uses the quantity actually filled and retains
sub-cent precision. Position serialization preserves real zero marks and P&L.
These shared contracts are verified offline. Native execution proof is recorded
below; historical-data and simulated-execution proof are recorded separately.

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

The check submits no orders. It prints cash, holdings value, total value
and the provider's update timestamp. Successful construction or public market
access alone does not prove authentication.

`DATA_SOURCE=kalshi` is an optional override. When selected with the Kalshi
broker, both share its client. An explicitly supplied unrelated data source
remains selected; the broker does not replace it.

## Balance contract and precision

The standard Broker hook returns `(cash, holdings_value, total_value)` as floats
for compatibility. Parsing and arithmetic use `Decimal` first:

- Cash uses `balance_dollars`, with legacy `balance / 100` fallback.
- Holdings use `portfolio_value / 100`. On 2026-10-26 an authenticated demo
  account returned cash `100.0000`, `portfolio_value=0`, and the independent
  website showed cash/total of 100. Therefore current demo portfolio_value
  excludes cash. Total is cash plus holdings.
- The October 7, 2025 changelog describes that field as cash-inclusive total;
  that description disagrees with current measured demo behavior. The original
  foundation followed it and was corrected after a recorded-response regression
  reproduced negative holdings and zero total on a funded-flat account.
- Holdings inherit the cent-valued field's precision; direct-member cash may
  have sub-cent precision. Open-position valuation, reserved collateral and
  production-account semantics still need independent qualification.
- Missing, invalid or non-finite fields raise a sanitized `KalshiAPIError`.
  They never become a zero-valued account.

Consecutive cash/equity requests share a snapshot for one second per broker
instance because balance has its own Basic/Advanced one-request-per-second
bucket. This is not a cross-process rate limiter. HTTP GET retries are bounded;
uncertain writes are never automatically replayed. Trading integration must
invalidate or refresh balance evidence on actual account events.

The funded-flat account and an open three-YES-contract snapshot are verified.
The latter returned cash 98.1792, holdings 1.77 and total 99.9492 after buying
three contracts at 0.59 with 0.0508 in actual fill fees. This supports the same
cash-plus-holdings interpretation with an open position. Reserved collateral,
production semantics, restricted keys and individual nonzero pools still need
independent account qualification; do not extend this evidence to those cases.

## Trading through normal Strategy functions

```python
yes = Asset("<market-ticker>", "prediction_contract", outcome="yes")
no = Asset("<market-ticker>", "prediction_contract", outcome="no")
quote = self.get_quote(no)
order = self.create_order(no, 1, "buy", limit_price=quote.ask, time_in_force="ioc")
self.submit_order(order)
order = self.get_order(order.identifier)
orders = self.get_orders()
position = self.get_position(no)
positions = self.get_positions()
# For a resting limit order:
# self.modify_order(order, limit_price=0.40)
# self.cancel_order(order)
```

Prices are USD per selected contract; quantities are contracts in increments of
0.01. A YES buy maps to a YES-book bid; a YES sell maps to an ask. A NO buy maps
to an ask at `1 - selected_price`; a NO sell maps to a bid at that complement.
The original Asset, action and order type remain on the submitted Order.
Kalshi's directional read representation cannot reconstruct the original intent
of an order created elsewhere: such imported orders use canonical YES-book
identity and do not replay historical fills as new strategy executions.

The venue returns signed net YES exposure. Positive exposure becomes a positive
YES Position; negative exposure becomes a positive NO Position. Buying one NO
against two YES leaves one YES and no NO Position. Its fill callback retains
the real NO buy and receives a zero-quantity selected NO Position; the tracked
portfolio has already been refreshed to the actual net holding before callback
queueing. No fake closing order or simultaneous synthetic YES/NO holding is added.

`limit` uses the market's published `price_ranges`, including sub-cent grids.
`market` is implemented as an IOC limit crossing all valid grid levels; there is
no guaranteed fill or slippage cap. The Order retains type `market`. The adapter
uses actual cumulative fills, not an empty remaining count, to distinguish full
fills from canceled remainders. Native stop, trailing and bracket orders are
not available through this endpoint and produce visible errors.

Time in force supports GTC, IOC and FOK; GTD uses a future timezone-aware
`good_till_date`; DAY expires at the next midnight in America/New_York. Provider
options go through existing `custom_params`, for example `post_only`,
`cancel_order_on_pause`, `self_trade_prevention_type`, `order_group_id` and
`reduce_only`. The provider requires reduce-only to use IOC. Parameters cannot
overwrite the selected ticker, side, price, quantity, client ID or account route.
The internal `_kalshi` entry is reserved for serialized client/fill checkpoints.

Market metadata selects `exchange_index`; a conflicting explicitly configured
pool is a visible routing error. Omitted subaccount remains omitted. The adapter
never transfers funds to another pool automatically. Listed scalar/multivariate
contracts are not rejected by category; that does not claim those account and
settlement paths have been qualified.

Kalshi's V2 write acknowledgement is compact and can lead both order-list and
direct GET visibility. An accepted write stays accepted while follow-up reads
catch up. A transient 404 is not a cancellation. Each submit gets a client UUID
before POST, and uncertain writes are not automatically replayed. In-process
recovery links that UUID to the original Order even if its first visible state
is already filled. Repeated observations apply only the newly filled quantity,
selected-contract notional and actual six-decimal `fee_cost` totals. Partial
fills retain their own execution price and never divide by the requested size.

Client IDs and cumulative checkpoints survive the existing Order serializer via
`custom_params`. This does not establish a durable exactly-once restart system:
callers must retain and restore their original Orders to retain original intent.
An in-memory pending map alone cannot prove process-crash recovery or ownership
of unrelated account history.

`get_quote` derives the selected bid from its own book and the ask from the
opposite bid, complementing prices and swapping sizes for NO. Empty sides are
`None`, not zero; actual zero last prices remain zero. Quote timestamps are
receipt times unless the provider supplies a source timestamp. History is implemented below; point-in-time backtesting is the next packet.

## Historical prices

`get_historical_prices(asset, length, timestep, timeshift=...)` now returns normal
`Bars`; the existing `get_bars` dispatcher preserves plural asset reads and
per-asset failures. It keeps YES/NO as separate Asset keys. No new strategy
history function or provider-specific bar type is needed.

The current batch-candle endpoint supplies native minute/hour/day data. Older
settled markets and metadata fall back to the historical endpoints. Archive
membership follows the market's settlement, not the requested candle date;
the adapter does not split a current market's old candles using an unrelated
trade/order cutoff. Each inclusive request window stays below the aggregate
10,000-candle limit. Results are sorted, deduplicated and clipped by bar end.

History timestamps are **completed period ends**. Explicit `start`/`end` (also
`start_dt`/`end_dt`, `start_date`/`end_date`, `datetime_start`/`datetime_end`) accept
normal date/datetime inputs; naive timestamps are interpreted as UTC. `length=None`
with an explicit range loads that range; without a start it uses market open time.
Positive minute/hour/day/week multiples reuse native intervals and aggregate
with right-labelled, completed boundaries. Timedelta timeshifts and numeric
bar-count shifts move the request end. No prior-price synthetic candle is requested.

Current `*_dollars`/`*_fp` fields and archival unsuffixed decimal-string fields
have separate parsing; archive dollar strings are not divided by 100. Fractional
volume is retained. Quote-only candles keep missing trade OHLC as missing; no
midpoint or previous close is substituted for a trade. NO open/close complement
YES; NO high complements YES low and NO low complements YES high. NO bid/ask
complement and swap YES ask/bid. Actual zero values survive.

Qualification: six new recorded-response history contracts and 108 affected
offline contracts passed. Four read-only native API cases returned populated
current and archived candles in demo and public production, including YES/NO
normalization. The historical fixture uses provider-recorded shapes with neutral
identifiers. This is historical-data proof, not backtest execution proof.

## Backtesting with the same Strategy API

`KalshiBacktesting(PandasData)` reuses the same `Data`, `Bars`, `Quote`, `Order`,
`Position`, Strategy callbacks and `BacktestingBroker` used elsewhere in LumiBot.
Polymarket's data-loading pattern was the reference; Kalshi is a sibling source,
not a subclass of the concrete Polymarket adapter. No new Strategy function is
needed. `get_last_price`, `get_last_prices`, `get_quote`, `get_historical_prices`,
`get_historical_prices_for_assets`, `create_order`, `submit_order`, `get_order`,
`get_orders`, `get_position` and `get_positions` retain their normal interfaces.

Pass your existing Strategy subclass and selected assets to the normal runner:

```python
from datetime import datetime, timezone
from lumibot.backtesting import KalshiBacktesting
from lumibot.entities import Asset

yes = Asset("<historical-market-ticker>", "prediction_contract", outcome="yes")
no = Asset(yes.symbol, "prediction_contract", outcome="no")

result, strategy = YourStrategy.run_backtest(
    KalshiBacktesting,
    datetime(2026, 1, 1, tzinfo=timezone.utc),
    datetime(2026, 1, 2, tzinfo=timezone.utc),
    assets=[yes, no],
    timestep="hour",
    market="24/7",
    budget=100,
    benchmark_asset=None,
)
```

Replace the ticker and dates with a market's actual historical interval. Public
market reads need no trading key. The source defaults to the demo market API;
`config={"IS_DEMO": False}` selects Kalshi's public production market API without
placing real orders. An existing `KalshiData` can instead be passed as
`kalshi_data=` and remains caller-owned. `BACKTESTING_DATA_SOURCE=kalshi` selects
this class through the existing environment override; as usual, an environment
selection takes precedence over the class argument.

The `assets` list preloads only the requested backtest range. For earlier signal
warm-up, prepare ordinary `Data` objects containing that earlier history and
pass `pandas_data=`. Supply their matching `market_metadata=` mapping for a fully
offline replay. All asset identity keys retain the selected outcome. An unloaded
asset or empty history stays empty and does not break unrelated loaded assets.

### Clock, fills, positions and settlement

- Completed period-end candles become visible only when the simulated clock
  reaches their timestamp. Source frames retain gaps and missing trade OHLC;
  Pandas forward-fill repair is not used. Quotes use observed candle-end bid/ask;
  last price uses the most recent observed trade close. They never fetch today's
  live quote during replay. Coarser requests aggregate only completed windows;
  requests finer than the loaded interval retain the available coarse data and
  do not synthesize ticks.
- Market buys use the historical ask and sells use the bid. A limit fills only
  if it crosses that observed quote. Missing or stale interval quotes produce
  no execution. IOC/FOK remainder cancellation and DAY/GTD expiry reuse existing
  broker behavior. Reduce-only IOC quantity is capped by the holding it closes,
  preserving the requested quantity, partial-fill event and canceled remainder.
- Post-only requires a non-crossing limit at submission. A later crossing is
  estimated to fill at its resting limit. Candle data has no order queue or
  depth: full executable quantity and passive fills are **simulation assumptions**,
  not a claim of exchange matching or available historical size.
- Buying the opposite outcome offsets the existing holding, redeeming the
  matched YES/NO pair for $1. Selling beyond the held quantity posts the remaining
  collateral and opens the opposite holding. Netting finishes before callbacks;
  a selected-outcome callback may carry zero while the remaining holding is on
  the other outcome. Cash collateral and fee checks stay in BacktestingBroker.
- Market close stops fills and cancels resting orders. Settlement waits for the
  provider's actual `settlement_ts`, not `close_time` or the backtest end. Before
  then, metadata hides the result and payout. When the clock reaches settlement,
  selected payouts use the existing cash owner and `CASH_SETTLED` lifecycle;
  nonzero payouts also enter existing cash-event history as `other_cash` with
  `is_external_cash_flow=False`. Zero payouts still retire the position with a
  terminal lifecycle record. Scalar payout values between zero and one are
  preserved. Missing settlement timing leaves the contract unresolved.

The source supplies optional internal fill-price, fill-quantity, fee,
opposite-contract and lifecycle hooks to the existing BacktestingBroker. Other
sources keep their current execution paths and configured fees. There is no
second matching engine, account ledger, entity schema or migration.

### Fees and historical fidelity

Default fee estimates use `fee_rate="0.07"`, `maker_fee_rate="0"` and
`balance_precision="0.0001"`. A non-post-only fill is estimated as a taker even
when submitted as a limit. The model rounds the quadratic fee up to six decimal
places, then aligns signed revenue minus fee down to the balance precision.
Configure these values for the account and series; `balance_precision="0.01"`
models cent-aligned balances. Explicit normal Strategy `TradingFee` lists
override this provider estimate. Regression cases match the recorded native
whole-contract and fractional fees, but this is **not** a complete reconstruction
of historical fee schedules, event overrides, rebates or per-order rounding
accumulators.

Today's metadata is not a complete historical catalog or rule-version archive.
Mutable price/volume fields and future result fields are withheld during replay;
historical discovery, rule changes, cross-market/event collateral offsets,
full-depth liquidity and queue priority need separate data and qualification.
Choose dates and assumptions accordingly and report those limits with results.
No synthetic resolution candle or end-of-test payout is inserted.

Backtest qualification: 489 affected offline contracts passed, including existing
Polymarket, entity/serialization, Pandas, broker execution and cash-financing
coverage. Five read-only native checks passed: populated current/archive history
in demo/public production plus a complete normal Strategy backtest loading real
archived candles and making a simulated fractional fill. Separate deterministic
runner coverage verifies netting, actual-time settlement and cash-event history.
Native data is real; backtest orders and payouts are simulated.

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

## Verification history

The corrected affected offline inventory passed 217 tests, including signing, pure balance
arithmetic, Strategy cash/equity wiring, eager/lazy credential selection,
Polymarket compatibility and CCXT derivative/startup regressions. The public demo
metadata API check passed. Subsequent signed demo reads exposed and corrected
the balance-field discrepancy described above; the standard Strategy account
check now reports cash 100, holdings 0 and total 100, matching the website. No order, fill, settlement or backtest is claimed by this
account-read increment.

## Tests and remaining proof

Offline coverage includes the selected-contract direction matrix, dynamic ticks,
provider options, partial fill deltas, net-position callback ordering, duplicate
reads, serialized checkpoints, held NEW events and uncertain submit recovery.
Provider response shapes in `tests/fixtures/kalshi/trading_responses.json` were
recorded from the demo API and neutralized; they are not invented database rows.

```shell
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_client.py tests/test_kalshi_account.py tests/test_kalshi_trading.py tests/test_prediction_contract_identity.py
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_apitest.py
# Public market reads and simulated orders only; no API keys required.
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_history.py tests/test_kalshi_backtesting.py
LUMIBOT_DISABLE_DOTENV=1 BACKTESTING_DATA_SOURCE=none python -m pytest tests/test_kalshi_history_apitest.py tests/test_kalshi_backtesting_apitest.py
# Explicit demo writes: dedicated demo keys plus a funded, liquid test ticker.
KALSHI_TEST_MARKET='<demo-market-ticker>' python -m pytest tests/test_kalshi_trading_apitest.py
```

Both the whole-contract and fractional-contract execution cases passed against
the real demo API. Each completed five fills with five callbacks, including
netting/flip and return to flat. The last fractional case returned cash 98.5207,
holdings zero and total 98.5207; the website independently displayed 98.52 and
no positions. Holdings are supplied in cents by the balance endpoint: the
fractional open snapshot reported 0.44 for 0.75 contracts marked at 0.59, so
account equity inherits that provider valuation precision. This is not an exact
sub-cent independent mark model.

The execution API test fixes the host to Kalshi demo, starts its selected market
flat and cleans up its own orders/position. It verifies amend/cancel plus YES buy,
YES sell, NO buy, netting/flip, NO market sell, exact callback count and return to
flat. It does not move funds or touch a production account. Missing credentials
or an explicit test market skip visibly. Functional reconciliation waits have
no elapsed-time acceptance threshold; real errors/interruption remain failures.

Remaining proof includes native partial/cancel races, FOK/GTD/DAY, nonzero
subaccounts/pools, restricted keys, process-crash recovery, WebSockets, live settlement,
scalar/multivariate native permutations and advanced historical execution fidelity. Passing
replayed wire contracts does not certify those native paths or a release.

This foundation adapts transport, scaffolding and offline contract tests from
[the public Kalshi draft](https://github.com/Lumiwealth/lumibot/pull/1167), pinned
at `1ed4d0f3dc361de96b04c795355f2e4e7bb6b670`. Its signed-YES order model was adapted to the selected-contract identity and
normal shared reconciliation hooks described above. Its
balance arithmetic agrees with current demo evidence; the initial contrary
interpretation of the older changelog was corrected after authenticated testing. Its historical
test claims do not replace current qualification.

Sources: [API keys](https://docs.kalshi.com/getting_started/api_keys),
[balance](https://docs.kalshi.com/api-reference/portfolio/get-balance),
[changelog](https://docs.kalshi.com/changelog),
[rate limits](https://docs.kalshi.com/getting_started/rate_limits),
[demo](https://docs.kalshi.com/getting_started/demo_env).

Trading sources: [V2 create](https://docs.kalshi.com/api-reference/orders/create-order-v2),
[V2 amend](https://docs.kalshi.com/api-reference/orders/amend-order-v2),
[direction](https://docs.kalshi.com/getting_started/order_direction),
[orders](https://docs.kalshi.com/api-reference/orders/get-order),
[fills](https://docs.kalshi.com/api-reference/portfolio/get-fills),
[positions](https://docs.kalshi.com/api-reference/portfolio/get-positions).

History and simulation sources:
[current batch candles](https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks),
[archived candles](https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks),
[archive routing](https://docs.kalshi.com/getting_started/historical_data),
[fee rounding](https://docs.kalshi.com/getting_started/fee_rounding),
[series fee changes](https://docs.kalshi.com/api-reference/exchange/get-series-fee-changes),
[event fee changes](https://docs.kalshi.com/api-reference/events/get-event-fee-changes).
