# Polymarket Broker Integration Game Plan

One-line description: Research and implementation plan for adding Polymarket prediction-market trading to LumiBot without changing implementation code yet.

Last Updated: 2026-07-01

Status: Superseded by the international CLOB implementation/proof documents. Implementation code has now changed.

Audience: LumiBot maintainers, broker-adapter implementers, BotSpot integration engineers

## 2026-07-01 Update: Current Account Evidence Changes The First Target

Follow-up Chrome inspection of Rob's logged-in Polymarket session showed `https://polymarket.com/`, a wallet-address account surface, crypto-style deposit/cash balances, and an `APIs` menu item. That looks like the international `polymarket.com` CLOB/deposit-wallet flow, not the separate `polymarket.us` account/API-key flow.

This document is still useful as the first-pass architecture plan, but its recommendation to start with the US SDK is stale for Rob's currently visible account. For current-account testing, use the deeper follow-up plan:

- `docs/investigations/2026-07-01_POLYMARKET_DEEP_INTEGRATION_RESEARCH.md`
- `docs/investigations/2026-07-01_POLYMARKET_INTERNATIONAL_CLOB_LIVE_PROOF.md`

The product-facing BotSpot decision may still favor `polymarket.us` later, especially for US users, but the immediate LumiBot spike should verify the `polymarket.com` CLOB/relayer/private-key requirements first.

## Overview

Polymarket can fit LumiBot's broker/data-source model, but it should not be treated as a stock broker clone. A Polymarket trade is still an order against a tradable asset, a position is still a quantity of outcome shares, and account value is still cash plus marked positions. The structural difference is that the tradable asset is an outcome contract inside a prediction market, priced between roughly 0 and 1 in USD collateral, with settlement at 0 or 1 after market resolution.

The recommended architecture is:

1. Add a first-class prediction contract asset type in LumiBot.
2. Implement a `PolymarketData` data source that owns market discovery, order-book quotes, last prices, and historical price series.
3. Implement a `Polymarket` broker adapter that owns account balances, positions, order submission, cancellation, order parsing, and private order updates.
4. Start with the US Polymarket SDK/API path as the default for Rob/BotSpot US users, while designing the adapter so an international CLOB implementation can be added behind the same LumiBot surface later.
5. Keep backtesting honest: use real Polymarket price/trade history only. Do not synthesize missing bars or carry prices forward to make a strategy run.

This is a moderate-to-large broker integration, not a one-file adapter. The first usable milestone can be fairly small if it is scoped to US Polymarket limit/market orders, balances, positions, open orders, and quotes.

## Primary Sources Reviewed

### LumiBot repo sources

- `lumibot/brokers/broker.py`: base live broker contract, order/position sync, order event processing, stream/polling integration.
- `lumibot/brokers/alpaca.py`: gold-standard adapter for auth config, data-source ownership, balances, position parsing, order mapping, order submission, streaming/polling fallback, and quote passthrough.
- `lumibot/brokers/tradier.py`: gold-standard adapter for explicit broker errors, token-file durability, polling stream, order parsing, order-side mapping, order submit/cancel/modify, and account balances.
- `lumibot/brokers/example_broker.py`: broker skeleton. Useful checklist only, not enough to copy directly.
- `lumibot/data_sources/data_source.py`: required data-source methods: `get_chains`, `get_historical_prices`, `get_last_price`, `get_quote`, plus batch `get_bars`.
- `lumibot/data_sources/example_broker_data.py`: data-source skeleton. Useful checklist only.
- `lumibot/data_sources/alpaca_data.py`: market-data client separation and multi-symbol batching pattern.
- `lumibot/data_sources/tradier_data.py`: quote, last-price, historical-bar, and chain mapping pattern.
- `lumibot/entities/asset.py`: current asset types do not include prediction markets.
- `lumibot/entities/order.py`: order classes, sides, types, statuses, custom params, and serialization.
- `lumibot/entities/position.py`: position quantity/value fields and broker-attached mark data.
- `lumibot/entities/quote.py`: normalized quote shape with price, bid, ask, mid, sizes, timestamp, and raw data.
- `docs/BROKER_ORDER_SEMANTICS.md`: read-path resilience policy and live broker behavior principles.
- `docs/LIVE_ORDER_POSITION_REFRESH.md`: live strategy accessor freshness and broad-order-list reconciliation safety.
- `docs/BACKTESTING_ARCHITECTURE.md`: no fabricated data, live-broker realism first, and backtesting data-source responsibilities.
- `docsrc/brokers.alpaca.rst`, `docsrc/brokers.tradier.rst`, `docsrc/brokers.rst`, `docsrc/lumibot.data_sources.rst`: public docs patterns.
- `lumibot/credentials.py`, `docs/ENV_VARS.md`, `docsrc/environment_variables.rst`: broker config, env var, and auto-detection patterns.

### Polymarket sources

- Polymarket developer docs home: `https://docs.polymarket.com/`
- Polymarket API introduction: `https://docs.polymarket.com/api-reference/introduction`
- Polymarket authentication: `https://docs.polymarket.com/api-reference/authentication`
- Polymarket rate limits and geographic restrictions: `https://docs.polymarket.com/api-reference/rate-limits`
- Polymarket Python tooling: `https://docs.polymarket.com/dev-tooling/python`
- Polymarket CLOB order creation: `https://docs.polymarket.com/trading/orders/create`
- Polymarket REST order placement reference: `https://docs.polymarket.com/api-reference/trade/post-a-new-order`
- Polymarket markets endpoint reference: `https://docs.polymarket.com/api-reference/markets/list-markets`
- Polymarket price history endpoint reference: `https://docs.polymarket.com/api-reference/markets/get-prices-history`
- Polymarket portfolio positions reference: `https://docs.polymarket.com/api-reference/positions/get-current-positions-for-a-user`
- Polymarket official Python SDK repo: `https://github.com/Polymarket/py-sdk`
- Polymarket US SDK docs: `https://docs.polymarket.us/api-reference/sdks/introduction`
- Polymarket US Python quickstart: `https://docs.polymarket.us/api-reference/sdks/python/quickstart`
- Polymarket US Python orders docs: `https://docs.polymarket.us/api-reference/sdks/python/orders`
- Polymarket US Python portfolio docs: `https://docs.polymarket.us/api-reference/sdks/python/portfolio`
- Polymarket US Python WebSocket docs: `https://docs.polymarket.us/api-reference/sdks/python/websocket`

## LumiBot Broker Architecture Findings

### Required broker adapter methods

Every live broker must implement:

- `cancel_order(order)`: explicit state-changing broker cancel request.
- `_modify_order(order, limit_price=None, stop_price=None)`: explicit broker modify request, or raise if unsupported.
- `_submit_order(order)`: convert LumiBot order to provider order, submit, set broker id, set status, retain raw payload.
- `_get_balances_at_broker(quote_asset, strategy)`: return `(cash, positions_value, portfolio_value)`.
- `get_historical_account_value()`: return historical account value payload or a documented empty result if provider does not support it.
- `_get_stream_object()`, `_register_stream_events()`, `_run_stream()`: either real streaming or polling stream integration.
- `_pull_positions(strategy)`, `_pull_position(strategy, asset)`: return normalized `Position` objects.
- `_parse_broker_order(response, strategy_name, strategy_object=None)`: convert raw broker order into LumiBot `Order`.
- `_pull_broker_order(identifier)`, `_pull_broker_all_orders()`: direct order lookup and broad order list.

The base `Broker` already handles higher-level sync, order reconciliation, event dispatch, strategy callbacks, and local trackers once provider methods return normalized entities.

### Read-path resilience versus mutation failures

Current docs and code draw an important distinction:

- Position/order/balance reads should tolerate unfamiliar broker rows where possible. Unknown but representable rows should become `UNKNOWN` enum values with raw metadata, warnings, or skipped rows only when truly unrepresentable.
- Submit, cancel, and modify should fail loudly. They should not silently no-op because a local order looks terminal.

Polymarket should follow this exact split. For example, an unknown order status returned by Polymarket should not crash account refresh, but a failed cancel should raise a `LumibotBrokerAPIError` with a sanitized provider message.

### Data source responsibilities

A Polymarket data source must implement:

- `get_quote(asset, quote=None, exchange=None)`: best bid, best ask, mid, last price, timestamp, sizes, raw payload.
- `get_last_price(asset, quote=None, exchange=None)`: last traded price or last provider mark for the outcome contract.
- `get_historical_prices(asset, length, timestep, timeshift=None, ...)`: real historical price bars only, if the API provides enough data.
- `get_chains(asset, quote=None)`: probably not applicable. Return `{}` or raise a clear unsupported error unless we later model "markets and outcomes" as an option-chain-like surface.

Because Polymarket order books are the most realistic live execution source, `get_quote()` is more important than `get_historical_prices()` for the first live implementation.

## Polymarket API Findings

### There are two Polymarket integration paths

There are now two relevant Polymarket API families:

1. `polymarket.com` international APIs:
   - Gamma Markets API for event and market metadata.
   - Data API for public price/trade/position-style data.
   - CLOB API for order book, orders, trades, balances, allowances, and authenticated trading.
   - Authentication has distinct public, API-key, and wallet-signature levels.
   - Python docs currently point to the unified `polymarket-client` package and keep `py-clob-client-v2` available.
   - Docs list geographic restrictions, including that users in the United States are blocked from placing orders on `polymarket.com`.

2. `polymarket.us` US APIs:
   - Separate SDK-oriented API surface.
   - API keys are generated from account settings after sign-in and identity verification.
   - Python package is `polymarket-us`, imported as `polymarket`.
   - SDK surfaces include orders, portfolio, users, prices, WebSocket, and markets.
   - Portfolio docs expose balances, positions, trade history, and related account data.
   - WebSocket docs include public and private subscription patterns.

For BotSpot and Rob's local US workflow, the practical default should be the Polymarket US API/SDK. The international CLOB path should be a second adapter mode or later implementation layer, not the initial default.

### International CLOB model

The international CLOB model is useful for understanding core semantics:

- A market has one or more outcomes.
- Tradable outcome tokens are identified by token ids.
- Orders are CLOB orders against a token id, side, price, size, time-in-force, and owner credentials.
- Prices are probabilities/collateral prices between 0 and 1.
- The CLOB API has endpoints for markets, books, prices, order creation, open orders, order cancel, balances, allowances, and private order/trade data.
- Authentication for private trading uses API keys and wallet-derived signing material.

This model maps cleanly to a LumiBot "prediction contract" asset where the asset is one outcome token, not the entire event.

### Polymarket US model

The US SDK model looks more like the first implementation target:

- Users create API keys from Polymarket account settings after KYC/verification.
- SDK client configuration takes API key credentials.
- Orders are created, listed, canceled, and inspected through the SDK.
- Portfolio functions expose account balances and positions.
- Price and market functions expose current prices and market metadata.
- WebSockets can stream public market updates and private account/order updates.

This aligns with LumiBot's current broker architecture without introducing wallet-private-key signing into public LumiBot code.

## Recommended LumiBot Domain Model

### Add a prediction-contract asset type

Do not shoehorn Polymarket into `stock` or `crypto`. It may work for one smoke test, but it will create avoidable ambiguity in serialization, routing, docs, and future venues.

Recommended new asset type:

```python
Asset.AssetType.PREDICTION_CONTRACT = "prediction_contract"
```

Why `prediction_contract` rather than `prediction_market`:

- The tradable asset is an outcome share or contract, such as YES on one market.
- A market/event can contain multiple tradable outcomes.
- Positions are per outcome contract.
- Orders are submitted against one contract id or token id.
- This generalizes to Polymarket, Kalshi, PredictIt-style markets, and other venues.

Initial representation:

```python
Asset(
    symbol="<provider-contract-id-or-token-id>",
    asset_type=Asset.AssetType.PREDICTION_CONTRACT,
    precision="0.000001",
)
```

Provider display metadata should be attached by broker/data-source raw payloads and downstream snapshots, not required in the first constructor. Longer term, we may want a generic metadata field on `Asset` because prediction contracts need human-readable market slug, event title, outcome label, condition id, token id, expiration/resolution time, venue, and collateral currency. That is larger than the first adapter and should be handled carefully because `Asset.to_dict()` is a shared serialization contract.

### Asset identity rules

The canonical `Asset.symbol` should be the provider's stable tradable contract identifier:

- International Polymarket CLOB: outcome token id.
- Polymarket US: SDK/API contract id or equivalent stable order-book contract identifier.
- Future Kalshi: contract/ticker id.
- Future PredictIt: contract id.

Human-friendly names should come from helper methods and raw metadata:

- `market_slug`
- `event_slug`
- `question`
- `outcome`
- `venue`
- `condition_id`
- `token_id`
- `market_id`
- `expires_at`
- `resolution_status`

This avoids breaking broker lookups when human-readable slugs change.

### Quote asset

Use USD/USDC as the quote side:

```python
quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
```

For international CLOB, the collateral is USDC on Polygon, but LumiBot account values and BotSpot user displays should still normalize to USD unless we intentionally expose USDC as a cash asset. For US Polymarket, the SDK appears to expose account balances directly in cash terms.

### Order semantics

The initial support matrix should be intentionally small:

Supported in milestone 1:

- Simple orders only.
- `BUY` and `SELL`.
- `MARKET` if provider SDK supports market-style order helpers, or implemented as provider-recommended aggressive limit/FOK semantics.
- `LIMIT`.
- Time in force mapped only where provider supports it, such as GTC/FOK/FAK or SDK equivalents.
- Decimal shares/contract quantities as allowed by the provider.

Explicitly unsupported in milestone 1:

- Shorting or sell-short style orders.
- Bracket/OCO/OTO.
- Stop, stop-limit, trailing stop, smart-limit.
- Multi-leg.
- Cross-market spread packages.
- Automatic resolution/settlement callbacks beyond position refresh.

If unsupported order shapes are submitted, the broker should raise or surface the provider/LumiBot error. Do not add regex routing, hidden tool guards, or fake order success paths.

### Position semantics

Each held outcome share maps to one `Position`:

- `asset`: prediction contract asset.
- `quantity`: outcome shares/contracts.
- `avg_fill_price`: average entry price when available.
- `current_price`: mark/last/mid price from provider.
- `market_value`: `quantity * current_price`.
- `pnl`: provider-reported PnL if available, otherwise derived from cost basis only when reliable.
- `raw_broker_payload`: raw provider position row for diagnostics.

At resolution, a settled winning contract is worth 1 and a losing contract is worth 0. The initial adapter can rely on provider positions/balances to reflect settlement. Later, LumiBot backtesting can model resolution explicitly if we have reliable historical resolution data.

### Account value semantics

`_get_balances_at_broker()` should return:

- `cash`: available cash/collateral.
- `positions_value`: marked value of all open prediction positions.
- `portfolio_value`: cash plus positions value.

Do not return `0` when balance refresh fails. Follow existing live-refresh behavior and return `None` or raise depending on the adapter path so strategy accessors can treat balance as unavailable.

## Proposed File-Level Implementation Plan

### Phase 0: Dependency and product-path decision

Before implementation, decide whether milestone 1 targets:

1. Polymarket US only.
2. International Polymarket CLOB only.
3. One public LumiBot `Polymarket` class with internal `mode="us"` and `mode="clob"` implementations.

Recommendation: choose option 3 for public API shape, but implement only `mode="us"` first.

Reason:

- It keeps user-facing LumiBot imports stable: `from lumibot.brokers import Polymarket`.
- It avoids blocking the US product path on wallet-signing details.
- It leaves room for non-US users and future CLOB support without a separate public broker name.

### Phase 1: Entity and exports

Files likely touched:

- `lumibot/entities/asset.py`
- `tests/test_asset.py`
- `lumibot/brokers/__init__.py`
- `lumibot/data_sources/__init__.py`

Work:

- Add `Asset.AssetType.PREDICTION_CONTRACT`.
- Verify constructor validation, equality, hashing, `to_minimal_dict()`, `to_dict()`, and `from_dict()` behavior.
- Add unit tests for serialization and round-trip reconstruction.
- Export `Polymarket` and `PolymarketData` lazily once adapter files exist.

Open design choice:

- Whether to add a generic `metadata` field to `Asset`. Recommendation: do not add it in milestone 1 unless serialization consumers need it immediately. Store provider metadata on raw broker/data payloads and normalized quote/position fields first.

### Phase 2: PolymarketData

New file:

- `lumibot/data_sources/polymarket_data.py`

Responsibilities:

- Initialize SDK/client from config.
- Resolve human inputs to contract ids where possible.
- Fetch market/event metadata.
- Fetch current order book or best bid/ask.
- Fetch current/last price.
- Fetch historical price data when available.
- Return normalized `Quote` and `Bars`.

Minimum methods:

- `__init__(config=None, mode="us", ...)`
- `_resolve_contract(asset)`
- `get_quote(asset, quote=None, exchange=None)`
- `get_last_price(asset, quote=None, exchange=None)`
- `get_historical_prices(asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs)`
- `get_chains(asset, quote=None)`

Historical bars:

- Use Polymarket's price-history endpoint or US SDK equivalent where available.
- Build OHLC bars only from real observed price points.
- If only sparse trade prices exist, aggregate real points into bars. Empty intervals should remain missing, not filled.
- If the API cannot provide enough historical data for the requested asset/timestep, return `None` or empty bars with a clear warning.

Quote behavior:

- For marketable buy, realistic execution price is ask.
- For marketable sell, realistic execution price is bid.
- Mid is useful for mark value, not guaranteed fill price.

### Phase 3: Polymarket broker

New file:

- `lumibot/brokers/polymarket.py`

Core implementation:

- Accept config or explicit kwargs.
- Default to `mode="us"`.
- Construct/share `PolymarketData`.
- Implement balances from SDK portfolio/balance endpoint.
- Implement positions from SDK portfolio/positions endpoint.
- Implement open/all order reads.
- Implement direct order lookup.
- Implement order parser.
- Implement submit/cancel.
- Implement modify as cancel-and-replace only if provider supports it safely, otherwise raise `NotImplementedError` with a clear message.
- Implement polling stream first. Add WebSocket private stream after base correctness is proven.

Suggested class shape:

```python
class Polymarket(Broker):
    NAME = "Polymarket"

    def __init__(
        self,
        config=None,
        mode="us",
        data_source=None,
        connect_stream=True,
        polling_interval=5.0,
        max_workers=5,
    ):
        ...
```

Config shape:

```python
POLYMARKET_CONFIG = {
    "MODE": os.environ.get("POLYMARKET_MODE", "us"),
    "API_KEY": os.environ.get("POLYMARKET_API_KEY"),
    "API_SECRET": os.environ.get("POLYMARKET_API_SECRET"),
    "API_PASSPHRASE": os.environ.get("POLYMARKET_API_PASSPHRASE"),
    "PRIVATE_KEY": os.environ.get("POLYMARKET_PRIVATE_KEY"),
    "FUNDER": os.environ.get("POLYMARKET_FUNDER"),
    "CHAIN_ID": os.environ.get("POLYMARKET_CHAIN_ID"),
    "BASE_URL": os.environ.get("POLYMARKET_BASE_URL"),
}
```

Only include mode-specific keys in docs once verified against the actual SDK constructors. Do not expose private keys or wallet material in logs, docs, snapshots, or errors.

### Phase 4: Credentials and runtime selection

Files likely touched:

- `lumibot/credentials.py`
- `docs/ENV_VARS.md`
- `docsrc/environment_variables.rst`

Work:

- Add `POLYMARKET_CONFIG`.
- Add `TRADING_BROKER=polymarket` selection.
- Add optional auto-detect only when explicit Polymarket env vars are present.
- Add `DATA_SOURCE=polymarket` only after `PolymarketData` can function independently.

Recommendation:

- Require explicit `TRADING_BROKER=polymarket` for the first release. Do not let Polymarket auto-detect outrank existing brokers just because an API key exists in a shared environment.

### Phase 5: Tests

Unit tests:

- Asset type validation and serialization.
- Data-source quote normalization from mocked SDK payloads.
- Historical price aggregation from mocked sparse price-history payloads.
- Position parsing from mocked portfolio payloads.
- Balance parsing.
- Order submit mapping for buy/sell, market/limit, time-in-force.
- Unsupported order shapes raise helpful errors.
- Cancel sends explicit broker request even if local status is terminal.
- Unknown statuses map to `Order.OrderStatus.UNKNOWN` with raw payload retained.
- Order-list miss behavior remains compatible with base `Broker` reconciliation.

API tests:

- Add `pytest.mark.apitest` tests that are skipped unless real Polymarket test credentials are configured.
- Start read-only: balance, positions, market metadata, quote, last price.
- Then paper/sandbox or smallest safe order flow if Polymarket US provides a sandbox.
- If no sandbox exists, do not add live order tests that can spend real money by default. Keep destructive/live order tests behind existing apitest conventions plus explicit credential/safety gating.

Docs tests:

- Public broker docs page.
- Environment variables page.
- Example strategy with no real secrets and no real market recommendation.

## Order Mapping Details

### LumiBot to Polymarket

| LumiBot field | Polymarket meaning |
| --- | --- |
| `order.asset.symbol` | Provider contract id or token id |
| `order.asset.asset_type` | `prediction_contract` |
| `order.side` | Buy or sell outcome shares |
| `order.quantity` | Number of outcome shares/contracts |
| `order.limit_price` | Limit price between 0 and 1 |
| `order.order_type` | Provider market or limit equivalent |
| `order.time_in_force` | Provider GTC/FOK/FAK or SDK equivalent |
| `order.custom_params` | Advanced provider params, only passed through when documented |

### Polymarket to LumiBot

| Provider field | LumiBot field |
| --- | --- |
| order id | `Order.identifier` |
| contract/token id | `Order.asset.symbol` |
| side | `Order.side` |
| original size | `Order.quantity` |
| remaining/filled size | raw payload plus status updates |
| price | `Order.limit_price` or `Order.avg_fill_price` |
| status | `Order.status` |
| created/updated timestamps | `broker_create_date`, `broker_update_date` |
| raw order row | `order.update_raw(response)` |

Status mapping should be conservative. Known active states should map to `SUBMITTED`, `OPEN`, or `PARTIALLY_FILLED`. Known terminal success maps to `FILLED`. Canceled maps to `CANCELED`. Rejections/errors map to `ERROR`. Unknown states map to `UNKNOWN` and should not crash refresh.

## Backtesting Plan

Backtesting should be a second milestone, not a blocker for first live broker support.

Minimum honest backtesting:

- Use real Polymarket historical price points only.
- Aggregate real observed points into OHLC bars.
- Use bid/ask snapshots only if historical book data is genuinely available.
- If no data exists for a timestep, leave it missing.
- Market orders can fill against ask/bid only when quote data exists for the simulated time.
- Limit orders can fill only when observed price/book data supports the fill.

Do not:

- Fill every minute by carrying forward the last Polymarket price.
- Use midpoint as a guaranteed execution price.
- Treat a resolved/closed market as open.
- Invent volume.
- Invent OHLC ranges from one stale price.

Later enhancements:

- Resolution-aware settlement in backtests.
- Market close/resolution calendars per contract.
- Historical order-book replay if Polymarket exposes it.
- Prediction-market-specific slippage model.

## Structural Issues and Decisions

### 1. New asset class is warranted

Prediction contracts are not stocks, options, futures, forex, or crypto. They are cash-settled event outcome contracts. A first-class `prediction_contract` type is the cleanest path and makes future venues easier.

### 2. International and US Polymarket must not be conflated

The international API uses CLOB/wallet-signing concepts and is geographically restricted for US order placement. Polymarket US has a separate SDK/API-key model. BotSpot's US-facing path should start with Polymarket US.

### 3. Market identity needs a contract-id-first model

User-friendly slugs are great for discovery and logs, but order placement must use stable provider contract/token ids. The `Asset.symbol` should be canonical and stable.

### 4. Market data is quote/order-book first

For live trading, best bid/ask is the correct execution reference. Last price is useful but insufficient for marketable order fills.

### 5. Settlement is different from normal mark-to-market

Positions eventually settle to 0 or 1. Live adapter can rely on provider account state first. Backtesting settlement should be implemented only when reliable resolution data is available.

### 6. Account values are straightforward but must be precise

Cash, positions value, and portfolio value map cleanly. The danger is failure handling. Do not write `0` on failed balance refresh.

### 7. Modify support may not exist

If Polymarket does not support native modify, implement `cancel_order()` and require users to submit a replacement. Do not silently emulate modify unless cancel-and-replace semantics are explicit and safe.

### 8. Rate limits need batching and polling discipline

Data-source batching and polling intervals should follow existing broker patterns. A private WebSocket stream is useful later, but a polling stream is enough for first correctness.

### 9. Regulatory/compliance scope is product-relevant

Prediction markets have jurisdiction and eligibility constraints. LumiBot should expose provider errors clearly. BotSpot should enforce user/account eligibility outside LumiBot as product/security controls, not hidden runtime tool guards.

## Milestone Sequence

### Milestone 1: Read-only Polymarket US adapter

Goal: instantiate broker/data source and read account/market data safely.

Deliverables:

- `Asset.AssetType.PREDICTION_CONTRACT`.
- `PolymarketData` with quote, last price, market metadata, and minimal historical prices if available.
- `Polymarket` broker with balances and positions.
- `TRADING_BROKER=polymarket` explicit config.
- Unit tests for parsing and config.
- Public docs with setup and limitations.

### Milestone 2: Basic order lifecycle

Goal: submit and cancel simple orders.

Deliverables:

- Simple buy/sell limit orders.
- Market orders if SDK/provider supports them cleanly.
- Open order list and direct order lookup.
- Polling stream that updates local orders.
- Unsupported shape errors for stops, brackets, shorts, multileg, and modify.
- Mocked unit tests for order mapping and status mapping.

### Milestone 3: Private WebSocket and API smoke

Goal: reduce polling lag and prove real provider integration.

Deliverables:

- Private WebSocket order/account updates if SDK supports them.
- Read-only apitests.
- Safe sandbox or manual live-order runbook if no sandbox exists.
- Clear skip reasons when credentials or sandbox are unavailable.

### Milestone 4: Backtesting support

Goal: realistic historical prediction-market testing, not fake fill success.

Deliverables:

- Historical price endpoint integration.
- Real-only bar aggregation.
- Backtesting fill rules for prediction contracts.
- Settlement modeling only after reliable resolution data is available.
- Regression tests proving missing data does not get fabricated.

### Milestone 5: Generalize for Kalshi/PredictIt

Goal: make prediction markets a provider family.

Deliverables:

- Shared helper for prediction-contract asset construction and metadata.
- Provider-neutral docs and examples.
- Venue-specific brokers/data sources.
- Optional market discovery helpers.

## Initial Example Shape

This is conceptual only. Do not use until the adapter exists.

```python
from lumibot.brokers import Polymarket
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader


class PredictionMarketStrategy(Strategy):
    def initialize(self):
        self.set_market("24/7")
        self.sleeptime = "5M"
        self.contract = Asset(
            symbol="provider-contract-id",
            asset_type=Asset.AssetType.PREDICTION_CONTRACT,
            precision="0.000001",
        )

    def on_trading_iteration(self):
        quote = self.get_quote(self.contract)
        if quote.ask is None:
            return
        if quote.ask <= 0.40 and not self.get_position(self.contract):
            order = self.create_order(
                self.contract,
                quantity=10,
                side="buy",
                order_type="limit",
                limit_price=0.40,
            )
            self.submit_order(order)


broker = Polymarket(mode="us")
strategy = PredictionMarketStrategy(broker=broker)
trader = Trader()
trader.add_strategy(strategy)
trader.run_all()
```

## Risks

- SDK drift: Polymarket API/SDK surfaces are newer and may change faster than Alpaca/Tradier.
- Jurisdiction: US versus international API behavior must be explicit.
- No sandbox: If Polymarket lacks paper/sandbox order placement, live order tests need strong safety gates.
- Historical data limits: Price history may not be enough for realistic high-frequency backtests.
- Asset serialization: Adding metadata to `Asset` could have broad downstream effects if done too early.
- Settlement modeling: Resolution and redemption mechanics are venue-specific.
- Liquidity: Market orders can have worse execution than midpoint or last price suggests.

## Open Questions

1. Should milestone 1 be Polymarket US only, or should international CLOB be included from day one for open-source users outside the US?
2. Does Polymarket US provide a sandbox or paper environment for order placement?
3. What exact SDK fields identify a US tradable contract in orders, positions, and prices?
4. Does Polymarket US expose native market orders, or should LumiBot only support limit orders initially?
5. What are current min size, min price increment, tick-size, and time-in-force rules for US and international APIs?
6. Should BotSpot store Polymarket credentials as simple API-key credentials, OAuth-style token files, or a provider-specific secret bundle?
7. Should `Asset` gain a generic metadata field now, or should metadata remain in raw provider payloads until a second prediction-market venue exists?
8. How should BotSpot display prediction contracts: by event question/outcome, by ticker, by slug, or by provider contract id?

## Recommended Next Step

Before coding, do a short credential and SDK spike outside LumiBot implementation:

1. Create or identify a Polymarket US account eligible for API access.
2. Generate API keys from the documented account settings flow.
3. In a scratch environment, instantiate the official `polymarket-us` Python SDK.
4. Read balance, positions, one market, one contract quote, and open orders.
5. Save only redacted response shapes in `docs/investigations/` or ignored artifacts.
6. Use those real shapes to implement the mocked parser tests first.

After that spike, implement Milestone 1 and Milestone 2 in one version-scoped LumiBot branch/commit series with docs and tests.
