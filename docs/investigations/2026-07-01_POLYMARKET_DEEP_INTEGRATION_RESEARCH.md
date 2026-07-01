# Polymarket Deep Integration Research

One-line description: Deep research and implementation game plan for adding Polymarket prediction-market trading to LumiBot, with BotSpot and Bot Manager follow-on implications.

Last Updated: 2026-07-01

Status: Planning only. No implementation code changed.

Audience: LumiBot maintainers, BotSpot broker-connection engineers, Bot Manager runtime engineers

## Executive Summary

The current browser evidence does not look like Polymarket US. The logged-in site was `https://polymarket.com/`, the account menu showed a wallet-address style identity, crypto deposit/cash balances, and an `APIs` menu item. That matches the international `polymarket.com` web3/CLOB account surface, not the separate `polymarket.us` SDK/account-key surface.

This changes the first implementation target from the earlier assumption. For Rob's visible account, the practical first LumiBot spike should target the current `polymarket.com` Python SDK and CLOB/deposit-wallet flow. For a future BotSpot customer-facing product in the United States, we should still keep a separate `polymarket.us` lane because the US docs and credential model are different.

The biggest structural issues are:

1. Prediction contracts need a real LumiBot asset type. They are not stocks and they are not crypto spot pairs.
2. The international Polymarket trading flow may require a private key or session signer plus wallet/funder address, not only a simple API key.
3. The `Relayer API Key` visible in the website/docs appears to support wallet deployment and approval transactions. It is not, by itself, the full credential required for order signing and private user streams.
4. Fast market support needs WebSockets for both public order-book updates and private user/order events. Polling alone is not enough for short-horizon markets.
5. BotSpot can reuse the saved broker credential and runtime-secret architecture, but raw wallet private keys inside user strategy containers are a serious security boundary problem. This needs an explicit product decision before customer-facing support.

I did not create or read any API keys. Key creation changes account security state and should be done only after an explicit go-ahead for that action.

## Account Surface Finding

### What Was Verified In Chrome

Rob explicitly allowed inspection through the Codex Chrome extension. The observed account/page state:

- URL: `https://polymarket.com/`
- Logged in with visible portfolio and cash balances.
- Account menu showed a wallet-address style identity.
- Menu included `APIs`, `Documentation`, `Status`, `Support`, and other Polymarket web app items.
- Deposit/cash flow was crypto-style, consistent with Rob's description.

This is enough to say: the current session is on `polymarket.com`, not `polymarket.us`.

### What This Means

There are two different integration tracks:

| Track | Site/API | Practical use | Credential shape |
| --- | --- | --- | --- |
| International CLOB | `polymarket.com`, `clob.polymarket.com`, Gamma/Data APIs | Rob's currently visible account, subject to availability/geographic restrictions | Private key or session signer, wallet/deposit-wallet address, SDK session credentials, optional relayer API key for wallet operations |
| Polymarket US | `polymarket.us` docs and SDK | Future US-facing BotSpot product if available and compliant | API key, API secret, user id, and SDK config after account verification |

Do not blur these together in code or docs. The same public class can eventually support both modes, but credentials, account setup, and eligibility are different.

## Primary Sources Reviewed

### Local LumiBot Sources

- `lumibot/brokers/broker.py`: live broker abstract contract, order events, position sync, and stream lifecycle.
- `lumibot/brokers/alpaca.py`: best current model for full broker implementation with balances, positions, order mapping, and real stream/polling fallback.
- `lumibot/brokers/tradier.py`: best current model for explicit errors, polling stream, status reconciliation, and token/broker edge cases.
- `lumibot/brokers/example_broker.py`: skeleton only. Useful checklist, not a production pattern.
- `lumibot/data_sources/data_source.py`: required data-source contract.
- `lumibot/data_sources/example_broker_data.py`: data-source skeleton.
- `lumibot/entities/asset.py`: current asset classes do not include prediction contracts.
- `lumibot/entities/order.py`: order type, side, status, and serialization semantics.
- `lumibot/entities/position.py`: normalized position model.
- `lumibot/trading_builtins/custom_stream.py`: `CustomStream` and `PollingStream` queue/event-dispatch primitives.
- `lumibot/lumibot/credentials.py`: broker auto-detection and `TRADING_BROKER` instantiation.
- `lumibot/docs/BROKER_ORDER_SEMANTICS.md`: read resilience versus mutation failure behavior.
- `lumibot/docs/LIVE_ORDER_POSITION_REFRESH.md`: live position/order refresh expectations.
- `lumibot/docs/BACKTESTING_ARCHITECTURE.md`: no fabricated market data.
- `lumibot/docs/ENV_VARS.md` and `lumibot/docsrc/environment_variables.rst`: env var documentation patterns.

### Local BotSpot And Bot Manager Sources

- `botspot_react/src/site/brokerConnectionSource.js`: broker connection catalog and credential-field definitions.
- `botspot_react/src/utils/brokerConnectionCatalog.js`: broker logo hydration and lookup.
- `botspot_react/src/pages/BrokerConnections/BrokerConnectionsPage.tsx`: account settings UI for manual API key and OAuth broker connections.
- `botspot_node/src/SavedSecrets/brokerCredentialRequirements.ts`: server-side broker credential requirements and allowed key names.
- `botspot_node/src/Mcp/handlers/savedSecrets.ts`: saved broker credential MCP contract, raw-secret non-disclosure, and metadata-only responses.
- `botspot_node/docs/security/2026-06-12_mcp-broker-data-fetch.md`: broker-data boundary through saved credentials and runtime-secret refs.
- `botspot_node/docs/mcp/single-trades.md`: high-risk single-trade approval and broker capability model.
- `bot_manager/flask_app.py`: allowed broker config keys for single-trade, broker-data, and portfolio-snapshot paths.
- `bot_manager/bot_manager/runtime_secrets.py`: Bot Manager runtime-secret issuance and rotation handoff.
- `bot_manager/bot_manager/single_trade_runner.py`: `lumibot.credentials.broker` runtime entrypoint for one-shot orders.
- `bot_manager/bot_manager/broker_data.py`: broker-data fetch entrypoint through LumiBot.
- `bot_manager/bot_manager/portfolio_snapshot.py`: portfolio snapshot entrypoint through LumiBot.

### Polymarket Official Sources

- Polymarket API introduction: `https://docs.polymarket.com/api-reference/introduction`
- Polymarket Python SDK: `https://docs.polymarket.com/dev-tooling/python`
- Polymarket trading quickstart: `https://docs.polymarket.com/trading/quickstart`
- Polymarket order overview: `https://docs.polymarket.com/trading/orders/overview`
- Polymarket order creation: `https://docs.polymarket.com/trading/orders/create`
- Polymarket orderbook/WebSocket docs: `https://docs.polymarket.com/trading/orderbook`
- Polymarket market channel: `https://docs.polymarket.com/market-data/websocket/market-channel`
- Polymarket user channel: `https://docs.polymarket.com/market-data/websocket/user-channel`
- Polymarket authentication: `https://docs.polymarket.com/developers/CLOB/authentication`
- Polymarket public client methods: `https://docs.polymarket.com/trading/clients/public`
- Polymarket secure client methods: `https://docs.polymarket.com/trading/clients/l2`
- Polymarket deposit wallets: `https://docs.polymarket.com/trading/deposit-wallets`
- Polymarket fees: `https://docs.polymarket.com/trading/fees`
- Polymarket error codes: `https://docs.polymarket.com/resources/error-codes`
- Polymarket rate limits/geographic restrictions entry point: `https://docs.polymarket.com/api-reference/rate-limits`
- Polymarket relayer API keys reference: `https://docs.polymarket.com/api-reference/relayer-api-keys/get-all-relayer-api-keys`
- Polymarket US Python quickstart: `https://docs.polymarket.us/api-reference/sdks/python/quickstart`
- Polymarket US Python account docs: `https://docs.polymarket.us/api-reference/sdks/python/account`
- Polymarket US Python WebSocket docs: `https://docs.polymarket.us/api-reference/sdks/python/websocket`

## Polymarket API Model

### International `polymarket.com`

The current `polymarket.com` docs describe several distinct API families:

- Gamma API: markets, events, tags, series, comments, sports, and search.
- Data API: positions, trades, activity, holder data, leaderboard, and builder analytics.
- CLOB API: order books, prices, tick size, orders, trades, balances, allowances, and authenticated trading.
- Relayer API: wallet deployment and on-chain wallet transaction submission.
- WebSocket APIs: market, user, sports, and related realtime feeds.

The current Python SDK is the unifying integration point:

- Package: `polymarket-client`
- Public clients: `AsyncPublicClient` and `PublicClient`
- Authenticated clients: `AsyncSecureClient` and `SecureClient`
- Realtime subscriptions are async only.
- Public stream specs include market/orderbook updates.
- Secure stream specs add user-scoped order and trade events.

The secure Python client currently matters most for LumiBot because it exposes direct trading helpers such as:

- `AsyncSecureClient.create(...)`
- `place_limit_order(...)`
- `place_market_order(...)`
- `create_limit_order(...)`
- `post_order(...)`
- `setup_trading_approvals()`
- `subscribe(UserSpec())`

The public Python client matters most for data and discovery:

- `list_markets(...)`
- `get_market(...)`
- order-book, price, history, and stream subscriptions

### Authentication Levels

The CLOB docs distinguish public reads, API-key auth, and wallet-signature auth.

The important implementation consequence is that placing and managing orders is not just "paste an API key":

- A secure client authenticates with a local private key.
- The wallet/funder address identifies where funds and conditional tokens sit.
- The SDK can create or derive API credentials for authenticated sessions.
- A relayer API key can be used when the SDK needs to deploy a deposit wallet or submit approval transactions.
- Existing proxy, Safe, EOA, and deposit wallet flows use different signature types.

For a new API user, the docs emphasize the deposit wallet flow. That flow can require:

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_WALLET_ADDRESS` or a default deterministic deposit wallet
- `POLYMARKET_RELAYER_API_KEY`
- `POLYMARKET_RELAYER_API_KEY_ADDRESS`
- SDK session credentials if we want to reuse them instead of deriving each time
- pUSD funding and trading approvals

The website's `Relayer API Key` label is therefore not enough to assume we can trade through LumiBot. Early implementation must verify exactly which credential set the site can create for Rob's current wallet.

### Orders

Polymarket maps well to simple LumiBot orders, with important constraints:

- The traded object is an outcome token id.
- Prices are probabilities/collateral prices, normally between 0 and 1.
- Tick size is market-specific and can be `0.1`, `0.01`, `0.001`, or `0.0001`.
- Tick size can change while a market is live, especially near extremes.
- Multi-outcome negative-risk markets require a `negRisk` flag or equivalent market metadata.
- Limit order types include GTC and GTD.
- Market-style orders use FAK or FOK semantics.
- Post-only orders are only valid for resting limit orders.
- Marketable orders may have short placement delays in selected fast categories.
- Sports markets have special behaviors around game start.

For LumiBot milestone 1:

- Support simple `BUY` and `SELL`.
- Support `LIMIT` first.
- Add `MARKET` only after the provider helper and quantity/amount semantics are tested.
- Reject stop, stop-limit, trailing stop, smart-limit, bracket, OCO, OTO, multileg, short, and cross-market packages.
- Use `Decimal` internally for prices and quantities.
- Cache and refresh tick size and `negRisk` by token id.

### Market Data And WebSockets

For fast markets, HTTP polling is not enough.

Public market channel events include:

- Full order-book snapshots.
- Price level changes.
- Last trade price.
- Tick-size changes.
- Best bid/ask events when enabled.
- New market and market-resolved events when enabled.

Private user channel events include:

- User order events.
- User trade events.

The adapter should bootstrap via HTTP, then keep state fresh with WebSockets:

1. Discover the market/outcome token id through the public client.
2. Fetch an order-book snapshot and current CLOB market info.
3. Subscribe to the market channel for that token id.
4. Subscribe to the user channel for the authenticated wallet.
5. Reconcile private order state with HTTP on reconnect or any missed/unknown event.
6. Use polling as a fallback, not the primary fast-market mechanism.

### Fees

Fees are applied at match time by the protocol. Makers are not charged; takers can be charged depending on category. Fees need to be represented in raw order/trade payloads first, then normalized into LumiBot trade/fill accounting only after real fills show the fields consistently.

Do not ignore fees in PnL. The first live adapter can rely on provider account value and positions, but any internal realized PnL view should account for taker fees once fill payloads expose them reliably.

### Geographic Restrictions

The docs explicitly link rate limits and geographic restrictions, and the trading quickstart calls out geoblock failures. The adapter must not try to bypass location restrictions. If provider APIs reject trading because of geography, LumiBot should surface the provider error clearly and stop.

For BotSpot, this means:

- `polymarket_clob` should not become a US-customer default.
- `polymarket_us` should remain separate until we have verified its account/API eligibility and order coverage.
- The product should not encourage VPN-based trading.

## LumiBot Domain Model

### Add `prediction_contract`

Add:

```python
Asset.AssetType.PREDICTION_CONTRACT = "prediction_contract"
```

Do not model outcome tokens as stocks or crypto. The asset is a contract/outcome share inside a market.

Recommended canonical identity:

```python
Asset(
    symbol="<provider stable contract id>",
    asset_type=Asset.AssetType.PREDICTION_CONTRACT,
    precision="0.000001",
)
```

For international Polymarket, the symbol should be the outcome token id. The market slug, question, outcome label, condition id, event id, expiration/resolution time, category, and provider should live in raw metadata or a provider-specific resolver cache, not in the core `Asset.symbol`.

Why this matters:

- A market/event can have multiple tradable outcomes.
- Outcome labels can be human-friendly but are not the most stable order key.
- Future Kalshi/PredictIt integrations need the same generic asset class.
- BotSpot single-trade schema and broker-data schema can extend one asset type instead of adding venue-specific hacks.

### Quote Asset

Use USD-like collateral as the quote side:

```python
quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
```

International Polymarket collateral is pUSD/USDC-like, but LumiBot account values and BotSpot displays should normalize to USD unless we intentionally expose the collateral token as a cash asset later.

### Positions

Each held outcome token maps to one `Position`:

- `asset`: `prediction_contract`
- `quantity`: shares/contracts
- `avg_fill_price`: provider average entry price when available
- `current_price`: last/mark/mid price
- `market_value`: shares times current price
- `pnl`: provider PnL when available, otherwise derived only when reliable
- `raw`: raw position row for diagnosis

At settlement, winning contracts are worth 1 and losing contracts are worth 0. The first live adapter should trust provider account/position reads for settled state. Backtesting can model resolution later from historical resolution data.

### Orders

Initial LumiBot mapping:

| LumiBot | Polymarket CLOB |
| --- | --- |
| `OrderSide.BUY` | `BUY` |
| `OrderSide.SELL` | `SELL` |
| `OrderType.LIMIT` | resting limit, GTC or GTD |
| `OrderType.MARKET` | provider market helper with FAK/FOK, after live spike |
| `time_in_force="gtc"` | GTC |
| `time_in_force="gtd"` | GTD with validated expiration |
| `time_in_force="fak"` | FAK market-style order |
| `time_in_force="fok"` | FOK market-style order |

Important quantity distinction:

- For BUY market orders, Polymarket SDK examples can use amount/max spend.
- For SELL market orders, SDK examples use shares.
- LumiBot `Order.quantity` is normally units/shares. The broker must not accidentally treat `quantity` as dollars for buy market orders.

Recommended milestone 1 rule: only allow limit orders until the market order semantics are tested with tiny live orders and explicit expected behavior.

## LumiBot Implementation Plan

### New Files And Exports

Add:

- `lumibot/data_sources/polymarket_data.py`
- `lumibot/brokers/polymarket.py`
- Tests under the existing LumiBot test layout.
- Public docs under `docsrc/` and `docs/ENV_VARS.md`.

Update:

- `lumibot/entities/asset.py`
- `lumibot/data_sources/__init__.py`
- `lumibot/brokers/__init__.py`
- `lumibot/lumibot/credentials.py`
- `docs/BROKER_ORDER_SEMANTICS.md` if prediction contracts need a new order caveat.
- `docs/BACKTESTING_ARCHITECTURE.md` if we add prediction backtesting semantics.

### Config Shape

Recommended environment names for international CLOB mode:

- `TRADING_BROKER=polymarket`
- `POLYMARKET_MODE=clob`
- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_WALLET_ADDRESS`
- `POLYMARKET_RELAYER_API_KEY`
- `POLYMARKET_RELAYER_API_KEY_ADDRESS`
- `POLYMARKET_CLOB_API_KEY`
- `POLYMARKET_CLOB_API_SECRET`
- `POLYMARKET_CLOB_API_PASSPHRASE`
- `POLYMARKET_API_CREDENTIALS_JSON`
- `POLYMARKET_BUILDER_CODE`

Credential policy:

- Treat every key above as secret except wallet address and builder code.
- Do not print these values in exceptions, logs, docs, or test output.
- Prefer `POLYMARKET_API_CREDENTIALS_JSON` or separate CLOB fields, not both.
- The SDK can derive session credentials from private-key auth. If we persist derived credentials, store them in the same secret store as broker credentials.
- Do not add auto-detection based on `POLYMARKET_PRIVATE_KEY` until we are confident. Prefer explicit `TRADING_BROKER=polymarket` to avoid accidental live-wallet initialization.

Potential future US mode:

- `TRADING_BROKER=polymarket_us`
- `POLYMARKET_US_API_KEY`
- `POLYMARKET_US_API_SECRET`
- `POLYMARKET_US_USER_ID`

Keep US and international env names separate.

### `PolymarketData`

Responsibilities:

- Own public market discovery.
- Resolve a URL/slug/question/outcome into a token id.
- Fetch quotes/order books.
- Fetch last price.
- Fetch historical prices when enough provider history exists.
- Maintain an optional in-memory market-data cache updated by WebSocket.

Required LumiBot methods:

- `get_chains(asset, quote=None)`: return `{}` or a documented prediction-market structure. Do not pretend these are option chains.
- `get_historical_prices(asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs)`: return real provider data only. If the provider cannot supply reliable bars, raise a clear unsupported error rather than fabricating bars.
- `get_last_price(asset, quote=None, exchange=None)`: return last trade/mark for token id.
- `get_quote(asset, quote=None, exchange=None)`: return best bid, ask, mid, sizes, timestamp, and raw book/event payload.

Recommended helper methods:

- `resolve_market(url=None, slug=None, market_id=None, condition_id=None)`
- `resolve_contract(market, outcome=None, token_id=None)`
- `get_clob_market_info(condition_id)`
- `get_tick_size(token_id)`
- `get_neg_risk(token_id)`
- `subscribe_market(token_ids)`

Use the SDK public client first. If a required method is missing or unstable in the beta SDK, isolate direct REST/WebSocket calls behind a small internal client wrapper so the broker surface does not change.

### `Polymarket` Broker

Constructor:

```python
class Polymarket(Broker):
    NAME = "Polymarket"

    def __init__(self, config=None, data_source=None, polling_interval=1.0, use_websocket=True):
        ...
```

Required methods:

- `cancel_order(order)`: call provider cancel, update local status, dispatch cancel when confirmed.
- `_modify_order(order, limit_price=None, stop_price=None)`: if Polymarket has no native modify, raise `NotImplementedError` or implement explicit cancel-replace only in a separate helper. Do not hide cancel-replace under modify unless semantics are exact.
- `_submit_order(order)`: validate asset type, token id, side, type, price, size, tick size, `negRisk`, and available config. Submit through secure client, set identifier/status/raw, dispatch `NEW_ORDER`.
- `_get_balances_at_broker(quote_asset, strategy)`: return cash/collateral balance, position value, and total account value using provider account/position data.
- `get_historical_account_value()`: return provider history if available; otherwise `{}` with documented unsupported behavior.
- `_get_stream_object()`: return a custom Polymarket stream object, not just `PollingStream`, when WebSockets are enabled.
- `_register_stream_events()`: map provider user events to LumiBot `NEW_ORDER`, `PARTIALLY_FILLED_ORDER`, `FILLED_ORDER`, `CANCELED_ORDER`, and `ERROR_ORDER`.
- `_run_stream()`: start the stream object.
- `_pull_positions(strategy)`: fetch provider positions and parse into `Position`.
- `_pull_position(strategy, asset)`: filter `_pull_positions` or provider endpoint by token id.
- `_parse_broker_order(response, strategy_name, strategy_object=None)`: normalize status, side, type, price, size, fill quantity, and raw payload into `Order`.
- `_pull_broker_order(identifier)`: fetch one order by provider id.
- `_pull_broker_all_orders()`: fetch open orders, optionally with asset/market filter.

Status mapping should be conservative:

| Provider status | LumiBot status |
| --- | --- |
| submitted/live/open | `OPEN` or `SUBMITTED` after local convention choice |
| matched/filled | `FILLED` |
| partially matched | `PARTIALLY_FILLED` |
| canceled/cancelled | `CANCELED` |
| expired | `EXPIRED` |
| rejected/error/failed | `ERROR` |
| unknown | `UNKNOWN` on reads, exception on submit if returned by order placement |

### Stream Design

Implement a dedicated `PolymarketStream(CustomStream)` that owns:

- An asyncio event loop in a background thread.
- An async public market subscription for subscribed token ids.
- An async secure user subscription for authenticated order/trade events.
- Reconnect with backoff.
- HTTP reconciliation after reconnect.
- Subscription mutation when the strategy starts tracking new contracts.
- Queue dispatch into LumiBot's existing broker stream actions.

Why not `PollingStream` only:

- The user specifically wants fast trading.
- Short-duration crypto up/down markets can move faster than a polling interval.
- Tick-size changes can reject orders if not captured promptly.
- Private fill/cancel events should update local order state immediately.

Polling still has a role:

- Startup reconciliation.
- Reconnect reconciliation.
- Fallback when WebSockets fail.
- Tests with fake clients.

### Backtesting

Do not fabricate bars.

Acceptable milestone 1:

- Live trading only.
- Public market-data reads.
- Unit tests with fake clients and recorded provider message shapes.

Acceptable milestone 2:

- Historical price support through provider history endpoints.
- Use real timestamped price points only.
- If there is no bar for a minute, either return sparse bars or fail clearly based on existing LumiBot expectations. Do not carry forward a flat price just to make backtests run.

Prediction-market backtesting also needs resolution data:

- Market close time.
- Resolution status.
- Winning outcome.
- Final settlement value.
- Fees.
- Delisted/canceled market behavior.

Without resolution history, PnL and drawdown can be misleading.

## BotSpot And Bot Manager Implications

### BotSpot React

Files likely touched later:

- `botspot_react/src/site/brokerConnectionSource.js`
- `botspot_react/src/utils/brokerConnectionCatalog.js`
- `botspot_react/src/pages/BrokerConnections/BrokerConnectionsPage.tsx`
- Broker logo assets and tests.

Needed product decisions:

- Add `polymarket` or split `polymarket_clob` and `polymarket_us`.
- Show a strong eligibility/compliance warning for international CLOB.
- Manual credential fields are enough for first version; OAuth-style browser redirect is not the right model unless Polymarket US provides it.
- Do not collect raw private keys in a customer-facing UI until security signs off on the runtime signer model.

Possible internal-only catalog entry for first BotSpot experiments:

- id: `polymarket_clob`
- asset class: `Prediction Markets`
- modes: `live` only
- auth method: `api_key` or `wallet`
- credential fields:
  - `POLYMARKET_PRIVATE_KEY` secret
  - `POLYMARKET_WALLET_ADDRESS`
  - `POLYMARKET_RELAYER_API_KEY` secret
  - `POLYMARKET_RELAYER_API_KEY_ADDRESS`
  - optional CLOB session credential fields

For public BotSpot, prefer `polymarket_us` if the US API supports the required trading and websocket workflows.

### BotSpot Node

Files likely touched later:

- `botspot_node/src/SavedSecrets/brokerCredentialRequirements.ts`
- `botspot_node/src/Mcp/handlers/savedSecrets.ts`
- `botspot_node/src/Deploy/deploy.controller.ts`
- `botspot_node/src/Trades/placeTrade.service.ts`
- `botspot_node/src/Trades/brokerData.service.ts`
- `botspot_node/docs/security/...`
- `botspot_node/docs/mcp/single-trades.md`

Needed backend work:

- Add broker credential requirements for Polymarket.
- Ensure raw values are never returned through MCP or frontend metadata.
- Add broker capability checks for prediction contracts.
- Extend single-trade schemas to include `assetType: "prediction_contract"`.
- Add validation for Polymarket-only fields: token id, market id, outcome label, price range, tick size if supplied, and supported order types.
- Add read-only broker-data support for quotes/order books.
- Decide whether `place_trade` supports Polymarket in v1 or waits for deployment-only support.

Security requirement:

- If raw private keys are involved, add a dedicated security doc before product implementation.
- Confirm whether the runtime secret grants expose private keys to user-authored Python strategy code. The current BotSpot runtime model treats user strategy code as untrusted, so a raw wallet private key is higher-risk than an ordinary read/write API token.

### Bot Manager

Files likely touched later:

- `bot_manager/flask_app.py`
- `bot_manager/bot_manager/single_trade.py`
- `bot_manager/bot_manager/single_trade_runner.py`
- `bot_manager/bot_manager/broker_data.py`
- `bot_manager/bot_manager/portfolio_snapshot.py`
- `bot_manager/bot_manager/runtime_secrets.py`
- Lambda packaging/dependency config.

Needed runtime work:

- Allow Polymarket-derived server-owned config keys only when needed.
- Add `prediction_contract` to single-trade supported asset types.
- Ensure runtime secret key allowlists accept Polymarket credential env names.
- Ensure the deployed Bot Manager image includes the right LumiBot version and `polymarket-client`.
- Add broker-data operations for prediction-contract quote/last price/order book.
- Add portfolio-snapshot support after `_get_balances_at_broker()` and `_pull_positions()` are reliable.

Do not pass legacy raw `broker_config` from Node to Bot Manager. Keep using saved broker credentials and short-lived runtime-secret refs.

### BotSpot Product Boundary

Recommended sequencing:

1. LumiBot local/internal adapter.
2. Bot Manager internal runtime support.
3. BotSpot saved credential support for an internal-only feature flag.
4. Broker-data read-only quotes and portfolio snapshot.
5. Single-trade support with high-risk approval.
6. Deployment support.
7. Public product support only after legal/compliance and private-key custody decisions are settled.

## Structural Risks And Mitigations

### Private Key Custody

Risk: The international SDK examples authenticate with a private key. In BotSpot, customer strategy code is untrusted and runs in a runtime that can often see broker env vars.

Mitigations:

- Start local LumiBot only.
- For BotSpot, require a design review before storing private keys.
- Prefer provider-scoped session credentials that cannot withdraw or transfer funds if Polymarket supports them.
- Consider a broker execution sidecar or signer service for future productization.
- Limit accounts to small funded balances during early testing.

### Geography And Compliance

Risk: Rob's current account may work only because of location/VPN behavior. We should not embed that assumption into LumiBot or BotSpot.

Mitigations:

- Make provider errors visible.
- Do not advise or automate geoblock bypass.
- Keep `polymarket.com` CLOB and `polymarket.us` separate.
- Gate customer-facing support behind compliance review.

### No Paper Trading

Risk: Polymarket may not offer a paper environment equivalent to Alpaca paper.

Mitigations:

- Heavy fake-client tests.
- Public read-only live data tests.
- Tiny explicit live-order smoke only after approval.
- Per-order notional caps.
- Market allowlist for smoke tests.

### Dynamic Tick Size

Risk: Orders get rejected if using stale tick size.

Mitigations:

- Fetch tick size before submit.
- Subscribe to `tick_size_change`.
- Cache tick size with short TTL and event invalidation.
- Validate all limit prices with Decimal quantization.

### Fast Market Delay And Race Conditions

Risk: Order book changes between quote and submit; marketable orders may have provider delays.

Mitigations:

- Use current WebSocket top-of-book.
- Add max slippage controls for market orders.
- Prefer limit orders first.
- Reconcile every submit response with private user events and HTTP order lookup.

### Negative Risk Markets

Risk: Multi-outcome markets require `negRisk` metadata and different exchange behavior.

Mitigations:

- Pull `negRisk` from `getClobMarketInfo` or market object.
- Include `negRisk` in the order helper.
- Unit-test binary and multi-outcome markets separately.

### Settlement And Resolution

Risk: Backtests and portfolio views can be wrong if they ignore resolution.

Mitigations:

- Trust provider live positions/account values in v1.
- Backtesting only after we have resolution history.
- Represent resolved positions from provider raw data.

### Cross-Venue "Arbitrage"

Risk: Polymarket, Kalshi, and PredictIt may have similar-looking markets with different resolution criteria.

Mitigations:

- Do not infer equivalence by title.
- Future multi-venue strategies need explicit market-resolution comparison fields.
- Keep venue-specific raw metadata attached to contracts.

## Testing Strategy

### Unit Tests

Add tests for:

- `Asset.AssetType.PREDICTION_CONTRACT` serialization and round trip.
- Polymarket order type/side/time-in-force mapping.
- Price validation by tick size.
- `negRisk` propagation.
- Position parsing.
- Balance parsing.
- Order parsing for live, matched, partially filled, canceled, expired, rejected, unknown.
- Error redaction.
- Credential config without logging secret values.

### Stream Tests

Use fake WebSocket event payloads:

- `book`
- `price_change`
- `last_trade_price`
- `tick_size_change`
- `best_bid_ask`
- user order event
- user trade event
- reconnect and reconciliation path

### SDK Boundary Tests

Wrap the SDK behind small interfaces so fake clients can test LumiBot behavior without live Polymarket credentials:

- `PolymarketPublicClientProtocol`
- `PolymarketSecureClientProtocol`
- `PolymarketStreamProtocol`

### Live Smoke Tests

Only after explicit approval:

1. Public quote smoke for one token id.
2. Authenticated account/balance read.
3. Open orders read.
4. Positions read.
5. Tiny limit order far from market, then cancel.
6. Tiny marketable order only after limit/cancel works.

Smoke tests must use an explicit market allowlist, maximum notional, and no hidden "fake filled" behavior.

## Implementation Phases

### Phase 0: Credential And Access Spike

Goals:

- Confirm whether Rob wants `polymarket.com` CLOB mode or a real `polymarket.us` account.
- Confirm what the website `APIs` page can generate.
- Determine if Relayer API Key plus private key/session signer is enough for Rob's current account.
- Create credentials only after explicit approval.
- Store secrets only in approved local env/secret-store paths. Do not put them in docs, chat, screenshots, git, or logs.

Exit criteria:

- We know the exact env vars and credential object required for one authenticated account read.
- We can call a read-only authenticated endpoint or SDK account method without placing orders.
- We know whether trading requires raw private-key custody.

### Phase 1: LumiBot Asset And Data Source

Goals:

- Add `prediction_contract`.
- Add `PolymarketData` with public market discovery, quote, last price, and real history where available.
- Add tests for data parsing and asset serialization.

Exit criteria:

- A strategy can ask for a quote/last price for a known token id.
- No live orders are possible yet.

### Phase 2: Read-Only Broker

Goals:

- Add `Polymarket` broker with authenticated balances, positions, and open-order reads.
- No submit/cancel yet, or submit disabled behind explicit config.

Exit criteria:

- `_get_balances_at_broker()` works.
- `_pull_positions()` works.
- `_pull_broker_all_orders()` works.
- Raw values are redacted in logs.

### Phase 3: Limit Orders And Cancel

Goals:

- Support simple limit orders.
- Support cancel.
- Support `_parse_broker_order`.
- Add safe provider error mapping.

Exit criteria:

- Tiny live limit order and cancel smoke passes after explicit approval.
- Unknown order rows do not crash refresh.
- Submit/cancel errors fail loudly.

### Phase 4: WebSockets

Goals:

- Add public market WebSocket for subscribed token ids.
- Add private user WebSocket for order/trade events.
- Add HTTP reconciliation after reconnect.

Exit criteria:

- Live quote state updates without polling.
- Private order status updates dispatch into LumiBot's stream events.
- Reconnect path does not duplicate fills.

### Phase 5: Market Orders And Fast Trading Controls

Goals:

- Add FAK/FOK market-style order support.
- Add slippage/max-spend controls.
- Add per-order notional caps in examples/smoke tests.

Exit criteria:

- Market order semantics are proven with tiny live trades.
- BUY amount/max_spend and SELL shares behavior is documented and tested.

### Phase 6: BotSpot Internal Support

Goals:

- Add broker credential metadata and validation in Node.
- Add catalog/UI behind internal flag.
- Add Bot Manager runtime support.
- Add read-only broker-data/portfolio snapshot before trading.

Exit criteria:

- Internal saved credential can run a read-only broker snapshot.
- No raw secrets are returned through MCP/frontend APIs.

### Phase 7: BotSpot Trading Support

Goals:

- Add single-trade support with high-risk approval.
- Add deployment support.
- Add product/legal gates.

Exit criteria:

- Single trade uses saved broker credential and high-risk approval.
- Deployment uses runtime-secret refs only.
- Security docs cover private-key/signing model.
- Public launch decision is made separately for `polymarket.com` versus `polymarket.us`.

## Questions For Rob

1. Should the first live LumiBot spike target your current `polymarket.com` account, even though it is not Polymarket US and may have geography/compliance constraints?
2. Do you want me to open the Polymarket `APIs` page and create/read API credentials in a later turn? If yes, where should the generated values be stored locally?
3. Are you comfortable with a local LumiBot prototype requiring a Polymarket private key or session signer, or do we need to find a no-private-key credential path first?
4. Is the first strategy target the fast 5-minute crypto up/down markets, or should we start with slower/liquid markets to reduce execution risk?
5. Should BotSpot support be internal-only until `polymarket.us` support is verified, or are we planning to support international CLOB accounts for specific non-US users?
6. What hard risk limits should we enforce in early live tests: max order size, max daily spend, market allowlist, and limit-only trading?

## Recommended Immediate Next Step

Do not start implementation yet. The next useful action is a credential/access spike:

1. Open the `polymarket.com` account `APIs` page with explicit approval.
2. Identify whether it creates Relayer API Keys, CLOB API session credentials, builder keys, or something else.
3. Do not expose values in chat or logs.
4. Save only the credential names and required env var mapping in this doc.
5. Run a read-only authenticated SDK call if credentials can be loaded safely.

After that, implement Phase 1 in LumiBot with fake-client tests and public data only.
