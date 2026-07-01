# Polymarket Deep Integration Research

One-line description: Deep research and implementation game plan for adding Polymarket prediction-market trading to LumiBot, with BotSpot and Bot Manager follow-on implications.

Last Updated: 2026-07-01

Status: Initial LumiBot implementation in progress. Credential/login/live-trading proof still blocked by missing local
Polymarket credentials.

Audience: LumiBot maintainers, BotSpot broker-connection engineers, Bot Manager runtime engineers

## 2026-07-01 Implementation Status

Initial LumiBot implementation has started and covers the international `polymarket.com` CLOB lane only.

Implemented in LumiBot:

- `Asset.AssetType.PREDICTION_CONTRACT`.
- `PolymarketData` public CLOB data source with market resolution, token resolution, order book, quote, last price, and
  supported price-history requests.
- `Polymarket` broker with required broker methods, authenticated read paths, market order routing, limit order routing,
  cancel, and polling reconciliation.
- `PolymarketCLOBStream` bridge with testable market/user event handlers and polling reconciliation.
- Explicit `TRADING_BROKER=polymarket` / `DATA_SOURCE=polymarket` plumbing.
- `py-clob-client-v2>=1.0.1` dependency. The documented beta `polymarket-client` package was not available from PyPI in
  the local verification environment.
- Unit tests for asset/data/broker behavior and provider-gated live smoke tests.

Verified locally:

- `python3 -m py_compile lumibot/data_sources/polymarket_data.py lumibot/brokers/polymarket.py lumibot/credentials.py`
- `python3 -m pytest tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py -q`
  passed with 15 tests.
- `python3 -m pytest tests/test_polymarket_apitest.py -q` skipped all 3 live/API tests because local Polymarket env vars
  are not present.
- Public `https://clob.polymarket.com/markets` returned HTTP 200 during a no-credential sanity check.

Not yet verified:

- Actual credential/login proof.
- Reading Rob's real Polymarket account value, positions, open orders, and recent trades.
- Deriving/storing real CLOB API credentials in `.env.local`.
- Any live order, including the approved $1-$5 market-order smoke.

Current blocker: no Polymarket private key, wallet address, or CLOB credentials were present in local LumiBot `.env` or
`.env.local` during implementation. The live smoke tests are ready but intentionally skip until those env vars are
provided.

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

## 2026-07-01 Follow-Up: US And International CLOB Support

### Direct Answers

**CLOB means Central Limit Order Book.** Polymarket's international trading docs describe it as the order-book system that matches orders offchain and settles matched trades onchain.

**Yes, LumiBot can support both Polymarket US and the international CLOB, but they should be separate broker adapters.** The right structure is separate public broker/data-source classes with only small shared helpers where the model is truly common:

- `Polymarket` or `PolymarketCLOB`: international `polymarket.com` CLOB/deposit-wallet broker.
- `PolymarketData` or `PolymarketCLOBData`: international CLOB market-data source.
- `PolymarketUS`: US `polymarket.us` broker.
- `PolymarketUSData`: US market-data source.
- Shared helpers only for prediction-contract parsing, decimal/tick validation, error redaction, and fake stream fixtures.

This means a strategy chooses one broker explicitly: `TRADING_BROKER=polymarket` for international CLOB or `TRADING_BROKER=polymarket_us` for the US platform. Do not route both through a single mode-switched broker in the first implementation. For BotSpot's saved broker credentials, split the product ids into `polymarket_us` and `polymarket_clob` so eligibility, compliance text, and credential fields cannot be mixed accidentally.

**Polymarket US is not app-only from an API standpoint.** The Polymarket US docs say users must create an account and complete identity verification in the iOS app before generating API keys, but they also document a developer portal, official Python/TypeScript SDKs, public endpoints, authenticated account/portfolio/orders endpoints, and WebSocket support for market and private updates. The US Python SDK uses `POLYMARKET_KEY_ID` and `POLYMARKET_SECRET_KEY` style credentials. We still need to verify whether Rob personally has access to a Polymarket US account and developer portal.

**Rob's current visible browser session still appears to be international `polymarket.com`, not Polymarket US.** The URL, crypto deposit surface, wallet-style identity, and `APIs` menu match the international account surface. That does not mean US cannot be supported; it means the first usable credentials from the current browser session are probably CLOB/deposit-wallet credentials unless Rob separately logs into `polymarket.us`.

### Credential Model: Relayer Key vs CLOB API Key

The international stack has at least three separate credential concepts. We should name them separately in LumiBot and BotSpot:

| Credential layer | What it is for | Typical fields | Notes |
| --- | --- | --- | --- |
| Signer / L1 | Proves wallet control, creates or derives CLOB API credentials, signs order payloads | `POLYMARKET_PRIVATE_KEY`, optional session signer, wallet/deposit-wallet address | Highest-risk secret. For BotSpot, this is a private-key custody problem, not a normal broker API token. |
| CLOB / L2 API credentials | Authenticates CLOB private REST requests, user-order reads, cancels, balances, and posting signed orders | `POLYMARKET_CLOB_API_KEY`, `POLYMARKET_CLOB_API_SECRET`, `POLYMARKET_CLOB_API_PASSPHRASE`, `POLY_ADDRESS`/wallet address | L2 auth does not remove the need to sign newly created orders. It authenticates the request around a signed order. |
| Relayer / builder credential | Deploys deposit wallets and submits wallet-operation batches such as approvals | `POLYMARKET_RELAYER_API_KEY`, `POLYMARKET_RELAYER_API_KEY_ADDRESS`, or builder key/secret/passphrase for older relayer client paths | This is not the same thing as a CLOB trading API key. It is needed for wallet setup and approvals, not a standalone LumiBot trading credential. |

The website label `Relayer API Key` is therefore not enough for trading by itself. If the API page also shows a normal API key underneath it, we need to identify whether that key is:

- a CLOB L2 key/secret/passphrase for authenticated order, cancel, balance, and stream operations;
- a relayer key/address pair for wallet deployment and approvals;
- a builder API credential used by older relayer clients;
- or a UI-level key that the beta unified SDK can consume directly.

No implementation should infer this from the page labels alone. The next access spike should inspect the labels and required fields without exposing values in chat, then run only a read-only authenticated account or balance call.

### What "Support Both At The Same Time" Should Mean

Support both should mean:

- The LumiBot entity model, broker contract, data-source contract, and strategy API can represent prediction contracts independent of venue.
- One broker instance is configured for one account/provider mode at a time.
- A strategy that needs cross-venue trading later should use multiple broker instances/accounts, not a single broker instance that silently routes some orders to US and some to CLOB.
- Tests should share prediction-market contract behavior, but fixture payloads should remain mode-specific.

Do not make one large `Polymarket` class full of `if mode == "us"` branches for every API call. Use two brokers and two data sources. That matches the useful part of the Alpaca/Tradier pattern: each broker presents LumiBot's normalized contract, while provider-specific parsing, auth, and streaming remain isolated.

### WebSockets Must Be In The Early Design

Alpaca is the better broker model here because it uses a real trading stream when credentials support it and falls back to polling only when necessary. Its polling path also reconciles positions and tracked orders carefully instead of assuming a missing order is canceled immediately.

Tradier is useful as a cautionary pattern: its polling stream explicitly cannot reliably track partial fills. That limitation is not acceptable for fast Polymarket markets where order-book levels, partial fills, tick-size changes, and private order events can move quickly.

For Polymarket, the first production-quality design should include:

1. HTTP snapshot at startup: market metadata, order book, balances, open orders, and positions.
2. Public market WebSocket: book, price change, last trade, best bid/ask, tick-size change, new-market, and resolved-market events where available.
3. Private user WebSocket: order, trade, position, and balance events where available by mode.
4. Reconnect reconciliation: after reconnect, reread open orders, positions, balances, and the current book before trusting local state.
5. Polling fallback: only for reconciliation and degraded operation, not as the primary fast-trading mechanism.

The CLOB SDK currently exposes async realtime stream subscriptions, so `PolymarketCLOBStream` should own an asyncio loop in a background thread and dispatch normalized events into LumiBot's existing `CustomStream` action queue. US WebSockets are also documented as async-only and provide separate private and market streams, so `PolymarketUSStream` should use the same LumiBot stream pattern but remain a separate provider implementation.

## 2026-07-01 Chosen Blueprint: International CLOB First

Scope decision after follow-up discussion: build only the international `polymarket.com` CLOB adapter first. Polymarket US should be a later, separate broker adapter after US access is available. Starting with CLOB should still make US easier later because it establishes LumiBot's prediction-contract asset semantics, test fixtures, stream event normalization pattern, and order/position/account-value expectations, but the US API should not be treated as the same implementation.

### Build Order

Every step below should include a focused unit-test layer and, when credentials are required, a separate
`pytest.mark.apitest` live smoke that is skipped unless explicit Polymarket env vars are present. Tests are not a final
cleanup phase; they are the acceptance gate for each slice.

1. Credential/login proof, using Rob's approved local `.env.local` storage and no implementation code changes beyond a
   small read-only harness/test when implementation starts.
2. Minimal `prediction_contract` asset plumbing.
3. CLOB data source skeleton and required public data methods.
4. CLOB broker skeleton with all required inherited methods present, even where a method initially raises a clear
   unsupported exception.
5. Authenticated account-value, positions, and orders read path.
6. Market-order submit path with explicit Polymarket `custom_params` for BUY notional and slippage protection, capped at
   the approved $1-$5 notional for the first live proof.
7. WebSocket market/user streams wired into LumiBot order and quote events.
8. Limit-order and cancel path, including far-from-market limit submit/cancel live smoke.
9. Examples, docs, BotSpot follow-on notes, and broader regression coverage.

### Credential/Login Proof

The first proof should not be a trade. It should prove that the local LumiBot process can authenticate and read account state.

Expected local secret fields:

- `POLYMARKET_PRIVATE_KEY`: wallet or session signer key used to create/derive CLOB API credentials and sign orders.
- `POLYMARKET_WALLET_ADDRESS`: deposit wallet/proxy/safe/funder address. For new API users, this is likely the deposit wallet.
- `POLYMARKET_CLOB_API_KEY`, `POLYMARKET_CLOB_API_SECRET`, `POLYMARKET_CLOB_API_PASSPHRASE`: derived or generated CLOB L2 credentials.
- `POLYMARKET_RELAYER_API_KEY`, `POLYMARKET_RELAYER_API_KEY_ADDRESS`: only if wallet deployment or approval setup requires relayer auth.
- Optional `POLYMARKET_BUILDER_CODE`: attribution only, not authentication.

Credential proof sequence:

1. Load secrets from a local `.env`/`.env.local` path, never from committed docs.
2. Initialize a secure client with private key and wallet address.
3. Create or derive CLOB API credentials if they are not already stored.
4. Initialize the authenticated CLOB client with `chain_id=137`, signature type `POLY_1271` for deposit wallets, and funder wallet address.
5. Read CLOB collateral balance/allowance.
6. Read portfolio value and positions through the secure unified SDK or Data API.
7. Read open orders and recent trades.
8. Stop if any credential, geoblock, approval, or funder mismatch error occurs.

### `PolymarketData`

File: `lumibot/data_sources/polymarket_data.py`

Purpose: public market data and market/contract resolution for international CLOB.

Required methods:

- `get_chains(asset, quote=None)`: return `{}` for now because prediction markets are not option chains. Add a documented prediction-market helper instead of pretending they are chains.
- `get_historical_prices(asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs)`: call CLOB price history only when a token id is known and the requested interval can map to provider history. Return real `Bars`; raise a clear unsupported error for unavailable bars.
- `get_last_price(asset, quote=None, exchange=None)`: for a token-id asset, call last-trade price first; fall back to midpoint if explicitly configured.
- `get_quote(asset, quote=None, exchange=None)`: call order book or best bid/ask, return `Quote(asset, price, bid, ask, bid_size, ask_size, timestamp, raw_data)`.

Provider helper methods:

- `resolve_market(url=None, slug=None, condition_id=None, market_id=None)`: find a market/event and cache condition id, market id, question, slug, end date, outcomes.
- `resolve_contract(market, outcome=None, token_id=None)`: return the tradable outcome token id and normalized `Asset(asset_type="prediction_contract")`.
- `get_order_book(token_id)`: CLOB order book snapshot.
- `get_clob_market_info(condition_id)`: tick size, neg-risk, min order size, fee fields.
- `get_tick_size(token_id)` and `get_neg_risk(token_id)`: direct cacheable reads.
- `calculate_market_price(token_id, side, amount, order_type="FOK")`: pre-trade estimate for market-order slippage controls.
- `subscribe_market(token_ids)`: used by the stream, not by strategy code directly.

### `Polymarket` Broker

File: `lumibot/brokers/polymarket.py`

Purpose: international CLOB live broker. It should inherit from `Broker` and be structured like Alpaca/Tradier: constructor creates/receives a data source, broker methods normalize provider payloads, stream actions dispatch into existing LumiBot order lifecycle methods.

Required methods and implementation intent:

- `__init__(config=None, data_source=None, polling_interval=1.0, connect_stream=True, use_websocket=True)`: build `PolymarketData` if missing, load config, set `market="24/7"`, initialize clients lazily enough that read-only imports do not accidentally place approvals.
- `_initialize_clients()`: create public, secure, and CLOB clients. Derive/load API credentials. Do not print secrets.
- `_ensure_trading_ready()`: run idempotent approvals only when trading is about to happen or when explicitly requested, not during every read-only balance call.
- `_get_balances_at_broker(quote_asset, strategy)`: return `(cash, positions_value, portfolio_value)` using CLOB collateral balance/allowance plus Data API/unified SDK portfolio value.
- `_pull_positions(strategy)`: call secure `list_positions` or Data API `/positions`, parse each row into `Position(strategy, Asset(token_id, prediction_contract), size, avg_fill_price=avgPrice)` with current price, market value, PnL, outcome, condition id, slug, and raw payload.
- `_pull_position(strategy, asset)`: filter `_pull_positions` by token id.
- `_pull_broker_all_orders()`: call open orders endpoint, return raw open orders. Add recent trades in a separate helper for fill reconciliation.
- `_pull_broker_order(identifier)`: call single order endpoint; if not open, fall back to recent trades by order id when needed.
- `_parse_broker_order(response, strategy_name, strategy_object=None)`: map CLOB raw order/trade response into `Order`, including token-id asset, side, quantity, order type, time in force, price, matched size, status, create/update timestamps, and raw payload.
- `_submit_order(order)`: validate `prediction_contract`, side, order type, token id, min order size, tick size, neg risk, and custom market-order params. Dispatch submit through `_submit_market_order` or `_submit_limit_order`.
- `_submit_market_order(order)`: first trading path. Use Polymarket market order semantics: BUY requires dollar `amount` and optional `max_spend`/max price; SELL requires shares. Values should come from `order.custom_params` so LumiBot's normal `quantity` meaning is not silently changed.
- `_submit_limit_order(order)`: use limit price and size, map GTC/GTD.
- `cancel_order(order)`: call provider cancel by order id; do not no-op only because local status says cancelling.
- `_modify_order(order, limit_price=None, stop_price=None)`: raise unsupported or implement explicit cancel-replace later. Do not fake native modification.
- `get_historical_account_value()`: return provider history if available later; initially return empty/unsupported like Tradier.
- `_get_stream_object()`: return `PolymarketCLOBStream` when websockets are enabled, otherwise `PollingStream` only as a fallback.
- `_register_stream_events()`: register `NEW_ORDER`, `PARTIALLY_FILLED_ORDER`, `FILLED_ORDER`, `CANCELED_ORDER`, `ERROR_ORDER`, plus poll/reconcile event if needed.
- `_run_stream()`: run stream object.
- `do_polling()`: fallback/reconcile path: sync positions, pull open orders, compare to tracked orders, query missing tracked orders individually before treating as final.

Status mapping:

- CLOB placement/open -> `SUBMITTED` or `OPEN`.
- User order update with partial matched size -> `PARTIALLY_FILLED`.
- Trade `CONFIRMED` or fully matched order -> `FILLED`.
- Cancellation -> `CANCELED`.
- Rejected/failed provider response -> `ERROR`.
- Expired GTD -> `EXPIRED`.
- Unknown provider status -> `UNKNOWN` on read, explicit exception on submit when the response is not accepted.

### WebSocket Design

File/class: `PolymarketCLOBStream(CustomStream)`

Implementation:

- Own an asyncio event loop in a background thread.
- Maintain market subscriptions by token id using market channel or SDK `MarketSpec`.
- Maintain private user subscription using API credentials or SDK `UserSpec`.
- Dispatch public book/price events into a quote/order-book cache owned by `PolymarketData`.
- Dispatch private order/trade events into broker stream actions.
- On reconnect, reread balances, positions, open orders, and order book before trusting local state.
- Use polling only as startup/reconnect fallback, not the primary source for fills.

Event handling:

- `book`: replace book snapshot for token id.
- `price_change`: update changed levels; size `0` removes a level.
- `best_bid_ask`: update quote cache.
- `tick_size_change`: invalidate tick-size cache.
- `last_trade_price`: update last price and volume/trade cache.
- `order` placement/update/cancellation: parse broker order and dispatch new/partial/cancel as appropriate.
- `trade` matched/mined/confirmed/failed: update fills; dispatch fill only when enough price/quantity detail exists.

### Why Market Orders Can Be First

Polymarket market orders are still signed CLOB orders that execute against resting liquidity using `FOK` or `FAK`. It is reasonable to make market orders the first live trade proof if the implementation makes Polymarket's quantity semantics explicit:

- BUY market order: use `custom_params["amount"]` for dollars to spend and `custom_params["max_spend"]` or max price/slippage protection.
- SELL market order: use shares, either from `order.quantity` or `custom_params["shares"]`.
- Default first smoke should be tiny notional, liquid market, FOK or FAK chosen explicitly, with a hard max notional.

Do not silently redefine LumiBot's generic `Order.quantity` as dollars for BUY market orders.

### Final Method Contract Matrix

This is the implementation checklist for the international CLOB adapter. The goal is to make the new broker look boring
to LumiBot strategies: strategies see normal assets, quotes, balances, positions, orders, and streams; only the adapter
knows about token ids, wallet signing, CLOB credentials, relayer approvals, tick sizes, and `negRisk`.

| Area | Class / method | Required behavior | Alpaca / Tradier comparison | Polymarket implementation detail |
| --- | --- | --- | --- | --- |
| Asset identity | `Asset.AssetType.PREDICTION_CONTRACT` | Add a stable core asset type for prediction contracts. | Alpaca/Tradier rely on existing stock/option/crypto types. | Use CLOB outcome token id as `Asset.symbol`; keep slug/question/outcome/condition id in raw metadata/cache. |
| Credentials | `credentials.py` broker config branch | Instantiate broker only when `TRADING_BROKER=polymarket`. | Alpaca and Tradier have explicit config objects and env vars. | Do not auto-detect from `POLYMARKET_PRIVATE_KEY`; avoid accidental wallet initialization. |
| Public data | `PolymarketData.get_last_price` | Return last price or a documented fallback. | Alpaca/Tradier normalize provider data into LumiBot price primitives. | Use CLOB last-trade endpoint first; optionally midpoint only when configured. |
| Quotes | `PolymarketData.get_quote` | Return bid/ask/sizes/timestamp/raw payload. | Tradier and Alpaca expose provider quote normalization. | Use `GET /book` or cached WebSocket best bid/ask; include `min_order_size`, `tick_size`, and `neg_risk` in raw data. |
| History | `PolymarketData.get_historical_prices` | Return real `Bars` only when provider history supports the request. | Alpaca has mature bar support; Tradier has narrower live data behavior. | Map supported intervals (`1m`, `1h`, `6h`, `1d`, `1w`, `all`, `max`); raise unsupported for unavailable bar semantics. |
| Chains | `PolymarketData.get_chains` | Satisfy abstract method without pretending prediction contracts are options. | Both stock brokers implement real option-chain logic. | Return `{}` and add explicit prediction-market resolver helpers. |
| Market resolver | `PolymarketData.resolve_market` | Convert URL/slug/condition id/market id into market metadata. | No direct Alpaca/Tradier equivalent; closest is option-chain lookup. | Use Gamma/public SDK market discovery; cache condition id, token ids, outcomes, close/resolution fields. |
| Contract resolver | `PolymarketData.resolve_contract` | Convert market + outcome into tradable `prediction_contract` asset. | Similar to selecting an option contract from a chain. | Validate token id exists and market is tradable before returning an `Asset`. |
| Client ownership | `Polymarket._initialize_clients` | Build public, secure, CLOB, and optional relayer clients. | Alpaca/Tradier constructors own provider clients. | Keep SDK clients behind internal wrapper protocols so unit tests use fakes and SDK churn is isolated. |
| Account value | `Polymarket._get_balances_at_broker` | Return `(cash, positions_value, portfolio_value)`. | Alpaca and Tradier both implement this exact tuple. | Cash from collateral balance; positions value from secure/Data API portfolio values; total = provider portfolio/equity when available. |
| Positions | `Polymarket._pull_positions` | Return `Position` objects for held contracts. | Same normalized broker duty as Alpaca/Tradier. | Parse wallet positions into `Position(strategy, Asset(token_id, prediction_contract), shares)` plus raw avg price/current value/PnL. |
| Single position | `Polymarket._pull_position` | Return the matching position or `None`. | Alpaca/Tradier filter provider positions. | Filter by token id; do not match by human outcome label. |
| Open orders | `Polymarket._pull_broker_all_orders` | Return open broker order payloads. | Alpaca pulls all statuses; Tradier normalizes list/dict shapes carefully. | Use secure `list_open_orders`; add market/token filters later. |
| One order | `Polymarket._pull_broker_order` | Return one provider order or recent trade/fill evidence. | Alpaca has provider order lookup; Tradier fetches by id and handles empty shapes. | Use `get_order`; if closed/missing, consult recent account trades by order id before declaring final. |
| Parsing | `Polymarket._parse_broker_order` | Normalize raw CLOB order/trade payload into LumiBot `Order`. | Alpaca is the fuller mapping model; Tradier is useful for defensive shape handling. | Map `LIVE`, `PLACEMENT`, `UPDATE`, `MATCHED`, `MINED`, `CONFIRMED`, `CANCELLATION`, `FAILED` conservatively. |
| Submit router | `Polymarket._submit_order` | Validate and route supported order types. | Alpaca has broad type routing; Tradier rejects unsupported combinations. | Support only `prediction_contract`, buy/sell, market first, limit second; reject stop/trailing/multileg/brackets clearly. |
| Market buy | `Polymarket._submit_market_order` | Spend dollars with hard cap and slippage guard. | This is unlike equities; avoid overloading `quantity`. | Require `order.custom_params["amount"]`; require `price` or `max_price`; use `FOK`/`FAK`; enforce live-test cap. |
| Market sell | `Polymarket._submit_market_order` | Sell shares/contracts with worst-price guard. | Similar to selling shares, but CLOB uses market helper. | Use `order.quantity` or `custom_params["shares"]`; require worst acceptable price. |
| Limit | `Polymarket._submit_limit_order` | Place resting order with price/size/TIF. | Alpaca/Tradier limit submit/cancel is the model. | Validate Decimal price against current tick size and `min_order_size`; support GTC/GTD. |
| Cancel | `Polymarket.cancel_order` | Always send provider cancel when possible. | Existing broker docs require mutation failures to surface, not silently skip. | Do not no-op because local state is stale; call provider cancel and then read back/reconcile. |
| Modify | `Polymarket._modify_order` | Explicitly unsupported or cancel-replace later. | Alpaca supports replace; Tradier has its own endpoint. | Start with clear unsupported exception; add cancel-replace only when semantics are explicit. |
| Account history | `Polymarket.get_historical_account_value` | Return provider history or clear unsupported value. | Alpaca supports it; Tradier returns limited/unsupported behavior. | Initially unsupported unless Data API account history is reliable. |
| Stream object | `Polymarket._get_stream_object` | Return WebSocket stream when enabled, polling only fallback. | Copy Alpaca's real-stream-first shape, not Tradier polling-only limitations. | Return `PolymarketCLOBStream(CustomStream)` with public and private subscriptions. |
| Stream actions | `Polymarket._register_stream_events` | Register LumiBot order lifecycle events. | Alpaca dispatches order events; Tradier polling misses partial fills. | Dispatch placement/update/fill/cancel/error from user channel plus reconciliation. |
| Polling | `Polymarket.do_polling` | Reconcile positions/orders and recover from missed streams. | Copy Alpaca's cautious tracked-order reconciliation. | Never treat a missing open order as canceled without single-order/trade lookup. |

### Alpaca And Tradier Patterns To Copy Or Avoid

Use Alpaca as the primary structural model:

- The broker owns provider-client setup, account balances, positions, order parsing, order submission, cancellation,
  account history, and stream lifecycle.
- The data source owns market data and converts provider payloads into LumiBot price/quote/bar primitives.
- Real streaming is first-class when credentials support it, and polling exists as fallback/reconciliation.
- Order refresh does not assume local state is truth; it reads broker state and reconciles tracked orders.

Use Tradier for defensive lessons:

- Its account and order parsing handles provider payloads that change shape or return empty results.
- Its polling-stream limitation around partial fills is a warning. A Polymarket adapter cannot rely on polling only,
  especially for short-duration crypto markets.
- Broker auth failures and unsupported order mutations should surface as real broker errors, not hidden waiting states.

Do not copy the wrong parts:

- Do not build a polling-only first version and call it production-ready.
- Do not make one combined `Polymarket` class with `if us else clob` branches. US and CLOB should be separate public
  brokers later.
- Do not fake account value, fills, or bars to make tests pass.

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
- Polymarket quickstart: `https://docs.polymarket.com/quickstart`
- Polymarket trading overview/CLOB definition: `https://docs.polymarket.com/trading/overview`
- Polymarket order overview: `https://docs.polymarket.com/trading/orders/overview`
- Polymarket order creation: `https://docs.polymarket.com/trading/orders/create`
- Polymarket CLOB order endpoint: `https://docs.polymarket.com/api-reference/trade/post-a-new-order`
- Polymarket order book endpoint: `https://docs.polymarket.com/api-reference/market-data/get-order-book`
- Polymarket price history endpoint: `https://docs.polymarket.com/api-reference/markets/get-prices-history`
- Polymarket market WebSocket channel: `https://docs.polymarket.com/api-reference/wss/market`
- Polymarket user WebSocket channel: `https://docs.polymarket.com/api-reference/wss/user`
- Polymarket authentication: `https://docs.polymarket.com/api-reference/authentication`
- Polymarket public client methods: `https://docs.polymarket.com/trading/clients/public`
- Polymarket secure client methods: `https://docs.polymarket.com/trading/clients/l2`
- Polymarket deposit wallets: `https://docs.polymarket.com/trading/deposit-wallets`
- Polymarket fees: `https://docs.polymarket.com/trading/fees`
- Polymarket error codes: `https://docs.polymarket.com/resources/error-codes`
- Polymarket rate limits/geographic restrictions entry point: `https://docs.polymarket.com/api-reference/rate-limits`
- Polymarket relayer API keys reference: `https://docs.polymarket.com/api-reference/relayer-api-keys/get-all-relayer-api-keys`
- Polymarket US SDK introduction: `https://docs.polymarket.us/api-reference/sdks/introduction`
- Polymarket US quickstart: `https://docs.polymarket.us/getting-started/quickstart`
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

For the first trading milestone, after login/account reads work:

- Support simple `BUY` and `SELL`.
- Support tiny `MARKET` orders first because Rob wants the fastest usable proof, but require explicit BUY notional and
  worst-price/slippage controls in `Order.custom_params`.
- Add `LIMIT` plus cancel immediately after the market-order smoke works, because far-from-market limit/cancel is the
  safest order-lifecycle proof.
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

Recommended rule: do not overload `Order.quantity` for BUY market notional. Require explicit `custom_params` such as
`{"amount": "1.00", "max_price": "0.52", "order_type": "FAK"}` for BUY market orders, and cap first live smoke tests at
the approved $1-$5 notional.

## LumiBot Implementation Plan

### New Files And Exports

Add for international CLOB:

- `lumibot/data_sources/polymarket_data.py`
- `lumibot/brokers/polymarket.py`
- Tests under the existing LumiBot test layout.
- Public docs under `docsrc/` and `docs/ENV_VARS.md`.

Add later or in parallel for US mode:

- `lumibot/data_sources/polymarket_us_data.py`
- `lumibot/brokers/polymarket_us.py`
- Separate tests and docs for the US SDK/auth/order model.

Update:

- `lumibot/entities/asset.py`
- `lumibot/data_sources/__init__.py`
- `lumibot/brokers/__init__.py`
- `lumibot/lumibot/credentials.py`
- `docs/BROKER_ORDER_SEMANTICS.md` if prediction contracts need a new order caveat.
- `docs/BACKTESTING_ARCHITECTURE.md` if we add prediction backtesting semantics.

### Internal Class Structure

Use separate public classes, with shared helpers only where the models are truly common:

- `Polymarket`: international CLOB broker class.
- `PolymarketData`: international CLOB data source.
- `PolymarketUS`: US broker class.
- `PolymarketUSData`: US data source.
- `polymarket_common.py` or a private helper module only for shared decimal, redaction, and asset metadata utilities.

This keeps the public broker imports explicit and avoids provider-specific branches throughout every broker method.

### Config Shape

Recommended international CLOB selector:

- `TRADING_BROKER=polymarket`
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
- `POLYMARKET_US_KEY_ID`
- `POLYMARKET_US_SECRET_KEY`

Keep US and international env names separate. For BotSpot saved credentials, use separate provider ids (`polymarket_us` and `polymarket_clob`).

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

- Split `polymarket_clob` and `polymarket_us` in BotSpot saved credentials and in the LumiBot public broker classes.
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

Possible US catalog entry:

- id: `polymarket_us`
- asset class: `Prediction Markets`
- modes: `live` only
- auth method: `api_key`
- credential fields:
  - `POLYMARKET_US_KEY_ID` secret
  - `POLYMARKET_US_SECRET_KEY` secret

For public BotSpot, prefer `polymarket_us` if Rob can verify US account/API eligibility and if the US API supports the required trading and WebSocket workflows.

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

### Test Marker And Credential Gating

Current test infrastructure has a broad `apitest` marker and provider-specific markers for some data providers. A
Polymarket implementation should add a provider marker instead of reusing the legacy Polygon/Theta default path.

Required test-runner changes when implementation starts:

- Add `polymarket: Polymarket CLOB live/API tests` to `setup.cfg`.
- Extend `tests/conftest.py` so tests marked `@pytest.mark.apitest` and `@pytest.mark.polymarket` require only
  Polymarket-specific env vars, not Polygon and ThetaData credentials.
- Keep the default coverage command excluding `apitest`, so live CLOB tests never run in ordinary unit-test/CI paths.
- Add a `POLYMARKET_LIVE_TRADING_ENABLED=true` gate for submit/cancel/market-order apitests.
- Add `POLYMARKET_TEST_MAX_NOTIONAL=5` or lower; default to `1` when absent.
- Add `POLYMARKET_TEST_TOKEN_ID` and optionally `POLYMARKET_TEST_MARKET_ID` for smoke tests, so tests never auto-pick a
  random live market to trade.

### Unit Test Files

Recommended new tests, created incrementally with the implementation phase they validate:

| Test file | Purpose | Should use live network? |
| --- | --- | --- |
| `tests/test_polymarket_asset.py` | `prediction_contract` asset validation, equality, serialization, invalid type rejection. | No |
| `tests/test_polymarket_credentials.py` | Config loading, missing-secret errors, redaction, explicit broker selection. | No |
| `tests/test_polymarket_data.py` | Market resolver, quote parsing, order-book parsing, history interval mapping, unsupported history failures. | No by default |
| `tests/test_polymarket_broker.py` | Balance/position/order parsing, submit validation, market BUY/SELL payload construction, unsupported order types. | No |
| `tests/test_polymarket_stream.py` | Fake `book`, `price_change`, `best_bid_ask`, `last_trade_price`, `tick_size_change`, order, trade, reconnect events. | No |
| `tests/test_polymarket_credentials_apitest.py` | Create/derive or reuse CLOB API credentials and perform read-only authenticated account proof. | Yes, skipped by env |
| `tests/test_polymarket_data_apitest.py` | Public market/order-book/quote/history smoke on an explicit token id. | Yes, skipped by env |
| `tests/test_polymarket_broker_apitest.py` | Account value, positions, open orders, tiny market order, limit/cancel lifecycle. | Yes, skipped by env and live-trading flag |
| `tests/test_polymarket_stream_apitest.py` | Short market/user WebSocket smoke with clean close and HTTP reconciliation. | Yes, skipped by env |

### Fake Client Boundary

Wrap Polymarket SDK and REST/WebSocket access behind small internal protocols so most tests never need real credentials:

- `PolymarketPublicClientProtocol`: market discovery, market info, order book, prices, price history.
- `PolymarketSecureClientProtocol`: account value, positions, open orders, trades, order placement, cancel, approvals.
- `PolymarketStreamClientProtocol`: public market stream, private user stream, close/reconnect.
- `PolymarketCredentialStore`: env loading, credential derivation result, redaction.

This is important because the official Python SDK is beta and CLOB libraries have already had multiple naming/version
surfaces (`polymarket-client`, `py-clob-client-v2`, relayer clients). The LumiBot adapter should not make every test
depend on the third-party SDK's exact class names.

### Live Smoke Safety Rules

Live smoke tests should be small and explicit:

- No live submit/cancel test runs without `POLYMARKET_LIVE_TRADING_ENABLED=true`.
- No live submit/cancel test runs without an explicit `POLYMARKET_TEST_TOKEN_ID`.
- No live market order exceeds `min(POLYMARKET_TEST_MAX_NOTIONAL, 5)`.
- First market BUY must use explicit `amount`, `max_price`, and `FOK` or `FAK`.
- First market SELL must be skipped unless the account already holds enough of the selected token or the test first
  bought a tiny amount and then sells that exact amount.
- No test should claim a fill happened unless provider HTTP or user WebSocket evidence confirms it.
- Geoblock, approval, insufficient balance, or restricted-market errors should fail or skip clearly; tests must not try
  to route around them.

## Implementation Phases

### Phase 0: Credential And Login Proof

Goals:

- Target only Rob's current `polymarket.com` international CLOB account.
- Create or derive CLOB API credentials from that account if needed. Rob approved this in the 2026-07-01 planning
  thread.
- Store local prototype credentials in `.env.local`. Rob approved this for the prototype.
- Determine whether the visible website API page exposes relayer keys, CLOB L2 credentials, builder credentials, or SDK
  session material.
- Determine exactly when relayer approval setup is needed versus ordinary CLOB order/account authentication.
- Store secrets only in approved local env/secret-store paths. Do not put them in docs, chat, screenshots, git, or logs.

Methods/classes touched:

- None for the first manual credential proof if using a disposable local harness.
- When implementation starts: `PolymarketCredentialStore`, `Polymarket._initialize_clients()`, and redaction helpers.

Tests:

- Unit: missing env vars produce clear errors; secret values are redacted; config refuses to initialize live trading
  without explicit broker selection.
- Live apitest: authenticated read-only account snapshot: collateral balance/allowance, portfolio value, positions, open
  orders, recent trades.

Exit criteria:

- We know the exact env vars and credential object required for one authenticated account read.
- We can call a read-only authenticated endpoint or SDK account method without placing orders.
- We know whether CLOB trading for Rob's account requires raw private-key custody or can reuse safer derived/session
  credentials for reads and order posting around locally signed orders.

### Phase 1: Asset Type And Broker Plumbing

Goals:

- Add `prediction_contract`.
- Add Polymarket imports/exports and explicit `TRADING_BROKER=polymarket` config path.
- Add stub `PolymarketData` and `Polymarket` classes with all abstract/required methods present.
- Unsupported methods should fail clearly rather than silently returning fake data.

Methods/classes touched:

- `Asset.AssetType.PREDICTION_CONTRACT`
- `lumibot/data_sources/polymarket_data.py`
- `lumibot/brokers/polymarket.py`
- `lumibot/data_sources/__init__.py`
- `lumibot/brokers/__init__.py`
- `lumibot/credentials.py`

Tests:

- Unit: asset type accepts `prediction_contract`; invalid unrelated types still fail.
- Unit: credentials only instantiate Polymarket on explicit `TRADING_BROKER=polymarket`.
- Unit: broker and data source instantiate with fake clients and no accidental network calls.

Exit criteria:

- A Polymarket broker object can be constructed with fake clients.
- Abstract method requirements are satisfied.
- No live order path is enabled.

### Phase 2: Public Data Source

Goals:

- Resolve market/event/outcome into token id.
- Fetch order book, best bid/ask, last price, and supported price history.
- Expose `get_quote`, `get_last_price`, `get_historical_prices`, and `get_chains`.

Methods/classes touched:

- `PolymarketData.resolve_market(...)`
- `PolymarketData.resolve_contract(...)`
- `PolymarketData.get_order_book(...)`
- `PolymarketData.get_quote(...)`
- `PolymarketData.get_last_price(...)`
- `PolymarketData.get_historical_prices(...)`
- `PolymarketData.get_chains(...)`

Tests:

- Unit: parse CLOB order-book payload into bid/ask sizes and quote raw data.
- Unit: history interval mapping accepts only provider-supported intervals.
- Unit: `get_chains` returns documented empty/unsupported behavior.
- Live apitest: public quote/order-book/last-price call for `POLYMARKET_TEST_TOKEN_ID`, no credentials required.

Exit criteria:

- A strategy can get a real quote/last price for a known token id.
- No account credentials are needed for public data.
- No fabricated bars are returned.

### Phase 3: Read-Only Broker

Goals:

- Read account value, cash/collateral, positions, open orders, and recent trades.
- Normalize provider positions and orders into LumiBot entities.
- Keep submit/cancel disabled except for validation stubs.

Methods/classes touched:

- `Polymarket._initialize_clients()`
- `Polymarket._get_balances_at_broker(...)`
- `Polymarket._pull_positions(...)`
- `Polymarket._pull_position(...)`
- `Polymarket._pull_broker_all_orders()`
- `Polymarket._pull_broker_order(identifier)`
- `Polymarket._parse_broker_order(...)`

Tests:

- Unit: parse positions with shares, avg price, current price/value, PnL, outcome, slug, and raw payload.
- Unit: parse order statuses into LumiBot statuses.
- Unit: missing/unknown provider fields do not crash refresh.
- Live apitest: account snapshot reads balances, portfolio values, positions, and open orders.

Exit criteria:

- `_get_balances_at_broker()` returns numeric cash, positions value, and portfolio value.
- `_pull_positions()` works for current account.
- `_pull_broker_all_orders()` works for current account.
- Secrets are redacted in all exceptions/logs.

### Phase 4: Market Orders First Live Trading Proof

Goals:

- Support simple market BUY and SELL using Polymarket FAK/FOK semantics.
- Require explicit market BUY dollar amount and worst-price limit in `custom_params`.
- Enforce tiny notional cap for first live proof.
- Surface geoblock, approval, balance, or restricted-market provider errors clearly.

Methods/classes touched:

- `Polymarket._submit_order(order)`
- `Polymarket._submit_market_order(order)`
- `Polymarket._ensure_trading_ready()`
- `PolymarketData.calculate_market_price(...)` or equivalent pre-trade estimate helper.

Tests:

- Unit: BUY market rejects missing `custom_params["amount"]`.
- Unit: BUY market does not treat `Order.quantity` as dollars.
- Unit: SELL market uses shares and validates sufficient quantity when available.
- Unit: all submit payloads include tick size, `negRisk`, worst price, FAK/FOK, and token id.
- Live apitest: one tiny market BUY with max notional $1-$5 on explicit token id, then read account/order/trade evidence.

Exit criteria:

- Tiny live market-order proof either fills or returns a clear provider error with no hidden fake success.
- Account value, positions, orders, and recent trades can be read immediately after the attempt.
- No submit test exceeds the configured cap.

### Phase 5: WebSocket Market And User Streams

Goals:

- Add public market WebSocket for subscribed token ids.
- Add private user WebSocket for authenticated order/trade events.
- Maintain quote/order-book cache inside `PolymarketData`.
- Dispatch private order/trade events into LumiBot order lifecycle events.
- Add HTTP reconciliation after reconnect.

Methods/classes touched:

- `PolymarketCLOBStream(CustomStream)`
- `Polymarket._get_stream_object()`
- `Polymarket._register_stream_events()`
- `Polymarket._run_stream()`
- `Polymarket.do_polling()` as fallback/reconciliation.
- `PolymarketData` quote/order-book cache mutation helpers.

Tests:

- Unit: fake market events update book, best bid/ask, last trade, and tick-size cache.
- Unit: fake user order/trade events dispatch `NEW_ORDER`, `PARTIALLY_FILLED_ORDER`, `FILLED_ORDER`, `CANCELED_ORDER`,
  and `ERROR_ORDER` as appropriate.
- Unit: reconnect calls HTTP reconciliation and does not double-count fills.
- Live apitest: subscribe briefly to `POLYMARKET_TEST_TOKEN_ID`, receive or time out cleanly, close without thread leaks.

Exit criteria:

- Live public stream can update quote state without polling.
- Private stream event handling is proven with fake events and can connect with live credentials.
- Polling remains available as fallback, not the primary fast-market source.

### Phase 6: Limit Orders And Cancel

Goals:

- Support simple limit GTC/GTD orders.
- Support cancel.
- Add safe provider error mapping and order readback after submit/cancel.

Methods/classes touched:

- `Polymarket._submit_limit_order(order)`
- `Polymarket.cancel_order(order)`
- `Polymarket._pull_broker_order(identifier)`
- `Polymarket._parse_broker_order(...)`

Tests:

- Unit: limit price quantizes to current tick size.
- Unit: post-only only works for GTC/GTD, not FAK/FOK.
- Unit: cancel calls provider even when local status is stale.
- Live apitest: place far-from-market tiny limit order, confirm identifier, cancel, poll/read back canceled status.

Exit criteria:

- Limit submit/cancel lifecycle works or fails loudly with provider error.
- Unknown order rows do not crash refresh.
- Submit/cancel behavior is covered by fake-client unit tests and one gated live smoke.

### Phase 7: Examples, Docs, And Release Hardening

Goals:

- Add user-facing LumiBot examples for quote/account read and tiny controlled order.
- Add env-var docs.
- Add broker order semantics docs for prediction contracts.
- Run focused unit suites and skipped-live apitest discovery.
- Keep BotSpot implementation as follow-on, not part of this first LumiBot adapter.

Exit criteria:

- New unit tests pass locally.
- Live apitests are provider-gated and skipped unless env vars are present.
- Docs explain market BUY notional semantics and private-key/session-credential risks.
- Release notes clearly state this is international CLOB only, not Polymarket US.

### Phase 8: BotSpot And Bot Manager Follow-On

Do this only after the LumiBot adapter is proven locally.

Goals:

- Add BotSpot saved credential metadata and validation behind an internal flag.
- Add Bot Manager runtime-secret allowlist and dependency packaging.
- Add broker-data read-only quotes and portfolio snapshots before trading.
- Add single-trade support only after high-risk approval, private-key/signing design, and compliance review.

Exit criteria:

- Internal saved credential can run a read-only broker snapshot.
- No raw secrets are returned through MCP/frontend APIs.
- Single-trade and deployment support use runtime-secret refs only.
- Public launch decision is made separately for `polymarket.com` CLOB versus `polymarket.us`.

## Questions For Rob

Answered in the 2026-07-01 planning thread:

- Target the international `polymarket.com` CLOB account first. Do not block on Polymarket US.
- Create or derive CLOB credentials if needed.
- Store prototype credentials in `.env.local`.
- First live trading proof can use $1-$5 maximum notional.
- More liquid markets are acceptable for the first proof; slower markets are not required if sizing and max-price controls
  are tight.

Remaining non-blocking decisions:

1. Which explicit `POLYMARKET_TEST_TOKEN_ID` should be used for first live smoke, or should the implementation include a
   read-only discovery command that suggests a liquid token id before any order is placed?
2. Should early live tests sell/reduce the tiny position after a successful market BUY, or leave the small position open
   for subsequent position/order stream testing?
3. Should the first implementation depend on the beta unified `polymarket-client`, the lower-level
   `py-clob-client-v2`, or both behind a wrapper? My recommendation is wrapper-first with the unified SDK preferred for
   account/stream ergonomics and lower-level CLOB client available for any missing order helper.

## Recommended Immediate Next Step

The next implementation action is a credential/login proof for international CLOB:

1. Inspect the `polymarket.com` account `APIs` page and identify which fields it exposes, without exposing values.
2. Create or derive CLOB credentials if the SDK/login proof requires them.
3. Store only local prototype secrets in `.env.local`.
4. Run a read-only authenticated account snapshot.
5. Commit the smallest implementation slice only after the read-only proof and unit tests are in place.

After that, implement Phase 1 and Phase 2 in small slices with tests after each slice.
