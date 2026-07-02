Polymarket
===================================

LumiBot supports Polymarket prediction-contract trading and backtesting through the ``Polymarket`` broker,
``PolymarketData`` data source, and ``PolymarketBacktesting`` historical data source. A Polymarket outcome token is a
``prediction_contract`` asset priced between ``0`` and ``1`` in USD collateral. The same strategy structure can discover
markets, read order books, backtest historical prices, and submit live orders through the normal LumiBot broker API.

Quick Start
-----------

Use the Polymarket broker explicitly:

.. code-block:: bash

   TRADING_BROKER=polymarket

Use Polymarket historical data explicitly for backtests:

.. code-block:: bash

   BACKTESTING_DATA_SOURCE=polymarket

In code, create prediction-contract assets from CLOB token ids:

.. code-block:: python

   from lumibot.entities import Asset

   asset = Asset(
       "<clob_token_id>",
       asset_type=Asset.AssetType.PREDICTION_CONTRACT,
       precision="0.000001",
   )

For a complete runnable example, see:

.. code-block:: bash

   python -m lumibot.example_strategies.polymarket_prediction_contract

Configuration
-------------

The live broker uses the same environment-driven broker selection as other LumiBot brokers. ``TRADING_BROKER`` selects
the trading broker. ``DATA_SOURCE`` is a separate optional data-source override and should not be used as the primary
live broker selector.

Required and optional live variables:

.. code-block:: bash

   TRADING_BROKER=polymarket
   POLYMARKET_PRIVATE_KEY=0x...
   POLYMARKET_OWNER_ADDRESS=0x...
   POLYMARKET_PROXY_WALLET_ADDRESS=0x...
   POLYMARKET_DEPOSIT_WALLET_ADDRESS=0x...
   POLYMARKET_WALLET_ADDRESS=0x...
   POLYMARKET_SIGNATURE_TYPE=3
   POLYMARKET_CLOB_API_KEY=...
   POLYMARKET_CLOB_API_SECRET=...
   POLYMARKET_CLOB_API_PASSPHRASE=...
   POLYMARKET_BUILDER_API_KEY=...
   POLYMARKET_BUILDER_SECRET=...
   POLYMARKET_BUILDER_PASSPHRASE=...
   POLYMARKET_MAX_MARKET_ORDER_NOTIONAL=5

Backtesting variables:

.. code-block:: bash

   IS_BACKTESTING=true
   BACKTESTING_DATA_SOURCE=polymarket
   POLYMARKET_TEST_TOKEN_ID=<clob_token_id>

Keep private keys, CLOB credentials, and builder credentials in a local ignored file such as ``.env.local`` or a real
secret manager. Never commit or log raw values.

Credential Model
----------------

Polymarket CLOB trading uses two authentication layers:

- L1 wallet signing with ``POLYMARKET_PRIVATE_KEY``. LumiBot signs order payloads locally and can derive CLOB
  credentials from this signer.
- L2 CLOB credentials: ``POLYMARKET_CLOB_API_KEY``, ``POLYMARKET_CLOB_API_SECRET``, and
  ``POLYMARKET_CLOB_API_PASSPHRASE``. LumiBot uses these for private balance, order, trade, cancel, and user-WebSocket
  requests.

Builder or relayer credentials are separate. They are used for deposit-wallet setup, relayer transactions, proxy
transfers, and approval batches. They do not replace the CLOB trading credentials used for normal order placement.

Deposit Wallet Flow
-------------------

Polymarket accounts that use the deposit-wallet flow must trade from the deposit wallet, not from the owner EOA or an
old proxy wallet. LumiBot includes a helper for the setup flow:

.. code-block:: bash

   python3 scripts/polymarket_deposit_wallet_setup.py --create-builder-key
   python3 scripts/polymarket_deposit_wallet_setup.py --deploy
   python3 scripts/polymarket_deposit_wallet_setup.py --fund-amount 5
   python3 scripts/polymarket_deposit_wallet_setup.py --approve
   python3 scripts/polymarket_deposit_wallet_setup.py --approve-conditional
   python3 scripts/polymarket_deposit_wallet_setup.py --activate

The deposit wallet needs:

- pUSD collateral held by the deposit wallet;
- pUSD approvals submitted from the deposit wallet for CLOB spenders;
- conditional-token ``setApprovalForAll`` approvals for sells;
- ``POLYMARKET_WALLET_ADDRESS`` set to the deposit wallet;
- ``POLYMARKET_SIGNATURE_TYPE=3``.

If the platform returns ``maker address not allowed, please use the deposit wallet flow`` or a signer/API-key address
mismatch, migrate the account through the deposit-wallet helper and re-run the read-only smoke before submitting orders.

Market And Token Discovery
--------------------------

``PolymarketData`` resolves markets, outcomes, and token ids. The most common flow is:

.. code-block:: python

   from lumibot.data_sources.polymarket_data import PolymarketData

   data = PolymarketData()

   markets = data.search_markets("fed decision", limit=5)
   market = data.resolve_market(slug="will-the-fed-cut-rates-in-july")
   yes = data.resolve_contract(market, outcome="Yes")

   quote = data.get_quote(yes)
   book = data.get_order_book(yes)
   history = data.get_historical_prices(yes, length=None, timestep="minute", start=start, end=end)

Prediction-Market Data Methods
------------------------------

The following methods are implemented on ``PolymarketData``. The base ``DataSource`` class also exposes safe default
implementations so strategy code can call these helpers without breaking other brokers or data sources.

``search_markets(query, limit=...)``
   Search Polymarket markets/events/profiles and return matching rows from Polymarket search APIs.

``get_event(event_id=None, slug=None)``
   Return event-level metadata such as title, description, markets, tags, and category when Polymarket exposes it.

``get_market_metadata(market=None, slug=None, url=None, market_id=None, condition_id=None, token_id=None)``
   Return normalized Gamma market metadata. This is the source used by most other helpers.

``get_market_rules(market)``
   Return tradability rules: question, description, outcome labels, CLOB token ids, tick size, minimum order size,
   negative-risk flag, market close, accepting-orders flag, and raw metadata.

``get_resolution_status(market)``
   Return resolution status, closed flag, winner when known, condition id, and raw metadata.

``get_spread(asset)``
   Return the CLOB spread for an outcome token.

``get_midpoint(asset)``
   Return the CLOB midpoint for an outcome token.

``get_recent_trades(market=None, limit=...)``
   Return recent trades for a market, user, or token depending on the supplied filters.

``get_open_interest(market)``
   Return open interest when available from Polymarket's data API.

``get_holders(market, limit=...)``
   Return holder rows for a token or market.

Additional helpers include ``get_market_close``, ``get_resolution_source``, ``get_min_order_size``, and
``get_settlement_price``.

Orders
------

Polymarket market BUY orders use dollar notional, not share quantity. Pass the dollar amount in
``custom_params["amount"]``:

.. code-block:: python

   order = self.create_order(
       asset,
       quantity=1,
       side="buy",
       order_type="market",
       time_in_force="fak",
       custom_params={
           "amount": "1.00",
           "price": "0.99",
           "order_type": "FAK",
       },
   )
   self.submit_order(order)

Supported order and cancel surface:

- FAK market BUY: dollar amount in ``custom_params["amount"]``.
- FOK market BUY: dollar amount plus ``custom_params["order_type"]="FOK"``.
- FAK/FOK market SELL: shares in ``order.quantity`` or ``custom_params["shares"]``.
- GTC limit BUY/SELL: ``order_type="limit"``, ``time_in_force="gtc"``.
- GTD limit BUY/SELL: ``time_in_force="gtd"`` with ``good_till_date`` or ``custom_params["expiration"]``.
- Post-only limit BUY/SELL: ``custom_params["post_only"]=True`` on GTC/GTD limit orders.
- Single cancel: ``cancel_order(order)``.
- Multiple cancel: ``cancel_orders([order1, order2])``.
- Cancel all: ``cancel_all_orders()``.
- Cancel by market: ``cancel_market_orders(market=..., asset_id=...)``.
- Batch limit orders: ``submit_orders(..., batch=True)`` for up to 15 limit orders when the SDK exposes ``post_orders``.

The live broker reads the market's tick size and negative-risk setting from the order book/market metadata and passes
those values into the CLOB SDK. Market BUY orders are capped by ``POLYMARKET_MAX_MARKET_ORDER_NOTIONAL``.

Buy And Sell Semantics
----------------------

BUY and SELL are not symmetric on Polymarket:

- BUY market orders spend pUSD/collateral from the active funder wallet.
- SELL market orders sell existing outcome-token shares.
- BUYs need collateral allowance.
- SELLs need conditional-token approvals for the token being sold.
- Limit prices must be inside ``0`` and ``1`` and should conform to the market tick size.

WebSockets
----------

The broker can use both public and private WebSocket streams:

- public market stream: ``wss://ws-subscriptions-clob.polymarket.com/ws/market``;
- private user stream: ``wss://ws-subscriptions-clob.polymarket.com/ws/user``.

Public market events update the ``PolymarketData`` quote cache. Private user events are authenticated with CLOB L2
credentials, deduplicated, and normalized into LumiBot order lifecycle events. HTTP polling remains active as
reconciliation after reconnects and as a fallback.

Backtesting
-----------

Use ``PolymarketBacktesting`` for historical prediction-contract tests:

.. code-block:: python

   from datetime import datetime, timezone
   from lumibot.backtesting import PolymarketBacktesting
   from lumibot.entities import Asset
   from lumibot.example_strategies.polymarket_prediction_contract import PolymarketPredictionContractStrategy

   asset = Asset("<clob_token_id>", asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")

   PolymarketPredictionContractStrategy.backtest(
       PolymarketBacktesting,
       datetime(2026, 6, 1, tzinfo=timezone.utc),
       datetime(2026, 6, 2, tzinfo=timezone.utc),
       assets=[asset],
       market="24/7",
       timestep="minute",
       parameters={"token_id": asset.symbol, "backtest_trade": True},
       benchmark_asset=None,
   )

Polymarket backtesting:

- loads real Polymarket CLOB ``prices-history`` data as OHLCV bars;
- keeps prediction-contract prices between ``0`` and ``1``;
- preserves sub-cent prediction prices instead of rounding them to stock-style cents;
- uses USD collateral accounting for ``prediction_contract`` assets;
- fills simple market and limit orders from the Polymarket backtesting data source;
- validates tick size and minimum order size when rules are available;
- honors market close metadata when available;
- marks resolved outcome tokens to ``1`` or ``0`` when resolved metadata is available.

Live Smoke Tests
----------------

The live smoke matrix is intentionally off by default because Polymarket orders use real funds. Read-only tests can run
with credentials. Live tests require explicit environment flags and hard notional caps.

Focused unit tests:

.. code-block:: bash

   python3 -m pytest -q tests/test_prediction_market_data_source_defaults.py tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py tests/test_polymarket_backtesting.py

Read-only/API smoke:

.. code-block:: bash

   python3 -m dotenv -f .env.local run -- python3 -m pytest -q tests/test_polymarket_apitest.py -k "not tiny_market_buy_smoke"

Live LumiBot smoke examples:

.. code-block:: bash

   POLYMARKET_LIVE_TRADING_ENABLED=true python3 scripts/polymarket_lumibot_smoke.py --order-kind fak-buy --amount 1.00
   POLYMARKET_LIVE_TRADING_ENABLED=true python3 scripts/polymarket_lumibot_smoke.py --order-kind fok-buy --amount 1.00
   POLYMARKET_LIVE_TRADING_ENABLED=true python3 scripts/polymarket_lumibot_smoke.py --order-kind fak-sell --limit-size 1 --limit-price 0.01
   POLYMARKET_LIVE_TRADING_ENABLED=true python3 scripts/polymarket_lumibot_smoke.py --order-kind cancel-single --limit-size 5 --limit-price 0.01
   POLYMARKET_LIVE_TRADING_ENABLED=true python3 scripts/polymarket_lumibot_smoke.py --websocket --order-kind fak-sell --limit-size 1 --limit-price 0.01

The smoke helper also supports ``fok-sell``, ``gtc-buy``, ``gtc-sell``, ``gtd-buy``, ``gtd-sell``,
``post-only-buy``, ``post-only-sell``, ``cancel-multiple``, ``cancel-all``, and ``cancel-market``.

Troubleshooting
---------------

``maker address not allowed, please use the deposit wallet flow``
   Deploy/fund/approve/activate the deposit wallet and set ``POLYMARKET_SIGNATURE_TYPE=3``.

``the order signer address has to be the address of the API KEY``
   Re-check owner address, funder wallet, CLOB API credentials, and signature type. The active CLOB credentials must
   match the signer/funder path being used.

``invalid post-only order: order crosses book``
   The post-only order is marketable. Use a less aggressive limit price or submit a normal GTC/GTD limit order.

Backtest tries another provider for a long token id
   Set ``BACKTESTING_DATA_SOURCE=polymarket`` or pass ``PolymarketBacktesting`` directly. Prediction contracts should
   stay on Polymarket data and must not fall through to stock/IBKR/Yahoo lookup paths.

No fake-money live matrix
   The public Polymarket CLOB docs expose staging hosts in places, but LumiBot does not treat them as a complete
   fake-money paper trading environment. Use mocked unit tests for CI and run live smoke tests only when explicitly
   funded and approved.
