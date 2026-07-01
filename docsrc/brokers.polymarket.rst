Polymarket
===================================

This guide covers the LumiBot broker adapter for the international ``polymarket.com`` CLOB. It does not cover
Polymarket US, which uses a separate account/API surface and should be implemented as a separate broker adapter.

Status
------

The current adapter supports authenticated read paths, market data, and WebSocket subscriptions. Live order submission
is implemented, but existing Magic/proxy accounts can be rejected by Polymarket's current deposit-wallet/API-key binding
rules. On Rob's verified 2026-07-01 local account, balances, positions, open orders, recent trades, quotes, history, and
public/private WebSockets worked; tiny market and limit orders were rejected by Polymarket before any order was created.

Configuration
-------------

Use explicit broker selection:

.. code-block:: bash

   TRADING_BROKER=polymarket

Only set ``DATA_SOURCE=polymarket`` when you intentionally want the Polymarket public data source as a separate data
source override. Normal live trading should select the broker with ``TRADING_BROKER=polymarket``.

Required and optional variables:

.. code-block:: bash

   POLYMARKET_PRIVATE_KEY=0x...
   POLYMARKET_OWNER_ADDRESS=0x...        # optional but recommended for Magic/proxy accounts
   POLYMARKET_WALLET_ADDRESS=0x...       # funder/proxy/deposit wallet
   POLYMARKET_SIGNATURE_TYPE=1           # 0 EOA, 1 proxy/Magic, 2 Safe, 3 deposit wallet
   POLYMARKET_CLOB_API_KEY=...
   POLYMARKET_CLOB_API_SECRET=...
   POLYMARKET_CLOB_API_PASSPHRASE=...
   POLYMARKET_MAX_MARKET_ORDER_NOTIONAL=5

Never commit these values. For local prototypes, keep them in ``.env.local`` and make sure that file is ignored by git.

Credential Model
----------------

Polymarket CLOB uses two authentication layers:

- L1 wallet signing with ``POLYMARKET_PRIVATE_KEY``. LumiBot uses this to create/derive CLOB API credentials and sign
  order payloads locally.
- L2 CLOB credentials: ``POLYMARKET_CLOB_API_KEY``, ``POLYMARKET_CLOB_API_SECRET``, and
  ``POLYMARKET_CLOB_API_PASSPHRASE``. LumiBot uses these for private balance/order/trade reads, posting signed orders,
  cancellations, and the private user WebSocket.

Relayer API keys are separate from CLOB trading credentials. They are for wallet deployment or wallet-operation batches
in deposit-wallet flows, not a standalone order-trading credential.

Assets
------

Polymarket outcome tokens use:

.. code-block:: python

   from lumibot.entities import Asset

   asset = Asset(
       "<clob_token_id>",
       asset_type=Asset.AssetType.PREDICTION_CONTRACT,
       precision="0.000001",
   )

Use ``PolymarketData.resolve_market(...)`` and ``PolymarketData.resolve_contract(...)`` to resolve a market slug, URL,
condition id, or outcome label into the CLOB token id.

Market Data
-----------

``PolymarketData`` handles:

- market and outcome token resolution;
- order-book snapshots;
- quotes and last trade price;
- supported CLOB price history;
- public market WebSocket cache updates.

History is only returned from real Polymarket data. LumiBot must not synthesize missing prediction-market bars.

Orders
------

Market BUY orders require explicit dollar notional in ``custom_params["amount"]`` because Polymarket market BUY
semantics are not the same as a stock quantity order.

Example tiny FAK market order:

.. code-block:: python

   order = self.create_order(
       asset,
       quantity=1,
       side="buy",
       order_type="market",
       custom_params={
           "amount": "1.00",
           "price": "0.99",
           "order_type": "FAK",
       },
   )
   self.submit_order(order)

The broker enforces ``POLYMARKET_MAX_MARKET_ORDER_NOTIONAL`` for market BUY orders. The default cap is ``5``.

Limit orders use the LumiBot limit price and quantity. LumiBot passes Polymarket SDK order options using the live book's
``tick_size`` and ``neg_risk`` fields.

WebSockets
----------

The adapter subscribes to:

- public market stream: ``wss://ws-subscriptions-clob.polymarket.com/ws/market``;
- private user stream: ``wss://ws-subscriptions-clob.polymarket.com/ws/user``.

HTTP polling remains active as reconciliation after reconnects and as a degraded fallback. The user stream is filtered by
CLOB API credentials and dispatches normalized LumiBot order lifecycle events where possible.

Testing
-------

Focused unit tests:

.. code-block:: bash

   python3 -m pytest -q tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py

Live/API smoke tests with local credentials:

.. code-block:: bash

   python3 -m dotenv -f .env.local run -- python3 -m pytest -q tests/test_polymarket_apitest.py

Direct SDK/API proof helper:

.. code-block:: bash

   python3 scripts/polymarket_smoke.py
   python3 scripts/polymarket_smoke.py --live-order
   python3 scripts/polymarket_smoke.py --limit-cancel

The direct smoke helper redacts secrets and writes proof artifacts under gitignored ``logs/``.

Known Limitations
-----------------

- Polymarket US is not supported by this adapter.
- Current live submit may fail for Magic/proxy or deposit-wallet accounts if Polymarket rejects the signer/funder/API-key
  relationship.
- Private user WebSocket fill reconciliation has been connection-tested, but real fill events are not proven until
  Polymarket accepts a live order for the account.
- BotSpot/Bot Manager credential UI and runtime-secret injection are intentionally deferred until LumiBot has a working
  live submit path.
