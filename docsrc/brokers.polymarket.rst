Polymarket
===================================

This guide covers the LumiBot broker adapter for the international ``polymarket.com`` CLOB. It does not cover
Polymarket US, which uses a separate account/API surface and should be implemented as a separate broker adapter.

Status
------

The current adapter supports authenticated read paths, market data, WebSocket subscriptions, market orders, limit
orders, and cancels for Polymarket International CLOB deposit-wallet accounts. On Rob's verified 2026-07-01 local
account, the deposit wallet was deployed, funded with pUSD, approved for CLOB spenders, and then proven with:

- direct SDK read/account/data/WebSocket smoke;
- direct SDK tiny market order;
- direct SDK tiny limit order plus cancel;
- LumiBot ``Strategy.create_order(...)`` and ``Strategy.submit_order(...)`` tiny market order;
- LumiBot tiny limit order plus ``broker.cancel_order(...)``.

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
   POLYMARKET_PROXY_WALLET_ADDRESS=0x... # optional old proxy wallet kept for funding/migration
   POLYMARKET_DEPOSIT_WALLET_ADDRESS=0x...
   POLYMARKET_WALLET_ADDRESS=0x...       # active CLOB funder; use deposit wallet for trading
   POLYMARKET_SIGNATURE_TYPE=3           # 0 EOA, 1 proxy/Magic, 2 Safe, 3 deposit wallet
   POLYMARKET_CLOB_API_KEY=...
   POLYMARKET_CLOB_API_SECRET=...
   POLYMARKET_CLOB_API_PASSPHRASE=...
   POLYMARKET_BUILDER_API_KEY=...        # relayer/deposit-wallet setup only
   POLYMARKET_BUILDER_SECRET=...
   POLYMARKET_BUILDER_PASSPHRASE=...
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

Relayer/builder API keys are separate from CLOB trading credentials. They are for wallet deployment, proxy transfers,
and deposit-wallet approval batches, not standalone order-trading credentials. LumiBot order submission still uses CLOB
L1/L2 auth after the deposit wallet is ready.

Deposit Wallet Setup
--------------------

Use the helper script to create builder credentials, deploy/discover the deterministic deposit wallet, optionally fund it
from the existing Polymarket proxy wallet, approve CLOB pUSD spenders, and activate it as the trading funder:

.. code-block:: bash

   python3 scripts/polymarket_deposit_wallet_setup.py --create-builder-key
   python3 scripts/polymarket_deposit_wallet_setup.py --deploy
   python3 scripts/polymarket_deposit_wallet_setup.py --fund-amount 5
   python3 scripts/polymarket_deposit_wallet_setup.py --approve
   python3 scripts/polymarket_deposit_wallet_setup.py --activate

For a fresh local prototype, ``--all --fund-amount 5`` runs deploy, fund, approve, and activate in one pass after builder
credentials already exist. The script writes only to ``.env.local`` and redacts values in console output.

Deposit-wallet trading requires:

- pUSD held by the deposit wallet, not only by the owner EOA or old proxy wallet;
- pUSD approvals submitted from the deposit wallet through a relayer ``WALLET`` batch;
- ``POLYMARKET_SIGNATURE_TYPE=3``;
- ``POLYMARKET_WALLET_ADDRESS`` set to the deposit wallet.

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

LumiBot-level proof helper:

.. code-block:: bash

   python3 scripts/polymarket_lumibot_smoke.py
   python3 scripts/polymarket_lumibot_smoke.py --market-order --amount 1.00
   python3 scripts/polymarket_lumibot_smoke.py --limit-cancel

The direct smoke helper redacts secrets and writes proof artifacts under gitignored ``logs/``.

Known Limitations
-----------------

- Polymarket US is not supported by this adapter.
- Old Magic/proxy accounts can read balances but can be rejected for live order submit unless migrated to the supported
  deposit-wallet flow.
- Immediate post-submit cash reads can lag inside the same SDK client. A fresh CLOB read reconciles the final balance.
- BotSpot/Bot Manager credential UI and runtime-secret injection are intentionally deferred until LumiBot has a working
  live submit path.
