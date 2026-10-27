Bitunix API Perpetual Futures Trading with LumiBot
==================================================

.. meta::
   :description: Configure Bitunix API perpetual futures trading with LumiBot, including USDT futures funding, credentials, order setup, and supported behavior.

.. include:: _snippets/botspot_mcp_callout.rst

How to Use Bitunix
------------------

Bitunix integration in Lumibot supports **only perpetual futures trading**. Spot trading is not supported.

.. list-table:: Bitunix support at a glance
   :header-rows: 1
   :widths: 30 25 45

   * - Capability
     - Status
     - Requirement
   * - USDT perpetual futures
     - Supported
     - Fund the Futures wallet with USDT
   * - Spot trading
     - Not supported
     - Use another broker integration
   * - Hedge-mode positions
     - Required
     - The account must confirm ``HEDGE`` mode
   * - Historical futures bars
     - Supported
     - Use a native interval and available exchange history

**Account Funding and Cash Calculation:**

- You must transfer funds to the **Futures** section of your Bitunix account. Money in the spot wallet will not be available for trading.
- For accurate calculation of available cash, it is strongly recommended to deposit **USDT** into your Bitunix Futures account. If you deposit crypto (e.g., BTC), it will not register as available cash for order sizing, though it can still be used as margin by Bitunix.

**Environment Variables**

Set the following environment variables in your `.env` file or system environment:

.. code-block:: shell

    BITUNIX_API_KEY=your_bitunix_api_key
    BITUNIX_API_SECRET=your_bitunix_api_secret

Position Refresh Failures
----------------------------

Position reads require a complete successful response. A transport error,
rejected request, or malformed position raises ``LumibotBrokerAPIError`` and
leaves tracked positions unchanged. A failed read does not mean the account is
flat and is not cached as a successful refresh; a later read can retry.
An explicitly successful empty snapshot removes all stale non-cash positions.
Concurrent polling and strategy reads preserve the latest successfully applied
request: an older response cannot remove, resurrect, or overwrite its positions.
Failed reads remain retryable and do not discard another successful response.
Positions added locally during a pending read retain their fields and ownership
until the next fresh snapshot.
Polling reports the failure and retries on its next cycle. Strategy code using
fresh ``get_position()`` or ``get_positions()`` reads should allow the error to
stop that decision, rather than treating it as permission to open a position.

HEDGE mode can hold a long and a short position on the same symbol at once.
Each one is tracked as its own position with ``position_side`` (``"long"`` or
``"short"``) and ``position_id`` (the exchange ``positionId``); the raw exchange
row stays available as ``position._raw``. ``get_positions()`` returns both.
Select one with ``get_position(asset, position_side="short")`` or by
``position_id``; an unselected ``get_position(asset)`` raises ``ValueError``
while both exist rather than silently picking one. With a single position on a
symbol, ``get_position(asset)`` works exactly as before.

``close_position(asset, position_side="short")`` sends a hedge-mode ``CLOSE``
for that position only. ``close_position(asset)`` with both open closes each by
its own ``positionId`` and returns a list of orders. Opening orders record the
position they open (a buy opens the long side, a sell the short side), so fills
attach to the right position. Two nonzero rows for the same symbol and side
without distinct position IDs are still ambiguous: they raise and preserve
tracked state. Zero-quantity rows are ignored.

.. code-block:: python

    long_leg = self.get_position(contract, position_side="long")
    short_leg = self.get_position(contract, position_side="short")
    if short_leg is not None:
        self.close_position(contract, position_side="short")

Setting Leverage for Bitunix Orders
-----------------------------------

Specify leverage in the ``CRYPTO_FUTURE`` Asset constructor or set its
``leverage`` attribute before creating an order. The constructor preserves the
requested leverage; its default is 1. LumiBot requests that leverage from
Bitunix before submitting an opening order. Reduce-only orders, including full
and fractional ``close_position`` calls, preserve the existing exchange leverage
without requesting a leverage change. This also applies after a restart when
the local leverage cache is empty. If the exchange rejects an opening leverage
change, LumiBot logs a warning; the Asset value does not confirm the exchange's
actual leverage.

**Example: Setting Leverage on a Bitunix Futures Order**

.. code-block:: python

    from lumibot.entities import Asset, Order

    asset = Asset("HBARUSDT", Asset.AssetType.CRYPTO_FUTURE, leverage=10)
    order = self.create_order(
        asset=asset,
        quantity=100,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.18
    )
    submitted_order = self.submit_order(order)
    if submitted_order:
        self.log_message(f"Placed order: ID={submitted_order.identifier}, Status={submitted_order.status}")

Order Precision and Position Mode
---------------------------------

LumiBot loads and caches Bitunix trading-pair rules for each symbol during the
broker session. Quantities round down to ``basePrecision`` decimal places;
limit, take-profit, and stop-loss prices round down to ``quotePrecision``.
All quantity and price fields are sent as decimal strings. For example, with
BTCUSDT rules of ``basePrecision=4`` and ``minTradeVolume=0.0001``, a requested
quantity of ``0.008868641`` becomes ``"0.0088"``. The tracked order quantity
uses this executable size. Rounding down can leave a small residual position
after a partial close.

Quantities below ``minTradeVolume`` after rounding return an order with
``ERROR`` status without placing an exchange order. Missing or invalid pair
rules also block submission; failed lookups are retried on the next order.

The adapter requires confirmed ``HEDGE`` mode before submitting. If mode
initialization fails or reports ``ONE_WAY``, the order receives a clear error
and is not sent. Check the account mode and outstanding positions/orders
before retrying: Bitunix can reject mode changes while positions or orders
exist. Opens send ``tradeSide="OPEN"``. Reduce-only closes send
``tradeSide="CLOSE"`` with the matching exchange position ID and hedge side.
An absent or ambiguous matching position blocks the close.

See the Bitunix `place-order contract
<https://www.bitunix.com/api-docs/futures/trade/place_order.html>`_ and
`trading-pair rules
<https://www.bitunix.com/api-docs/futures/market/get_trading_pairs.html>`_.

Historical Bars
---------------

Bitunix serves native crypto-futures intervals including ``1m``, ``15m``,
``1h``, ``2h``, ``4h``, and ``1d``. LumiBot requests a native interval when it
matches the strategy timeframe instead of downloading one-minute bars and
resampling them locally.

The Bitunix futures API limits each kline response to 200 candles. LumiBot
automatically paginates timestamp-bounded windows when ``length`` is greater
than 200. If the symbol does not have enough exchange history to satisfy the
request, ``get_historical_prices`` raises a clear error with the returned and
requested counts instead of silently returning a short frame.

Example Usage
-------------

Below are practical examples using the Bitunix broker in Lumibot, based on the `bitunix_futures_example.py` strategy.

**Placing a Limit Order for a Futures Contract**

.. code-block:: python

    from lumibot.entities import Asset, Order

    asset = Asset("HBARUSDT", Asset.AssetType.CRYPTO_FUTURE)
    asset.leverage = 5  # Example: set leverage to 5x
    order = self.create_order(
        asset=asset,
        quantity=100,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.18
    )
    submitted_order = self.submit_order(order)
    if submitted_order:
        self.log_message(f"Placed order: ID={submitted_order.identifier}, Status={submitted_order.status}")

**Closing a Position**

.. code-block:: python

    # Wait for a few seconds if needed
    import time
    time.sleep(10)
    self.close_position(asset)

``close_position`` uses reduce-only semantics. Partial closes are supported by
passing ``fraction`` between 0 and 1, for example
``self.close_position(asset, fraction=0.5)``.

**Cancelling Open Orders**

.. code-block:: python

    orders = self.get_orders()
    for order in orders:
        if order.asset.symbol == "HBARUSDT" and order.asset.asset_type == Asset.AssetType.CRYPTO_FUTURE and order.status in [
            Order.OrderStatus.NEW, Order.OrderStatus.SUBMITTED, Order.OrderStatus.OPEN, Order.OrderStatus.PARTIALLY_FILLED
        ]:
            self.cancel_order(order)
            self.log_message(f"Order {order.identifier} cancellation submitted.")

**Checking Available Cash**

.. code-block:: python

    cash = self.get_cash()
    self.log_message(f"Current cash {cash}")

.. note::
    For best results, ensure all funds in your Bitunix Futures account are in USDT. Crypto balances may not be counted as available cash for order sizing.

Frequently asked questions
--------------------------

Can I run a Bitunix trading bot with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, for USDT perpetual futures. Set ``BITUNIX_API_KEY`` and ``BITUNIX_API_SECRET``. Choose it with ``TRADING_BROKER=bitunix``. If no other broker is configured, LumiBot also picks Bitunix on its own when both keys are set. A working example ships with LumiBot as ``lumibot/example_strategies/bitunix_futures_example.py``.

Does LumiBot support Bitunix spot trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. The Bitunix integration only trades perpetual futures. Orders must use a ``CRYPTO_FUTURE`` asset such as ``Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE)``. Any other asset type is rejected before it reaches the exchange.

Does LumiBot use CCXT for Bitunix?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Bitunix has its own native LumiBot broker that talks to the Bitunix futures API directly, so you do not need CCXT settings for it. The `Bitunix API documentation <https://www.bitunix.com/api-docs/>`__ covers the exchange side, including how requests are signed.

Is there a Bitunix testnet or paper trading mode in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. The LumiBot Bitunix broker connects to the live Bitunix futures API (``fapi.bitunix.com``) and has no testnet switch. Every order uses real funds, so start with very small sizes.

Which order types can I use on Bitunix?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Market, limit, stop and stop limit orders. You can also attach a take-profit price and a stop-loss price to an order. Multi-leg, OCO, OTO and bracket order groups are not supported.

How do I set leverage for a Bitunix order?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass ``leverage`` when you create the ``CRYPTO_FUTURE`` asset, or set ``asset.leverage`` before you create the order. The default is 1. LumiBot asks Bitunix for that leverage before an opening order. See `Setting Leverage for Bitunix Orders`_ above for details.

Why did my Bitunix order get an ERROR status without reaching the exchange?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The two most common causes are size and account mode. LumiBot rounds the quantity down to the pair precision, and if the result is below the pair minimum the order stops with an error. LumiBot also needs the account in ``HEDGE`` position mode. If the account reports ``ONE_WAY`` mode, the order is not sent.

How much price history can I get from Bitunix?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Bitunix futures API returns up to 200 candles per request. LumiBot pages through more requests on its own when you ask for more than 200 bars. If the symbol does not have enough history, ``get_historical_prices`` raises a clear error instead of returning a short result.

Why does LumiBot show less cash than my Bitunix balance?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot counts USDT in the Futures wallet as cash. Money in the spot wallet is not available for trading, and crypto like BTC in the Futures wallet is not counted as cash for order sizing. Move USDT into the Futures wallet to fix this.

Documentation
---------------

.. automodule:: lumibot.brokers.bitunix
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:

.. automethod:: lumibot.brokers.bitunix.Bitunix.get_time_to_close
   :no-index:

.. automethod:: lumibot.brokers.bitunix.Bitunix.get_time_to_open
   :no-index:

.. automethod:: lumibot.brokers.bitunix.Bitunix.get_timestamp
   :no-index:

.. automethod:: lumibot.brokers.bitunix.Bitunix.is_market_open
   :no-index:

.. automethod:: lumibot.brokers.bitunix.Bitunix.close_position
   :no-index:
