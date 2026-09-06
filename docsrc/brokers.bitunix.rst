Bitunix
======================================================

How to Use Bitunix
------------------

Bitunix integration in Lumibot supports **only perpetual futures trading**. Spot trading is not supported.

**Account Funding and Cash Calculation:**

- You must transfer funds to the **Futures** section of your Bitunix account. Money in the spot wallet will not be available for trading.
- For accurate calculation of available cash, it is strongly recommended to deposit **USDT** into your Bitunix Futures account. If you deposit crypto (e.g., BTC), it will not register as available cash for order sizing, though it can still be used as margin by Bitunix.

**Environment Variables**

Set the following environment variables in your `.env` file or system environment:

.. code-block:: shell

    BITUNIX_API_KEY=your_bitunix_api_key
    BITUNIX_API_SECRET=your_bitunix_api_secret

Setting Leverage for Bitunix Orders
-----------------------------------

Specify leverage in the ``CRYPTO_FUTURE`` Asset constructor or set its
``leverage`` attribute before creating an order. The constructor preserves the
requested leverage; its default is 1. LumiBot requests that leverage from
Bitunix before submitting the order. If the exchange rejects the leverage
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
