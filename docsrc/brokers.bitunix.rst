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

You can specify the leverage for a Bitunix futures order by setting the `leverage` attribute on the `Asset` object before creating the order. If not set, the default leverage configured at the broker will be used.

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
