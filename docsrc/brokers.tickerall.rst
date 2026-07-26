TickerAll
======================================================

`TickerAll <https://tickerall.com>`_ is a hosted MetaTrader 5 API. This broker
lets a Lumibot strategy trade any MetaTrader 5 account (Forex, metals, indices,
CFDs, crypto) through that hosted API, so it runs on **any operating system with
no local MetaTrader 5 terminal installed** - unlike the official MetaTrader5
Python package, which is Windows-only and requires a running terminal.

How to Use TickerAll
--------------------

1. Create an account at `tickerall.com <https://tickerall.com>`_ and connect one
   or more MetaTrader 5 broker accounts in the dashboard.
2. Generate an API key.
3. Set the environment variables below (or pass a ``config`` dict to the broker).

The hosted API supports market data (historical bars, last price, quotes),
account balances and open positions, and order management (market, limit and
stop orders, with optional stop-loss and take-profit).

**Environment Variables**

Set the following in your ``.env`` file or system environment:

.. code-block:: shell

    TICKERALL_API_KEY=your_tickerall_api_key
    # Optional: only needed when the API key has more than one connected account
    TICKERALL_ACCOUNT_ID=your_connected_account_id

Instruments are addressed by their MetaTrader 5 symbol (for example
``EURUSDm``, ``XAUUSDm`` or ``BTCUSD``). Construct assets with the ``forex``
asset type so open positions reconcile against the orders you submit:

.. code-block:: python

    from lumibot.entities import Asset

    asset = Asset("EURUSDm", asset_type="forex")

Example Usage
-------------

**Creating the broker**

.. code-block:: python

    from lumibot.brokers import TickerAll
    from lumibot.traders import Trader

    config = {
        "API_KEY": "your_tickerall_api_key",
        # "ACCOUNT_ID": "your_connected_account_id",  # only if the key has several accounts
    }
    broker = TickerAll(config)

    trader = Trader()
    strategy = MyStrategy(broker=broker)
    trader.add_strategy(strategy)
    trader.run_all()

**Placing a market order with stop-loss and take-profit**

.. code-block:: python

    from lumibot.entities import Asset, Order

    asset = Asset("EURUSDm", asset_type="forex")
    order = self.create_order(
        asset=asset,
        quantity=0.10,                 # volume in lots
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        # stop-loss / take-profit are attached as a bracket order
        secondary_stop_price=1.0800,   # stop-loss price
        secondary_limit_price=1.1000,  # take-profit price
    )
    submitted_order = self.submit_order(order)
    if submitted_order:
        self.log_message(f"Placed order: ID={submitted_order.identifier}, Status={submitted_order.status}")

**Placing a limit order**

.. code-block:: python

    asset = Asset("XAUUSDm", asset_type="forex")
    order = self.create_order(
        asset=asset,
        quantity=0.10,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=2350.00,
    )
    self.submit_order(order)

**Reading bars, last price and account value**

.. code-block:: python

    asset = Asset("EURUSDm", asset_type="forex")
    bars = self.get_historical_prices(asset, 100, "day")
    last = self.get_last_price(asset)
    cash = self.get_cash()
    self.log_message(f"Cash {cash}, last {last}, {len(bars.df)} bars")

**Closing a position**

.. code-block:: python

    asset = Asset("EURUSDm", asset_type="forex")
    self.close_position(asset)

**Cancelling open (pending) orders**

.. code-block:: python

    for order in self.get_orders():
        if order.is_active():
            self.cancel_order(order)

.. note::
    The hosted API offers ``market``, ``limit`` and ``stop`` orders. ``stop_limit``
    and ``trailing_stop`` order types are not available and are rejected with a
    clear message rather than being silently mishandled. Order volume is
    expressed in lots.

Documentation
---------------

.. automodule:: lumibot.brokers.tickerall
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: lumibot.data_sources.tickerall_data
   :noindex:
   :members:
   :undoc-members:
   :show-inheritance:
