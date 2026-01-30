Common Mistakes and How to Avoid Them
======================================

This page documents the most common mistakes made when writing Lumibot strategies, along with the correct patterns to use instead.

Critical Mistakes (Will Break Your Strategy)
--------------------------------------------

Using datetime.now() Instead of self.get_datetime()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Backtest results will be completely wrong. Your strategy will think it's the current date instead of the simulated date.

.. code-block:: python

    # WRONG
    current_time = datetime.now()
    target_date = datetime.today() + timedelta(days=30)

    # CORRECT
    current_time = self.get_datetime()
    target_date = self.get_datetime() + timedelta(days=30)

Adding 'from __future__ import annotations'
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Causes immediate crash during backtesting.

.. code-block:: python

    # NEVER DO THIS - WILL CRASH YOUR STRATEGY
    from __future__ import annotations

Simply remove this import. It's not needed and will break everything.

Assigning Attributes Directly on self
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Overrides Lumibot internals, causing crashes or unexpected behavior.

.. code-block:: python

    # WRONG - collides with framework
    def initialize(self):
        self.name = "MyBot"
        self.asset = Asset("SPY")
        self.symbol = "SPY"

    # CORRECT - use self.vars
    def initialize(self):
        self.vars.strategy_label = "MyBot"
        self.vars.target_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        self.vars.target_symbol = "SPY"

Forgetting set_market("24/7") for Crypto
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Crypto bot stops trading at 4pm EST every day.

.. code-block:: python

    # WRONG - crypto bot stops at 4pm
    def initialize(self):
        self.sleeptime = "1M"

    # CORRECT - crypto trades 24/7
    def initialize(self):
        self.set_market("24/7")  # REQUIRED for crypto
        self.sleeptime = "1M"

Data Mistakes
-------------

Not Checking if get_last_price() Returns None
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Crashes with NoneType error when data is unavailable.

.. code-block:: python

    # WRONG - will crash on None
    price = self.get_last_price(asset)
    quantity = self.portfolio_value / price  # Crashes if price is None

    # CORRECT - always check for None
    price = self.get_last_price(asset)
    if price is None:
        self.log_message(f"No price for {asset.symbol}", color="red")
        return
    quantity = self.portfolio_value / price

Using get_historical_prices() for Real-Time Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Strategy uses stale data, missing current price movements.

.. code-block:: python

    # WRONG - historical data can be 1 minute delayed
    bars = self.get_historical_prices(asset, 1, "minute")
    current_price = bars.df.iloc[-1]["close"]

    # CORRECT - use get_last_price for real-time
    current_price = self.get_last_price(asset)

    # BEST - use get_quote for bid/ask
    quote = self.get_quote(asset)
    if quote and quote.bid and quote.ask:
        mid_price = quote.mid_price

Returning Early When get_greeks() is None
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Strategy stops running for the entire iteration just because one option has no Greeks.

.. code-block:: python

    # WRONG - blocks entire strategy
    greeks = self.get_greeks(option_asset)
    if greeks is None:
        return  # Strategy stops here!

    # CORRECT - continue with other logic
    greeks = self.get_greeks(option_asset)
    if greeks is not None:
        delta = greeks.get("delta")
        # Use delta here
    else:
        self.log_message("Greeks unavailable, skipping delta check", color="yellow")

    # Strategy continues with other logic...

Options Mistakes
----------------

Manually Selecting Option Expirations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Selected expiration may not have tradeable data during backtesting.

.. code-block:: python

    # WRONG - expiration may not exist
    target_expiry = self.get_datetime() + timedelta(days=30)
    option = Asset("SPY", asset_type=Asset.AssetType.OPTION,
                   expiration=target_expiry.date(), strike=400, right="call")

    # CORRECT - use OptionsHelper
    chains = self.get_chains(underlying_asset)
    target_expiry = self.get_datetime() + timedelta(days=30)
    valid_expiry = self.options_helper.get_expiration_on_or_after_date(
        target_expiry, chains, "call"
    )

Forgetting Options are 100x Multiplied
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Position sizing is off by 100x.

.. code-block:: python

    # WRONG - buys 100x too many contracts
    option_price = 1.50  # $1.50 premium
    contracts = 10000 / option_price  # Wrong: 6666 contracts!

    # CORRECT - account for multiplier
    option_price = 1.50
    actual_cost_per_contract = option_price * 100  # $150
    contracts = int(10000 / actual_cost_per_contract)  # Correct: 66 contracts

Using get_last_price() for Options Pricing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Stale or missing prices for illiquid options.

.. code-block:: python

    # WRONG - last trade can be very stale for options
    price = self.get_last_price(option_asset)

    # CORRECT - use quote for bid/ask
    quote = self.get_quote(option_asset)
    if quote and quote.bid and quote.ask:
        fair_price = quote.mid_price
    else:
        self.log_message("No valid quote for option", color="yellow")

Order Mistakes
--------------

Expecting Immediate Position Updates After submit_order()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Strategy logic based on outdated position data.

.. code-block:: python

    # WRONG - position hasn't updated yet
    self.submit_order(order)
    position = self.get_position(asset)  # Still shows old data!

    # CORRECT - check on next iteration
    self.submit_order(order)
    # In the NEXT on_trading_iteration():
    position = self.get_position(asset)  # Now updated

Using submit_order() to Close Crypto Futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Opens a new position instead of closing existing one.

.. code-block:: python

    # WRONG - opens opposite position instead of closing
    order = self.create_order(futures_asset, quantity, "sell")
    self.submit_order(order)

    # CORRECT - use close_position
    self.close_position(futures_asset)

Using Deprecated take_profit_price/stop_loss_price
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** May cause order errors or unexpected behavior.

.. code-block:: python

    # WRONG - deprecated parameters
    order = self.create_order(asset, 100, "buy",
        take_profit_price=110,
        stop_loss_price=90)

    # CORRECT - use secondary_ parameters
    order = self.create_order(asset, 100, "buy",
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110,      # Take profit
        secondary_stop_price=90)        # Stop loss

Visualization Mistakes
----------------------

Adding Markers Every Iteration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Chart crashes or becomes unusable due to thousands of markers.

.. code-block:: python

    # WRONG - adds marker every iteration
    def on_trading_iteration(self):
        self.add_marker("Price", price, color="blue")  # Chart explodes!

    # CORRECT - markers only for significant events
    def on_trading_iteration(self):
        if signal_detected:  # Only when something happens
            self.add_marker("Buy Signal", price, color="green", asset=my_asset)

        # Use add_line for continuous data
        self.add_line("SMA_20", sma_value, color="blue", asset=my_asset)

Using 'text' Parameter in add_marker/add_line/add_ohlc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** TypeError crash - there is no 'text' parameter.

.. code-block:: python

    # WRONG - causes TypeError
    self.add_marker("Signal", price, text="Buy now!")

    # CORRECT - use detail_text for hover text
    self.add_marker("Signal", price, detail_text="Buy signal triggered", asset=my_asset)

Forgetting to Pass asset Parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Indicators appear in separate subplot instead of overlaying price chart.

.. code-block:: python

    # WRONG - indicator in separate subplot
    self.add_line("SMA_20", sma_value, color="blue")

    # CORRECT - overlays on asset's price chart
    self.add_line("SMA_20", sma_value, color="blue", asset=spy_asset)

Code Organization Mistakes
--------------------------

Hardcoding API Keys
~~~~~~~~~~~~~~~~~~~

**Impact:** Security risk and deployment problems.

.. code-block:: python

    # WRONG - hardcoded fallback
    api_key = os.getenv('PERPLEXITY_API_KEY', 'your_api_key_here')

    # CORRECT - None fallback
    api_key = os.getenv('PERPLEXITY_API_KEY')
    if api_key is None:
        self.log_message("PERPLEXITY_API_KEY not set", color="red")

Setting Parameters in Multiple Places
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Confusing, parameters override each other unpredictably.

.. code-block:: python

    # WRONG - parameters set in multiple places
    class MyStrategy(Strategy):
        parameters = {"symbol": "SPY"}

    if __name__ == "__main__":
        strategy = MyStrategy(parameters={"symbol": "AAPL"})  # Which one wins?

    # CORRECT - one place only
    class MyStrategy(Strategy):
        parameters = {
            "symbol": "SPY",
            "period": 20
        }
        # Never override parameters elsewhere

Using try/except to Hide Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Bugs are hidden, making debugging nearly impossible.

.. code-block:: python

    # WRONG - hides real errors
    try:
        price = self.get_last_price(asset)
        quantity = self.portfolio_value / price
    except:
        pass  # What went wrong? No idea!

    # CORRECT - explicit error handling
    price = self.get_last_price(asset)
    if price is None:
        self.log_message(f"No price for {asset.symbol}", color="red")
        return

    quantity = self.portfolio_value / price

Using sleep() in Strategy
~~~~~~~~~~~~~~~~~~~~~~~~~

**Impact:** Blocks the entire bot, preventing important code from running.

.. code-block:: python

    # WRONG - blocks everything
    def on_trading_iteration(self):
        self.submit_order(order)
        time.sleep(5)  # Bot frozen for 5 seconds!

    # CORRECT - check conditions next iteration
    def on_trading_iteration(self):
        if not hasattr(self.vars, "order_time"):
            self.submit_order(order)
            self.vars.order_time = self.get_datetime()
            return

        elapsed = self.get_datetime() - self.vars.order_time
        if elapsed > timedelta(seconds=5):
            # Now do the next step
            pass
