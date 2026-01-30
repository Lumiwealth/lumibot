Frequently Asked Questions (FAQ)
=================================

This page answers common questions about Lumibot. If you're new to Lumibot, start with the :doc:`getting_started` guide.

DateTime and Timing
-------------------

Why can't I use datetime.now() in my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Short answer:** During backtesting, ``datetime.now()`` returns the real current time, not the simulated time. Your strategy will think it's 2024 when it's actually simulating trades from 2020.

**Solution:** Always use ``self.get_datetime()`` instead:

.. code-block:: python

    # WRONG - will break backtesting
    current_time = datetime.now()

    # CORRECT - works in both backtesting and live trading
    current_time = self.get_datetime()

Why is my historical data delayed or not up-to-date?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``get_historical_prices()`` returns completed bars and may be delayed by up to a minute. For real-time price data, use ``get_last_price()`` or ``get_quote()`` instead.

**Performance note (backtesting):** Backtests are fastest on warm cache. LumiBot caches historical data per asset and timestep so repeated calls to ``get_historical_prices()`` should not re-download data.

.. code-block:: python

    # For historical analysis (may be 1 minute delayed)
    bars = self.get_historical_prices(asset, 20, "minute")

    # For real-time price (latest tick)
    price = self.get_last_price(asset)

    # For bid/ask spread and detailed market data
    quote = self.get_quote(asset)
    if quote is not None:
        self.log_message(f"Bid: {quote.bid}, Ask: {quote.ask}, Mid: {quote.mid_price}")

Variables and State
-------------------

Why do my variables reset between trading iterations?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Local variables are reset each iteration. Use ``self.vars`` for persistent state:

.. code-block:: python

    # WRONG - resets each iteration
    def on_trading_iteration(self):
        count = 0
        count += 1  # Always 1

    # CORRECT - persists between iterations
    def on_trading_iteration(self):
        if not hasattr(self.vars, "count"):
            self.vars.count = 0
        self.vars.count += 1  # Actually increments

Why can't I set self.name or other attributes on the strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Never assign arbitrary attributes directly on ``self`` (like ``self.name``, ``self.asset``, ``self.symbol``). These can collide with Lumibot internals and crash your strategy.

.. code-block:: python

    # WRONG - will crash or override framework behavior
    def initialize(self):
        self.name = "MyBot"        # Collides with internal name
        self.asset = Asset("SPY")  # May conflict with internal methods

    # CORRECT - use self.vars
    def initialize(self):
        self.vars.strategy_label = "MyBot"
        self.vars.target_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

Why does 'from __future__ import annotations' break my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This import breaks Lumibot's type checking system and causes backtests to crash. Never use it.

.. code-block:: python

    # NEVER DO THIS - will crash your strategy
    from __future__ import annotations

    # Just remove this import - it's not needed

Options Trading
---------------

Why do I get None when calling get_chains()?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not all option expiries and strikes are available. Always check if the result is None before using it:

.. code-block:: python

    chains = self.get_chains(underlying_asset)
    if chains is None:
        self.log_message("No options chains available")
        return

    # Use OptionsHelper for reliable expiration finding
    expiry = self.options_helper.get_expiration_on_or_after_date(target_date, chains, "call")

Why does get_greeks() return None?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Greeks may be unavailable for illiquid options. Always check for None but **don't return early** - let your strategy continue:

.. code-block:: python

    greeks = self.get_greeks(option_asset, underlying_price=price)

    if greeks is not None:
        delta = greeks.get("delta")
        self.log_message(f"Delta: {delta}")
        # Execute Greeks-dependent logic here
    else:
        self.log_message("Greeks unavailable - option may be illiquid", color="yellow")

    # Strategy continues regardless

Why do my option calculations seem off by 100x?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Options are multiplied by 100. A $1.50 option premium costs $150 per contract:

.. code-block:: python

    option_price = 1.50  # Premium per share
    actual_cost = option_price * 100  # $150 per contract

    # To buy $10,000 worth of options at $1.50 premium:
    contracts = int(10000 / (option_price * 100))  # = 66 contracts

Crypto Trading
--------------

Why does my crypto strategy stop trading at 4pm?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Crypto markets are 24/7, but Lumibot defaults to stock market hours. Add this to initialize:

.. code-block:: python

    def initialize(self):
        self.set_market("24/7")  # REQUIRED for crypto
        self.sleeptime = "15S"   # Optional: run every 15 seconds

How do I close a crypto futures position?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For ``Asset.AssetType.CRYPTO_FUTURE``, you must use ``close_position()`` instead of ``submit_order()``:

.. code-block:: python

    # WRONG - opens a new position on the other side
    order = self.create_order(futures_asset, quantity, "sell")
    self.submit_order(order)

    # CORRECT - actually closes the position
    self.close_position(futures_asset)

Orders and Positions
--------------------

Why isn't my position updated immediately after submit_order()?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Positions update at the start of each trading iteration. Check on the next iteration:

.. code-block:: python

    # submit_order() is async - position won't update immediately
    self.submit_order(order)

    # WRONG - will still show old position
    position = self.get_position(asset)

    # CORRECT - wait for next iteration to confirm
    # In next on_trading_iteration():
    position = self.get_position(asset)
    if position is None:
        self.log_message("Exit confirmed, position is closed")

Why does get_last_price() return None?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Price data may be unavailable for illiquid assets. Always check:

.. code-block:: python

    price = self.get_last_price(asset)
    if price is None:
        self.log_message(f"No price data for {asset.symbol}", color="red")
        return

    # Now safe to use price

Why does get_positions() return USD?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cash is treated as a position. Filter it out:

.. code-block:: python

    positions = self.get_positions()
    for position in positions:
        if position.asset.symbol == "USD" and position.asset.asset_type == Asset.AssetType.FOREX:
            continue  # Skip cash position
        # Process real positions here

Brokers
-------

Why shouldn't I mention specific brokers in my strategy code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lumibot strategies should be broker-agnostic. Broker configuration is handled via environment variables at deployment time, not in strategy code. This allows the same strategy to work with any supported broker.

.. code-block:: python

    # WRONG - mentioning specific broker
    # "This strategy works with Interactive Brokers..."

    # CORRECT - broker-agnostic code
    # Strategy logic only - broker set at deployment

Debugging and Logging
---------------------

Why should I use self.log_message() instead of print()?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.log_message()`` integrates with Lumibot's logging system and supports colors:

.. code-block:: python

    # Available colors: white, red, green, blue, yellow
    self.log_message("Position opened", color="green")
    self.log_message("Warning: low volume", color="yellow")
    self.log_message("Error: no price data", color="red")

How should I use markers and lines for debugging?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``add_ohlc()`` for price bars, ``add_line()`` for continuous data (indicators), and ``add_marker()`` for infrequent events (signals):

.. code-block:: python

    # add_ohlc - for price bars (OHLC data)
    self.add_ohlc("SPY", open=o, high=h, low=l, close=c, asset=my_asset)

    # add_line - for continuous data like moving averages
    self.add_line("SMA_20", sma_value, color="blue", asset=my_asset)

    # add_marker - for infrequent events like buy/sell signals
    # NEVER add markers every iteration - it crashes the chart!
    if crossover_detected:
        self.add_marker("Buy Signal", price, color="green", symbol="arrow-up", asset=my_asset)

.. warning::

    ``add_ohlc()``, ``add_line()``, and ``add_marker()`` do NOT have a ``text`` parameter. Use ``detail_text`` for hover text.

Backtesting
-----------

What data source should I use for options backtesting?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yahoo does not support options. Use ThetaData or Polygon:

.. code-block:: python

    # For options backtesting
    from lumibot.backtesting import ThetaDataBacktesting
    # or
    from lumibot.backtesting import PolygonDataBacktesting

What data source should I use for futures backtesting?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Only DataBento supports futures:

.. code-block:: python

    from lumibot.backtesting import DataBentoDataBacktesting

    # Use flat fees for futures (typical: $0.50 per contract)
    from lumibot.entities import TradingFee
    trading_fee = TradingFee(flat_fee=0.50)

Why does my minute-level backtest fail with "no data"?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most data sources limit minute-level data to 2 years. Set your start date accordingly:

.. code-block:: python

    # For minute-level backtests, use < 2 years of data
    from datetime import datetime, timedelta

    end_date = datetime.now()
    start_date = end_date - timedelta(days=600)  # ~1.5 years, safe margin
