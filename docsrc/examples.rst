Code Examples
=============

This page contains practical code examples for common Lumibot tasks. These examples cover stocks, options, crypto, futures, and advanced features like the PerplexityHelper for AI-powered trading decisions.

AI Agents
---------

LumiBot now supports AI agents directly inside the normal strategy lifecycle. The recommended agent docs path is:

- :doc:`agents` for the main guide
- :doc:`agents_quickstart` for the core ``self.agents.create(...)`` / ``.run(...)`` pattern
- :doc:`agents_canonical_demos` for the Alpaca news, FRED macro, and M2 demos
- :doc:`agents_observability` for traces, replay cache, and warnings

The canonical AI examples are intentionally strategy-shaped rather than toy snippets. They show:

- how to create an agent in ``initialize()``
- how to expose ``BuiltinTools`` and external ``MCPServer`` tools
- how to run the agent from lifecycle methods
- how to inspect ``result.summary``, traces, warnings, and replay behavior
- how to evaluate the resulting strategy with benchmarked tearsheets

Typical AI agent pattern:

.. code-block:: python

    from lumibot.components.agents import BuiltinTools, MCPServer

    def initialize(self):
        self.agents.create(
            name="research",
            default_model="gemini-3.1-flash-lite-preview",
            system_prompt="Use the available tools and return a short summary.",
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.account.portfolio(),
                BuiltinTools.market.last_price(),
                BuiltinTools.docs.search(),
            ],
        )

    def on_trading_iteration(self):
        result = self.agents["research"].run(
            context={"symbol": "SPY", "current_datetime": self.get_datetime().isoformat()}
        )
        self.log_message(f"[research] {result.summary}", color="yellow")

Stocks
------

Get Historical Prices for a Stock
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve historical price data for a stock asset:

.. code-block:: python

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    bars = self.get_historical_prices(asset, 2, "day")
    if bars is not None:
        df = bars.df  # DatetimeIndex (tz-aware) with open/high/low/close/volume/return columns
        last_ohlc = df.iloc[-1]  # Most recent bar
        self.log_message(f"Last price of SPY: {last_ohlc['close']}, open: {last_ohlc['open']}")

Get Multiple Assets' Historical Prices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve historical prices for multiple assets at once:

.. code-block:: python

    assets = [
        Asset("AAPL", asset_type=Asset.AssetType.STOCK),
        Asset("MSFT", asset_type=Asset.AssetType.STOCK),
        Asset("GOOGL", asset_type=Asset.AssetType.STOCK),
    ]
    historical_prices = self.get_historical_prices_for_assets(assets, 30, "minute")
    for asset_obj, bars in historical_prices.items():
        if bars is None:
            self.log_message(f"No data available for {asset_obj}")
            continue
        df = bars.df
        last_bar = df.iloc[-1]

Get Quote with Bid/Ask Spread
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get detailed market data including bid/ask spreads:

.. code-block:: python

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = self.get_quote(asset)
    if quote is not None:
        self.log_message(f"Bid: {quote.bid}, Ask: {quote.ask}, Mid: {quote.mid_price}")

Compare get_last_price vs Historical Bars
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In live trading, ``get_last_price`` returns the broker's latest tick while historical bars may lag:

.. code-block:: python

    spy_stock = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    latest_price = self.get_last_price(spy_stock)
    minute_bar = self.get_historical_prices(spy_stock, 1, "minute")

Calculate Moving Average with Up-to-Date Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compute a moving average using both historical data and the latest available price:

.. code-block:: python

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    df = self.get_historical_prices(asset, 20, "minute").df
    last = self.get_last_price(asset)
    sma20 = (df["close"].iloc[-19:].sum() + last) / 20
    self.log_message(f"SMA-20 (live): {sma20:.4f}")

Calculate Technical Indicators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Calculate indicators using historical price data:

.. code-block:: python

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    bars = self.get_historical_prices(asset, 100, "day")  # Get more data than needed for indicators

    if bars is not None:
        df = bars.df
        df["SMA_50"] = df["close"].rolling(window=50).mean()  # 50-day moving average
        last_ohlc = df.iloc[-1]

Handle Missing Data
~~~~~~~~~~~~~~~~~~~

Always handle the case when data is unavailable:

.. code-block:: python

    missing_asset = Asset("XYZ", asset_type=Asset.AssetType.STOCK)
    bars = self.get_historical_prices(missing_asset, 30, "minute")
    if bars is None:
        self.log_message(f"No data available for {missing_asset.symbol}")
    else:
        df = bars.df

Positions and Orders
--------------------

Get Position Details
~~~~~~~~~~~~~~~~~~~~

Retrieve information about a specific position:

.. code-block:: python

    position = self.get_position(Asset("AAPL", asset_type=Asset.AssetType.STOCK))

    if position is not None:
        self.log_message(f"Position for AAPL: {position.quantity} shares")
        quantity = position.quantity

Sell a Position
~~~~~~~~~~~~~~~

Liquidate an existing stock position:

.. code-block:: python

    position = self.get_position(Asset("AAPL", asset_type=Asset.AssetType.STOCK))
    if position is not None:
        asset = position.asset
        quantity = position.quantity
        order = self.create_order(asset, quantity, Order.OrderSide.SELL)
        self.submit_order(order)

Filter Out USD Cash Position
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When processing positions, filter out the USD cash position:

.. code-block:: python

    positions = self.get_positions()

    for position in positions:
        if position.asset.symbol == "USD" and position.asset.asset_type == Asset.AssetType.FOREX:
            continue
        # Process real positions here

Persistent Variables
--------------------

Using self.vars for State
~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``self.vars`` for variables that persist between trading iterations:

.. code-block:: python

    def initialize(self):
        self.vars.my_variable = 10

    def on_trading_iteration(self):
        self.log_message(f"My variable is {self.vars.my_variable}")
        self.vars.my_variable += 1

Check if Variable Exists
~~~~~~~~~~~~~~~~~~~~~~~~

Safely check if a persistent variable exists before using it:

.. code-block:: python

    def on_trading_iteration(self):
        if not hasattr(self.vars, "filled_count"):
            self.vars.filled_count = 0

        self.log_message(f"The number of filled orders is {self.vars.filled_count}")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        if not hasattr(self.vars, "filled_count"):
            self.vars.filled_count = 0

        self.vars.filled_count += 1

Dictionary-Style Access
~~~~~~~~~~~~~~~~~~~~~~~

Use dictionary-style access for signal counts:

.. code-block:: python

    self.vars.signal_counts = self.vars.get("signal_counts", {})
    self.vars.signal_counts.setdefault("SPY", 0)
    self.vars.signal_counts["SPY"] += 1

Logging and Debugging
---------------------

Log What Triggered a Decision
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Always log the reasoning behind trading decisions:

.. code-block:: python

    rsi = self.get_indicator("RSI", symbol="SPY", period=14)
    self.log_message(f"RSI gate check: value {rsi:.2f} vs sell > 70")
    if rsi > 70:
        self.log_message("RSI gate passed, preparing to sell SPY", color="yellow")
        # submit_order(...) here
    else:
        self.log_message("RSI gate failed, holding position")

Visualization with Markers and Lines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``add_ohlc`` for price bars, ``add_line`` for continuous indicators, and ``add_marker`` for infrequent events:

.. code-block:: python

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    bars = self.get_historical_prices(asset, 100, "day")

    if bars is not None:
        df = bars.df
        last_bar = df.iloc[-1]

        # Plot SPY price as OHLC candles (pass asset parameter for proper charting)
        self.add_ohlc(
            "SPY",
            open=last_bar["open"],
            high=last_bar["high"],
            low=last_bar["low"],
            close=last_bar["close"],
            detail_text="SPY Price",
            asset=asset,
        )

        df["SMA_50"] = df["close"].rolling(window=50).mean()

        # Add a line for the moving average (pass asset to overlay on price chart)
        self.add_line("SMA_50", df["SMA_50"].iloc[-1], color="blue", width=2,
                      detail_text="50-day SMA", asset=asset)

        # Markers only for significant events (not every iteration!)
        if last_bar["close"] > last_bar["SMA_50"]:
            self.add_marker("Buy Signal", last_bar["close"], color="green",
                          symbol="arrow-up", size=10, detail_text="Buy Signal", asset=asset)
        else:
            self.add_marker("Sell Signal", last_bar["close"], color="red",
                          symbol="arrow-down", size=10, detail_text="Sell Signal", asset=asset)

.. warning::

    Never add markers every iteration - this crashes the chart! Only use markers for significant events.
    Use ``add_line`` for continuous data like indicators, and ``add_ohlc`` for price bars.

Options
-------

Get Option Chains
~~~~~~~~~~~~~~~~~

Retrieve options chains for a stock:

.. code-block:: python

    chains_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    chains = self.get_chains(chains_asset)

    # Dict-style access (backwards compatible):
    calls_dict = chains["Chains"]["CALL"]
    puts_dict = chains["Chains"]["PUT"]

    # Convenience methods (cleaner):
    calls = chains.calls()  # All CALL options
    puts = chains.puts()  # All PUT options
    expirations = chains.expirations("CALL")  # List of expiration dates

    # Get strikes for a specific date:
    expiry_date = datetime.date(2024, 1, 15)
    strikes = chains.strikes(expiry_date, "CALL")

.. note::

    ``self.get_chains()`` is slow - cache results when possible.

Get Historical Prices for an Option
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    asset = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.datetime(2020, 1, 1),
        strike=100,
        right=Asset.OptionRight.CALL)
    bars = self.get_historical_prices(asset, 30, "minute")

    if bars is not None:
        df = bars.df
        last_ohlc = df.iloc[-1]
        self.log_message(f"Last price of AAPL option: {last_ohlc['close']}")

Create Option Order with Trailing Stop
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.entities import Asset, Order
    import datetime

    asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime.date(2019, 1, 1),
        strike=100.00,
        right=Asset.OptionRight.CALL,
    )
    order = self.create_order(
        asset,
        1,
        "buy",
        order_type=Order.OrderType.TRAIL,
        trail_percent=0.05,
    )
    self.submit_order(order)

Sell an Option
~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.entities import Asset
    import datetime

    asset = Asset(
       "SPY",
       asset_type=Asset.AssetType.OPTION,
       expiration=datetime.date(2025, 12, 31),
       strike=100.00,
       right=Asset.OptionRight.CALL)
    order = self.create_order(asset, 10, "sell")
    self.submit_order(order)

Get Option Quote for Precise Orders
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get bid/ask data to place more precise limit orders:

.. code-block:: python

    option_asset = Asset("SPY", asset_type=Asset.AssetType.OPTION,
                        expiration=expiry, strike=400, right=Asset.OptionRight.CALL)
    quote = self.get_quote(option_asset)
    if quote is not None and quote.bid is not None and quote.ask is not None:
        mid_price = quote.mid_price
        order = self.create_order(option_asset, 1, "buy", limit_price=mid_price)
        self.submit_order(order)

Get Greeks for an Option
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    underlying_price = self.get_last_price(underlying_asset)

    option_asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=expiry_date,
        strike=400,
        right=Asset.OptionRight.CALL
    )

    # Get Greeks - check if None before using, but DON'T return if None
    greeks = self.get_greeks(option_asset, underlying_price=underlying_price)

    if greeks is not None:
        delta = greeks.get("delta")
        gamma = greeks.get("gamma")
        theta = greeks.get("theta")
        vega = greeks.get("vega")
        iv = greeks.get("implied_volatility")

        self.log_message(f"Delta: {delta:.3f}, Gamma: {gamma:.4f}, Theta: {theta:.3f}")

        if delta is not None and 0.3 <= delta <= 0.5:
            self.log_message("Delta is in target range", color="green")
    else:
        # Log but let strategy continue
        self.log_message(f"Greeks unavailable for {option_asset.symbol}", color="yellow")

    # Strategy continues here

.. warning::

    ``get_greeks()`` can return None for illiquid options. Always check for None but don't use ``return`` - let your strategy continue with other logic.

Find Valid Option Strikes
~~~~~~~~~~~~~~~~~~~~~~~~~

Handle cases where desired expiration might not be available:

.. code-block:: python

    pltr_asset = Asset("PLTR", asset_type=Asset.AssetType.STOCK)
    current_price = self.get_last_price(pltr_asset)
    if current_price is None:
        self.log_message(f"{pltr_asset.symbol} price unavailable", color="red")
        return

    # Use self.get_datetime() instead of datetime.now()
    target_expiration_dt = self.get_datetime() + timedelta(days=30)
    target_expiration_date = target_expiration_dt.date()
    target_expiration_str = target_expiration_date.strftime("%Y-%m-%d")

    chains_res = self.get_chains(pltr_asset)
    if not chains_res:
        self.log_message("Option chains unavailable", color="red")
        return

    call_chains = chains_res.get("Chains", {}).get("CALL")
    if not call_chains:
        return

    # Check if target expiration exists; if not, select closest
    if target_expiration_str in call_chains:
        expiration_str = target_expiration_str
    else:
        available_expirations = []
        for exp_str in call_chains.keys():
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                available_expirations.append(exp_date)
            except Exception:
                pass
        if not available_expirations:
            return
        expiration_date = min(available_expirations,
                             key=lambda x: abs((x - target_expiration_date).days))
        expiration_str = expiration_date.strftime("%Y-%m-%d")

    strikes = call_chains.get(expiration_str)

Options Strategies with OptionsHelper
-------------------------------------

For strike/expiry/delta selection helpers, see :doc:`options_helper`.

Calendar Spread
~~~~~~~~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    strike = 400

    dt = self.get_datetime()
    near_expiry = dt + timedelta(days=7)
    far_expiry = dt + timedelta(days=30)

    chains = self.get_chains(underlying_asset)
    if chains is not None:
        near_expiry = self.options_helper.get_expiration_on_or_after_date(
            near_expiry, chains, "call")
        far_expiry = self.options_helper.get_expiration_on_or_after_date(
            far_expiry, chains, "call")

        if self.options_helper.execute_calendar_spread(
            underlying_asset, strike, near_expiry, far_expiry,
            quantity=1, right="call", limit_type="mid"
        ):
            self.log_message("Calendar spread executed", color="green")

Straddle
~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    chains = self.get_chains(underlying_asset)

    dt = self.get_datetime()
    expiry = dt + timedelta(days=10)
    expiry = self.options_helper.get_expiration_on_or_after_date(expiry, chains, "call")

    strike = 150
    if self.options_helper.execute_straddle(
        underlying_asset, expiry, strike, quantity=1, limit_type="mid"
    ):
        self.log_message("Straddle executed", color="green")

Strangle
~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    dt = self.get_datetime()
    chains = self.get_chains(underlying_asset)
    expiry = dt + timedelta(days=10)
    expiry = self.options_helper.get_expiration_on_or_after_date(expiry, chains, "call")

    lower_strike = 145
    upper_strike = 155
    if self.options_helper.execute_strangle(
        underlying_asset, expiry, lower_strike, upper_strike,
        quantity=1, limit_type="mid"
    ):
        self.log_message("Strangle executed", color="green")

Diagonal Spread
~~~~~~~~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    dt = self.get_datetime()
    chains = self.get_chains(underlying_asset)

    near_expiry = dt + timedelta(days=7)
    far_expiry = dt + timedelta(days=30)

    near_expiry = self.options_helper.get_expiration_on_or_after_date(
        near_expiry, chains, "call")
    far_expiry = self.options_helper.get_expiration_on_or_after_date(
        far_expiry, chains, "call")

    near_strike = 410
    far_strike = 405
    if self.options_helper.execute_diagonal_spread(
        underlying_asset, near_expiry, far_expiry, near_strike, far_strike,
        quantity=1, right="call", limit_type="mid"
    ):
        self.log_message("Diagonal spread executed", color="green")

Ratio Spread
~~~~~~~~~~~~

.. code-block:: python

    underlying_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
    dt = self.get_datetime()
    chains = self.get_chains(underlying_asset)
    expiry = dt + timedelta(days=10)
    expiry = self.options_helper.get_expiration_on_or_after_date(expiry, chains, "call")

    buy_strike = 148
    sell_strike = 152
    buy_qty = 1
    sell_qty = 2
    if self.options_helper.execute_ratio_spread(
        underlying_asset, expiry, buy_strike, sell_strike, buy_qty, sell_qty,
        right="call", limit_type="mid"
    ):
        self.log_message("Ratio spread executed", color="green")

Evaluate Option Market
~~~~~~~~~~~~~~~~~~~~~~

Inspect quotes and spreads before placing orders:

.. code-block:: python

    option_asset = Asset("SPY", asset_type=Asset.AssetType.OPTION,
                         expiration=self.get_next_expiration_date(date.today(), 5),
                         strike=400, right="call", underlying_asset=Asset("SPY"))
    evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.15)

    if evaluation.spread_too_wide:
        self.log_message("Spread exceeds max threshold; skipping", color="red")
    elif evaluation.buy_price is not None:
        tp_price = evaluation.buy_price * 1.5
        sl_price = evaluation.buy_price * 0.6
        order = self.create_order(
            option_asset,
            1,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=evaluation.buy_price,
            order_class=Order.OrderClass.BRACKET,
            secondary_limit_price=tp_price,
            secondary_stop_price=sl_price,
        )
        self.submit_order(order)

Cryptocurrency
--------------

Get Crypto Historical Prices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    bars = self.get_historical_prices(asset, 30, "minute")

    if bars is not None:
        df = bars.df

Get Crypto Last Price
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    last_price = self.get_last_price(asset)
    if last_price is not None:
        self.log_message(f"Last price of BTC in USD: {last_price}")

Create Crypto Order
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.entities import Asset

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.CRYPTO)
    order = self.create_order(base, 0.05, "buy", quote=quote)
    self.submit_order(order)

Set 24/7 Market Hours for Crypto
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def initialize(self):
        self.set_market("24/7")  # REQUIRED for crypto
        self.sleeptime = "15S"  # Run every 15 seconds

    def on_trading_iteration(self):
        dt = self.get_datetime()

        if dt.weekday() < 5:
            self.log_message(f"Current datetime: {dt}")
        else:
            self.log_message("It's the weekend!")

Crypto Futures (Bitunix)
------------------------

Trade Crypto Futures
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    last_price = self.get_last_price(asset)

    if last_price is not None:
        futures_asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
        order = self.create_order(futures_asset, 0.1, "buy", order_type="market")
        self.submit_order(order)
    else:
        self.log_message("BTC price unavailable", color="red")

Close Crypto Futures Position
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For crypto futures, you **must** use ``close_position()`` instead of ``submit_order()``:

.. code-block:: python

    positions = self.get_positions()

    for position in positions:
        if position.asset.asset_type == Asset.AssetType.CRYPTO_FUTURE:
            # CORRECT - use close_position for futures
            self.close_position(position.asset)
            self.log_message(f"Closed position for {position.asset.symbol}", color="green")

.. warning::

    Using ``submit_order()`` to "sell" a crypto future will open another position instead of closing!

Futures (DataBento)
-------------------

Trade Futures Contracts
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    futures_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro E-mini S&P 500

    bars = self.get_historical_prices(futures_asset, 100, "minute")
    if bars and not bars.df.empty:
        df = bars.df
        df["sma_20"] = df["close"].rolling(window=20).mean()

        current_price = df["close"].iloc[-1]
        current_sma = df["sma_20"].iloc[-1]

        if current_price > current_sma:
            order = self.create_order(futures_asset, 5, "buy")
            self.submit_order(order)
        elif current_price < current_sma:
            order = self.create_order(futures_asset, 5, "sell")
            self.submit_order(order)

Futures Backtesting
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.backtesting import DataBentoDataBacktesting
    from lumibot.entities import TradingFee

    class FuturesStrategy(Strategy):
        def initialize(self):
            self.asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)

        def on_trading_iteration(self):
            # Your futures trading logic here
            pass

    if __name__ == "__main__":
        if IS_BACKTESTING:
            # Use per-contract fees for futures (typical: $0.85 per standard contract, $0.50 for micros)
            trading_fee = TradingFee(per_contract_fee=0.85)

            results = FuturesStrategy.backtest(
                DataBentoDataBacktesting,
                benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
                buy_trading_fees=[trading_fee],
                sell_trading_fees=[trading_fee]
            )

Options Backtesting Fees
~~~~~~~~~~~~~~~~~~~~~~~~

For options strategies, use ``per_contract_fee`` instead of ``flat_fee``. ``per_contract_fee`` is multiplied
by the number of contracts in each order, which correctly models broker commissions like IBKR's $0.65/contract.

.. code-block:: python

    from lumibot.entities import TradingFee

    # IBKR charges $0.65 per contract per leg
    # For a 40-contract spread, that's $26.00 per leg
    trading_fee = TradingFee(per_contract_fee=0.65)

    result = OptionsStrategy.backtest(
        ThetaDataBacktesting,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
    )

.. note::
    Do NOT use ``flat_fee`` for options or futures commissions. ``flat_fee`` is a fixed amount per order
    regardless of contract count. For example, ``TradingFee(flat_fee=0.65)`` charges only $0.65 total on a
    40-contract order, while ``TradingFee(per_contract_fee=0.65)`` correctly charges $26.00 (40 x $0.65).

Multiple Futures Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def initialize(self):
        self.futures_assets = [
            Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE),   # S&P 500
            Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE),  # Micro S&P 500
            Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE),   # NASDAQ 100
            Asset("CL", asset_type=Asset.AssetType.CONT_FUTURE),   # Crude Oil
        ]

    def on_trading_iteration(self):
        for asset in self.futures_assets:
            bars = self.get_historical_prices(asset, 50, "day")
            if bars and not bars.df.empty:
                # Your trading logic for each contract
                pass

FOREX
-----

Create FOREX Order
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.entities import Asset

    asset = Asset(
       symbol="CHF",
       currency="EUR",
       asset_type=Asset.AssetType.FOREX)
    order = self.create_order(asset, 100, "buy", limit_price=100.00)
    self.submit_order(order)

AI-Powered Trading (PerplexityHelper)
-------------------------------------

Trade Based on Earnings News
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use AI analysis to make trading decisions based on earnings reports:

.. code-block:: python

    news_query = "What are the latest earnings reports for major tech companies?"
    news_data = self.perplexity_helper.execute_financial_news_query(news_query)

    for item in news_data.get("items", []):
        sentiment = item.get("sentiment_score", 0)
        popularity = item.get("popularity_metric", 0)
        if sentiment >= 5 and popularity > 100:
            symbol = item.get("symbol")
            asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
            order = self.create_order(asset, 100, "buy")
            self.submit_order(order)
            self.log_message(f"Bought {symbol} based on positive earnings", color="green")
            break

Trade Volatile Stocks
~~~~~~~~~~~~~~~~~~~~~

Identify and trade volatile stocks:

.. code-block:: python

    general_query = "List stocks that are showing unusually high volatility."
    general_data = self.perplexity_helper.execute_general_query(general_query)

    if "symbols" in general_data and len(general_data["symbols"]) > 0:
        for symbol in general_data["symbols"]:
            asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
            current_price = self.get_last_price(asset)
            if current_price and current_price < 50:
                order = self.create_order(asset, 200, "buy")
                self.submit_order(order)
                self.log_message(f"Bought {symbol} (volatile, under $50)", color="green")
                break

Use Custom Schema for Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Query with a custom JSON schema for structured results:

.. code-block:: python

    custom_schema = {
        "query": "<string, echo the user's query>",
        "stocks": [
            {
                "symbol": "<string, ticker symbol>",
                "earnings_growth": "<float, earnings growth percentage>",
                "analyst_rating": "<float, average analyst rating from 1 to 5>",
                "price_target": "<float, consensus price target in USD>"
            }
        ],
        "summary": "<string, overall summary of findings>"
    }

    general_query = "List stocks with high earnings growth and strong analyst ratings."
    import os
    perplexity_model = os.getenv("PERPLEXITY_MODEL", "sonar-pro")
    custom_data = self.perplexity_helper.execute_general_query(
        general_query, custom_schema, model=perplexity_model
    )

    for stock in custom_data.get("stocks", []):
        earnings_growth = stock.get("earnings_growth", 0)
        analyst_rating = stock.get("analyst_rating", 0)
        if earnings_growth > 50 and analyst_rating >= 4.5:
            symbol = stock.get("symbol")
            asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
            current_price = self.get_last_price(asset)
            avg_target = stock.get("price_target")
            if current_price and avg_target and current_price < avg_target:
                order = self.create_order(asset, 150, "buy")
                self.submit_order(order)
                break
