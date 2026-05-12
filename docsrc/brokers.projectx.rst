ProjectX / TopstepX
===================

Lumibot's ProjectX path is a **futures-only broker integration** currently documented for TopstepX. The lower-level adapter contains firm-specific ProjectX environment-variable prefixes, but new firms should be treated as advanced and validation-required until they are tested in Lumibot.

Features
--------

* **TopstepX-first futures trading**: Continuous futures contracts through the documented TopstepX path
* **ProjectX adapter hooks**: Additional firm-specific prefixes may exist in the adapter, but they are not advertised as ready until validated
* **Real-time Data**: Live order and position updates
* **Order Types**: Market, limit, stop orders
* **Auto-Detection**: Automatically connects when environment variables are set

Prerequisites
-------------

1. **Account**: Demo or live futures trading account with TopstepX
2. **API Credentials**: Username and API key from your broker

Documented Broker
-----------------

TopstepX
^^^^^^^^

.. code-block:: bash

   PROJECTX_TOPSTEPX_API_KEY=your_api_key
   PROJECTX_TOPSTEPX_USERNAME=your_username
   PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME=your_account_name

Advanced Adapter Prefixes
^^^^^^^^^^^^^^^^^^^^^^^^^

The adapter can read other ``PROJECTX_<FIRM>_*`` prefixes for testing or future expansion, but those paths are not presented as supported broker docs here. Validate fills, positions, account selection, market data, and order error handling before using any non-TopstepX ProjectX firm in production.

Supported Functionality
-----------------------

.. list-table:: ProjectX Capabilities
   :widths: 25 15 60
   :header-rows: 1

   * - Feature
     - Support
     - Notes
   * - Futures trading
     - Yes
     - Supports CME micro and standard contracts through the documented TopstepX ProjectX path.
   * - Order types
     - Market, Limit, Stop
     - Submit bracket-style risk controls directly from strategies.
   * - Market data
     - Real time
     - Streams price updates when ProjectX is running alongside Lumibot.
   * - Account detection
     - Automatic
     - Reads the configured environment variables to pick the correct broker.

Usage
-----

1. **Set Environment Variables**: Configure your broker's API credentials.

2. **Create Strategy**: Import Lumibot and create your trading strategy.

3. **Run**: ProjectX will auto-detect and connect.

.. code-block:: python

   from lumibot.strategies import Strategy
   from lumibot.entities import Asset

   class MyStrategy(Strategy):
       def initialize(self):
           self.sleeptime = "1D"

       def on_trading_iteration(self):
           # Trade Micro E-mini S&P 500 futures
           mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
           
           # Get current price
           last_price = self.get_last_price(mes)
           
           # Place a limit order
           if last_price:
               order = self.create_order(
                   asset=mes,
                   quantity=1,
                   side="buy",
                   order_type="limit", 
                   limit_price=last_price * 0.999
               )
               self.submit_order(order)

   # Run the strategy (ProjectX auto-detects from environment variables)
   strategy = MyStrategy()
   strategy.run_live()

Supported Features
------------------

✅ **Futures Trading**: Continuous futures contracts
✅ **Market Orders**: Immediate execution
✅ **Limit Orders**: Execute at specified price
✅ **Stop Orders**: Stop-loss functionality  
✅ **Real-time Data**: Live market data
✅ **Historical Data**: Minute, hour, day timeframes

❌ **Stock Trading**: Futures only
❌ **Options Trading**: Futures only
❌ **Order Modification**: Must cancel and re-place 
