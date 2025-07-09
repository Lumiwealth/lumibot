ProjectX
========

ProjectX is a **futures-only broker** that connects to multiple prop trading firms and futures brokers through a unified gateway. Each broker requires its own specific environment variables.

Features
--------

* **Futures Trading Only**: Continuous futures contracts
* **Multiple Brokers**: Supports 19+ different prop trading firms and futures brokers
* **Real-time Data**: Live order and position updates
* **Order Types**: Market, limit, stop orders
* **Auto-Detection**: Automatically connects when environment variables are set

Prerequisites
-------------

1. **Account**: Demo or live futures trading account with a supported broker
2. **API Credentials**: Username and API key from your broker
Supported Brokers
-----------------

TopstepX
^^^^^^^^

.. code-block:: bash

   PROJECTX_TOPSTEPX_API_KEY=your_api_key
   PROJECTX_TOPSTEPX_USERNAME=your_username
   PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME=your_account_name

Top One Futures
^^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_TOPONE_API_KEY=your_api_key
   PROJECTX_TOPONE_USERNAME=your_username
   PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME=your_account_name

TickTickTrader
^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_TICKTICKTRADER_API_KEY=your_api_key
   PROJECTX_TICKTICKTRADER_USERNAME=your_username
   PROJECTX_TICKTICKTRADER_PREFERRED_ACCOUNT_NAME=your_account_name

AlphaTicks
^^^^^^^^^^

.. code-block:: bash

   PROJECTX_ALPHATICKS_API_KEY=your_api_key
   PROJECTX_ALPHATICKS_USERNAME=your_username
   PROJECTX_ALPHATICKS_PREFERRED_ACCOUNT_NAME=your_account_name

Aqua Futures
^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_AQUAFUTURES_API_KEY=your_api_key
   PROJECTX_AQUAFUTURES_USERNAME=your_username
   PROJECTX_AQUAFUTURES_PREFERRED_ACCOUNT_NAME=your_account_name

Blue Guardian Futures
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_BLUEGUARDIANFUTURES_API_KEY=your_api_key
   PROJECTX_BLUEGUARDIANFUTURES_USERNAME=your_username
   PROJECTX_BLUEGUARDIANFUTURES_PREFERRED_ACCOUNT_NAME=your_account_name

Blusky
^^^^^^

.. code-block:: bash

   PROJECTX_BLUSKY_API_KEY=your_api_key
   PROJECTX_BLUSKY_USERNAME=your_username
   PROJECTX_BLUSKY_PREFERRED_ACCOUNT_NAME=your_account_name

Bulenox
^^^^^^^

.. code-block:: bash

   PROJECTX_BULENOX_API_KEY=your_api_key
   PROJECTX_BULENOX_USERNAME=your_username
   PROJECTX_BULENOX_PREFERRED_ACCOUNT_NAME=your_account_name

E8 Futures
^^^^^^^^^^

.. code-block:: bash

   PROJECTX_E8X_API_KEY=your_api_key
   PROJECTX_E8X_USERNAME=your_username
   PROJECTX_E8X_PREFERRED_ACCOUNT_NAME=your_account_name

Funding Futures
^^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_FUNDINGFUTURES_API_KEY=your_api_key
   PROJECTX_FUNDINGFUTURES_USERNAME=your_username
   PROJECTX_FUNDINGFUTURES_PREFERRED_ACCOUNT_NAME=your_account_name

The Futures Desk
^^^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_THEFUTURESDESK_API_KEY=your_api_key
   PROJECTX_THEFUTURESDESK_USERNAME=your_username
   PROJECTX_THEFUTURESDESK_PREFERRED_ACCOUNT_NAME=your_account_name

Futures Elite
^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_FUTURESELITE_API_KEY=your_api_key
   PROJECTX_FUTURESELITE_USERNAME=your_username
   PROJECTX_FUTURESELITE_PREFERRED_ACCOUNT_NAME=your_account_name

FXIFY Futures
^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_FXIFYFUTURES_API_KEY=your_api_key
   PROJECTX_FXIFYFUTURES_USERNAME=your_username
   PROJECTX_FXIFYFUTURES_PREFERRED_ACCOUNT_NAME=your_account_name

Goat Funded Futures
^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_GOATFUNDEDFUTURES_API_KEY=your_api_key
   PROJECTX_GOATFUNDEDFUTURES_USERNAME=your_username
   PROJECTX_GOATFUNDEDFUTURES_PREFERRED_ACCOUNT_NAME=your_account_name

Hola Prime
^^^^^^^^^^

.. code-block:: bash

   PROJECTX_HOLAPRIME_API_KEY=your_api_key
   PROJECTX_HOLAPRIME_USERNAME=your_username
   PROJECTX_HOLAPRIME_PREFERRED_ACCOUNT_NAME=your_account_name

Nexgen Futures
^^^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_NEXGEN_API_KEY=your_api_key
   PROJECTX_NEXGEN_USERNAME=your_username
   PROJECTX_NEXGEN_PREFERRED_ACCOUNT_NAME=your_account_name

TX3 Funding
^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_TX3FUNDING_API_KEY=your_api_key
   PROJECTX_TX3FUNDING_USERNAME=your_username
   PROJECTX_TX3FUNDING_PREFERRED_ACCOUNT_NAME=your_account_name

DayTraders
^^^^^^^^^^

.. code-block:: bash

   PROJECTX_DAYTRADERS_API_KEY=your_api_key
   PROJECTX_DAYTRADERS_USERNAME=your_username
   PROJECTX_DAYTRADERS_PREFERRED_ACCOUNT_NAME=your_account_name

Demo/Testing
^^^^^^^^^^^^

.. code-block:: bash

   PROJECTX_DEMO_API_KEY=your_api_key
   PROJECTX_DEMO_USERNAME=your_username
   PROJECTX_DEMO_PREFERRED_ACCOUNT_NAME=your_account_name

Supported Functionality
-----------------------

.. list-table:: ProjectX Capabilities
  :widths: 25 15 60
  :header-rows: 1
Usage
-----

1. **Set Environment Variables**: Configure your broker's API credentials
2. **Create Strategy**: Import Lumibot and create your trading strategy  
3. **Run**: ProjectX will auto-detect and connect

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