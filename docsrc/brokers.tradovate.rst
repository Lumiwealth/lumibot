Tradovate
===================================

.. important::
   
   **Tradovate does not provide market data.** You must use a separate data source for live trading. Recommended data sources include DataBento, ProjectX, or Interactive Brokers (all support futures data).

Tradovate is a futures broker that provides access to CME Group markets. This guide will help you set up Tradovate with Lumibot for futures trading.

Getting Started
---------------

To get started with Tradovate, you'll need:

1. A Tradovate account (create one at `tradeovate.com <https://www.tradeovate.com/>`_)
2. API credentials from your Tradovate dashboard
3. A separate data source for market data

Obtaining API Credentials
-------------------------

1. Log into your Tradovate account
2. Navigate to the API section in your dashboard
3. Create a new API application to get your credentials
4. Note down your:
   - Username
   - Dedicated Password (for API access)
   - CID (Client ID)
   - Secret

Required Environment Variables
------------------------------

.. list-table:: Required Tradovate Environment Variables
   :header-rows: 1
   :widths: 40 60

   * - Variable
     - Description
   * - ``TRADOVATE_USERNAME``
     - Your Tradovate username
   * - ``TRADOVATE_DEDICATED_PASSWORD``
     - Your dedicated API password (not your login password)
   * - ``TRADOVATE_CID``
     - Your Client ID from Tradovate API dashboard
   * - ``TRADOVATE_SECRET``
     - Your Secret from Tradovate API dashboard

Optional Environment Variables
------------------------------

.. list-table:: Optional Tradovate Environment Variables
   :header-rows: 1
   :widths: 40 60

   * - Variable
     - Description
   * - ``TRADOVATE_IS_PAPER``
     - Set to ``true`` for demo trading, ``false`` for live (default: ``true``)
   * - ``TRADOVATE_APP_ID``
     - Application identifier (default: ``Lumibot``)
   * - ``TRADOVATE_APP_VERSION``
     - Application version (default: ``1.0``)
   * - ``TRADOVATE_MD_URL``
     - Market data URL override (not recommended)

Configuration Examples
----------------------

**Paper Trading Configuration:**

.. code-block:: bash

   # Required credentials
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # Use paper trading (default)
   TRADOVATE_IS_PAPER=true

   # Set Tradovate as broker with separate data source
   TRADING_BROKER=tradovate
   DATA_SOURCE=databento  # or projectx, ibrest, ib

**Live Trading Configuration:**

.. code-block:: bash

   # Required credentials
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # Enable live trading
   TRADOVATE_IS_PAPER=false

   # Set Tradovate as broker with separate data source
   TRADING_BROKER=tradovate
   DATA_SOURCE=databento

Broker + Data Source Combinations
----------------------------------

Since Tradovate doesn't provide market data, you must configure a separate data source. Here are the supported combinations:

**Tradovate + DataBento (Recommended for Futures)**

.. code-block:: bash

   # Tradovate broker
   TRADING_BROKER=tradovate
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # DataBento data source
   DATA_SOURCE=databento
   DATABENTO_API_KEY=your_databento_key

**Tradovate + ProjectX**

.. code-block:: bash

   # Tradovate broker
   TRADING_BROKER=tradovate
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # ProjectX data source
   DATA_SOURCE=projectx
   PROJECTX_TOPONEFUTURES_API_KEY=your_projectx_key
   PROJECTX_TOPONEFUTURES_USERNAME=your_projectx_username
   PROJECTX_FIRM=toponefutures

**Tradovate + Interactive Brokers (REST API)**

.. code-block:: bash

   # Tradovate broker
   TRADING_BROKER=tradovate
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # Interactive Brokers data source (REST API)
   DATA_SOURCE=ibrest
   IB_USERNAME=your_ib_username
   IB_PASSWORD=your_ib_password

**Tradovate + Interactive Brokers (Legacy TWS)**

.. code-block:: bash

   # Tradovate broker
   TRADING_BROKER=tradovate
   TRADOVATE_USERNAME=your_username
   TRADOVATE_DEDICATED_PASSWORD=your_api_password
   TRADOVATE_CID=your_client_id
   TRADOVATE_SECRET=your_secret_key

   # Interactive Brokers data source (Legacy TWS)
   DATA_SOURCE=ib
   INTERACTIVE_BROKERS_IP=127.0.0.1
   INTERACTIVE_BROKERS_PORT=7497
   INTERACTIVE_BROKERS_CLIENT_ID=1

Running Your Strategy
---------------------

**Method 1: Using Environment Variables (Recommended)**

Set your environment variables as shown above, then run your strategy:

.. code-block:: python

   from lumibot.strategies import Strategy
   from lumibot.traders import Trader
   from lumibot.entities import Asset

   class MyFuturesStrategy(Strategy):
       def initialize(self):
           self.sleeptime = "1M"  # Run every minute
           # Use continuous futures contracts (recommended approach)
           self.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

       def on_trading_iteration(self):
           # Your futures trading logic here
           price = self.get_last_price(self.asset)
           self.log_message(f"{self.asset.symbol} price: {price}")

   # Strategy will automatically use Tradovate + DataBento from environment
   strategy = MyFuturesStrategy()
   trader = Trader()
   trader.add_strategy(strategy)
   trader.run_all()

Futures Symbol Format
----------------------

When trading futures with Tradovate, use Lumibot's continuous futures format with Asset objects:

.. code-block:: python

   from lumibot.entities import Asset

   # Recommended approach - continuous futures
   mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro E-mini S&P 500
   es_asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini S&P 500
   nq_asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini NASDAQ
   ym_asset = Asset("YM", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini Dow

.. list-table:: Common Futures Symbols
   :header-rows: 1
   :widths: 30 70

   * - Symbol
     - Description
   * - ``MES``
     - Micro E-mini S&P 500 futures
   * - ``ES``
     - E-mini S&P 500 futures
   * - ``NQ``
     - E-mini NASDAQ futures  
   * - ``YM``
     - E-mini Dow futures
   * - ``RTY``
     - E-mini Russell 2000 futures
   * - ``CL``
     - Crude Oil futures
   * - ``GC``
     - Gold futures
   * - ``ZN``
     - 10-Year Treasury Note futures

.. note::
   
   Using continuous futures (``CONT_FUTURE``) is recommended as Lumibot automatically handles contract rollover and expiration dates.

Important Notes
---------------

1. **No Market Data**: Tradovate does not provide market data. You must configure a separate data source.

2. **Data Source Pricing**: You'll need to pay for market data through your chosen data source (DataBento, ProjectX, etc.).

3. **Futures Only**: Tradovate specializes in futures contracts. Stocks and options are not supported.

3. **Paper vs Live**: Always start with paper trading (``TRADOVATE_IS_PAPER=true``) to test your strategies.

4. **API Limits**: Be aware of Tradovate's API rate limits. Lumibot handles this automatically, but excessive requests may be throttled.

5. **Account Requirements**: Ensure your Tradovate account has sufficient margin for futures trading.

6. **Timezone**: Tradovate operates in US Central Time. Lumibot handles timezone conversions automatically.

Troubleshooting
---------------

**Authentication Issues**

- Verify your credentials are correct
- Ensure you're using the dedicated API password, not your login password
- Check that your CID and Secret match your API application

**Connection Problems**

- Confirm your internet connection is stable
- Check if Tradovate's API is experiencing downtime
- Verify your account is in good standing

**Data Issues**

- Remember that Tradovate doesn't provide market data
- Ensure your separate data source is properly configured
- Check that your data source supports the symbols you're trading

**Trading Issues**

- Verify your account has sufficient margin
- Check that you're trading during market hours
- Ensure the contract symbols are correctly formatted

For additional support, consult the `Tradovate API documentation <https://api.tradeovate.com/>`_ or contact Tradovate support directly.
