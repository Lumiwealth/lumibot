ProjectX
========

ProjectX is a **futures-only broker** integration that provides access to multiple underlying futures brokers through a unified gateway. This broker is designed specifically for **prop trading firms** and retail futures trading with practice/demo accounts.

Features
--------

* **Futures Trading Only**: Continuous futures contracts with sophisticated contract resolution
* **Multi-Firm Gateway**: Single interface supporting multiple underlying futures brokers
* **Prop Trading Firms**: Works with major proprietary trading firms and retail brokers
* **Practice Accounts**: Demo/practice account trading only
* **Real-time Streaming**: SignalR-based live order and position updates
* **Order Types**: Market, limit, stop, trailing stop, join bid, join ask orders
* **Rate Limiting**: Built-in API rate limiting and caching for optimal performance
* **Auto-Detection**: Automatically detects and connects when environment variables are configured

Supported Prop Trading Firms
----------------------------

ProjectX works with a wide range of proprietary trading firms and retail futures brokers, including:

**Major Prop Trading Firms:**

* **Topstep** - Leading futures prop trading firm
* **Alpha Futures** - Proprietary trading evaluations
* **Nexgen Futures** - Futures trading firm
* **TickTickTrader** - Prop trading platform
* **TradeDay** - Day trading prop firm
* **Bulenox** - Trading evaluation platform
* **Blusky** - Futures prop trading
* **Goat Futures** - Proprietary trading firm
* **The Futures Desk** - Professional trading firm
* **DayTraders** - Day trading platform
* **E8 Futures** - Futures prop trading
* **Blue Guardian Futures** - Trading firm
* **FuturesElite** - Elite futures trading
* **FXIFY** - Forex and futures prop trading
* **Top One Futures** - Proprietary trading platform
* **Aqua Futures** - Futures trading firm
* **Funding Futures** - Prop trading evaluations
* **TX3 Funding** - Trading capital provider

**And many other retail and institutional futures brokers that support the ProjectX gateway.**

Prerequisites
-------------

1. **Practice Account**: ProjectX integration requires a demo/practice futures trading account
2. **API Credentials**: Username and API key from your futures broker
3. **Base URL**: API endpoint URL for your specific broker firm
4. **Firm Configuration**: Environment variables configured for your specific firm

Environment Variables
---------------------

ProjectX supports multiple firms through a standardized environment variable pattern. Replace `{FIRM}` with your broker name (e.g., TOPONE, TSX):

.. list-table::
   :widths: 35 50 15
   :header-rows: 1

   * - **Variable**
     - **Description**
     - **Required**
   * - `PROJECTX_{FIRM}_API_KEY`
     - Your API key from the broker
     - ✅ Yes
   * - `PROJECTX_{FIRM}_USERNAME`
     - Your username for the broker
     - ✅ Yes
   * - `PROJECTX_{FIRM}_BASE_URL`
     - Base API URL for the broker
     - ✅ Yes
   * - `PROJECTX_{FIRM}_PREFERRED_ACCOUNT_NAME`
     - Specific account name to use (optional)
     - ❌ No
   * - `PROJECTX_{FIRM}_STREAMING_BASE_URL`
     - Streaming endpoint URL (optional)
     - ❌ No
   * - `PROJECTX_FIRM`
     - Specify which firm to use (optional)
     - ❌ No

**Auto-Detection**

ProjectX will **automatically detect** and connect when you have the required environment variables configured. You do **not** need to set `TRADING_BROKER=projectx` - it will auto-detect based on the presence of ProjectX environment variables.

Multi-Firm Support
------------------

ProjectX can automatically detect and use any configured firm. If multiple firms are configured, it will use the first available one, or you can specify which firm to use:

.. code-block:: bash

   # Example for TOPONE firm
   PROJECTX_TOPONE_API_KEY=your_api_key_here
   PROJECTX_TOPONE_USERNAME=your_username
   PROJECTX_TOPONE_BASE_URL=https://api.yourbroker.com/
   
   # Example for TSX firm  
   PROJECTX_TSX_API_KEY=your_api_key_here
   PROJECTX_TSX_USERNAME=your_username
   PROJECTX_TSX_BASE_URL=https://api.tsx.com/
   
   # Optional: Specify which firm to use
   PROJECTX_FIRM=TOPONE

Supported Functionality
-----------------------

.. list-table:: ProjectX Capabilities
  :widths: 25 15 60
  :header-rows: 1

  * - **Feature**
    - **Supported**
    - **Notes**
  * - Futures Trading
    - ✅ Yes
    - Continuous futures with automatic contract resolution
  * - Market Orders
    - ✅ Yes
    - Immediate execution at market price
  * - Limit Orders
    - ✅ Yes
    - Execute at specified price or better
  * - Stop Orders
    - ✅ Yes
    - Stop-loss and stop-limit functionality
  * - Trailing Stop Orders
    - ✅ Yes
    - Dynamic stop orders that follow price
  * - Join Bid/Ask Orders
    - ✅ Yes
    - Advanced order types for liquidity provision
  * - Order Modification
    - ❌ No
    - Must cancel and re-place orders
  * - Real-time Streaming
    - ✅ Yes
    - SignalR-based live updates (optional)
  * - Historical Data
    - ✅ Yes
    - Minute, hour, day, week, month timeframes
  * - Stock Trading
    - ❌ No
    - Futures only
  * - Options Trading
    - ❌ No
    - Futures only
  * - Cryptocurrency
    - ❌ No
    - Futures only

Example `.env` Configuration
---------------------------

.. code-block:: bash

   # Required: Your firm's API credentials
   PROJECTX_TOPONE_API_KEY=your_actual_api_key
   PROJECTX_TOPONE_USERNAME=your_username
   PROJECTX_TOPONE_BASE_URL=https://api.yourbroker.com/
   
   # Optional: Specify preferred account and firm
   PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME=Practice Account 1
   PROJECTX_FIRM=TOPONE

**That's it!** ProjectX will automatically detect these environment variables and initialize. No need to set `TRADING_BROKER=projectx`.

Example Strategy
----------------

.. code-block:: python

   from lumibot.strategies import Strategy
   from lumibot.entities import Asset

   class FuturesStrategy(Strategy):
       def initialize(self):
           self.sleeptime = "1D"

       def on_trading_iteration(self):
           # Trade Micro E-mini S&P 500 futures
           mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
           
           # Get current price
           last_price = self.get_last_price(mes)
           self.log_message(f"MES price: {last_price}")
           
           # Place a limit order
           if last_price:
               limit_price = last_price * 0.999  # 0.1% below market
               order = self.create_order(
                   asset=mes,
                   quantity=1,
                   side="buy",
                   order_type="limit",
                   limit_price=limit_price
               )
               self.submit_order(order)

   # The broker will automatically initialize from environment variables
   strategy = FuturesStrategy()
   strategy.run_backtest(
       backtesting_start=datetime(2023, 1, 1),
       backtesting_end=datetime(2023, 12, 31)
   )

Continuous Futures Support
--------------------------

ProjectX handles continuous futures automatically using sophisticated contract resolution:

.. code-block:: python

   # These symbols are automatically resolved to active contracts
   mes = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)  # Micro E-mini S&P 500
   es = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini S&P 500
   nq = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini NASDAQ
   ym = Asset("YM", asset_type=Asset.AssetType.CONT_FUTURE)    # E-mini Dow Jones
   rty = Asset("RTY", asset_type=Asset.AssetType.CONT_FUTURE)  # E-mini Russell 2000

The system automatically:

- Resolves symbols to current active contracts
- Handles contract rollovers
- Maps to broker-specific contract identifiers
- Manages expiration dates

Account Requirements
-------------------

**Practice/Demo Accounts Only**

ProjectX integration is designed for practice trading and will only connect to demo accounts. The system automatically:

- Filters for accounts with names starting with "prac" or "tof-px"
- Selects the account with the highest balance if multiple practice accounts exist
- Uses the preferred account name if specified in configuration

Important Notes
---------------

**Order Management**
   - **No Order Modification**: ProjectX does not support order modification. To change an order, you must cancel the existing order and place a new one.
   - **Rate Limiting**: Built-in rate limiting prevents API overuse with 50ms delays between requests.

**Streaming Connection**
   - Real-time streaming is **optional** and will fail gracefully if unavailable
   - Requires `signalrcore` library for streaming functionality: `pip install signalrcore>=0.9.2`
   - **Note**: Due to dependency conflicts, `signalrcore` must be installed separately if you want streaming
   - Provides live updates for orders, positions, trades, and account information

**Performance**
   - **Caching**: 30-second cache for account, position, and order data
   - **Auto-reconnection**: Automatic connection management and retry logic
   - **Efficient Resolution**: Smart contract resolution reduces API calls

**Asset Types**
   - **Futures Only**: ProjectX exclusively supports futures trading
   - **Continuous Contracts**: All futures are handled as continuous contracts
   - **No Options/Stocks**: Options chains and stock trading are not available

**Auto-Detection**
   - ProjectX automatically detects when environment variables are configured
   - No need to explicitly set `TRADING_BROKER=projectx`
   - Will be selected automatically if ProjectX credentials are available

Troubleshooting
---------------

**Authentication Issues**
   - Verify your API key and username are correct
   - Check that the base URL is properly formatted
   - Ensure your account has API access enabled

**No Practice Accounts Found**
   - Confirm you have demo/practice accounts available
   - Check account naming (should start with "prac" or "tof-px")
   - Contact your broker if no practice accounts are available

**Rate Limiting**
   - Built-in rate limiting should prevent most issues
   - If you encounter rate limits, the system will automatically retry
   - Consider reducing trading frequency if persistent

**Streaming Connection Issues**
   - Streaming failures are non-critical and won't stop trading
   - Install `signalrcore` library if streaming is required: `pip install signalrcore>=0.9.2`
   - **Note**: Due to dependency conflicts, `signalrcore` must be installed separately if you want streaming
   - Check network connectivity and firewall settings

**Auto-Detection Not Working**
   - Ensure you have the required environment variables: `PROJECTX_{FIRM}_API_KEY`, `PROJECTX_{FIRM}_USERNAME`, `PROJECTX_{FIRM}_BASE_URL`
   - Check that your firm name is uppercase in environment variables
   - Verify no typos in environment variable names

.. note::
   ProjectX is specifically designed for futures trading with practice accounts. Production trading capabilities depend on your broker's API policies and account permissions.

.. important::
   Always test thoroughly with practice accounts before considering any live trading implementation. 