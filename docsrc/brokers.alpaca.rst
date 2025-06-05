Alpaca
======================================================

The Alpaca broker integration allows you to trade stocks, options, and cryptocurrencies through Alpaca Markets. This is one of the most popular brokers for algorithmic trading.

Features
--------

* **Multiple Asset Types**: Stocks, ETFs, options, and cryptocurrencies
* **Paper Trading**: Full paper trading support for testing strategies
* **Real-time Data**: Live market data and order execution
* **Order Types**: Market, limit, stop, stop-limit, trailing stop orders
* **Advanced Orders**: Bracket orders, one-cancels-other (OCO), one-triggers-other (OTO)
* **Multi-leg Options**: Support for complex options strategies
* **Live Streaming**: Real-time trade updates and market data

Getting Started
---------------

1. **Create Alpaca Account**
   
   Sign up for a free account at `Alpaca Markets <https://app.alpaca.markets/signup>`_

2. **Get Your API Keys**
   
   After creating your account:
   
   - Log into your `Alpaca Dashboard <https://app.alpaca.markets/paper/dashboard/overview>`_
   - Go to "API Keys" in the left sidebar (or visit `API Keys page <https://app.alpaca.markets/paper/dashboard/api-keys>`_)
   - Click "Create New Key"
   - Give it a name (e.g., "Lumibot Trading")
   - Copy your **API Key** and **Secret Key** - you'll need both
   - **Important**: Save these keys securely - you won't be able to see the secret again

3. **Set Environment Variables**
   
   Create a `.env` file in your project root and add your keys:

   .. code-block:: bash

       # Alpaca API Keys (RECOMMENDED)
       ALPACA_API_KEY=your_api_key_here
       ALPACA_API_SECRET=your_secret_key_here
       ALPACA_IS_PAPER=true

4. **Start Trading**

   .. code-block:: python

       from lumibot.strategies import Strategy
       from lumibot.brokers import Alpaca
       from lumibot.traders import Trader
       from lumibot.entities import Asset

       class MyStrategy(Strategy):
           def on_trading_iteration(self):
               if self.first_iteration:
                   # Buy 100 shares of SPY
                   order = self.create_order(
                       Asset(symbol="SPY", asset_type="stock"),
                       quantity=100,
                       side="buy"
                   )
                   self.submit_order(order)

       # The broker will automatically use your environment variables
       broker = Alpaca()
       strategy = MyStrategy(broker=broker)
       trader = Trader()
       trader.add_strategy(strategy)
       trader.run_all()

Authentication Methods
----------------------

API Keys (Recommended)
~~~~~~~~~~~~~~~~~~~~~~

**This is the recommended method.** Set up your API keys in environment variables:

.. code-block:: bash

    # In your .env file
    ALPACA_API_KEY=your_api_key_here
    ALPACA_API_SECRET=your_secret_key_here
    ALPACA_IS_PAPER=true  # Set to false for live trading

Then simply create your broker without any configuration:

.. code-block:: python

    from lumibot.brokers import Alpaca
    
    # Automatically uses environment variables
    broker = Alpaca()

OAuth Token (Advanced)
~~~~~~~~~~~~~~~~~~~~~~

For OAuth authentication, visit `botspot.trade <https://botspot.trade>`_ to set up OAuth integration. Then set your OAuth token:

.. code-block:: bash

    # In your .env file
    ALPACA_OAUTH_TOKEN=your_oauth_token_here
    ALPACA_IS_PAPER=true

Configuration Options
---------------------

All configuration should be done via environment variables in your `.env` file:

================== =============== =========== =======================================================
Environment Var    Type            Default     Description
================== =============== =========== =======================================================
``ALPACA_API_KEY``        str             None        Your Alpaca API key (get from dashboard)
``ALPACA_API_SECRET``     str             None        Your Alpaca API secret (get from dashboard)
``ALPACA_OAUTH_TOKEN``    str             None        OAuth token (get from botspot.trade)
``ALPACA_IS_PAPER``       bool            true        Whether to use paper trading account
================== =============== =========== =======================================================

Usage Examples
--------------

Basic Stock Trading
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.strategies import Strategy
    from lumibot.brokers import Alpaca
    from lumibot.traders import Trader
    from lumibot.entities import Asset

    class StockStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                # Buy 100 shares of Apple
                order = self.create_order(
                    Asset(symbol="AAPL", asset_type="stock"),
                    quantity=100,
                    side="buy"
                )
                self.submit_order(order)

    # No configuration needed - uses environment variables
    broker = Alpaca()
    strategy = StockStrategy(broker=broker)
    trader = Trader()
    trader.add_strategy(strategy)
    trader.run_all()

Cryptocurrency Trading
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from lumibot.entities import Asset

    class CryptoStrategy(Strategy):
        def on_trading_iteration(self):
            # Buy Bitcoin
            btc = Asset(symbol="BTC", asset_type="crypto")
            usd = Asset(symbol="USD", asset_type="forex")
            
            order = self.create_order(
                asset=btc,
                quantity=0.1,
                side="buy",
                quote=usd
            )
            self.submit_order(order)

Options Trading
~~~~~~~~~~~~~~~

.. code-block:: python

    from datetime import datetime

    class OptionsStrategy(Strategy):
        def on_trading_iteration(self):
            # Buy a call option
            expiration = datetime(2024, 12, 20)
            spy_call = Asset(
                symbol="SPY",
                asset_type="option",
                expiration=expiration,
                strike=450,
                right="CALL"
            )
            
            order = self.create_order(
                asset=spy_call,
                quantity=1,
                side="buy"
            )
            self.submit_order(order)

Advanced Order Types
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Bracket order (market order with profit target and stop loss)
    bracket_order = self.create_order(
        Asset("AAPL"),
        quantity=100,
        side="buy",
        order_type="market",
        order_class="bracket",
        take_profit_price=155.0,
        stop_loss_price=145.0
    )
    
    self.submit_order(bracket_order)

Important Notes
---------------

**Paper vs Live Trading**
    * Paper trading is enabled by default (safe for testing)
    * Set ``ALPACA_IS_PAPER=false`` in your `.env` file to enable live trading
    * Always test strategies thoroughly in paper trading first

**API Keys Security**
    * Never put API keys directly in your code
    * Always use environment variables (`.env` file)
    * Add `.env` to your `.gitignore` file
    * Keep your secret key private - treat it like a password

**OAuth Integration**
    * For OAuth setup, visit `botspot.trade <https://botspot.trade>`_
    * OAuth tokens are managed through botspot.trade platform
    * OAuth provides enhanced security for third-party integrations

**Data & Pricing**
    * Stock and options data has a 15-minute delay unless you have a paid data subscription
    * Cryptocurrency data is real-time
    * Price precision varies by asset type

**Order Limitations**
    * Crypto orders support "gtc" and "ioc" time-in-force
    * Options orders default to "day" time-in-force
    * Some advanced features require both OAuth token and API credentials

**Important Notes:**

* **Paper Trading**: All examples default to paper trading (``ALPACA_IS_PAPER=true``)
* **Rate Limits**: Alpaca has rate limits on API calls - the library handles basic rate limiting
* **Market Hours**: Stock trading is limited to market hours; crypto trading is 24/7
* **Minimum Quantities**: Some assets have minimum quantity requirements
* **Authentication Issues**: If you encounter "Unauthorized" errors:
  
  - **For OAuth users**: Check your ``ALPACA_OAUTH_TOKEN`` is valid and not expired
  - **For API key users**: Verify ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET`` are correct
  - **Account permissions**: Ensure your account has access to the data/trading features you're using
  - **Re-authentication**: OAuth tokens may need periodic renewal via the OAuth flow

**Error Handling:**

The broker provides clear error messages for common authentication issues:

.. code-block:: text

    ❌ ALPACA AUTHENTICATION ERROR: Your OAuth token appears to be invalid or expired.

    🔧 To fix this:
    1. Check that your ALPACA_OAUTH_TOKEN environment variable is set correctly
    2. Verify your OAuth token is valid and not expired  
    3. Re-authenticate at: https://localhost:3000/oauth/alpaca/success
    4. Or use API key/secret instead by setting ALPACA_API_KEY and ALPACA_API_SECRET

**Polling vs Streaming:**

* **OAuth-only configurations** use polling (default: 5-second intervals) since TradingStream doesn't support OAuth
* **API key/secret configurations** use real-time WebSocket streaming
* **Mixed configurations** (OAuth + API credentials) can use either method

Troubleshooting
---------------

**"No API credentials found" Error**
    Make sure your `.env` file is in the correct location and contains:
    
    .. code-block:: bash
    
        ALPACA_API_KEY=your_actual_key_here
        ALPACA_API_SECRET=your_actual_secret_here

**Paper Trading Not Working**
    Verify ``ALPACA_IS_PAPER=true`` is set in your `.env` file

**Live Trading Issues**
    * Ensure you have sufficient buying power
    * Check if your account is approved for the asset type you're trading
    * Verify ``ALPACA_IS_PAPER=false`` for live trading

API Reference
-------------

.. automodule:: lumibot.brokers.alpaca
   :members:
   :undoc-members:
   :show-inheritance:

Market Hours Methods
~~~~~~~~~~~~~~~~~~~~

.. automethod:: lumibot.brokers.alpaca.Alpaca.get_time_to_close
   :no-index:

.. automethod:: lumibot.brokers.alpaca.Alpaca.get_time_to_open
   :no-index:

.. automethod:: lumibot.brokers.alpaca.Alpaca.get_timestamp
   :no-index:

.. automethod:: lumibot.brokers.alpaca.Alpaca.is_market_open
   :no-index:

Additional Resources
--------------------

* `Alpaca Markets <https://alpaca.markets>`_ - Main website
* `Alpaca Dashboard <https://app.alpaca.markets>`_ - Account management
* `Alpaca API Documentation <https://alpaca.markets/docs/api-documentation/>`_ - Technical details
* `botspot.trade <https://botspot.trade>`_ - OAuth integration platform
* `Lumibot Examples <https://github.com/Lumiwealth/lumibot/tree/master/lumibot/example_strategies>`_ - More strategy examples