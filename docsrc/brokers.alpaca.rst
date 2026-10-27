Alpaca API Trading with LumiBot
===============================

.. meta::
   :description: Connect LumiBot to Alpaca for stock, ETF, option, and crypto trading with API credentials, paper or live mode, and runnable Python examples.

The Alpaca broker integration allows you to trade stocks, options, and cryptocurrencies through Alpaca Markets. This is one of the most popular brokers for algorithmic trading.

.. include:: _snippets/botspot_mcp_callout.rst

Go directly to :ref:`Alpaca setup <alpaca-getting-started>`,
:ref:`authentication <alpaca-authentication>`, :ref:`configuration
<alpaca-configuration>`, or :ref:`runnable examples <alpaca-examples>`.

Features
--------

* **Multiple Asset Types**: Stocks, ETFs, options, and cryptocurrencies
* **Paper Trading**: Full paper trading support for testing strategies
* **Real-time Data**: Live market data and order execution
* **Order Types**: Market, limit, stop, stop-limit, trailing stop orders
* **Advanced Orders**: Bracket orders, one-cancels-other (OCO), one-triggers-other (OTO)
* **Multi-leg Options**: Support for complex options strategies
* **Live Streaming**: Real-time trade updates and market data
* **Cash Events**: Live cloud payloads can include normalized broker cash events such as deposits, withdrawals, interest, dividends, fees, journals, and adjustments

Alpaca market-data reads support stocks, options and crypto. Requests for futures,
continuous futures, crypto futures, forex bases or indices raise ``ValueError``
before making a provider request. This error remains visible through
``Strategy.get_quote()`` and ``Strategy.get_last_price()`` instead of becoming
an empty quote or missing price. They are never treated as equity tickers sharing
the same symbol. A forex quote currency such as USD remains valid for crypto pairs.
Historical batches containing unsupported asset types fail before any request;
separate those assets and use a data source that supports their contract type.

.. _alpaca-getting-started:

Getting Started
---------------

1. **Create Alpaca Account**
   
   Sign up for a free account at `Alpaca Markets <https://app.alpaca.markets/signup>`__

2. **Get Your API Keys**
   
   After creating your account:
   
   - Log into your `Alpaca Dashboard <https://app.alpaca.markets/paper/dashboard/overview>`__
   - Go to "API Keys" in the left sidebar (or visit `API Keys page <https://app.alpaca.markets/paper/dashboard/api-keys>`__)
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

.. _alpaca-authentication:

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

For OAuth authentication, visit `botspot.trade <https://botspot.trade>`__ to set up OAuth integration. Then set your OAuth token:

.. code-block:: bash

    # In your .env file
    ALPACA_OAUTH_TOKEN=your_oauth_token_here
    ALPACA_IS_PAPER=true

.. _alpaca-configuration:

Configuration Options
---------------------

All configuration should be done via environment variables in your `.env` file:

.. list-table:: Alpaca Environment Variables
   :widths: 25 10 10 55
   :header-rows: 1

   * - Environment Var
     - Type
     - Default
     - Description
   * - ``ALPACA_API_KEY``
     - str
     - None
     - Your Alpaca API key (available from the Alpaca dashboard).
   * - ``ALPACA_API_SECRET``
     - str
     - None
     - Your Alpaca API secret (download from the dashboard).
   * - ``ALPACA_OAUTH_TOKEN``
     - str
     - None
     - OAuth token issued via `botspot.trade <https://botspot.trade>`__.
   * - ``ALPACA_IS_PAPER``
     - bool
     - ``true``
     - Toggle paper trading (``true``) versus live trading (``false``).

.. _alpaca-examples:

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

Cash Events
-----------

Lumibot can emit normalized live ``cash_events`` in the cloud payload for Alpaca strategies. This is separate from the
order event pipeline and is intended for non-trade account activity such as:

* deposits and withdrawals
* interest
* dividends and withholding
* fees
* journals and other cash adjustments

The Alpaca implementation uses the authenticated Trading API client already configured inside LumiBot and calls the
account activities endpoint through the SDK's inherited REST client. No extra broker client or raw ``requests`` setup
is required.

Normalized cash-event shape
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each emitted event includes:

* ``event_id`` (stable deterministic ID for downstream idempotency)
* ``broker_event_id`` (when Alpaca provides one)
* ``broker_name``
* ``event_type``
* ``raw_type``
* ``raw_subtype``
* ``amount``
* ``currency``
* ``occurred_at``
* ``description``
* ``direction``
* ``is_external_cash_flow``

Notes
~~~~~

* Cash events are polled and deduplicated before they are added to the live cloud payload.
* Payloads stay bounded and omit full raw broker blobs to avoid unnecessary listener payload growth.
* Alpaca paper credentials can be used to validate the read path when the account activities endpoint is authorized.

See also: :doc:`cash_accounting`

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
    * For OAuth setup, visit `botspot.trade <https://botspot.trade>`__
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
   :noindex:
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

Frequently asked questions
--------------------------

Where do I find my Alpaca API key and secret key?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Log in to the `Alpaca dashboard <https://app.alpaca.markets>`__ and open the API Keys page. Click "Create New Key" and copy both the API key and the secret key.

Save the secret key right away. Alpaca only shows it once.

Are Alpaca paper trading keys different from live keys?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Alpaca says your paper trading account has a different API key from your live account (see the `Alpaca paper trading docs <https://docs.alpaca.markets/docs/paper-trading>`__).

Use paper keys with ``ALPACA_IS_PAPER=true`` and live keys with ``ALPACA_IS_PAPER=false``.

What environment variables does LumiBot use for Alpaca?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot reads ``ALPACA_API_KEY``, ``ALPACA_API_SECRET`` and ``ALPACA_IS_PAPER``. If you use OAuth, it reads ``ALPACA_OAUTH_TOKEN``.

``ALPACA_IS_PAPER`` defaults to ``true`` when it is not set. LumiBot does not read the ``APCA_API_KEY_ID`` or ``APCA_API_SECRET_KEY`` names that some Alpaca tools use, so copy your keys into the ``ALPACA_`` names.

Do I have to put my Alpaca keys in my code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. If you create a strategy without a broker, LumiBot picks one from your environment variables. It chooses Alpaca when ``ALPACA_API_KEY`` or ``ALPACA_OAUTH_TOKEN`` is set. You can also set ``TRADING_BROKER=alpaca`` to force it.

To build the broker yourself, import the settings with ``from lumibot.credentials import ALPACA_CONFIG`` and pass them in with ``Alpaca(ALPACA_CONFIG)``.

How do I switch from Alpaca paper trading to live trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``ALPACA_IS_PAPER=false`` and use the API key and secret from your live account. Then restart your strategy.

Test the same strategy in paper trading first.

What can I trade on Alpaca with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stocks, ETFs, options and crypto. Order types include market, limit, stop, stop-limit and trailing stop, plus bracket, OCO and OTO orders.

Alpaca data requests for futures, forex bases or indexes raise a ``ValueError``. Use a different data source for those assets.

What time in force does LumiBot use for Alpaca crypto and options orders?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot sends Alpaca crypto orders as ``gtc`` (good till canceled) and Alpaca option orders as ``day``. Stock orders keep the time in force you set on the order.

Is Alpaca market data real time on the free plan?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Partly. On Alpaca's free Basic plan, real-time stock data comes from the IEX exchange only, and options use an indicative price feed. The paid Algo Trader Plus plan covers all US stock exchanges and the OPRA options feed.

See `Alpaca's market data docs <https://docs.alpaca.markets/docs/about-market-data-api>`__ for the current plans.

Can I use Alpaca OAuth instead of API keys?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Set ``ALPACA_OAUTH_TOKEN`` instead of the key and secret. You can get an OAuth token through `botspot.trade <https://botspot.trade>`__.

With OAuth only, LumiBot checks for order updates by polling every 5 seconds. With an API key and secret, it uses Alpaca's live stream.

Additional Resources
--------------------

* `Alpaca Markets <https://alpaca.markets>`__ - Main website
* `Alpaca Dashboard <https://app.alpaca.markets>`__ - Account management
* `Alpaca API Documentation <https://alpaca.markets/docs/api-documentation/>`__ - Technical details
* `botspot.trade <https://botspot.trade>`__ - OAuth integration platform
* `Lumibot Examples <https://github.com/Lumiwealth/lumibot/tree/master/lumibot/example_strategies>`__ - More strategy examples

Caller-owned SDK clients
------------------------

Applications that manage Alpaca SDK clients can supply ``trading_client`` to
``Alpaca`` and ``stock_client``, ``crypto_client`` and ``option_client`` to
``AlpacaData``. Each supplied client owns its authentication. The equivalent
configuration keys are ``TRADING_CLIENT``, ``STOCK_CLIENT``, ``CRYPTO_CLIENT``
and ``OPTION_CLIENT``; explicit constructor arguments take precedence.

Passing these keys to ``Alpaca(config)`` also configures its default data source.
Supply a separate ``data_source`` to keep using another market-data provider.
Injected trading clients use the existing REST order-polling lifecycle.
Authentication resets retain supplied clients; they do not replace their
authentication or credentials. API-key and OAuth construction remains available.
