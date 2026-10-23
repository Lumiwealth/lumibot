.. _ccxt backtesting:

CCXT Crypto Trading and Backtesting with LumiBot
================================================

.. meta::
   :description: Use CCXT crypto exchanges with LumiBot for trading and backtesting. Compare verified exchange integrations, credentials, order handling, and example setup.

This is the guide for Lumibot's CCXT-based cryptocurrency broker integrations. CCXT is a popular library for cryptocurrency trading. If you are interested (but not required!), you can find the documentation for CCXT here: https://ccxt.readthedocs.io/en/latest/

The shared ``Ccxt`` broker accepts exchange IDs provided by your installed CCXT
version. You do not need a new LumiBot broker class or an approved-exchange list
for each venue. Exchange products, API permissions, order features, credentials,
regional availability and test environments still differ.

Available adapters and qualification
------------------------------------

**Available** means the adapter can be selected through generic configuration.
**Contract tested** means LumiBot's order and balance handling passed offline
checks. **Exchange tested** means the workflow was exercised against that
exchange's API. These are different levels of evidence.

.. list-table:: Current shared spot qualification
   :header-rows: 1
   :widths: 30 35 35

   * - Exchange IDs
     - Offline qualification
     - Authenticated exchange qualification in this change
   * - ``phemex``
     - Unified spot orders, balances, cancellation; native CCXT request serialization
     - Pending testnet credentials
   * - ``coinbase``, ``kraken``, ``kucoin``, ``okx``, ``bybit``
     - Unified spot orders, balances and cancellation
     - Not exercised
   * - Other installed CCXT adapters
     - Available through the same configuration
     - Not qualified by this change

Qualification on October 23, 2026 used CCXT 4.5.64: 105 affected offline tests
passed and two pre-existing historical-cache tests were skipped. Coverage:
``test_ccxt_unified_spot``, ``test_ccxt``, ``test_symbol_normalization``,
``test_position_serialization``, ``test_position_signs``,
``test_broker_sync_positions``, ``test_crypto_future_close_position``,
``test_backtesting_ccxt_execution_semantics`` and ``test_ccxt_store``.
The pre-fix shared spot regression had 17 failures. No authenticated orders
were placed during this qualification.

There is no LumiBot exchange allowlist. The following adapter IDs were available
in CCXT 4.5.64; this is an availability list, not a claim that every product or
operation has passed an exchange test:

.. code-block:: text

   aftermath, alpaca, apex, aster, backpack, bequant, bigone, binance,
   binancecoinm, binanceus, binanceusdm, bingx, bit2c, bitbank, bitbns,
   bitfinex, bitflyer, bitget, bithumb, bitmart, bitmex, bitopro, bitrue,
   bitso, bitstamp, bitteam, bittrade, bitvavo, blockchaincom, blofin,
   btcbox, btcmarkets, btcturk, bullish, bybit, bybiteu, bydfi, cex,
   coinbase, coinbaseexchange, coinbaseinternational, coincheck, coinex,
   coinmate, coinone, coinsph, coinspot, cryptocom, cryptomus, deepcoin,
   delta, deribit, derive, digifinex, dydx, exmo, extended, fmfwio, foxbit,
   gate, gateeu, gemini, grvt, hashkey, hibachi, hitbtc, hollaex, htx,
   hyperliquid, independentreserve, indodax, kraken, krakenfutures,
   kucoin, kucoineu, kucoinfutures, latoken, lbank, lighter, luno, mercado,
   mexc, modetrade, mudrex, myokx, ndax, okx, okxus, onetrading, p2b,
   pacifica, paradex, paymium, phemex, poloniex, tokocrypto, toobit, upbit,
   weex, whitebit, woo, woofipro, xt, zaif, zebpay

Inspect the installed list and each exchange's capabilities directly:

.. code-block:: python

   import ccxt

   print(ccxt.exchanges)
   exchange = ccxt.phemex()
   print(exchange.has)
   print(exchange.features)

Generic configuration
---------------------

.. code-block:: python

   from lumibot.brokers import Ccxt

   broker = Ccxt({
       "exchange_id": "phemex",  # Any installed CCXT exchange ID
       "apiKey": "<testnet-api-key>",
       "secret": "<testnet-api-secret>",
       "sandbox": True,
       "options": {"defaultType": "spot"},
   })

Sandbox selection happens before market discovery or other requests. Use
``sandbox=False`` for an exchange's production API. Some venues have no sandbox,
and a static-response sandbox does not prove matching, fills or cancellation.
Credentials are specific to the selected environment. Some exchanges additionally
require ``password`` or other CCXT credential fields.

Spot order quantities are base-coin units. LumiBot uses CCXT's own amount and
price precision methods, including tick-size and decimal-place modes. Native
exchange options remain available through the CCXT config and an order's
``custom_params``; stop orders use the unified ``triggerPrice`` parameter.
Provider-specific trigger direction and other required parameters must match
the exchange API. Compound-order lifecycle remains separate from native attached
orders supplied through ``custom_params``.

Perpetual contracts and position modes
------------------------------------

Use the complete CCXT instrument symbol so quote and settlement remain distinct:
``BTC/USDT:USDT``, ``BTC/USDC:USDC`` or ``BTC/USD:BTC``. The broker discovers
contracts and collateral currencies from exchange metadata. An explicit contract
never falls back to a spot instrument with the same base and quote.

.. code-block:: python

   contract = broker.get_asset("BTC/USDC:USDC")
   # Inside a Strategy:
   order = self.create_order(contract, 1, "buy", position_side="long")
   self.submit_order(order)

Spot quantities mean base-coin units. Contract quantities mean CCXT contract
units, which can be fractional when the market's amount precision permits it.
``contract.multiplier`` is the exchange's ``contractSize``. For a linear contract,
base exposure is quantity times contract size; for an inverse contract, quote
exposure is quantity times contract size. ``broker.get_contract_exposure(asset,
quantity, price)`` returns both exposures and the settlement currency.
Existing broker-specific quantity conventions are unchanged.

``position_side="long"`` and ``position_side="short"`` identify separate hedge
legs. For example, an account can retain ten long contracts and four short
contracts as two positions. ``self.get_position(contract, position_side="short")``
selects the short leg; closing that position retains its identity and reduce-only
intent. Omitting the selector when closing both legs closes each separately.
One-way accounts continue to use a single signed position without a leg selector.
An ambiguous single-position lookup asks for a selector instead of choosing a leg.

Account mode is never changed automatically. The broker exposes
``set_position_mode(hedged, asset, params=None)``,
``set_leverage(leverage, asset, params=None)`` and
``set_margin_mode(mode, asset, params=None)``. Native parameters remain available,
including separate long and short leverage where supported. The venue determines
which operations each contract supports. For Phemex, the V2 position-mode route
follows product metadata rather than restricting it to one settlement currency.

Phemex collateral and unrealized PnL are valued in the strategy's quote currency;
leveraged notional is not added to account equity. A collateral currency without
a direct or inverse conversion market is explicitly warned about and excluded
from converted equity. This is an unresolved valuation limit, not an assertion
that such collateral is worthless. ``fetch_funding_rate(asset)`` reads the current
venue funding rate; it does not simulate historical funding payments.

WebSocket market and account data
--------------------------------

The broker uses the CCXT Pro client included in CCXT. One lazy managed event loop
serves subscriptions for a broker. No network connection or private authentication
starts merely from construction. Live last prices and quotes use streamed data
after subscription, with REST fallback for unavailable or stale public snapshots.

.. code-block:: python

   source = broker.data_source
   source.get_last_price(contract)
   source.get_quote(contract)
   source.get_order_book(contract)
   source.get_trades(contract)
   source.get_live_candles(contract, timeframe="1m")

Historical bars continue through the existing REST history/cache owner. A live
candle subscription does not replace historical data. Order submission, amendment
and cancellation use REST. Account order updates feed the normal order/fill
callbacks, applying cumulative fill quantities once across REST acknowledgements,
WebSocket updates and replays. An uncertain submission retains its client ID for
reconciliation and is not automatically resubmitted. Balance updates invalidate
the collateral snapshot. Positions are reconciled through REST because the
installed Phemex Pro adapter has no ``watch_positions`` implementation.

Call ``broker.cleanup_streams()`` when finished; normal broker shutdown also
closes the managed client. ``connect_stream=False`` retains REST operation.
Phemex requires a symbol for REST open-order discovery, so a stream-disabled
account-wide refresh scans metadata-discovered active markets.

Phemex qualification and remaining work
--------------------------------------

The offline contract and stream tests cover linear USDT, linear USDC and
coin-settled inverse identities, quantity/exposure conversion, separate hedge
legs, native request serialization, cumulative fills, replay handling, fees,
amendment and cancellation. Transport doubles are not exchange trading proof.
The final affected regression run passed 337 tests with two pre-existing skips,
including shared history/cache behavior, Bitunix compatibility, position identity
and strategy execution callbacks. The new contract/stream tests are
``tests/test_ccxt_derivatives.py`` and ``tests/test_ccxt_stream.py``.

Public API qualification on October 23, 2026 with CCXT 4.5.64 established:

* Testnet spot, USDT perpetual and inverse perpetual market discovery and prices.
* Testnet ticker, order book, trades and candle subscriptions for those families.
* USDC perpetual ticker, book, trades and candles on the production **public**
  feed, without credentials or trading.
* USDC testnet prices returned provider error ``6001`` (invalid argument), and
  its public subscriptions failed to produce usable snapshots. The opt-in
  testnet checks retain this failure; USDC is not removed from discovery.

Authenticated testnet orders, private streams, mode changes, collateral balances
and a complete strategy trading cycle remain unqualified until testnet credentials
are available. Run the public checks with ``pytest
tests/test_ccxt_phemex_testnet.py -m apitest``; these make no account writes.
Other exchanges' private/perpetual streams have not been exchange-qualified by
this change. CCXT adapter availability is not a promise that every feature works.

Perpetual backtesting with contract-aware margin, inverse PnL, historical funding
and liquidation is still remaining work. Existing spot ``CcxtBacktesting`` support
does not establish that accounting.

Exchange-Specific Guides
------------------------

Use these pages for exchange-specific setup details:

.. toctree::
   :maxdepth: 1

   brokers.ccxt.coinbase
   brokers.ccxt.kraken
   brokers.ccxt.weex
   brokers.ccxt.kucoin
   brokers.ccxt.binance
   brokers.ccxt.bitmex
   brokers.ccxt.bybit
   brokers.ccxt.okx
   brokers.ccxt.phemex
   brokers.ccxt.bitvavo

Features
--------

* **Cryptocurrency Trading**: Spot, margin and metadata-resolved contracts, subject to venue capabilities and the qualification above
* **Exchange Selection**: Any installed CCXT adapter through explicit configuration; existing credential auto-detection remains available
* **Market Data**: Streamed prices, quotes, books, trades and live candles; REST historical candles and public snapshot fallback
* **Order Types**: Market, limit, stop orders
* **Auto-Detection**: Automatically connects when environment variables are set

Prerequisites
-------------

1. **Account**: Use an eligible account or the exchange's separate testnet account
2. **API Credentials**: Generate API credentials from your exchange's website
3. **Configuration**: Supply CCXT credentials explicitly, or use an existing credential auto-detection path

.. note::
   **Easy Setup with .env File**
   
   LumiBot automatically loads your API credentials from a `.env` file! Simply create a `.env` file in the same folder as your trading strategy and add your broker's environment variables. LumiBot will automatically detect and use these credentials - no additional configuration required.
   
   **Example .env file:**
   
   .. code-block:: bash
   
      # For Kraken
      KRAKEN_API_KEY=your_kraken_api_key_here
      KRAKEN_API_SECRET=your_kraken_secret_here
      
      # For WEEX
      WEEX_API_KEY=your_weex_api_key_here
      WEEX_API_SECRET=your_weex_secret_here
      WEEX_API_PASSPHRASE=your_weex_passphrase_here
   
   That's it! LumiBot handles the rest automatically.

Important note: If you want to use CCXT for backtesting, you should use `CcxtBacktesting` as shown here instead: :ref:`CCXT Backtesting<CCXT Backtesting>`.

Before running any strategy, you first need to create an account on your desired broker and then obtain API credentials from the broker's website. Remember, each broker's website may be different, but you can generally find the API settings under account settings or something similar. Please see the documentation for your broker for more information on getting API credentials.

For trading with cryptocurrencies, always remember to set your market to 24/7 in the ``initialize`` function:

.. code-block:: python

    self.set_market("24/7")

Auto-Detected Credential Paths
------------------------------

These are the CCXT credential sets Lumibot can auto-detect from environment variables.

Kraken
^^^^^^

.. code-block:: bash

   KRAKEN_API_KEY=your_api_key
   KRAKEN_API_SECRET=your_api_secret

Coinbase
^^^^^^^^

Coinbase's current Cloud Developer Platform (CDP) keys use ECDSA/Ed25519 **private keys** rather than the legacy HMAC API-key + secret scheme. Generate a key at https://portal.cdp.coinbase.com/ — the downloaded JSON contains ``name`` (an organization-qualified identifier like ``organizations/<org>/apiKeys/<id>``) and ``privateKey`` (a multi-line PEM block starting with ``-----BEGIN EC PRIVATE KEY-----``).

Paste the ``name`` into ``COINBASE_API_KEY_NAME`` and the full PEM block (newlines and all) into ``COINBASE_PRIVATE_KEY``. ``COINBASE_API_PASSPHRASE`` is only required for legacy HMAC keys and should be omitted for new CDP keys.

.. code-block:: bash

   COINBASE_API_KEY_NAME=organizations/<org-id>/apiKeys/<key-id>
   COINBASE_PRIVATE_KEY="-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n"
   # COINBASE_API_PASSPHRASE=your_passphrase   # only for legacy HMAC keys

WEEX
^^^^

WEEX requires **three** credentials: an API key, a secret, and a mandatory passphrase. Generate these in the WEEX web UI under API Management — make sure the key has *Trade* permission and, if needed, *Read* permission for balance/order lookups.

.. code-block:: bash

   WEEX_API_KEY=your_api_key
   WEEX_API_SECRET=your_api_secret
   WEEX_API_PASSPHRASE=your_passphrase

.. warning::
   WEEX's Terms of Use exclude residents of the United States, Canada, and several other jurisdictions (see https://weexsupport.zendesk.com/hc/en-us/articles/4417379529241). WEEX does **not** provide an API sandbox — all trades run against live infrastructure. Start with very small quantities until you have verified behavior.

.. note::
   The shared broker now resolves contract symbols and exposes leverage, margin
   and funding-rate operations through CCXT. WEEX perpetual balances, account
   synchronization and live trading have not been qualified by this change.

Manual CCXT Config Examples
---------------------------

These exchanges have selected Lumibot/CCXT examples or exchange-specific handling, but they are not currently auto-detected from global ``credentials.py`` environment variables. Use an explicit ``Ccxt`` config and validate balances, positions, order submission, fills, and cancellation with a very small test before increasing size.

KuCoin
^^^^^^

.. code-block:: python

   from lumibot.brokers import Ccxt

   KUCOIN_CONFIG = {
       "exchange_id": "kucoin",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "password": "your_passphrase",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(KUCOIN_CONFIG)

Binance
^^^^^^^

.. code-block:: python

   from lumibot.brokers import Ccxt

   BINANCE_CONFIG = {
       "exchange_id": "binance",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(BINANCE_CONFIG)

BitMEX
^^^^^^

.. code-block:: python

   from lumibot.brokers import Ccxt

   BITMEX_CONFIG = {
       "exchange_id": "bitmex",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(BITMEX_CONFIG)

Documented Backtesting Exchange IDs
-----------------------------------

The CCXT backtesting adapter has documented examples for Kraken, Binance, KuCoin, BitMEX, Bybit, and OKX. These examples are useful for historical crypto strategy tests, but they are not the same thing as tested live broker integrations. Do not assume a backtesting exchange id is live-trading ready unless the live broker path has been validated separately.

Usage
-----

1. **Set Environment Variables**: Configure your broker's API credentials
2. **Create Strategy**: Import Lumibot and create your trading strategy  
3. **Run**: CCXT will auto-detect and connect when one of the auto-detected credential sets is present, or you can pass an explicit ``Ccxt`` config as shown above

.. code-block:: python

   from lumibot.strategies import Strategy
   from lumibot.entities import Asset

   class MyStrategy(Strategy):
       def initialize(self):
           self.sleeptime = "1D"
           # Set the market to 24/7 since crypto markets are always open
           self.set_market("24/7")

       def on_trading_iteration(self):
           # Trade Bitcoin
           btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
           
           # Get current price
           last_price = self.get_last_price(btc)
           
           # Place a limit order
           if last_price:
               order = self.create_order(
                   asset=btc,
                   quantity=0.1,
                   side="buy",
                   order_type="limit", 
                   limit_price=last_price * 0.999
               )
               self.submit_order(order)

   # Run the strategy (CCXT auto-detects supported environment variables,
   # or use an explicit Ccxt config for manual exchange paths)
   strategy = MyStrategy()
   strategy.run_live()

Supported Features
------------------

✅ **Spot Trading**: Buy and sell cryptocurrencies
✅ **Margin Trading**: Trade with leverage (exchange dependent)  
✅ **Market Orders**: Immediate execution
✅ **Limit Orders**: Execute at specified price
✅ **Stop Orders**: Stop-loss functionality  
✅ **Real-time Data**: Live market data
✅ **Historical Data**: Minute, hour, day timeframes

❌ **Stock Trading**: Crypto only
❌ **Options Trading**: Crypto only

Full Example Strategy
---------------------

Here's a complete example of a strategy that demonstrates the use of important functions you might need when trading with these brokers:

.. code-block:: python

    import datetime

    import pandas_ta  # If this gives an error, run `pip install pandas_ta` in your terminal
    from lumibot.brokers import Ccxt
    from lumibot.entities import Asset
    from lumibot.strategies.strategy import Strategy
    from lumibot.traders import Trader


    class ImportantFunctions(Strategy):
        def initialize(self):
            # Set the time between trading iterations
            self.sleeptime = "30S"

            # Set the market to 24/7 since those are the hours for the crypto market
            self.set_market("24/7")

        def on_trading_iteration(self):
            ###########################
            # Placing an Order
            ###########################

            # Define the base and quote assets for our transactions
            base = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
            quote = self.quote_asset

            # Market Order for 0.1 BTC
            mkt_order = self.create_order(base, 0.1, "buy", quote=quote)
            self.submit_order(mkt_order)

            # Limit Order for 0.1 BTC at a limit price of $10,000
            lmt_order = self.create_order(base, 0.1, "buy", quote=quote, limit_price=10000)
            self.submit_order(lmt_order)

            ###########################
            # Getting Historical Data
            ###########################

            # Get the historical prices for our base/quote pair for the last 100 minutes
            bars = self.get_historical_prices(base, 100, "minute", quote=quote)
            if bars is not None:
                df = bars.df
                max_price = df["close"].max()
                self.log_message(f"Max price for {base} was {max_price}")

                ############################
                # TECHNICAL ANALYSIS
                ############################

                # Use pandas_ta to calculate the 20 period RSI
                rsi = df.ta.rsi(length=20)
                current_rsi = rsi.iloc[-1]
                self.log_message(f"RSI for {base} was {current_rsi}")

                # Use pandas_ta to calculate the MACD
                macd = df.ta.macd()
                current_macd = macd.iloc[-1]
                self.log_message(f"MACD for {base} was {current_macd}")

                # Use pandas_ta to calculate the 55 EMA
                ema = df.ta.ema(length=55)
                current_ema = ema.iloc[-1]
                self.log_message(f"EMA for {base} was {current_ema}")

            ###########################
            # Positions and Orders
            ###########################

            # Get all the positions that we own, including cash
            positions = self.get_positions()
            for position in positions:
                self.log_message(f"Position: {position}")

                # Get the asset of the position
                asset = position.asset

                # Get the quantity of the position
                quantity = position.quantity

                # Get the symbol from the asset
                symbol = asset.symbol

                self.log_message(f"we own {quantity} shares of {symbol}")

            # Get one specific position
            asset_to_get = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
            position = self.get_position(asset_to_get)

            # Get all of the outstanding orders
            orders = self.get_orders()
            for order in orders:
                self.log_message(f"Order: {order}")
                # Do whatever you need to do with the order

            # Get one specific order
            order = self.get_order(mkt_order.identifier)

            ###########################
            # Other Useful Functions
            ###########################

            # Get the current (last) price for the base/quote pair
            last_price = self.get_last_price(base, quote=quote)
            self.log_message(
                f"Last price for {base}/{quote} was {last_price}", color="green"
            )

            dt = self.get_datetime()
            self.log_message(f"The current datetime is {dt}")
            self.log_message(f"The current time is {dt.time()}")

            # If you want to check if it's after a certain time, you can do this (eg. trading only after 9:30am)
            if dt.time() > datetime.time(hour=9, minute=30):
                self.log_message("It's after 9:30am")

            # Get the value of the entire portfolio, including positions and cash
            portfolio_value = self.portfolio_value
            # Get the amount of cash in the account (the amount in the quote_asset)
            cash = self.cash

            self.log_message(f"The current value of your account is {portfolio_value}")
            self.log_message(f"The current amount of cash in your account is {cash}") # Note: Cash is based on the quote asset


    if __name__ == "__main__":
        # LumiBot automatically detects your broker from environment variables
        # Just make sure you have a .env file with your broker's credentials
        strategy = ImportantFunctions()
        strategy.run_live()


In this example, we've demonstrated the following:

- How to place a market order and a limit order
- How to get historical data
- How to use technical analysis indicators
- How to get positions and orders
- How to get the current price
- How to get the current datetime
- How to get the value of the portfolio and the amount of cash in the account

.. note::
    You can find the full source code for this example in the `example_strategies` folder of the `lumibot` GitHub repository.
