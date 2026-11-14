Crypto Perpetuals
=================

Backtest crypto perpetual futures with real perpetual prices, leverage, margin,
funding payments and liquidation.

A perpetual is a ``crypto_future`` asset. LumiBot's backtesting broker keeps a
futures wallet for it: margin is reserved (not spent), profit and loss settles
in the contract's settlement coin, funding is charged or paid at each real
settlement, and a position that runs out of collateral is liquidated.

Ways to get perpetual data
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Source
     - What you get
     - How to choose it
   * - ``CcxtBacktesting``
     - Perpetual candles, contract terms and funding history from any CCXT
       exchange that lists the contract and supports ``fetchFundingRateHistory``
     - ``datasource_class=CcxtBacktesting`` with ``exchange_id``, and an asset such
       as ``Asset("BTC/USDT:USDT", "crypto_future")``
   * - Bitunix public market data (routed backtests)
     - Bitunix perpetual candles, contract terms and settled funding (rate and
       mark price at settlement). Public endpoints, no API key
     - ``BACKTESTING_DATA_SOURCE='{"crypto_future":"bitunix", ...}'``
   * - Spot proxy (routed backtests, other providers)
     - Spot candles of the same coin. No contract terms and no funding, so
       perpetual orders are rejected with "Derivative contract terms are
       unavailable"
     - Any other ``crypto_future`` route, for example ``"coinbase"`` or ``"ibkr"``

Routing perpetuals to Bitunix
-----------------------------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"crypto_future":"bitunix","crypto":"coinbase","default":"ibkr"}'

.. code-block:: python

   from lumibot.entities import Asset
   from lumibot.strategies import Strategy


   class PerpTrend(Strategy):
       def initialize(self):
           self.sleeptime = "1H"
           self.set_market("24/7")
           self.perp = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE, leverage=5)

       def on_trading_iteration(self):
           if not self.get_position(self.perp):
               self.submit_order(self.create_order(self.perp, 0.05, "buy"))


   PerpTrend.backtest(
       None,  # BACKTESTING_DATA_SOURCE chooses the router
       quote_asset=Asset("USDT", Asset.AssetType.CRYPTO),
       budget=10000,
   )

Things to know:

- **Name the contract.** Use the exchange symbol, for example ``BTCUSDT`` or
  ``ETHUSDC``. A bare ``BTC`` needs an explicit USDT or USDC quote; LumiBot never
  guesses which contract you meant.
- **Fund the right wallet.** USDT contracts settle in USDT, so set
  ``quote_asset=Asset("USDT", "crypto")`` and give the budget in USDT. A USD budget
  is not converted silently; orders are rejected with a message saying how to fix it.
- **Quantities are in the coin.** ``0.05`` means 0.05 BTC, as on Bitunix.
- **Coin-margined (inverse) contracts such as ``BTCUSD`` are not supported yet.**
  Bitunix does not publish their contract size or margin coin, so the backtest stops
  before trading with a plain error instead of guessing the payoff.
- **Missing data stays missing.** If Bitunix has no candles for the window (for
  example before a contract was listed) or cannot be reached, the backtest stops
  with that reason. Spot prices are never substituted on this route.
- **Limits.** Maintenance-margin tiers and liquidation fees are not published by
  this source, so liquidation happens when collateral runs out and is checked on
  closed candles (intrabar liquidation is not modeled). The backtest lists these
  assumptions in its log.
- Candles are cached in LumiBot's shared bar cache when it is enabled, so a
  second run of the same window does not download them again.
