Synthetic Option Data for Backtests
===================================

Backtest an options strategy when you have no option data. LumiBot can compute option
chains, option prices, bid and ask, and greeks from the price history of the underlying
stock, ETF or index.

.. warning::

   Synthetic option prices are **estimates computed from a model**. They are not market
   data. Measured against real option trades, a typical synthetic price is 20 to 30% away
   from the real price, and further on volatile days and for far out-of-the-money
   contracts. Use synthetic data to check that your strategy's logic works. Do not read the
   profit and loss of a synthetic backtest as what you would have made.

What it is
----------

With the ``synthetic`` option route, LumiBot needs only the bars of the underlying. From
them it builds:

- **Option chains** from fixed listing rules: weekly and monthly expirations (daily ones for
  the underlyings that have them) and strikes on the usual grid around the price.
- **A price for any contract on any bar**, from the Black-Scholes family of models with a
  volatility that depends on strike and expiration.
- **A bid and an ask** around that price, wider for longer expirations, in-the-money
  contracts, volatile markets and violent bars.
- **Greeks** (delta, gamma, theta, vega). Volume and open interest are marked not available.

Where the volatility comes from, in this order:

1. Real option trades or quotes, when you also name a real option route (see below).
2. A volatility index for the underlying when one exists and your data source has it.
3. The underlying's own recent realized volatility, times a risk premium.

The same bars and settings always give the same prices.

When to use it
--------------

- You want to test the mechanics of an options strategy (entries, exits, position sizing,
  rolling, what happens at expiration) and have no option data.
- You need history that no option data source covers.
- You want a quick first look before paying for or connecting real option data.

When not to use it: to decide whether a strategy makes money, to tune strikes or deltas to
the cent, or for anything that depends on the exact premium of far out-of-the-money options
(iron condor wings, tail hedges).

How to use it
-------------

Route options to ``synthetic`` in the backtest data routing. Everything else keeps its
normal data source.

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import RoutedBacktestingPandas
    from lumibot.entities import Asset
    from lumibot.strategies import Strategy


    class PutCreditSpread(Strategy):
        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            if self.get_positions():
                return
            spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
            price = self.get_last_price(spy)
            chains = self.get_chains(spy)
            expiration = sorted(chains["Chains"]["PUT"])[4]
            strikes = chains["Chains"]["PUT"][expiration]
            short_strike = max(k for k in strikes if k <= price * 0.97)
            long_strike = max(k for k in strikes if k <= short_strike - 5)
            expiry = datetime.strptime(expiration, "%Y-%m-%d").date()
            legs = [
                self.create_order(Asset("SPY", asset_type="option", expiration=expiry, strike=short_strike, right="put"), 1, "sell"),
                self.create_order(Asset("SPY", asset_type="option", expiration=expiry, strike=long_strike, right="put"), 1, "buy"),
            ]
            self.submit_order(legs)


    PutCreditSpread.backtest(
        RoutedBacktestingPandas,
        datetime(2025, 1, 2),
        datetime(2025, 3, 31),
        config={"backtesting_data_routing": {"default": "ibkr", "option": "synthetic"}},
    )

Orders fill the same way as with real quotes: buys at the ask, sells at the bid, and limit
and smart-limit orders work against the synthetic bid and ask. See
:doc:`backtesting.how_orders_fill`.

How you can tell a result is synthetic
--------------------------------------

LumiBot always says so:

- A line in the log at the start: "Option prices in this backtest are synthetic: computed
  from the underlying price and volatility. They are estimates."
- Every fill in the trades file has ``price_source=synthetic``.
- ``data_provenance.json`` reports how many option fills came from each source (``quote``,
  ``trade``, ``estimated``, ``synthetic``), their shares, and which volatility input was
  used.

If you publish or share a backtest that used synthetic option data, say that it did.

Mixing in real option data
--------------------------

If a data source has real option trades or quotes for part of the history, name it as
``option_real``. Bars with a real quote or trade use the real values, and every other bar is
synthetic. Each fill is labeled with what it was priced from.

.. code-block:: python

    config = {"backtesting_data_routing": {"default": "ibkr", "option": "synthetic", "option_real": "alpaca"}}

How accurate it is
------------------

Checked against 1.85 million real one-minute option trade bars for two large index ETFs,
strikes within 10% of the price, expirations up to 45 days, default settings:

.. list-table::
   :header-rows: 1

   * - Market
     - Median error
     - 9 in 10 within
     - Median error, % of price
   * - Calm week
     - about 35 cents
     - about $1.05
     - about 22%
   * - Stressed days (large selloffs)
     - about $2.00
     - about $5.00
     - about 30%

.. list-table::
   :header-rows: 1

   * - Contract (calm week)
     - Median error, % of price
   * - At the money
     - about 24%
   * - 15 to 40 delta
     - about 36%
   * - 5 to 15 delta (far out of the money)
     - about 47%
   * - In the money
     - about 5%

Options expiring within a week tend to come out too cheap, and options a month out a little
too expensive. On days after a market shock, synthetic prices are far too cheap.

Limits
------

- No option market data is used unless you add a real route, so events the option market
  prices in advance (earnings, central bank days) are not in the prices.
- European-style pricing. Early exercise value and individual dividend dates are not
  modeled. The interest rate and dividend yield are constants.
- The bid/ask model was measured on index options and scaled to other underlyings.
- Volume and open interest are not available.
- Chains follow listing rules, not the actual list of contracts on each day.

Settings
--------

See :doc:`environment_variables` ("Synthetic option data") for the interest rate, dividend
yield, volatility input and parameter file settings.

Want real option data without wiring it up yourself? `BotSpot <https://botspot.trade>`_ runs
LumiBot strategies with connected broker data, and its free MCP lets an AI assistant build
and backtest option strategies for you.
