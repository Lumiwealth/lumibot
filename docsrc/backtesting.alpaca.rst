.. _backtesting.alpaca:

Alpaca Backtesting
==================

.. meta::
   :description: Backtest stock, crypto, and options strategies in LumiBot with historical data from your own Alpaca account, including option chains, option fills, caching, rate limits, and known limits.

``AlpacaBacktesting`` uses the historical data that comes with your own Alpaca account.
It covers stocks, ETFs, crypto, and US equity options. A free Alpaca account includes
option history, so this is the simplest way to backtest options with your own key.

.. include:: _snippets/botspot_mcp_callout.rst

Setup
-----

Set your Alpaca keys in the environment (a paper account is required for backtests):

.. code-block:: bash

    ALPACA_API_KEY=<your-alpaca-key>
    ALPACA_API_SECRET=<your-alpaca-secret>

An OAuth token (``ALPACA_OAUTH_TOKEN``) works as well. API key and secret take precedence,
the same as the live Alpaca broker.

Selecting Alpaca from the environment
-------------------------------------

You do not have to pass a data source or a config. Set ``BACKTESTING_DATA_SOURCE=alpaca`` and
call ``backtest()`` with ``datasource_class=None`` (this is how BotSpot runs Alpaca backtests):

.. code-block:: bash

    BACKTESTING_DATA_SOURCE=alpaca
    ALPACA_API_KEY=<your-alpaca-key>
    ALPACA_API_SECRET=<your-alpaca-secret>
    ALPACA_IS_PAPER=true
    BACKTESTING_START=2026-08-03
    BACKTESTING_END=2026-08-08

.. code-block:: python

    MyStrategy.backtest(datasource_class=None, benchmark_asset="SPY")

Without a config, ``AlpacaBacktesting`` reads the credentials above, uses minute bars (a
strategy that sleeps a day or more still gets daily bars), and runs through
``BACKTESTING_END`` like every other data source. Option chains and option fills work the
same way as below. If the paper Trading API rejects a live-account key, the read-only option
contract list is fetched from the live endpoint instead; backtests never send orders.

History holds finished bars only, the same rule as the IBKR, ThetaData and Polygon backtests.
Alpaca labels a bar with its start time, so at 10:00 the newest 5-minute bar is the 09:55 bar
(it closed at 10:00), the newest 1-minute bar is 09:59, and the newest daily bar is
yesterday's. Orders still fill at the open of the bar that starts at the current time, and
``get_last_price()`` returns that open.

History also reaches before ``BACKTESTING_START``, like IBKR and ThetaData. A strategy that asks
at its first bar for 250 five-minute bars, or for 15 daily bars for an ATR(14) filter, gets
real bars from the sessions before the start. Each reach is one extra request, sized to the
request, saved in the Alpaca cache folder so a rerun downloads nothing, and never filled in. If
a symbol simply has fewer bars (a recent listing, an option that rarely trades), you get the bars
that exist, so check ``len(bars.df)`` before using a long lookback.

Index data (VIX, SPX) in an Alpaca options backtest
---------------------------------------------------

Alpaca has no index history, so an option strategy that reads the VIX or SPX needs those bars
from another provider. Give ``BACKTESTING_DATA_SOURCE`` a JSON map whose ``default`` is
``alpaca`` and name the provider for the other asset types:

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE='{"default":"alpaca","index":"ibkr"}'

- Alpaca stays the backtest engine. Stocks, ETFs, crypto, option chains, option prices and option
  fills all come from Alpaca, exactly as with ``BACKTESTING_DATA_SOURCE=alpaca``.
- Only the asset types the map gives to another provider (``index`` above) are read from that
  provider, at the same simulated time and bar size.
- A map without another provider behaves like plain ``alpaca``.
- An index benchmark (``benchmark_asset=Asset("SPX", asset_type="index")``) that Alpaca cannot
  serve is read from the public daily series; if that is unavailable too, the benchmark line is
  left empty with a warning. The backtest itself still finishes and keeps its results.

.. code-block:: python

   vix = self.get_last_price(Asset("VIX", asset_type="index"))   # from the "index" provider
   chains = self.get_chains(Asset("SPY"))                        # from Alpaca

5, 10, 15 and 30-minute bars from the minute cache
--------------------------------------------------

When a stock backtest asks for 5, 10, 15 or 30-minute bars and its 1-minute bars are already
in the cache, LumiBot builds the bars from the minutes instead of downloading a second series.
It does this only when the answer is identical to Alpaca's own bars: every requested bar is
inside today's regular session (09:30 to 16:00 New York). Measured on recorded data, Alpaca's
regular-session 5, 10, 15 and 30-minute bars equal the rebuilt ones in open, high, low, close
and volume (8,448 of 8,448 bars, including a half day and a stressed week).

.. code-block:: python

    # At 10:15 with minute bars cached: the three finished bars (09:30, 09:45, 10:00), no download.
    bars = self.get_historical_prices("SPY", 3, "15 minutes")

Limits:

- A request that reaches before today's open (for example 20 five-minute bars at 10:00), or
  that is made after the close, downloads Alpaca's series as before, because that series also
  holds pre-market and after-hours bars. Once downloaded, Alpaca's series is always used.
- Stocks and ETFs only. Options, crypto, hourly and daily bars are unchanged.
- It is on by default. To always download Alpaca's series, pass
  ``"intraday_bars_from_minutes": False`` in the data source config or set
  ``BACKTESTING_INTRADAY_BARS_FROM_MINUTES=false``.

Options example
---------------

``get_chains()`` returns the listed contracts as of the simulated date, including contracts
that have since expired. This strategy buys one SPY call each week and sells it on Friday.

.. code-block:: python

    from datetime import date, datetime

    import pytz

    from lumibot.backtesting import AlpacaBacktesting
    from lumibot.entities import Asset
    from lumibot.strategies import Strategy


    class WeeklyCall(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.vars.contract = None

        def on_trading_iteration(self):
            now = self.get_datetime()
            spy = Asset("SPY")
            if self.vars.contract is None and now.weekday() == 0 and (now.hour, now.minute) >= (9, 35):
                chains = self.get_chains(spy)
                calls = chains["Chains"]["CALL"]
                expiry = min(e for e in calls if 7 <= (date.fromisoformat(e) - now.date()).days <= 21)
                price = self.get_last_price(spy)
                strike = min(calls[expiry], key=lambda s: abs(s - price))
                self.vars.contract = Asset(
                    "SPY", asset_type="option", expiration=date.fromisoformat(expiry), strike=strike, right="CALL"
                )
                self.submit_order(self.create_order(self.vars.contract, 1, "buy"))
            elif self.vars.contract is not None and now.weekday() == 4 and (now.hour, now.minute) >= (15, 30):
                position = self.get_position(self.vars.contract)
                if position is not None and position.quantity:
                    self.submit_order(self.create_order(self.vars.contract, position.quantity, "sell"))
                    self.vars.contract = None


    ny = pytz.timezone("America/New_York")
    WeeklyCall.run_backtest(
        AlpacaBacktesting,
        backtesting_start=ny.localize(datetime(2026, 7, 27)),
        backtesting_end=ny.localize(datetime(2026, 8, 19)),
        timestep="minute",
        market="NYSE",
        config={"API_KEY": "<your-alpaca-key>", "API_SECRET": "<your-alpaca-secret>", "PAPER": True},
    )

With an explicit ``config``, ``AlpacaBacktesting`` keeps its original behavior and stops three
trading days before ``backtesting_end``. Pass ``full_window=True`` (or select Alpaca from the
environment as shown above) to run through the end date.

An explicit ``config`` also keeps its data window: history starts at ``backtesting_start`` and a
request for more bars than the window holds raises "Not enough historical data". Pass
``history_before_start=True`` to reach back as in environment mode, or ``warm_up_trading_days``
to download a fixed number of earlier sessions.

History returns finished bars only in both modes (``remove_incomplete_current_bar=True`` is the
default). Passing ``remove_incomplete_current_bar=False`` opts in to the old explicit-config
behavior: history then includes the bar that is still forming at the simulated time, with its
final close, high, low and volume (for example today's daily bar at 09:30). In a backtest that
is a look into the future of up to one bar. With these options an explicit config behaves like
environment mode:

.. code-block:: python

    MyStrategy.backtest(
        AlpacaBacktesting,
        backtesting_start=ny.localize(datetime(2026, 7, 27)),
        backtesting_end=ny.localize(datetime(2026, 8, 19)),
        timestep="minute",
        config={"API_KEY": "<your-alpaca-key>", "API_SECRET": "<your-alpaca-secret>", "PAPER": True},
        remove_incomplete_current_bar=True,
        history_before_start=True,
        full_window=True,
    )

How option data works
---------------------

- **Chains.** Expirations run from the simulated date through 90 days later. ``OptionsHelper``
  can narrow or widen that with its expiration hints. Only standard 100-share contracts are
  listed. Contracts are cached per underlying and expiration month in memory and on disk, so
  each month is fetched from Alpaca once and calling ``get_chains()`` every bar is cheap. A
  2-year backtest makes a few dozen contract-listing calls, not one per day.
- **Index options.** Write ``Asset("SPX", asset_type="option", ...)`` for any expiration.
  Alpaca lists weekly and 0DTE index contracts under their own root (SPXW, NDXP, RUTW, VIXW)
  and the standard monthly under the index (SPX, NDX, RUT, VIX); LumiBot picks the right one
  from the expiration, and the chain for SPX includes both. Naming SPXW yourself also works.
- **Prices.** Option bars are real trades. LumiBot never fills the gaps between trades.
  ``get_last_price()`` returns the open of a bar that traded in the current minute (or day),
  otherwise the close of the most recent earlier trade, and ``None`` before the first trade.
- **Fills.** An option fill price always belongs to the fill bar. If the contract traded in the
  current minute (or day), the fill uses that real trade. If it did not, the fill is a
  Black-Scholes estimate for that bar. When the strategy has already loaded at least five other
  contracts of the same expiration (a delta or price search across strikes does this), the
  estimate uses a volatility smile fitted across their recent trades. Otherwise it is built from
  the contract's most recent real trade (or the nearest strike's when the contract never
  traded). Nothing extra is downloaded for the smile. Estimates are used as computed and marked
  ``price_source=estimated`` in the trades file. Alpaca has no historical option quotes, so
  fills here are either ``trade`` or ``estimated``. If no price can be found at all, the order
  keeps working until one can, or until its time in force ends. See
  :doc:`backtesting.how_orders_fill`.
- **No data.** A contract with no bars at all logs one clear error and its price is ``None``.
- **Speed on long backtests.** The first time a strategy reads an option contract, LumiBot loads
  its trades from the month a week before the simulated date through its expiry, not from the
  backtest start. Earlier trades load only when a lookup needs them (a contract with no trade
  yet, or a longer history request). Results are the same; a two-year
  weekly spread backtest reads far fewer cached files.

Known limits
------------

- Alpaca option history starts around February 2024.
- The contract list is today's list, not a point-in-time chain. Alpaca gives no listing date,
  so a strike listed after the simulated date can appear in that day's chain. Do not treat
  chain membership as proof the contract existed then. A contract with no trade yet has no
  price, so it cannot fill before its first real trade.
- There is no historical bid/ask for options, so fills use trade prices and do not model the
  spread. ``get_greeks()`` still works: greeks are computed from the last trade and the
  underlying price.
- Daily option bars begin with the day's first trade, which can be after the open. Use
  ``timestep="minute"`` when fill timing matters.
- Free keys allow about 200 requests per minute. LumiBot throttles below that and waits on
  HTTP 429 answers before retrying a bounded number of times.
- One reach before the start covers at most about one year of intraday bars or ten years of
  daily bars. Alpaca option history starts around February 2024, so option history before that
  date comes back empty.
- Daily bars count as finished on the next calendar day, so a call after the 16:00 close still
  returns the previous session as the newest daily bar.
- On a free key, stock history comes from SIP (all US exchanges), but the latest 15 minutes
  are not available. End backtests at least one full day before today.
.. note::

   LumiBot's portable release tests exercise the Alpaca adapter, backtest orders,
   fills and cash accounting with SDK-response fixtures. These tests need no API
   subscription. Live historical-data integration tests are marked ``apitest``
   and require your own authorized Alpaca credentials; fixture tests do not
   establish live-provider availability.

Frequently asked questions
--------------------------

Can I backtest options with a free Alpaca account?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. A free Alpaca account includes option history, starting around February 2024. ``AlpacaBacktesting`` uses that history for chains, prices and fills, so you can test options with your own key.

Which Alpaca keys does LumiBot need?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET``. An OAuth token in ``ALPACA_OAUTH_TOKEN`` also works, but the key and secret win if both are set. Backtests only read data and never send orders.

How do I select Alpaca without changing my code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``BACKTESTING_DATA_SOURCE=alpaca`` plus your keys and ``BACKTESTING_START`` and ``BACKTESTING_END``, then call ``backtest()`` with ``datasource_class=None``. This is the same way BotSpot runs Alpaca backtests.

Which assets can I backtest with Alpaca?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stocks, ETFs, crypto and US equity options. Option chains only list standard 100 share contracts.

Why can't my Alpaca backtest end today?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the free plan, Alpaca does not serve the latest 15 minutes of stock history (see `Alpaca's market data plans <https://docs.alpaca.markets/docs/about-market-data-api>`__). End your backtest at least one full day before today.

Why did my Alpaca options backtest stop with "Your Alpaca data plan does not include options history"?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca answered the option history request with HTTP 403, which means the key works but the account's market data plan does not cover options history (see `Alpaca's market data plans <https://docs.alpaca.markets/docs/about-market-data-api>`__). The backtest stops at once with ``AlpacaOptionsDataPlanError`` and Alpaca's own reason; LumiBot does not retry or substitute other prices. Enable options market data on that Alpaca account, or backtest options with another data source. Stock and crypto backtests on the same key are not affected.

What is the Alpaca rate limit for backtests?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca's free plan allows 200 historical API calls per minute (see `Alpaca's market data plans <https://docs.alpaca.markets/docs/about-market-data-api>`__). LumiBot stays below that, and if Alpaca answers with HTTP 429 it waits and retries a limited number of times.

Does Alpaca option data include bid and ask?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. There is no historical option bid and ask, so fills use real trade prices and do not model the spread. ``get_greeks()`` still works, using the last trade and the underlying price.

Why does my backtest stop three trading days early?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

That is the original behavior when you pass an explicit ``config``. Pass ``full_window=True``, or select Alpaca from the environment, to run through ``backtesting_end``.

Can an Alpaca backtest see future prices?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not by default. History returns finished bars only, because ``remove_incomplete_current_bar=True`` is the default. Setting it to ``False`` adds the bar that is still forming, which looks up to one bar into the future.
