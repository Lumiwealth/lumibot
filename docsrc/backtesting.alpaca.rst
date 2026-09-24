.. _backtesting.alpaca:

Alpaca Backtesting
==================

.. meta::
   :description: Backtest stock, crypto, and options strategies in LumiBot with historical data from your own Alpaca account, including option chains, option fills, caching, rate limits, and known limits.

``AlpacaBacktesting`` uses the historical data that comes with your own Alpaca account.
It covers stocks, ETFs, crypto, and US equity options. A free Alpaca account includes
option history, so this is the simplest way to backtest options with your own key.

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
  listed. Chains are cached per underlying and simulated date in memory and on disk, and one
  contract listing is reused for later days, so calling ``get_chains()`` every bar is cheap.
- **Prices.** Option bars are real trades. LumiBot never fills the gaps between trades.
  ``get_last_price()`` returns the open of a bar that traded in the current minute (or day),
  otherwise the close of the most recent earlier trade, and ``None`` before the first trade.
- **Fills.** An option order fills only on a bar that traded in the current minute (or day).
  If the contract did not trade, the order keeps working until the next real trade, or until
  its time in force ends.
- **No data.** A contract with no bars at all logs one clear error and its price is ``None``.

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
