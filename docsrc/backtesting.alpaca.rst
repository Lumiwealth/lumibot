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

``AlpacaBacktesting`` stops three trading days before ``backtesting_end``. Set the end date
three sessions after the last day you want to trade.

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
- The contract list has no "as of" date. A strike listed after the simulated date can appear
  in that day's chain. A contract with no trade yet has no price.
- There is no historical bid/ask for options, so fills use trade prices and do not model the
  spread. ``get_greeks()`` still works: greeks are computed from the last trade and the
  underlying price.
- Daily option bars begin with the day's first trade, which can be after the open. Use
  ``timestep="minute"`` when fill timing matters.
- Free keys allow about 200 requests per minute. LumiBot throttles below that and waits on
  HTTP 429 answers before retrying a bounded number of times.
- On a free key, stock history comes from SIP (all US exchanges), but the latest 15 minutes
  are not available. End backtests at least one full day before today.
