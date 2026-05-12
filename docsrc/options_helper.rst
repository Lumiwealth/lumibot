OptionsHelper
=============

``OptionsHelper`` is LumiBot's high-level helper for **options selection** (expirations, strikes, deltas) and **multi-leg order building/execution**.
For most options strategies, using ``OptionsHelper`` is both **more reliable** (avoids non-existent expiries/strikes during backtests) and **much faster** than brute-force approaches that scan large strike lists and call ``get_greeks()`` per strike.

Why use OptionsHelper?
----------------------

Options backtests are easy to accidentally make **slow** or **brittle**. ``OptionsHelper`` exists to push strategies toward the safe, high-performance path:

- **Reliability in backtests:** not every expiry/strike returned by a chain lookup is actually tradeable for the full backtest window. ``OptionsHelper`` methods validate that an option has usable market data.
- **Performance:** brute-force patterns (scan many strikes and call ``get_greeks()`` or quote history per strike) can trigger a large number of data requests and slow backtests dramatically.
- **Cleaner strategy code:** most common spreads can be built and executed with a few helper calls, without hand-assembling each leg.

If you are new to option data in LumiBot, also see:

- :doc:`strategy_methods.options` (low-level option methods on ``Strategy``)
- :doc:`backtesting.performance` (how to profile and reduce request fanout)
- :doc:`common_mistakes` (common options pitfalls)

Core conventions (important)
----------------------------

Rights and delta sign
~~~~~~~~~~~~~~~~~~~~~

``OptionsHelper`` uses the standard conventions:

- ``right`` is typically ``"call"`` or ``"put"`` (strings).
- ``target_delta`` is **positive** for calls and **negative** for puts. Example: 20Δ call = ``+0.20``; 20Δ put = ``-0.20``.

Always pass ``underlying_price`` (float)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many selection helpers require ``underlying_price``. Always pass it explicitly as a ``float``:

- Prefer ``underlying_price=float(self.get_last_price(underlying_asset))`` (after checking for ``None``).
- Call ``OptionsHelper.find_strike_for_delta(...)`` with **keyword arguments**. This prevents subtle argument-order mistakes.

Call ``get_chains`` once, then reuse it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``self.get_chains(...)`` can be expensive depending on data source. When you need chains (for example, to pick an expiry), fetch them once per iteration and reuse the result.

Performance & reliability tips
------------------------------

Avoid brute-force delta hunting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Do **not** do this:

- build a large strike list,
- loop over strikes, and
- call ``self.get_greeks()`` repeatedly to “hunt” a 20Δ strike.

That pattern is a common cause of slow runs because each greek/mark computation can trigger additional data work.

Instead, use:

- ``OptionsHelper.find_strike_for_delta(...)`` (bounded probing + caching)
- strategy-level caching: if you retry entry intraday, cache the selected expiry/strike(s) in ``self.vars`` and only recompute when the underlying moved materially.

Validate market data before trading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For live trading (and for realistic backtests), it is common to encounter:

- missing bid/ask,
- very wide spreads,
- stale last trade prints.

Use ``OptionsHelper.evaluate_option_market(...)`` and gate trading on ``OptionsHelper.has_actionable_price(...)``.

Quickstart (delta-based strike)
--------------------------------

.. code-block:: python

   from datetime import timedelta
   from lumibot.entities import Asset
   from lumibot.components.options_helper import OptionsHelper

   def initialize(self):
       self.options_helper = OptionsHelper(self)

	   def pick_20_delta_put(self):
	       underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
	       chains = self.get_chains(underlying)
	       if not chains:
           self.log_message("No option chains available", color="yellow")
           return None

       expiry = self.options_helper.get_expiration_on_or_after_date(
           self.get_datetime() + timedelta(days=0),
           chains,
           "put",
           underlying_asset=underlying,
       )
       if expiry is None:
           self.log_message("No valid expiry found", color="yellow")
           return None

       underlying_price = self.get_last_price(underlying)
       if underlying_price is None:
           self.log_message("Underlying price unavailable", color="yellow")
           return None

	       strike = self.options_helper.find_strike_for_delta(
	           underlying_asset=underlying,
	           underlying_price=float(underlying_price),
	           target_delta=-0.20,
	           expiry=expiry,
	           right="put",
	       )
	       return strike

Common workflows
----------------

1) Find a tradeable expiry near a target date
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``OptionsHelper.get_expiration_on_or_after_date(...)`` chooses an expiry on/after a target date and can validate that the expiry has tradeable data (when given ``underlying_asset``).

.. code-block:: python

   from datetime import timedelta
   from lumibot.entities import Asset

   underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
   chains = self.get_chains(underlying)
   if not chains:
       return

   target_dt = self.get_datetime() + timedelta(days=7)
   expiry = self.options_helper.get_expiration_on_or_after_date(
       target_dt,
       chains,
       "call",
       underlying_asset=underlying,
   )

2) Pick a strike by delta (fast path)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``OptionsHelper.find_strike_for_delta(...)`` instead of scanning strikes. Always pass keyword args.

.. code-block:: python

   underlying_price = self.get_last_price(underlying)
   if underlying_price is None:
       return

   put_strike = self.options_helper.find_strike_for_delta(
       underlying_asset=underlying,
       underlying_price=float(underlying_price),
       target_delta=-0.20,
       expiry=expiry,
       right="put",
   )

3) Validate liquidity / data quality before placing orders
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``evaluate_option_market`` returns an ``OptionMarketEvaluation`` with prices and flags. Only trade when prices are actionable.

.. code-block:: python

   option = Asset(
       underlying.symbol,
       asset_type=Asset.AssetType.OPTION,
       expiration=expiry,
       strike=put_strike,
       right="put",
       underlying_asset=underlying,
   )
   evaluation = self.options_helper.evaluate_option_market(option, max_spread_pct=0.25)
   self.log_message(f"Option evaluation: {evaluation}", color="blue")

   if not self.options_helper.has_actionable_price(evaluation):
       self.log_message(f"Skipping trade: {evaluation.data_quality_flags}", color="yellow")
       return

4) Build multi-leg orders (spreads/condors) without hand-coding legs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most common spreads have both:

- a ``build_*`` helper that returns orders (so you can configure order type, risk logic, etc.), and
- an ``execute_*`` helper that builds + submits for you.

Example: put credit spread (sell higher strike, buy lower strike).

.. code-block:: python

   orders = self.options_helper.build_put_vertical_spread_orders(
       underlying_asset=underlying,
       expiry=expiry,
       upper_strike=put_strike,           # short
       lower_strike=put_strike - 5.0,     # long wing
       quantity=1,
   )
   if not orders:
       return

   limit_price = self.options_helper.calculate_multileg_limit_price(orders, limit_type="mid")
   self.log_message(f"Mid price estimate: {limit_price}", color="blue")

   # Submit however your strategy prefers (market/limit/smart limit, etc.)
   self.submit_order(orders)

Examples (copy/paste patterns)
------------------------------

Sell a 20Δ put once per day (with caching)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This pattern avoids recomputing strike selection every iteration when nothing material changed.

.. code-block:: python

   def initialize(self):
       from lumibot.components.options_helper import OptionsHelper
       self.options_helper = OptionsHelper(self)
       self.vars.cached_put_strike = None
       self.vars.cached_put_expiry = None
       self.vars.cached_put_day = None
       self.vars.cached_spot = None

   def _get_cached_20d_put_strike(self, underlying):
       from datetime import timedelta

       now = self.get_datetime()
       spot = self.get_last_price(underlying)
       if spot is None:
           return None, None

       today = now.date()
       if (
           self.vars.cached_put_day == today
           and self.vars.cached_put_strike is not None
           and self.vars.cached_put_expiry is not None
           and self.vars.cached_spot is not None
           and abs(float(spot) - float(self.vars.cached_spot)) / float(self.vars.cached_spot) < 0.005  # 0.5%
       ):
           return self.vars.cached_put_expiry, float(self.vars.cached_put_strike)

       chains = self.get_chains(underlying)
       if not chains:
           return None, None

       expiry = self.options_helper.get_expiration_on_or_after_date(
           now + timedelta(days=0),
           chains,
           "put",
           underlying_asset=underlying,
       )
       if expiry is None:
           return None, None

       strike = self.options_helper.find_strike_for_delta(
           underlying_asset=underlying,
           underlying_price=float(spot),
           target_delta=-0.20,
           expiry=expiry,
           right="put",
       )
       if strike is None:
           return None, None

       self.vars.cached_put_day = today
       self.vars.cached_put_expiry = expiry
       self.vars.cached_put_strike = float(strike)
       self.vars.cached_spot = float(spot)
       return expiry, float(strike)

Iron condor: pick short legs by delta, validate wings exist
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For condors, a common pattern is:

1) select short call + short put by delta,
2) choose wing strikes at a fixed distance, and
3) ensure wing strikes are real strikes for the chosen expiry (for example, by checking chains strikes).

If you are building a condor strategy, prefer:

- ``find_strike_for_delta`` for the short legs, and
- ``build_*_vertical_spread_orders`` to build each spread leg.

Troubleshooting
---------------

``get_chains(...)`` returns ``None`` / empty
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Not all assets have options, and data availability depends on your data source.
- Always guard against ``None`` and log what asset/when you queried.

``get_greeks(...)`` returns ``None``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This often means the option does not have usable price/quote inputs at that time (illiquid, missing quote history, etc.).

- Always handle ``None`` and continue strategy execution.
- Prefer ``evaluate_option_market`` to understand what price inputs are missing and why.

``find_strike_for_delta(...)`` returns ``None``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Common causes:

- no valid expiry was found (start by validating expiry selection),
- the chain does not include the expected strike neighborhood,
- missing option marks for candidate strikes (data gap / illiquid contracts).

In these cases, log and skip for the iteration/day rather than crashing.

API Reference
-------------

.. currentmodule:: lumibot.components.options_helper

.. autoclass:: OptionMarketEvaluation
   :members:

.. autoclass:: OptionsHelper
   :members:
   :member-order: bysource
