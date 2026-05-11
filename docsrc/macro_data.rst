FRED Macro Data
===============

LumiBot includes native Federal Reserve Economic Data (FRED) macro tools for
strategies and AI agents. Use these tools for interest rates, inflation,
employment, growth, liquidity, credit spreads, and market-risk context.

.. image:: ../docs/assets/ai_committee/docs_fred_macro_data.png
   :alt: FRED macro data tools and point-in-time behavior in Lumibot

Strategy API
------------

.. code-block:: python

   self.macro.list_series()
   self.macro.get_series("DGS10")
   self.macro.get_latest("UNRATE")
   self.macro.get_snapshot(["FEDFUNDS", "DGS10", "CPIAUCSL", "UNRATE"])

Agent Tools
-----------

Agents receive these built-ins automatically:

- ``list_fred_series``
- ``get_fred_series``
- ``get_fred_latest``
- ``get_fred_snapshot``

These tools are available to read-only research agents and trading-enabled
portfolio agents. They do not submit, cancel, or modify orders.

API Key Behavior
----------------

``FRED_API_KEY`` is required for the official FRED/ALFRED API path and for
strict point-in-time macro backtests. With a key, LumiBot passes
``realtime_start`` and ``realtime_end`` based on the strategy datetime so the
backtest sees the vintage observations that were available at that time.

Without ``FRED_API_KEY``, LumiBot can fetch an allowlisted set of **live**
public FRED graph CSV endpoints. The data is fetched live from FRED rather than
bundled with LumiBot. It is still revised data, so use it only for quick
exploration or live context, not for strict point-in-time backtests.

Built-in FRED agent tools are hidden during backtests unless ``FRED_API_KEY`` is
configured. This prevents agents from accidentally using revised public CSV data
in historical simulations.

Backtest Date Safety
--------------------

In a backtest, ``as_of`` defaults to ``self.get_datetime()``.

LumiBot always filters observations to ``observation_date <= as_of``. With
``FRED_API_KEY``, it also requests the vintage data known as of that date.

Without a key, CSV mode is date-gated but not revision-safe. Tool results mark
this explicitly with ``point_in_time_safe=False`` and ``uses_revised_data=True``.

Cache
-----

FRED data is cached under ``~/.lumibot/cache/fred`` by default. Override this
with ``LUMIBOT_FRED_CACHE_DIR``.
