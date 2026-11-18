Options
===================================

.. meta::
   :description: Options methods are meant for managing options, including getting option chains, greeks and more. You can see a list of them below:.

Options methods are meant for managing options, including getting option chains, greeks and more. You can see a list of them below:

For high-level option selection (expirations/strikes/deltas) and multi-leg spread helpers, see :doc:`options_helper`.

.. currentmodule:: lumibot.strategies.strategy


.. autosummary::
    :toctree: strategy_methods.options
    :template: strategy_methods_template.rst

        Strategy.get_chain
        Strategy.get_chains
        Strategy.get_greeks
        Strategy.get_strikes
        Strategy.get_expiration
        Strategy.get_multiplier
        Strategy.options_expiry_to_datetime_date
        Strategy.get_next_trading_day

Index weekly options (SPX and SPXW)
-----------------------------------

Index options trade under two roots: the standard monthly contract uses the index symbol
(``SPX``, ``NDX``, ``RUT``, ``VIX``) and every other expiration (weeklies, 0DTE, end of month)
uses the weekly root (``SPXW``, ``NDXP``, ``RUTW``, ``VIXW``). Write
``Asset("SPX", asset_type="option", expiration=..., strike=..., right=...)`` for both. LumiBot
picks the right root from the expiration, and positions reported under the weekly root show up
as ``SPX``. ``XSP`` has one root and is unchanged.

Covered: Alpaca (backtesting, live broker, live data), Tradier (broker and data, including
chains), Schwab (broker orders, positions and option quotes). Polygon looks contracts up by
underlying, so it needs no mapping. Interactive Brokers uses its own trading class rule and is
not changed by this mapping.
