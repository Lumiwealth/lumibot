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
