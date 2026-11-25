Strategy Properties
************************

.. meta::
   :description: Inside your strategy you can also get a lot of information about the state of the strategy and set specific variables to determine how the strategy works.

Inside your strategy you can also get a lot of information about the state of the strategy and set specific variables to determine how the strategy works. Here is a list of those properties that you can use:

.. currentmodule:: lumibot.strategies.strategy

.. autosummary::
    :toctree: strategy_properties
    :template: strategy_properties_template.rst

    Strategy.cash
    Strategy.portfolio_value
    Strategy.first_iteration
    Strategy.is_backtesting
    Strategy.quote_asset
    Strategy.name
    Strategy.initial_budget
    Strategy.minutes_before_closing
    Strategy.minutes_before_opening
    Strategy.sleeptime
    Strategy.last_on_trading_iteration_datetime
    Strategy.timezone
    Strategy.pytz
    Strategy.unspent_money

Live speed settings
-------------------

Set these as class attributes on your strategy.

- ``wake_iteration_on_fill`` (default ``False``): in live trading, run the next trading iteration
  at once after an order fills instead of waiting out ``sleeptime``. Only a fill that arrives
  while no iteration is running wakes it; a fill during a running iteration does not, and the
  next iteration starts after ``sleeptime`` as usual. Leave it off if the strategy places an
  order in every iteration.
- ``wake_iteration_on_partial_fill`` (default ``False``): also wake on partial fills.

Quote and option chain reuse is set with environment variables, see :doc:`environment_variables`.
See :doc:`brokers.schwab` (section "Fewer requests for scanners").
