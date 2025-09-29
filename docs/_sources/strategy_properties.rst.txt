Strategy Properties
************************

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
