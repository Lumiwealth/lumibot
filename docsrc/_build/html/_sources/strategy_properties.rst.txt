Strategy Properties
************************

Inside your strategy you can also get a lot of information about the state of the strategy and set specific variables to determine how the strategy works. Here is a list of those properties that you can use:

.. currentmodule:: strategies.strategy.Strategy

.. autosummary::
    :toctree: strategy_properties
    :template: strategy_properties_template.rst

    cash
    portfolio_value
    first_iteration
    is_backtesting
    quote_asset
    name
    initial_budget
    minutes_before_closing
    minutes_before_opening
    sleeptime
    last_on_trading_iteration_datetime
    timezone
    pytz
    unspent_money