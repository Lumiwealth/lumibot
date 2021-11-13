DateTime
===================================

DataTime functions are made so that you can get the current date and time that your strategy thinks it is, regardless of whether you are backtesting or trading live. These can be especially useful when you're backtesting so that you can see what date/time it is according to the backtest (eg. if you're backtesting in the 1990s it can tell you that the strategy thinks it is Jan 10, 1991 rather than today's date). You can see a list of them below:

.. currentmodule:: strategies.strategy.Strategy


.. autosummary::
    :toctree: strategy_methods.datetime
    :template: strategy_methods_template.rst

    get_datetime
    get_timestamp
    get_round_minute
    get_last_minute
    get_round_day
    get_last_day
    get_datetime_range
    localize_datetime
    to_default_timezone