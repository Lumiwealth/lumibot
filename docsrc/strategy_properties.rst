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

- ``quote_basket_max_age_seconds`` (default ``1.0``): in live trading, quotes for the assets a
  strategy asked about in its previous iteration are fetched in one request and reused inside
  the same iteration for at most this many seconds. ``0`` asks for every quote separately.
- ``chain_cache_seconds`` (default ``900``): in live trading, how long an option chain
  (expirations and strikes) from ``get_chains`` is reused. ``0`` fetches it every call.
- ``wake_iteration_on_fill`` (default ``True``): in live trading, run the next trading iteration
  at once after an order fills instead of waiting out ``sleeptime``.

See :doc:`brokers.schwab` (section "Fewer requests for scanners").
