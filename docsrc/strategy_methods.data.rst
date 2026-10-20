Data
===================================

.. meta::
   :description: Data methods are made to get data for you to use in your strategies. You can use them to get historical pricing data (eg. get_symbol_bars()) and more.

Data methods are made to get data for you to use in your strategies. You can use them to get historical pricing data (eg. **get_symbol_bars()**) and more. You can see a list of them below:

Historical bar times
--------------------

Historical bars retain the provider's actual time basis. Epoch seconds or
milliseconds are UTC instants and are converted, rather than relabeled, when
displayed in another timezone. CCXT history uses a UTC-aware index; an explicit
timezone-aware endpoint refers to the same instant in every timezone. Legacy
naive CCXT endpoints denote UTC.

A bar timestamp can mark its opening time or a daily session label. It does
not necessarily mean that the final close was available at that instant.
When comparing account observations with market prices, check the returned
dates and the provider's bar convention. A previous session's last bar does
not measure a requested later session's return.

.. currentmodule:: lumibot.strategies.strategy

.. autosummary::
    :toctree: strategy_methods.data
    :template: strategy_methods_template.rst

        Strategy.get_last_price
        Strategy.get_last_prices
        Strategy.get_historical_prices
        Strategy.get_historical_prices_for_assets
        Strategy.get_quote
        Strategy.get_quotes
        Strategy.get_yesterday_dividend
        Strategy.get_next_trading_day
