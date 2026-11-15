.. _backtesting.backtesting_function:

Backtesting Function
---------------------------

.. meta::
   :description: Here is a description of the backtest function and all of its parameters. This is true for any kind of backtesting that you will be doing.

Here is a description of the backtest function and all of its parameters. This is true for any kind of backtesting that you will be doing.

.. _backtest-budget-conversion:

Starting cash and currency conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``budget`` is starting cash in ``quote_asset`` units. Without an explicit budget,
the existing default is 100,000 quote units: a BTC quote wallet means BTC, not
dollars. ``starting_positions`` contains quantities that are already denominated
in their assets; it does not convert a starting cash amount.

To declare a different input denomination, pass an explicit ``budget`` amount and
an optional ``budget_conversion`` record. The rate is **quote-wallet units per
one input-budget unit**. For example, this illustrative caller-supplied assumption
converts USD 10,000 into 0.2 BTC; it is not a fresh or independently verified price:

.. code-block:: python

   MyStrategy.backtest(
       datasource_class=MyDataSource,
       backtesting_start=start,
       backtesting_end=end,
       budget=10000,
       quote_asset=Asset("BTC", "crypto"),
       budget_conversion={
           "budget_currency": "USD",
           "quote_currency": "BTC",
           "quote_per_budget": "0.00002",
           # Optional provenance; use the observation actually available to you.
           "as_of": start.isoformat(),
           "source": "prepared historical conversion assumption",
           "note": "illustrative approximation",
       },
   )

The required conversion fields are ``budget_currency``, ``quote_currency`` and
a positive finite ``quote_per_budget``. The target must match ``quote_asset``.
An amount must be explicitly supplied through ``budget`` or ``BACKTESTING_BUDGET``.
There is no currency allowlist, automatic rate lookup, live ticker fallback or
assumption that USD and USDT have equal value. A supplied multi-currency rate
must already include the needed conversion legs.

``as_of``, ``source`` and ``note`` are optional. Missing values remain explicitly
unspecified. Approximate or unparseable dates remain usable. Known later dates
are flagged, so they cannot be mistaken for an exact simulation-start observation;
they are not a runtime refusal. The caller chooses and owns that approximation.
No provenance label independently verifies the supplied rate.

``BACKTESTING_BUDGET`` keeps its existing priority over the Python amount.
``BACKTESTING_BUDGET_CONVERSION`` accepts the same record as JSON and replaces the
Python record as a whole, rather than merging it. Without a conversion record,
all existing numeric-budget units and defaults are unchanged. This only initializes
backtest cash; live broker balances and wallets are never converted.

``strategy.budget_conversion`` and the optional ``budget_conversion`` field in
``settings.json`` retain the input amount, source and target currencies, supplied
rate and metadata, resulting quote cash, conversion-record argument/environment
origin and date relation to the simulation start. ``initial_budget`` and settings
``budget`` remain the resulting starting cash in quote units. Legacy runs add no
conversion field.

.. autofunction:: lumibot.strategies.strategy.Strategy.run_backtest
