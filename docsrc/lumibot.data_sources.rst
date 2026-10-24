:orphan:

Data Sources
=============================

.. meta::
   :description: Data Sources. Data Source LumiBot documentation in the LumiBot Python trading framework.

Data Source
-----------------------------------------

.. automodule:: lumibot.data_sources.data_source
   :members:
   :undoc-members:
   :show-inheritance:


Pandas
-----------------------------------

.. automodule:: lumibot.data_sources.pandas_data
   :members:
   :undoc-members:
   :show-inheritance:

Yahoo
----------------------------------

.. automodule:: lumibot.data_sources.yahoo_data
   :members:
   :undoc-members:
   :show-inheritance:

Module contents
---------------

.. automodule:: lumibot.data_sources
   :members:
   :undoc-members:
   :show-inheritance:

Frequently asked questions
--------------------------

What does get_historical_prices return in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It returns a ``Bars`` object with the last ``length`` bars for the asset at the
timestep you ask for. For strategy code, use ``bars.pandas_df`` to get a pandas
DataFrame. ``bars.df`` also works and returns the active DataFrame.

What columns are in bars.df?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The main columns are ``open``, ``high``, ``low``, ``close``, and ``volume``,
with a time zone aware datetime index. Some sources also include ``dividend``
and ``stock_splits``. When the data allows it, LumiBot adds calculated columns
such as ``price_change``, ``dividend_yield``, and ``return``.

Which timestep strings can I use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The standard names are ``"day"`` and ``"minute"``. LumiBot also accepts common
broker style aliases and converts them first, so ``"1Day"``, ``"1day"``,
``"1D"``, and ``"1d"`` all mean ``"day"``, and ``"1Min"`` means ``"minute"``.

Each data source still has its own list of supported timesteps. If a source
does not support the one you ask for, LumiBot raises an ``UnavailabeTimestep``
error.

Which timesteps does Yahoo data support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``YahooData`` supports ``"day"`` (or ``"1d"``), ``"15 minutes"`` (or ``"15m"``),
and ``"minute"`` (or ``"1m"``). Its default timestep is ``"day"``. See
:ref:`backtesting.yahoo` for how to run a Yahoo backtest.

What does auto_adjust do in YahooData?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With ``auto_adjust=True``, LumiBot replaces the open, high, low, and close
prices with Yahoo's adjusted prices (the ``Adj Close`` style columns). With
``auto_adjust=False``, it keeps the regular prices. ``YahooData`` defaults to
``False``. ``PandasData`` has its own ``auto_adjust`` option, which defaults to
``True``.

How do I use my own CSV or DataFrame data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Load your data into a pandas DataFrame, wrap it in a ``Data`` object, and put
it in a dictionary keyed by asset. Pass that dictionary as ``pandas_data`` to
the backtest so ``PandasData`` can serve it. See :ref:`backtesting.pandas` for a
full example.

What other data sources are in lumibot.data_sources?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The package also exports ``AlpacaData``, ``AlphaVantageData``, ``BitunixData``,
``CcxtData``, ``DataBentoData``, ``InteractiveBrokersData``,
``InteractiveBrokersRESTData``, ``PolygonDataBacktesting``,
``PolymarketData``, ``ProjectXData``, ``SchwabData``, ``TradierData``, and
``TradovateData``, among others. Each one is loaded only when you use it. See
:doc:`imports_and_startup`.