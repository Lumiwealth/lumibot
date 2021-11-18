Bars
----------------------------

This object contains all pricing data over time, including open, close, high, low, etc prices. You can get the raw pandas DataFrame by using ``bars.df``. The dataframe has the following columns:

* open
* high
* low
* close
* volume
* dividend
* stock_splits

The dataframe index is of type pd. Timestamp localized at the timezone ``America/New_York``.

Bars objects have the following fields:

* source: the source of the data e.g. (yahoo, alpaca, ...)
* symbol: the symbol of the bars
* df: the pandas dataframe containing all the datas

Bars objects has the following helper methods:

* get_last_price(): Returns the closing price of the last dataframe row
* get_last_dividend(): Returns the dividend per share value of the last dataframe row
* get_momentum(start=None, end=None): Calculates the global price momentum of the dataframe.
* aggregate_bars(frequency): Will convert a set of bars to a different timeframe (eg. 1 min to 15 min) frequency (string): The new timeframe that the bars should be in, eg. "15Min", "1H", or "1D". Returns a new Bars object.

When specified, ``start`` and ``end`` will be used to filter the daterange for the momentum calculation. If none of start or end are specified the momentum will be calculated from the first row untill the last row of the dataframe.

* ``get_total_volume(start = None, end = None)``: returns the sum of the volume column. When ``start`` and/or end is/are specified use them to filter for that given daterange before returning the total volume
* ``filter(start = None, end = None)``: Filter the bars dataframe. When ``start`` and/or ``end`` is/are specified use them to filter for that given daterange before returning the total volume

When getting historical data from Interactive Brokers, it is important to note that they do not consider themselves a data supplier. If you exceed these data access pacing rates, your data will be throttled. Additionally, with respect to above three mentioned helpers, when using Interactive Brokers live, tick data is called instead of bar data. This allows for more frequent and accurate pricing updates. ``get_last_dividend`` are not available in Interactive Brokers. (see [Interactive Brokers' pacing rules](https://interactivebrokers.github. io/tws-api/historical_limitations.html))

Documentation
"""""""""""""""""""

.. automodule:: entities.bars
   :members:
   :undoc-members:
   :show-inheritance: