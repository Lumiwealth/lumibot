On the price chart or in its own pane
=====================================

.. meta::
   :description: Choose whether a LumiBot indicator draws on top of the price chart (overlay) or in its own pane underneath.

One setting decides where an indicator draws:

``overlay = True``
    On top of the price candles. Use it when the values are prices:
    moving averages, Bollinger Bands, VWAP, a Donchian channel.

``overlay = False``
    In its own pane under the price chart. Use it when the values are on a
    different scale: RSI (0 to 100), MACD, a funding rate in percent, a
    probability.

::

    class DonchianMidline(Indicator):
        overlay = True          # prices: same pane as the candles

    class UpDownRatio(Indicator):
        overlay = False         # 0 to 100: its own pane
        levels = [Level(70, "High"), Level(30, "Low")]

A simple test: if a value of the indicator could be confused with a price of
the asset, it belongs on the price chart. Otherwise give it its own pane.

``describe()`` includes ``"overlay"``, so any chart that reads it places the
indicator correctly. LumiBot's own backtest charts are separate; see
:doc:`indicators_backtesting`.
