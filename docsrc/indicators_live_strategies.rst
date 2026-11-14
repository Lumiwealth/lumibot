Using an indicator in a live strategy
=====================================

.. meta::
   :description: Run the same LumiBot indicator code in a live or paper trading strategy on your own broker, and control how indicators run with the execution hook.

The strategy on :doc:`indicators_backtesting` runs live without changing a
line. Only how you start it changes::

    from lumibot.brokers import Alpaca
    from lumibot.traders import Trader
    from lumibot.example_indicators.indicator_strategy import CustomIndicatorStrategy

    broker = Alpaca({"API_KEY": "<your-key>", "API_SECRET": "<your-secret>", "PAPER": True})
    strategy = CustomIndicatorStrategy(broker=broker, parameters={"symbol": "SPY", "length": 20})

    trader = Trader()
    trader.add_strategy(strategy)
    trader.run_all()

Any broker LumiBot supports works (see :doc:`brokers`). Start with a paper
account.

What happens live
-----------------

- ``self.indicators.get(...)`` reads the bars the data source has at that
  moment and updates the indicator with any new closed bars.
- Values are for closed bars, so a signal does not flicker while a bar is
  still forming. For live values of a forming bar, use the streaming pattern
  on :doc:`indicators_live_crypto_stream`.

Running indicators outside a strategy
-------------------------------------

:py:class:`~lumibot.indicators.IndicatorRunner` runs indicators with any
LumiBot data source, for scripts, notebooks or a chart server::

    from lumibot.indicators import IndicatorRunner

    runner = IndicatorRunner(data_source, asset, timestep="minute")
    rsi = runner.add("rsi", length=14)
    snapshot = runner.load_history(500)            # history, all closed
    values = runner.on_bar(bar, closed=False)      # live tick
    values = runner.on_bar(bar, closed=True)       # bar closed

How indicators run (the execution hook)
---------------------------------------

By default an indicator runs in the same Python process as your strategy.
You never need to change that. A program that runs indicators written by
other people (for example a chart server) may want them to run somewhere
else, such as a separate process. It registers an
:py:class:`~lumibot.indicators.IndicatorExecutor`::

    from lumibot.indicators import IndicatorExecutor, use_indicator_executor

    class MyExecutor(IndicatorExecutor):
        name = "my-executor"

        def prepare(self, indicator):
            # Return an object with the indicator's update/reset/describe
            # methods. Returning the indicator itself keeps it in-process.
            return indicator

    with use_indicator_executor(MyExecutor()):
        runner.add("rsi", length=14)

``IndicatorRunner`` and ``self.indicators.get`` hand every new indicator to
the active executor. ``IndicatorRunner(..., executor=...)`` sets one for a
single runner. Indicator authors change nothing. LumiBot's tests run
indicators in a child process this way and check the numbers match an
in-process run exactly.

Whole-history computation in backtests (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Feeding bars one at a time is the right shape for live trading, but in a
backtest every bar is already stored. If the object ``prepare`` returns also
has a ``compute_history`` method, ``self.indicators.get`` uses it in
backtests::

    def compute_history(self, bars_df):
        # bars_df: the full stored bar series for this stream
        # (open, high, low, close, volume; DatetimeIndex)
        return {"rsi": [None, None, ..., 55.1]}  # one value per row, per output

The rules:

- One value per row for every output, in row order. Row ``i`` may use only
  rows ``0`` to ``i`` (no looking ahead). LumiBot raises a ``ValueError`` if a
  sequence has the wrong length.
- LumiBot calls it once per stream (asset, timestep, indicator, inputs) and
  caches the result. ``get`` at bar N returns row N, the same value the
  per-bar path would give. It calls again only when the stored series changes
  (more bars loaded, or the last bar corrected).
- Live trading ignores it and keeps calling ``update`` as bars arrive.
- Objects without the method work exactly as before.

.. currentmodule:: lumibot.indicators

.. autoclass:: IndicatorExecutor
   :members: prepare, close

.. autofunction:: set_indicator_executor

.. autofunction:: use_indicator_executor
