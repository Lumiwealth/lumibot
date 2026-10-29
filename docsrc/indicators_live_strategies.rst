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

.. currentmodule:: lumibot.indicators

.. autoclass:: IndicatorExecutor
   :members: prepare, close

.. autofunction:: set_indicator_executor

.. autofunction:: use_indicator_executor
