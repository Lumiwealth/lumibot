The ``lumibot`` Command
=======================

The ``lumibot`` command exists for one reason: to get you from
``pip install lumibot`` to a result on screen without first designing a
strategy. It is optional. It is not a wrapper, a framework, or a new API. It
writes an ordinary :doc:`Strategy <strategy_api_overview>` subclass to disk and runs it,
and everything else in this documentation applies to that file unchanged.

See a result before you write anything
--------------------------------------

.. code-block:: bash

   pip install lumibot
   lumibot demo

``demo`` backtests a moving-average strategy on free daily data. No API key and
no broker account are involved. It prints the return and the path to the
tearsheet it wrote, which you open in a browser.

Start your own project
----------------------

.. code-block:: bash

   lumibot init my-bot
   lumibot backtest my-bot --days 90
   lumibot run my-bot --paper

``init`` writes two files and nothing else:

.. code-block:: text

   my-bot/
     strategy.py   the strategy you edit
     README.md     the three commands above

For an AI strategy instead of a Python one:

.. code-block:: bash

   lumibot init my-bot --template ai

That template creates two agents, a researcher that may not trade and a trader
that may, and is the same shape as
:doc:`ai_researcher_trader <agents_quickstart>`. It needs ``OPENAI_API_KEY``.

What is in ``strategy.py``
--------------------------

This is the part people get stuck on, so it is worth being exact. The generated
file has two halves.

**The class.** An ordinary ``Strategy`` subclass with ``parameters``,
``initialize`` and ``on_trading_iteration``. This is the part you edit. Nothing
is generated around it and nothing is hidden from you.

**The runner block.** At the bottom:

.. code-block:: python

   if __name__ == "__main__":
       from lumibot.credentials import IS_BACKTESTING

       if IS_BACKTESTING:
           ...
           MyBot.backtest(YahooDataBacktesting, end - timedelta(days=365), end, budget=100000)
       else:
           from lumibot.traders import Trader

           trader = Trader()
           trader.add_strategy(MyBot())
           trader.run_all()

That block is why the file works three ways:

.. list-table::
   :header-rows: 1
   :widths: 38 62

   * - How you run it
     - What happens
   * - ``lumibot backtest my-bot --days 90``
     - The CLI imports the class and supplies the dates and budget. The block
       does not fire.
   * - ``lumibot run my-bot --paper``
     - The CLI imports the class and connects the paper broker. The block does
       not fire.
   * - ``python strategy.py``
     - The block fires. ``IS_BACKTESTING`` decides between a one-year backtest
       and live trading through ``Trader``.

This is the same layout a BotSpot strategy workspace uses in its ``main.py``, so
a file created here can be moved into BotSpot, or the other way, without being
rewritten.

If you delete the runner block, the CLI still works and ``python strategy.py``
does nothing at all. That silence is the single most common way a first LumiBot
project appears broken when it is not.

Growing out of the CLI
----------------------

There is nothing to migrate away from. When you need a second strategy, a
different data source, several symbols, or your own ``Trader`` setup, edit the
same file or call ``backtest()`` yourself as
:doc:`How to Backtest <backtesting.how_to_backtest>` describes. The CLI never
owned anything.

Commands
--------

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Command
     - What it does
   * - ``lumibot demo``
     - Backtest a built-in strategy on free data. No keys, no files written to
       your project.
   * - ``lumibot init <project>``
     - Write ``strategy.py`` and ``README.md``. ``--template ai`` writes the
       agent version. ``--force`` overwrites.
   * - ``lumibot backtest <project>``
     - Backtest the project. ``--days`` sets the window, ``--budget`` the
       starting cash.
   * - ``lumibot run <project>``
     - Trade the project. ``--paper`` or ``--live`` is required; ``--live`` also
       requires ``--yes``.
   * - ``lumibot --version``
     - Print the installed version.

``python -m lumibot`` works everywhere ``lumibot`` does, which is useful when the
console script is not on your ``PATH``.
