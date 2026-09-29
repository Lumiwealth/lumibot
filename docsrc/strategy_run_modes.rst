Backtest, paper, or live: choose the runner
===========================================

.. meta::
   :description: Understand how a LumiBot strategy class runs through a historical backtest or a paper/live broker, and check the direct-run mode of every AI example.

**The strategy class can stay the same. The code that starts it must select a
historical backtest or a broker run.** Calling ``MyStrategy.backtest(...)``
always starts a backtest. Constructing a strategy with a broker and calling
``run_live()`` or ``Trader.run_all()`` starts broker execution. A flag does not
rewrite one call into the other.

.. list-table:: What selects each path
   :header-rows: 1
   :widths: 25 40 35

   * - Path
     - Runner call
     - Account or data source
   * - Historical backtest
     - ``MyStrategy.backtest(...)``
     - Historical data source and dates; no trading broker is started.
   * - Paper broker
     - ``MyStrategy(broker=broker).run_live()`` or ``Trader.run_all()``
     - Broker credentials and an explicitly selected paper account.
   * - Live broker
     - The same broker runner call
     - Broker credentials and an explicitly selected live account.

``IS_BACKTESTING`` is a useful convention **only when the runner reads it and
branches on it**. Some examples assign a local Boolean in their ``__main__``
block, while the ``lumibot init`` template imports the environment value from
``lumibot.credentials``. Exporting ``IS_BACKTESTING=false`` cannot make a file
that only calls ``backtest()`` trade through a broker. Similarly,
``ALPACA_IS_PAPER`` selects an Alpaca account for a broker runner; it does not
select between backtesting and broker execution.

When using ``lumibot init``, the explicit commands are simplest:

.. code-block:: bash

   lumibot backtest my-bot --days 90
   lumibot run my-bot --paper

The CLI imports the strategy class, so a file's ``if __name__ == "__main__"``
block does not run. When you execute an example with ``python file.py`` or
``python -m package.module``, that block determines what happens. Read its
run-mode label before executing it. A historical example is not evidence that
its broker path has been qualified for your broker and market.

AI example entry points
-----------------------

The following list covers the AI strategy source files in
``lumibot/example_strategies``. These labels describe **direct file
execution**, not the capability of the importable strategy class.

**Backtest only:** ``agent_alpaca_news_builtin.py``,
``agent_discretionary.py``, ``agent_m2_liquidity.py``,
``agent_m2_liquidity_anthropic.py``, ``agent_m2_liquidity_grok.py``,
``agent_m2_liquidity_openai.py``, ``agent_macro_risk.py``,
``agent_momentum_allocator.py``, ``agent_news_sentiment.py``,
``ai_browser_research_showcase.py``, ``ai_credit_spread.py``,
``ai_iron_condor.py``, ``ai_opening_range_breakout.py``,
``ai_researcher_trader.py``, ``ai_spx_zero_dte_bear_call_team.py``, and
``ai_vwap.py``.

**Backtest and broker:** ``ai_trading_team_bill_ackman_concentrated.py``,
``ai_trading_team_bull_bear_large_cap_stocks.py``,
``ai_trading_team_bull_bear_leveraged_etf.py``,
``ai_trading_team_citadel_sector_pods.py``,
``ai_trading_team_citadel_sector_pods_leveraged.py``,
``ai_trading_team_ray_dalio_idea_meritocracy.py``,
``ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py``, and
``ai_trading_team_warren_buffett_value.py``. Four team files import
``IS_BACKTESTING`` from ``lumibot.credentials``: the two Citadel files and the
two Ray Dalio files. They read ``IS_BACKTESTING`` from the environment, so set
``IS_BACKTESTING=true`` to run their historical branch. Four other team files
assign a local ``IS_BACKTESTING`` Boolean in ``__main__``: the Bill Ackman,
bull/bear large-cap, bull/bear leveraged ETF, and Warren Buffett files. Edit
that assignment to choose their historical branch. All eight default to the
broker branch, and their Alpaca configuration defaults to paper unless
explicitly changed.

**No direct runner:** ``ai_congress_disclosures.py``,
``ai_public_web_fetch.py``, and ``ai_sec_insider_filings.py``. Import their
strategy classes into a separate runner. ``agent_cycle.py`` is a helper, and
``ai_trading_team.py`` is an import alias; neither is a standalone example
runner. The saved ``docs/assets/ai-trading/spy-20260913/strategy.py`` is a
historical proof artifact, not the current quickstart source.

For a complete backtest-to-broker walkthrough, see :doc:`getting_started`.
For the specific opening-range example, see
:doc:`agents_example_ai_opening_range_breakout`.
