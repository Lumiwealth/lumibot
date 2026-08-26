AI-Only VWAP
============

.. image:: ../docs/assets/ai-agent-workflows/ai-vwap.webp
   :alt: AI VWAP workflow using LumiBot runtime skills, rules, market evidence, and execution
   :width: 100%

``ai_vwap.py`` is a minimal AI-only equity strategy. Python creates one trading
agent and runs it each iteration. The prompt owns the VWAP policy while the
built-in ``stock-trading`` skill supplies reusable research, sizing, order, and
verification mechanics. Active rules limit it to one position and one entry per
day.

How it works
------------

* The agent computes VWAP only from bars visible at the simulated time.
* It evaluates the configured dip or reclaim threshold and sizes from current risk.
* It manages an existing position before considering another entry.
* It reconciles the exact submitted order, open orders, and fresh positions.
  In backtests, a short bounded terminal wait lets the simulator process the
  agent's own market order without creating an open-ended polling loop.

Verified backtest evidence
--------------------------

The final refactored strategy completed a bounded local backtest from 2026-08-04
through 2026-08-07 with hourly decisions over minute evidence. It bought 12 SPY
shares at $771.23 and sold those same 12 shares at $769.37. There were no
duplicate exit submissions and no residual position. The portfolio ended near
$99,978 from a $100,000 start. The tear sheet rounded total return to -0.00%,
annualized return to -2.02%, and maximum drawdown to -0.02%. This short result
is mechanical evidence, not a performance claim.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export DATADOWNLOADER_BASE_URL="https://data.example.test"
   export DATADOWNLOADER_API_KEY="your-data-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   python -m lumibot.example_strategies.ai_vwap

Set ``BACKTESTING_START``, ``BACKTESTING_END``, and optional ``AI_VWAP_*``
variables to reproduce a specific policy and window.

.. literalinclude:: ../lumibot/example_strategies/ai_vwap.py
   :language: python
   :linenos:
