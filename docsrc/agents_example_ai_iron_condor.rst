AI-Only Iron Condor
===================

.. image:: ../docs/assets/ai-agent-workflows/ai-iron-condor.webp
   :alt: AI iron-condor workflow using LumiBot runtime skills, rules, tools, and execution
   :width: 100%

``ai_iron_condor.py`` is a minimal AI-only options strategy. Python creates one
trading agent in ``initialize()`` and runs it in ``on_trading_iteration()``.
The agent owns market retrieval, contract selection, sizing, four-leg order
construction, submission, verification, and position management.

The system prompt contains only the strategy policy: underlying, delta, DTE,
wing width, exits, and risk limits. Reusable options mechanics are supplied by
LumiBot's built-in ``options-trading`` skill. Active ``rules.json`` entries are
loaded again before every agent call and appended to the runtime instructions.

The example uses ``gemini-3.5-flash-lite`` explicitly. Existing saved
strategies keep the model identifier already stored in their code.

How it works
------------

* The agent loads the built-in options skill when options become relevant.
* It reads the account, underlying, chain, exact contracts, Greeks, and quotes.
* It prices and submits all four legs as one atomic multi-leg package.
* It verifies the returned order and current signed positions before reporting state.

Verified backtest evidence
--------------------------

The preserved ``2026-08-05_00-33_9dcawc`` seven-day backtest opened one atomic
SPY iron condor with the 685/690 put spread and 771/776 call spread, all using a
single 2026-09-04 expiration. The backtest ended near flat with a -0.00% rounded
total return and a -0.08% maximum drawdown. This short window proves mechanics,
not expected performance.

The refactored strategy was also rerun over two current seven-day windows. The
active downloader reported no historical option chain, so the agent correctly
made no trade instead of inventing contracts. The release-gated real-model eval
provides a chain fixture and separately verifies chain retrieval, four valid
legs, explicit package pricing, one atomic submission, and post-submit state
verification.

Run a seven-day backtest ending today with an options-capable backtest data
source:

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   export DATADOWNLOADER_BASE_URL="https://<your-downloader-host>:8080"
   export DATADOWNLOADER_API_KEY="your-downloader-key"
   python -m lumibot.example_strategies.ai_iron_condor

Set ``BACKTESTING_START`` and ``BACKTESTING_END`` in ``YYYY-MM-DD`` format to
choose an exact historical window.

.. literalinclude:: ../lumibot/example_strategies/ai_iron_condor.py
   :language: python
   :linenos:
