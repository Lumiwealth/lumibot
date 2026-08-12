AI-Only Credit Spread
=====================

.. image:: ../docs/assets/ai-agent-workflows/ai-credit-spread.webp
   :alt: AI credit-spread workflow using LumiBot runtime skills, rules, tools, and execution
   :width: 100%

``ai_credit_spread.py`` is a minimal AI-only options strategy. Python creates
one agent and runs it each iteration. The prompt defines only the credit-spread
policy. LumiBot's built-in options skill provides reusable contract, pricing,
atomic-order, signed-position, and close-verification mechanics.

How it works
------------

* The agent selects a listed put or call vertical from current evidence.
* It verifies both exact contracts and calculates the per-unit package credit.
* It submits both legs atomically and verifies the exact order and positions.
* Active rules prevent duplicate structures and repeated closing orders.

Verified backtest evidence
--------------------------

The preserved pre-fix run demonstrated the original failure clearly: reversed
closing sides, repeated close attempts, and quantities that escalated to 480
contracts. That artifact is retained as the red baseline.

The repaired real-model eval now passes three consecutive repetitions. Each run
reconstructs the signed spread, maps long legs to ``sell_to_close`` and short
legs to ``buy_to_close``, submits one correctly sized atomic close, and verifies
the final state. The current historical downloader returned no option chain for
the canonical local window, so that backtest correctly remained flat. These
results validate mechanics without claiming strategy profitability.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   export DATADOWNLOADER_BASE_URL="https://<your-downloader-host>:8080"
   export DATADOWNLOADER_API_KEY="your-downloader-key"
   python -m lumibot.example_strategies.ai_credit_spread

Set ``BACKTESTING_START`` and ``BACKTESTING_END`` to choose an exact window.

.. literalinclude:: ../lumibot/example_strategies/ai_credit_spread.py
   :language: python
   :linenos:
