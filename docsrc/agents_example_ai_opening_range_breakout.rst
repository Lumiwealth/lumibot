AI-Only Opening Range Breakout
==============================

.. image:: ../docs/assets/ai-agent-workflows/ai-opening-range-breakout.webp
   :alt: AI opening-range breakout workflow using LumiBot runtime skills, rules, market evidence, and execution
   :width: 100%

``ai_opening_range_breakout.py`` is a minimal AI-only equity strategy. Python
creates one trading agent and runs it each iteration. Entry, exit, and sizing
rules live in the prompt, while the built-in ``stock-trading`` skill provides
the reusable market-evidence, stock-order, and verification workflow.

How it works
------------

* The agent scans the configured universe with batch prices and history.
* It builds the range only from completed regular-session bars beginning at 09:30 ET.
* It requires a completed close outside the range, then sizes from the stop distance.
* It manages exits and enforces the daily-entry and maximum-position rules.

Verified backtest evidence
--------------------------

The earlier five-day mechanical run completed with four fills across NVIDIA and
AMD and a 0.44% total return, but required 104 agent calls. The refactor moved
reusable stock mechanics into the runtime skill and changed the default decision
cadence to hourly while retaining minute evidence. A bounded current run reached
its third trading day with real position changes before the ten-minute wall-clock
guard stopped it. The production-gated ORB eval passes three consecutive
real-model repetitions and verifies completed 09:30 ET opening bars, a completed
breakout close, current price evidence, one submission, and post-order state.

The example is therefore qualified for mechanics and bounded model behavior, not
for expected returns. If minute bars for the true opening window are unavailable,
the agent must skip the symbol instead of inventing a range.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   python -m lumibot.example_strategies.ai_opening_range_breakout

Use ``AI_ORB_UNIVERSE`` for a smaller universe during local qualification and
``AI_ORB_*`` variables for other policy overrides.

.. literalinclude:: ../lumibot/example_strategies/ai_opening_range_breakout.py
   :language: python
   :linenos:
