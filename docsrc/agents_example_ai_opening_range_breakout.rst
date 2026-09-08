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

Run a bounded historical example
--------------------------------

Use Python 3.10 or later. From a current LumiBot source checkout, install the
package in your virtual environment so the runner and documentation match:

.. code-block:: bash

   python -m pip install -e .
   export GEMINI_API_KEY="your-gemini-key"
   export DATADOWNLOADER_BASE_URL="https://your-downloader-host"
   export DATADOWNLOADER_API_KEY="your-downloader-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   export BACKTESTING_START="2026-04-06"
   export BACKTESTING_END="2026-04-11"
   export AI_ORB_UNIVERSE="SPY,NVDA,AMD"
   export AI_ORB_SLEEPTIME="1H"
   export LUMIBOT_AGENT_MAX_MODEL_CALLS="60"
   python -m lumibot.example_strategies.ai_opening_range_breakout

The current source selects ``gemini-3.5-flash-lite``. Check that your provider
account supports it. The window spans April 6 through April 10, 2026, with an
April 11 end boundary. These are reproducible input settings, not a claim that
this exact current-source run has completed. Start with three symbols before
expanding to the default universe.

Use the configured Data Downloader for historical ThetaData requests. Do not
start another licensed terminal session as a shortcut. Yahoo daily bars cannot
supply a 09:30–09:45 opening range. Missing intraday evidence should result in a
skip with an explanation.

The call limit bounds agent invocations, not necessarily every provider
continuation or dollar of spend. Model and data charges depend on your accounts;
use a separately enforced budget for paid verification. Reaching the limit is an
incomplete run, not a passing demonstration.

Inspect the output
------------------

Record the source commit, package version, model, date range, symbol universe,
and data source together with the generated backtest logs and artifacts. Inspect
completed opening bars, decision timestamps, any submitted orders and fills,
exit decisions, and the terminal backtest status. Check that the strategy uses
minute evidence even though it makes hourly decisions.

A completed no-trade run is possible; do not add a trade merely to make a demo
look successful. A timeout, budget limit, or missing-chain/history failure is
not a completed full-window result. Save the terminal logs before showing a
tearsheet or making a performance statement.

Use ``AI_ORB_*`` parameters for opening-range length, sizing, position limits,
and profit exits; the source below lists their exact names and defaults.

.. literalinclude:: ../lumibot/example_strategies/ai_opening_range_breakout.py
   :language: python
   :linenos:
