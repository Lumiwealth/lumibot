AI-Only Opening Range Breakout
==============================

``ai_opening_range_breakout.py`` is a minimal AI-only equity strategy. Python
creates one trading agent and runs it each iteration. Entry, exit, and sizing
rules live in the system prompt.

Prefer minute data. If the active backtest data source only provides daily bars,
the agent should report that limitation and avoid inventing an intraday range.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   python -m lumibot.example_strategies.ai_opening_range_breakout

.. literalinclude:: ../lumibot/example_strategies/ai_opening_range_breakout.py
   :language: python
   :linenos:
