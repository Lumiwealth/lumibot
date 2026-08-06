AI-Only VWAP
============

``ai_vwap.py`` is a minimal AI-only equity strategy. Python creates one trading
agent and runs it each iteration. VWAP retrieval, reclaim logic, and exits live
in the system prompt.

Prefer minute data and ``get_indicator(..., indicator='vwap')`` when available.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   python -m lumibot.example_strategies.ai_vwap

.. literalinclude:: ../lumibot/example_strategies/ai_vwap.py
   :language: python
   :linenos:
