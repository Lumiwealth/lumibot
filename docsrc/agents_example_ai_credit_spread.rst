AI-Only Credit Spread
=====================

``ai_credit_spread.py`` is a minimal AI-only options strategy. Python creates
one trading agent and runs it each iteration. Put or call credit-spread
selection, sizing, execution, and management live in the system prompt and use
only generic option tools.

.. code-block:: bash

   export GEMINI_API_KEY="your-key"
   export BACKTESTING_DATA_SOURCE="ThetaData"
   export DATADOWNLOADER_BASE_URL="https://<your-downloader-host>:8080"
   export DATADOWNLOADER_API_KEY="your-downloader-key"
   python -m lumibot.example_strategies.ai_credit_spread

.. literalinclude:: ../lumibot/example_strategies/ai_credit_spread.py
   :language: python
   :linenos:
