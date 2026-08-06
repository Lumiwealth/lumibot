AI-Only Iron Condor
===================

``ai_iron_condor.py`` is a minimal AI-only options strategy. Its Python class
does two things: it creates one trading agent in ``initialize()`` and runs that
agent in ``on_trading_iteration()``. All market retrieval, option selection,
four-leg construction, sizing, execution, and position management belongs to
the agent and its system prompt. Strategy parameters such as ``wing_width``,
``target_delta``, DTE bounds, profit/loss exits, and risk caps are injected into
the prompt and per-iteration context. After every submission the prompt requires
``orders_get_status`` plus a fresh ``account_positions`` read before claiming a fill.

The example uses ``gemini-3.5-flash-lite`` explicitly. Existing saved
strategies keep the model identifier already stored in their code.

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
