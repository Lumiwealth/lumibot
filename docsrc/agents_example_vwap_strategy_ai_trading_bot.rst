VWAP Strategy AI Trading Bot
============================

.. meta::
   :description: An AI day trading bot that buys SPY when it bounces back above VWAP, the price big funds watch all day. Free Python code you can backtest in LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/vwap-strategy-ai-trading-bot.png
   :alt: Research agent spots dips below VWAP, trading agent buys the bounce and sets a stop, then out before the close
   :width: 100%

This day trading bot uses VWAP, the volume-weighted average price. Big funds grade their own trades against VWAP, so it is a price level the whole market watches (`Berkowitz, Logue and Noser, 1988 <https://onlinelibrary.wiley.com/doi/abs/10.1111/j.1540-6261.1988.tb02591.x>`__). When SPY dips below VWAP and then climbs back above it, the bot buys the bounce with a tight stop and is out by the close.

How it works
------------

1. **Research agent** checks SPY's minute bars once an hour, starting at 10:00 ET. It reports VWAP, the last price, and whether SPY dipped at least 0.15% below VWAP and then closed back above it since the last check.
2. **Trading agent** buys when that bounce happens and the bot is not already in a trade. It puts the stop just under the dip's low and sizes the trade so hitting the stop loses at most 1% of the account.
3. The trading agent sells at the next hourly check, and always before the close. It makes at most one new trade a day.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_vwap_strategy_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_vwap.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_vwap

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading. The backtest uses Alpaca's minute and option history, so it also needs free Alpaca paper keys.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
