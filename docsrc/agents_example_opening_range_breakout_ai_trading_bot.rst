Opening Range Breakout AI Trading Bot
=====================================

.. meta::
   :description: An AI bot that trades the opening range breakout. It waits for the first move of the day, then buys the breakout. Free Python code for LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/opening-range-breakout-ai-trading-bot.png
   :alt: Research agent finds stocks breaking the morning high, trading agent buys with a target and a stop, then out before the close
   :width: 100%

This day trading bot trades the opening range breakout (ORB). It marks each stock's high and low from the first 15 minutes of the day, then buys the stock that breaks out above that high. Why this setup? A study of a 5-minute opening range breakout on QQQ found 33% a year of return beyond the index after commissions from 2016 to 2023 (`Zarattini and Aziz, 2023 <https://static1.squarespace.com/static/5983d931579fb366729580d8/t/643ed6765176b45506e41a01/1681839734183/SSRN-id4416622.pdf>`__). Most day trading setups do not hold up that well, so test it yourself.

How it works
------------

1. **Research agent** scans 20 big, liquid stocks and ETFs once an hour, starting at 10:00 ET. It loads their 5-minute bars in one call, finds each opening range, and ranks the stocks that closed above their range high.
2. **Trading agent** buys the best breakout if the bot holds nothing. The stop is the range low, sized so hitting it loses at most 1% of the account.
3. Right after buying, the trading agent places a take-profit order at 1.5 times the risk and a stop order at the range low. It sells if the price falls back into the range and always before the close.

Change ``universe`` to scan different stocks.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_opening_range_breakout_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

Backtest tear sheet
-------------------

GPT-6 Luna, January 5 to 9, 2026, Alpaca minute bars, checked hourly, $100,000 start. The bot traded breakouts in TSLA, AMZN, AVGO, COST, DIS, JPM, and UBER with a target and a stop on each (14 fills) and ended at $99,502 (-0.5%) while SPY rose 1%. It held one stock at a time; cash never went below $63,880.

.. image:: ../docs/assets/ai-bot-backtests/opening-range-breakout-ai-trading-bot.png
   :alt: Backtest tear sheet for the Opening Range Breakout AI Trading Bot
   :width: 100%
   :target: tearsheets/opening-range-breakout-ai-trading-bot.html

`Open the full tear sheet <tearsheets/opening-range-breakout-ai-trading-bot.html>`__. A short backtest shows the bot works as written. It is not a promise of future returns.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_opening_range_breakout.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_opening_range_breakout

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading. The backtest uses Alpaca's minute and option history, so it also needs free Alpaca paper keys.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
