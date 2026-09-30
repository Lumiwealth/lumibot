0DTE Options AI Trading Bot
===========================

.. meta::
   :description: Two AI agents team up to trade SPY 0DTE options with bear call spreads that expire the same day. Free Python code you can backtest in LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/0dte-options-ai-trading-bot.png
   :alt: Research agent checks SPY every 5 minutes, trading agent sells today's call spread, then the trade that expires the same day
   :width: 100%

This bot trades 0DTE options: options that expire the same day. It sells a bear call spread on SPY, the S&P 500 ETF, and keeps the premium if SPY stays below the short strike by the close. 0DTE is now the biggest part of the index options market: in August 2025 it was a record 62.4% of all SPX options trading (`Cboe <https://www.cboe.com/insights/posts/spx-0-dte-options-jump-to-record-62-share-in-august>`__).

How it works
------------

1. **Research agent** checks SPY and today's expiring calls every 5 minutes. It finds the call closest to 0.20 delta and the call 5 points higher, with their prices.
2. **Trading agent** sells that spread once a day as a single 2-leg order for a credit, risking about 1% of the account and at most 2 contracts.
3. The trading agent watches the spread every 5 minutes and closes it early when you have kept 50% of the premium, the loss hits 2 times the premium, SPY rises above the short strike, or 10 minutes remain before the close.

Change ``symbol`` to ``"SPX"`` to trade S&P 500 index options, if your broker offers them.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_0dte_options_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_0dte_options_trading_bot.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_0dte_options_trading_bot

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading. The backtest uses Alpaca's minute and option history, so it also needs free Alpaca paper keys.

Good to know
------------

* 0DTE options move very fast. Paper trade first.
* The bot checks every 5 minutes, so it makes many AI calls per day.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
