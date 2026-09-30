Fear and Greed Index Trading Bot
================================

.. meta::
   :description: This AI bot opens a real web browser, reads the CNN Fear and Greed Index, and buys SPY on fear, sells on greed. Free Python code for LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/fear-and-greed-index-trading-bot.png
   :alt: Research agent reads CNN Fear and Greed in a web browser, trading agent buys on fear and sells on greed, then the trade order
   :width: 100%

This bot buys the S&P 500 when investors are scared and sells when they get greedy. Each day an AI agent opens a real web browser, goes to CNN's Fear & Greed Index page, and reads the score. The score blends seven market signals, like momentum, volatility, and demand for safe assets, into one number from 0 to 100. Buying when others are scared is an old idea: Warren Buffett wrote that he tries "to be fearful when others are greedy and to be greedy only when others are fearful" (`1986 Berkshire letter <https://www.berkshirehathaway.com/letters/1986.html>`__). It is also the simplest example of an AI agent using a browser to read a live website and trade on it.

How it works
------------

1. **Research agent** opens a real Chrome browser, goes to cnn.com/markets/fear-and-greed, and reads the score from 0 (extreme fear) to 100 (extreme greed) and the date it was last updated.
2. **Trading agent** sets how much of your account is in SPY: 100% below 25, 75% from 25 to 44, 50% from 45 to 55, 25% from 56 to 75, and 0% above 75. The rest stays in cash.
3. If the score is missing or more than 3 days old, the trading agent does nothing. The bot repeats this once a day.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_fear_and_greed_index_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_fear_and_greed_trading_bot.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install "lumibot[browser]"
   patchright install chromium
   python -m lumibot.example_strategies.ai_fear_and_greed_trading_bot

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading.

Good to know
------------

* CNN's page only shows today's score. In a backtest the research agent opens CNN's daily score history in the browser instead and uses only the score from the day before each test day. The history also shows later days, so this rule lives in the prompt, not in code.
* Change the percentages in the trading agent's prompt to make the bot more or less aggressive.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
