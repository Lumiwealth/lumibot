Iron Condor AI Trading Bot
==========================

.. meta::
   :description: An AI bot that sells iron condors on SPY for steady option income and closes them at a profit target, loss limit, or time stop. Free Python code for LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/iron-condor-ai-trading-bot.png
   :alt: Research agent picks 4 SPY option contracts, trading agent opens and closes the iron condor, then one 4-leg order
   :width: 100%

This bot sells an iron condor on SPY: it bets SPY stays inside a price range for about a month and collects option premium up front. You keep the premium if SPY stays in the range, and the long options cap how much you can lose. Why this trade? Over about 35 years, Cboe's iron condor index had a worst drop of 19%, compared with 51% for the S&P 500 (`Cboe <https://www.cboe.com/insights/posts/benchmark-indices-series-volatility-management-with-cboes-bfly-and-cndr-indices/>`__).

How it works
------------

1. **Research agent** checks SPY's price, recent moves, and the live option chain. It picks an expiration 30 to 45 days out and four contracts: a short put and a short call near 0.16 delta, each with a protective option 5 points further out.
2. **Trading agent** first manages any condor you hold. It closes it when you have kept 50% of the premium, when the loss hits 2 times the premium, with 21 days left, or when SPY moves too close to a short strike.
3. If you hold no condor, the trading agent opens the new one as a single 4-leg order for a net credit, risking about 2% of the account and at most 10 contracts. The bot checks once a day.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_iron_condor_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_iron_condor.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_iron_condor

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading. The backtest uses Alpaca's minute and option history, so it also needs free Alpaca paper keys.

Good to know
------------

* Options can lose money fast. Paper trade first.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
