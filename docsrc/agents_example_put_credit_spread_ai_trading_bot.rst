Put Credit Spread AI Trading Bot
================================

.. meta::
   :description: An AI bot that sells put credit spreads on SPY, picks the strikes, and closes losers early. Free Python code you can backtest in LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/put-credit-spread-ai-trading-bot.png
   :alt: Research agent picks 2 SPY put contracts, trading agent opens and closes the spread, then one 2-leg order
   :width: 100%

This bot sells a put credit spread on SPY about a month out. You get paid up front and keep the money if SPY stays above the short strike, and the second put caps how much you can lose. Why sell puts? Over more than 32 years, Cboe's index of selling S&P 500 puts earned almost the same yearly return as the S&P 500 (9.54% vs 9.80%) with much smaller swings (`Bondarenko for Cboe, 2019 <https://cdn.cboe.com/resources/education/research_publications/PutWriteCBOE19_v14_by_Prof_Oleg_Bondarenko_as_of_June_14.pdf>`__).

How it works
------------

1. **Research agent** checks SPY's price, trend, and the live option chain. It picks an expiration 30 to 45 days out, a short put near 0.16 delta, and a long put 5 points lower.
2. **Trading agent** first manages any spread you hold. It closes it when you have kept 50% of the premium, when the loss hits 2 times the premium, with 21 days left, or when the short put gets too close to the money.
3. If you hold no spread, the trading agent opens the new one as a single 2-leg order for a net credit, risking about 2% of the account and at most 10 contracts. The bot checks once a day.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_put_credit_spread_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_credit_spread.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_credit_spread

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading. The backtest uses Alpaca's minute and option history, so it also needs free Alpaca paper keys.

Good to know
------------

* A sharp market drop can turn a credit spread into a full loss. Paper trade first.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
