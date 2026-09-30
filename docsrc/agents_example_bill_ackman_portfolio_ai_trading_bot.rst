Bill Ackman Portfolio AI Trading Bot
====================================

.. meta::
   :description: An AI bot that invests like Bill Ackman. It holds a few big, high-conviction stocks and drops ideas that stop making the cut. Free Python code for LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/bill-ackman-portfolio-ai-trading-bot.png
   :alt: Research agent finds and attacks the best ideas, trading agent holds the best 3 to 5 stocks, then the trade order
   :width: 100%

This bot invests the way Bill Ackman describes his style at Pershing Square: own a small number of simple, high-quality companies and put real money behind each one. Pershing Square usually keeps most of its money in just 8 to 12 core holdings (`Pershing Square Holdings <https://pershingsquareholdings.com/about-us/>`__), and its value rose 70.2% in 2020, its best year (`2020 annual report <https://assets.pershingsquareholdings.com/2021/04/12201719/Pershing-Square-Holdings-Ltd.-2020-Annual-Report-1.pdf-Letter-Only.pdf>`__).

How it works
------------

1. **Research agent** studies each company for simple, predictable businesses that make lots of cash. Then it attacks each idea: too much debt, weak management, strong rivals, or a price that is too high.
2. **Trading agent** holds the 3 to 5 stocks that survive, with bigger weights on the best ideas. It sells a stock when it drops out of the top 5.

Change ``universe`` to pick from different companies.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_bill_ackman_portfolio_ai_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bill_ackman_concentrated.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading.

Good to know
------------

* Inspired by Bill Ackman's public comments on concentrated investing. Not affiliated with or endorsed by Bill Ackman or Pershing Square.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
