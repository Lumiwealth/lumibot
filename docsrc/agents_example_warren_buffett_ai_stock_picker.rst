Warren Buffett AI Stock Picker
==============================

.. meta::
   :description: An AI stock picker that thinks like Warren Buffett. It reads company filings, hunts for great companies at fair prices, and holds them. Free Python code for LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/warren-buffett-ai-stock-picker.png
   :alt: Research agent reads company reports and prices, trading agent owns great companies at fair prices, then buys and holds
   :width: 100%

This bot picks stocks the way Warren Buffett describes in his Berkshire Hathaway letters: buy wonderful businesses at fair prices and hold them for a long time. Why copy that? From 1965 through 2025, Berkshire's stock grew 19.7% a year, versus 10.5% for the S&P 500 (`Berkshire 2025 letter <https://www.berkshirehathaway.com/letters/2025ltr.pdf>`__).

How it works
------------

1. **Research agent** reads each company's latest SEC filings and financial statements. It checks for steady profits, high returns on capital, and a lasting edge, then checks the price: earnings yield, free cash flow yield, P/E, and debt.
2. **Trading agent** owns the companies the research calls great businesses at a fair price, split about evenly. It holds for the long run and only sells when the business gets worse or the price gets far too high.

Change ``universe`` to pick from different companies.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_warren_buffett_ai_stock_picker>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

Backtest tear sheet
-------------------

GPT-6 Luna, January 5 to 16, 2026, Yahoo daily prices and SEC filings, $100,000 start. The bot bought AXP, GOOGL, JPM, PG, and V and held them, ending at $97,542 (-2.5%) while SPY rose 1%. Two weeks is far too short to judge a buy-and-hold stock picker. Cash never went below $1,920.

.. image:: ../docs/assets/ai-bot-backtests/warren-buffett-ai-stock-picker.png
   :alt: Backtest tear sheet for the Warren Buffett AI Stock Picker
   :width: 100%
   :target: tearsheets/warren-buffett-ai-stock-picker.html

`Open the full tear sheet <tearsheets/warren-buffett-ai-stock-picker.html>`__. A short backtest shows the bot works as written. It is not a promise of future returns.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_warren_buffett_value.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_trading_team_warren_buffett_value

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading.

Good to know
------------

* Inspired by Warren Buffett's public shareholder letters. Not affiliated with or endorsed by Warren Buffett or Berkshire Hathaway.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
