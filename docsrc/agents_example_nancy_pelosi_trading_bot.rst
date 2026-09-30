Nancy Pelosi Stock Trading Bot
==============================

.. meta::
   :description: This AI bot copies the stock trades Nancy Pelosi and other Congress members report. Free Python code you can backtest and run with LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/nancy-pelosi-trading-bot.png
   :alt: Research agent reads Pelosi's trades on the House website, trading agent copies her portfolio, then the trade order
   :width: 100%

This bot copies Nancy Pelosi's stock portfolio for you. Every day it goes to the U.S. House of Representatives website, finds her newest trade reports, and buys and sells to match them. Why copy her? Her reported portfolio was up an estimated 70.9% in 2024, while the S&P 500 rose 24.9%, according to the Unusual Whales report covered by `Fortune <https://fortune.com/2025/01/08/congress-stock-trading-pelosi-2024>`__.

How it works
------------

1. **Research agent** opens a real web browser and goes to the House Clerk's financial disclosure search at disclosures-clerk.house.gov. It types "Pelosi", picks the year, and opens each new trade report.
2. The research agent writes down every stock she bought or sold and the dollar amount. It skips options, gifts, and anything filed after today.
3. **Trading agent** turns those trades into a portfolio. Stocks she bought the most of get the biggest share of your account. Anything she sold gets sold.
4. The trading agent checks your cash, places the orders, and checks that they filled. The bot repeats this once a day, so a new report reaches your account the day after it is posted.

Want a different member of Congress? Change ``last_name`` from ``"Pelosi"`` to any House member's last name.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_nancy_pelosi_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy.

.. literalinclude:: ../lumibot/example_strategies/ai_nancy_pelosi_trading_bot.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install "lumibot[browser]"
   patchright install chromium
   python -m lumibot.example_strategies.ai_nancy_pelosi_trading_bot

Add ``OPENAI_API_KEY`` to your ``.env`` file. The file runs a backtest first. To trade, set ``IS_BACKTESTING = False``: the bot then trades with the broker in your ``.env`` file, for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading.

Good to know
------------

* Members of Congress can take up to 45 days to report a trade, so the bot always sees trades late.
* Reports show dollar ranges, such as $1,000,001 to $5,000,000. The bot uses the middle of each range.
* In a backtest the website also shows reports from after the test date. The research agent reads the date on each report and skips any report filed after the test day.
* Pelosi said on November 6, 2025 that she will not run for re-election in 2026 (`NBC News <https://www.nbcnews.com/politics/congress/nancy-pelosi-first-female-speaker-house-wont-seek-re-election-congress-rcna239324>`__). Her reports stop after she leaves office, so change ``last_name`` to keep the bot trading.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
