Nancy Pelosi Stock Trading Bot
==============================

.. meta::
   :description: This AI bot owns the same stocks as Nancy Pelosi, rebuilt from her House reports. Free Python code you can backtest and run with LumiBot.

.. image:: ../docs/assets/ai-agent-workflows/nancy-pelosi-trading-bot.png
   :alt: Research agent works out what Pelosi owns from her House reports, trading agent makes your account match
   :width: 100%

This bot estimates Nancy Pelosi's disclosed household stock portfolio and mirrors its weights in your account. It reconstructs the portfolio from public reports instead of following only recent purchases. Reports arrive late and give value ranges, so the result is an approximation of disclosed holdings, not a view of her actual account today.

Want her call options too? The :doc:`agents_example_nancy_pelosi_copy_trading_bot` copies those as well.

How it works
------------

The bot has just two AI agents, and both run every day:

1. **Research agent** reads the newest available annual snapshot on the `House Clerk website <https://disclosures-clerk.house.gov/>`__ and applies disclosed transactions after that snapshot's valuation date. This includes transactions filed before the annual report itself. Partial sales reduce holdings, exercised calls add shares, and amendments correct earlier reports. Each answer retains the complete ledger and its sources, even on quiet days.
2. **Trading agent** buys the estimated basket on its first iteration, then compares actual holdings with the reconstructed target weights every day. It sells before dependent buys, checks fills and cash, and leaves weights within 2 percentage points of target alone. A failed lookup is not a reason to liquidate existing holdings.

There is no other code. Want a different member of Congress? Change the name in the research agent's prompt.

Run it on BotSpot
-----------------

Run this bot on `BotSpot <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_examples&utm_content=agents_example_nancy_pelosi_trading_bot>`_ without installing anything. BotSpot runs LumiBot in the cloud, backtests it, and connects it to your broker.

The code
--------

The whole bot is one short file. The two prompts are the strategy. The research agent uses LumiBot's built-in web tools, which read ZIP files, PDFs, spreadsheets and web pages from any website.

.. literalinclude:: ../lumibot/example_strategies/ai_nancy_pelosi_trading_bot.py
   :language: python

Run it yourself
---------------

.. code-block:: bash

   pip install lumibot
   python -m lumibot.example_strategies.ai_nancy_pelosi_trading_bot

Put these in your ``.env`` file: ``OPENAI_API_KEY``, and your broker keys (for example ``ALPACA_API_KEY``, ``ALPACA_API_SECRET``, and ``ALPACA_IS_PAPER=true`` for paper trading). With ``IS_BACKTESTING=false`` the bot trades. With ``IS_BACKTESTING=true`` it backtests instead; set ``BACKTESTING_START`` and ``BACKTESTING_END`` to pick the dates, and start with a week or two, because every AI call costs a little.

Good to know
------------

* Members of Congress can take up to 45 days to report a trade, so the bot always sees trades late.
* Reports show dollar ranges, such as $1,000,001 to $5,000,000. The agent estimates quantities from those ranges and available historical prices, then values remaining holdings as of the strategy date. The weights and any unknown quantities remain estimates.
* An annual report's filing date differs from its year-end holdings date. Applying only trades filed after the annual report would miss intervening changes.
* Both agents receive the strategy's explicit ``as_of`` timestamp. A backtest reconstructs what could reasonably have been known then, excluding later disclosures and explaining estimated availability dates. The researcher skips any report filed after the test day and any version known to have appeared after the simulated instant.
* Check the complete holdings, target-versus-actual weights, real simulated fills and remaining cash before trusting a backtest. A completed run with no trades is not proof that reconstruction worked.
* Pelosi said on November 6, 2025 that she will not run for re-election in 2026 (`NBC News <https://www.nbcnews.com/politics/congress/nancy-pelosi-first-female-speaker-house-wont-seek-re-election-congress-rcna239324>`__). Her reports stop after she leaves office, so change the name in the research prompt to keep the bot trading.
* The House website says its reports may not be used for most commercial purposes (5 U.S.C. 13107). Check that your use is allowed.

See :doc:`agents_examples` for more AI trading bots and :doc:`strategy_run_modes` for backtest and live runs.
