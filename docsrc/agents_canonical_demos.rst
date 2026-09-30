One-Agent AI Trading Bot Demos
==============================

.. meta::
   :description: Short one-agent AI trading bot demos for LumiBot: M2 liquidity, trend, momentum and news, news sentiment, market news, and a make-me-money bot. Each is about 30 lines.

These are the smallest AI trading bots in LumiBot. Each one is a single AI agent
with a few sentences of plain English, about 30 lines in all. The agent uses
LumiBot's built-in tools on its own: prices, news, Federal Reserve data, and
more. You never write a tool or name one in the prompt.

For bigger bots with several agents, see :doc:`agents_examples`.

The demos
---------

- **Make Me Money** (``agent_discretionary.py``): the whole prompt is *"Make as much money as you possibly can."* LumiBot's built-in rules handle risk, sizing, and look-ahead safety.
- **Market News** (``agent_alpaca_news_builtin.py``): reads the day's market news, opens the most important story, and holds SPY, QQQ, or SHV.
- **News Sentiment** (``agent_news_sentiment.py``): buys the 2 to 4 well-known stocks with the strongest good news, or SHV when the news is weak.
- **Trend** (``agent_macro_risk.py``): holds TQQQ while it trends up and SHV while it trends down.
- **Momentum and News** (``agent_momentum_allocator.py``): holds TQQQ when the trend is up and the news is not bad, otherwise SHV.
- **M2 Liquidity** (``agent_m2_liquidity.py``): holds TQQQ when the money supply is growing and SHV when it is shrinking, using the Federal Reserve's M2 data. The ``_openai``, ``_anthropic``, and ``_grok`` copies are the same bot on other AI models.

Example: M2 Liquidity
---------------------

.. literalinclude:: ../lumibot/example_strategies/agent_m2_liquidity.py
   :language: python

To run a demo on another AI model, add ``model="anthropic/claude-sonnet-4-6"``
(or another model) to ``self.agents.create(...)`` and put that provider's key in
your ``.env`` file.

Run a demo
----------

Put ``OPENAI_API_KEY`` in your ``.env`` file. The M2 bot also needs a free
``FRED_API_KEY``, and the news bots need free Alpaca keys (``ALPACA_API_KEY`` and
``ALPACA_API_SECRET``).

.. code-block:: bash

   python -m lumibot.example_strategies.agent_m2_liquidity

Every demo ends with the same block as a BotSpot ``main.py``: with
``IS_BACKTESTING=true`` it backtests (set ``BACKTESTING_START`` and
``BACKTESTING_END`` for the dates), otherwise it trades with the broker in your
``.env`` file. See :doc:`strategy_run_modes`.

What to look at after a run
---------------------------

- The tear sheet and its comparison with SPY
- ``trades.csv``: every order and fill
- ``*_agent_detail.parquet``: every AI call, the tools it used, and why it traded

See :doc:`agents_observability` for how to read them.

Related pages
-------------

- :doc:`agents` -- main guide
- :doc:`agents_quickstart` -- build your first agent
- :doc:`agents_examples` -- multi-agent AI trading bots
