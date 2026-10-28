AI Iron Condor Trading Bot
==========================

.. meta::
   :description: A hybrid SPY iron condor: Luna selects next-session contracts, while Python sizes, monitors and closes each package without repeated AI calls.

This example separates a daily AI decision from frequent numerical monitoring.
The planner proposes a next-session SPY iron condor or skips the session.
Python validates the contracts, submits the order and manages the actual
positions. The planner cannot place orders.

How it works
------------

1. Fifteen minutes before the exchange session closes, the planning agent checks
   yesterday's VIX close and skips if it exceeded 25 or is unavailable. Otherwise
   it selects next-session puts and calls near 0.14 absolute delta with $1 wings.
2. Python verifies the listed contracts, expiration, protective wings and package
   price. It sizes the order within 3% of portfolio value using maximum expiry
   loss plus an allowance for entry and exit commissions, and available cash.
3. Every minute, Python checks each expiration separately. It closes when SPY
   moves 40% of the distance from the short-strike midpoint toward either short
   strike. This is a price-distance rule, not a premium-loss percentage.
4. Expiring exposure is closed thirty minutes before the exchange closes.
   The next daily entry waits until positions are flat and related orders are
   terminal. Early-closing sessions use their actual exchange closing time.

Unrelated orders do not suppress monitoring. Pending entries are canceled before
closing partial exposure. A required exit remains latched after a price rebound.
Closing orders are reconciled before repricing; the example allows three close
attempts, five minutes apart. An uncertain submission or exhausted attempts is
reported as unresolved, never as a successful exit. Incomplete packages and
strategy-owned assignment stock are reconciled with their actual signed quantities.

Monitoring and exits make no AI calls. Market data and hosting still have their
own costs. An unavailable model cannot prevent Python from monitoring existing
positions.

Run locally with Login with ChatGPT
-----------------------------------

The default planner is ``chatgpt/gpt-6-luna`` with medium reasoning and Standard
processing. It uses your explicitly connected ChatGPT plan or authorized credit
allowance, without an API-key fallback. See :doc:`agents_chatgpt`.

.. code-block:: bash

   python -m lumibot.components.agents.chatgpt_auth login
   python -m lumibot.components.agents.chatgpt_auth status
   python -m lumibot.example_strategies.ai_iron_condor

Set ``IS_BACKTESTING=true``, ``BACKTESTING_START`` and ``BACKTESTING_END`` for
historical simulation. The default historical source requires your own Alpaca
data credentials; the VIX lookup requires your existing FRED access.
Starting the module with ``IS_BACKTESTING=false`` is a separate live-trading action.

Contributors can run a bounded historical window from a source checkout:

.. code-block:: bash

   python scripts/backtest_hybrid_iron_condor.py \
       --start 2026-01-05T09:30:00 --end 2026-01-23T15:40:00 \
       --output-dir /path/to/private/condor-run

The end is before the final session's next entry opportunity. This lets the
expiring package close without opening a position outside the requested window.
The runner uses minute bars, unadjusted underlying prices and an illustrative
$0.65 per-contract commission on both entry and exit. That fee is an assumption,
not a quote for every broker.

The run directory contains the normal trade/event logs, agent trace, tear sheet,
``ai-usage.json`` and ``summary.json``. Usage distinguishes daily planner turns
from completed inference responses, including continuations. Tokens are measured;
API-equivalent pricing is an estimate and does not establish the number of
ChatGPT credits debited. The inference response does not report that debit.

Recorded three-week smoke run
-----------------------------

The January 5–23, 2026 run at source revision ``3f768aaed`` completed eight
four-leg entries and eight signed closes, including entry, exit and later
re-entry. It ended with no positions or active orders. All eight exits were
price-distance stops; expiry timing, partial exposure and cancellation/repricing
are covered separately by the strategy's regression tests.

Starting equity was $10,000 and ending equity was $9,683.20, a loss of $316.80
(3.168%), including $124.80 in assumed commissions. This is a mechanics smoke
test, not evidence of profitability or an AI advantage.

There were thirteen daily planning sessions and 116 completed inference
responses, including continuations: 3,680,855 input tokens (2,170,240 cached) and
19,093 output tokens including reasoning. Monitoring and closing used no model
requests. Login with ChatGPT incurred no per-call API cash billing; using the
registered Luna Standard API rates gives an illustrative API-equivalent estimate
of $0.18231 for this run. That estimate is not a ChatGPT credit debit.

Backtest limitations
--------------------

Alpaca's historical option bars contain trades, not historical NBBO quotes.
When a historical quote has only a last-trade price, this example labels it
``historical_trade_mark_not_nbbo``. Such a fill is a simulation assumption,
not proof that a live four-leg order would execute at that price. Historical
Greeks are calculated locally from available marks, not recorded broker Greeks.
Missing prices
do not become invented quotes. Expiry loss limits assume the protected structure
remains intact; assignment, gaps, liquidity and order failures require separate
attention.

A completed historical lifecycle establishes mechanics for its recorded window.
It does not establish profitability, live execution quality or an AI advantage.

Run it on BotSpot
-----------------

This hybrid is source work awaiting its own release. An existing Marketplace
listing or installed package does not establish that it contains this revision.
Use the source runner above to reproduce its historical qualification.

The code
--------

.. literalinclude:: ../lumibot/example_strategies/ai_iron_condor.py
   :language: python

See :doc:`agents_examples` for other examples and
:doc:`strategy_run_modes` for backtest and live run configuration.
