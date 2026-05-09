Agent Flows
===========

An agent flow is the way your strategy calls one or more agents during normal
LumiBot lifecycle methods such as ``on_trading_iteration()``. It is just Python.
There is no required graph framework, no fixed committee structure, and no
single "correct" shape.

You can use one agent, fifteen agents, a deterministic strategy with one AI
review step, or a full multi-model debate. LumiBot provides the agent runtime,
built-in tools, point-in-time backtesting, replay artifacts, and order
permissions. Your strategy decides the flow.

Two Strategy Styles
-------------------

Most strategies are one of these, or a hybrid:

- **Deterministic strategy** -- Python code makes the decision with fixed rules:
  indicators crossing, time-of-day logic, thresholds, portfolio constraints,
  and explicit if/else branches.
- **Agent-powered strategy** -- one or more agents reason over evidence, call
  tools, summarize their views, and optionally place orders.
- **Hybrid strategy** -- Python gates the setup, agents review the context, and
  deterministic code may still size, filter, or submit orders.

The same LumiBot strategy can be backtested first and then run against paper or
live brokers.

Common Flow Patterns
--------------------

**Single analyst**
   One agent reads the current market state and returns a decision or a
   structured recommendation. This is the simplest starting point.

**Research then trade**
   A read-only research agent gathers evidence. A trading-enabled agent reviews
   that evidence and decides whether to submit orders.

**Bull, bear, neutral**
   Multiple read-only agents receive the same evidence pack and argue from
   different perspectives. A final portfolio manager weighs the views.

**Specialist research desk**
   Separate agents gather different inputs: news, SEC filings, macro data,
   technical indicators, alternative data, risk, sector context, or valuation.
   Their summaries feed a decision agent or deterministic order logic.

**Multi-model committee**
   Several providers or models produce independent bull and bear cases. A final
   synthesis step compares them. For example, OpenAI, Gemini, Claude, and Grok
   can each produce a view before one portfolio manager decides.

**Iterated debate**
   Agents can run in sequence more than once: bull, bear, bull rebuttal, bear
   rebuttal, risk review, final decision. This costs more tokens, so it is best
   used selectively.

**Deterministic execution**
   Agents can stop at research. The final trade can still be placed by normal
   Python code if you want deterministic sizing and order submission.

**Risk gate**
   A strategy can put hard Python risk checks after the agent decision:
   maximum position size, no shorting, symbol allowlist, drawdown stop, or
   per-trade dollar limits.

Minimal Two-Agent Flow
----------------------

.. code-block:: python

   def initialize(self):
       self.agents.create(
           name="researcher",
           model="openai/gpt-5.4-mini",
           allow_trading=False,
           system_prompt="Gather evidence. Do not trade.",
       )
       self.agents.create(
           name="trader",
           model="openai/gpt-5.5",
           allow_trading=True,
           system_prompt="Review evidence, check risk, and trade only when justified.",
       )

   def on_trading_iteration(self):
       evidence = self.agents["researcher"].run(
           task_prompt="Research the current setup for AAPL, MSFT, and NVDA."
       )
       decision = self.agents["trader"].run(
           task_prompt="Make the final decision.",
           context={"evidence": evidence.summary or evidence.text},
       )
       self.log_message(decision.summary)

Larger Specialist Flow
----------------------

This is still normal Python. You can create many read-only agents, run them,
and pass the outputs forward.

.. code-block:: python

   def initialize(self):
       for name, prompt in {
           "news_researcher": "Find recent news and catalysts.",
           "filing_researcher": "Search SEC filings for risk and opportunity.",
           "macro_researcher": "Review rates, inflation, labor, liquidity, and credit.",
           "technical_researcher": "Review trend, volatility, RSI, MACD, and moving averages.",
           "bull_case": "Build the strongest long thesis.",
           "bear_case": "Find the strongest reasons not to trade.",
           "neutral_case": "Give a balanced probability-weighted view.",
       }.items():
           self.agents.create(
               name=name,
               model="openai/gpt-5.4-mini",
               allow_trading=False,
               system_prompt=prompt,
           )

       self.agents.create(
           name="portfolio_manager",
           model="openai/gpt-5.5",
           allow_trading=True,
           system_prompt="Weigh the research, check risk limits, then place orders only if justified.",
       )

In the trading iteration, your strategy decides whether these agents run in a
chain, in parallel, only on certain symbols, or only when deterministic filters
find an interesting setup.

Hybrid Deterministic + Agent Flow
---------------------------------

Agents do not need to place trades. A strategy can ask agents for research and
then use normal Python for the actual order.

.. code-block:: python

   def on_trading_iteration(self):
       if not self.indicators.crossed_above("SPY", "sma_20", "sma_50"):
           return

       review = self.agents["risk_reviewer"].run(
           task_prompt="Review whether this SMA crossover is worth trading today."
       )

       if "approve" in review.summary.lower():
           order = self.create_order("SPY", 10, "buy")
           self.submit_order(order)

This pattern is useful when you want explainability or research from the agent
but still want deterministic order sizing and execution.

Choosing Models Per Agent
-------------------------

Every agent can use its own model. That does not mean every strategy needs many
models. The common pattern is:

- cheaper model for data gathering and summarization
- stronger model for adversarial reasoning or final trade decisions
- different providers when you want independent perspectives

For a four-agent committee, that can look like this:

.. code-block:: python

   self.agents.create(name="evidence_researcher", model="openai/gpt-5.4-mini", allow_trading=False)
   self.agents.create(name="bull_researcher", model="openai/gpt-5.5", allow_trading=False)
   self.agents.create(name="bear_researcher", model="google/gemini-3.1-pro", allow_trading=False)
   self.agents.create(name="portfolio_manager", model="openai/gpt-5.5", allow_trading=True)

Safety Defaults
---------------

Use ``allow_trading=False`` for every agent that should not mutate broker
state. That agent can still inspect read-only state and research tools. Only
the final trader, portfolio manager, or deterministic Python code should submit,
modify, or cancel orders.

Where To Go Next
----------------

- :doc:`agents_builtin_tools` explains the built-in tools available to agents.
- :doc:`agents_investment_committee` shows one concrete four-agent example.
- :doc:`agents_memory` explains how agents can remember decisions and lessons.
- :doc:`agents_observability` explains traces and replay artifacts.
