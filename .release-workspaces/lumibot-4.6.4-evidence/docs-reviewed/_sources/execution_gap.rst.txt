The Execution Gap
=================

.. meta::
   :description: Most agentic trading projects end at the decision. The model reasons, prints a recommendation, and stops.

Most agentic trading projects end at the decision. The model reasons, prints a
recommendation, and stops. Everything after that, the sizing, the gate, the
order and the record, is left to you.

LumiBot is built around that second half. This page shows the artifacts rather
than describing them, so you can decide for yourself.

What the research says is missing
---------------------------------

A 2026 review of more than twenty open-source DeFi trading agents found one
failure mode in nearly all of them: *"the strategy looks responsible at the LLM
step, and then nothing checks anything between 'model said yes' and
'transaction broadcasted'."* The same review named exits as its second-largest
gap, because an agent that wins on entries and treats exits as an afterthought
gives the gains back.

The academic survey *Agentic Quantitative Trading*
(`arXiv 2608.31041 <https://arxiv.org/abs/2608.31041>`_, August 2026) reaches
the same conclusion from the other direction. It reports that these systems
"remain concentrated on signal discovery, while complete integration with
portfolio construction, execution, and risk control is still uncommon," and
that strong model or forecasting capability "does not reliably translate into
trading performance under live market conditions and reliability controls."

Three requirements, and where LumiBot's primitives sit
------------------------------------------------------

The 2026 governance frameworks converge on the same short list for an agent
that moves money. Singapore's IMDA Model AI Governance Framework for Agentic AI
(January 2026) and the NIST AI Agent Standards Initiative (February 2026) both
name **identity**, **traceability** and **stoppability**.

Those are their requirements. LumiBot has not been assessed against either
framework and does not claim compliance with them. What it gives you is the
material you would need:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Requirement
     - What LumiBot provides
   * - Identity
     - Each agent is created separately with its own ``allow_trading``
       permission, so a research agent physically cannot submit an order.
   * - Traceability
     - Every prompt, tool call, tool result and piece of model reasoning is
       written to a parquet trace beside the run.
   * - Stoppability
     - Deterministic Python risk checks run after the model proposes and
       before an order leaves, plus broker-level cancel and kill controls.

The gate is Python, not a prompt
--------------------------------

A prompt is a request. A gate is code. LumiBot strategies put the hard limits in
Python, where the model has no vote:

.. code-block:: python

   class MyStrategy(Strategy):
       def initialize(self):
           self.agents.create(
               name="researcher",
               allow_trading=False,   # this agent cannot place an order at all
           )
           self.agents.create(
               name="trader",
               allow_trading=True,
           )

``allow_trading=False`` is not an instruction the model can reason around. The
order tools are never given to that agent.

The record you can open
-----------------------

Every run writes a trace. In a backtest it lands beside the logs; in live or
paper trading it goes to ``~/Library/Caches/lumibot/1.0/agent_runtime/`` on
macOS.

.. code-block:: text

   logs/<run>_<strategy>_agent_detail.parquet

That file holds, for every step the agent took:

* the exact prompt the model received, including the injected account state
* each tool call with its arguments
* each tool result the model actually saw
* the model's stated reasoning
* the resulting order identifier, if one was submitted

Read it with pandas:

.. code-block:: python

   import pandas as pd

   trace = pd.read_parquet("logs/<run>_<strategy>_agent_detail.parquet")
   print(trace.columns.tolist())
   print(trace[["tool_name", "tool_args"]].head(20))

This is what "show me why it made that trade in March" looks like in practice.
It is a file, not a dashboard, so it survives, it diffs, and it can be handed to
somebody who is asking hard questions.

Evidence, not promises
----------------------

None of the above is a claim about returns. It is a claim about what the
software records and what it refuses to do. Both are checkable:

* The agent evaluation suite in ``agent_eval_cases/`` runs the agent against
  fixed scenarios and fails the build when it behaves wrongly.
* Every eval must fail first for the customer's reason before its fix is
  accepted, and the red artifact is kept in ``agent_eval_baselines/``.

See also
--------

* :doc:`agents` for the agent runtime and its tools
* :doc:`agents_observability` for the full tracing surface
* :doc:`ai_trading_project_comparison` for a feature-by-feature comparison
