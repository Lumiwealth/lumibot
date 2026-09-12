LumiBot for coding agents
=========================

.. meta::
   :description: Exact starting points for coding agents using LumiBot: install, complete Strategy examples, AI model credentials, backtest artifacts, and supported tools.

Start from a complete example
-----------------------------

Use :doc:`agents_quickstart` for an AI strategy or :doc:`getting_started` for
ordinary Python rules. Both use ``from lumibot.strategies import Strategy``.
Create agents in ``initialize`` and invoke them in ``on_trading_iteration``.
Do not invent an alternative execution API.

The canonical two-agent source is
`ai_researcher_trader.py <https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/ai_researcher_trader.py>`_.
It uses ``gemini-3.5-flash-lite``, ``GEMINI_API_KEY``, and Yahoo daily prices.
Read its full source before changing it. Copy the complete file and execute it
in the same Python environment where LumiBot is installed.

Three ways agents participate
-----------------------------

* A **coding agent** writes and tests Python strategy files using LumiBot.
* An **in-strategy agent** reasons and calls tools during the strategy lifecycle.
  Research agents are read-only; the final trader has explicit trading permission.
* An **external MCP client** uses :doc:`BotSpot MCP <botspot_mcp>` to work in the
  hosted workspace. Hosted execution has its own account and approval requirements.

Verify evidence, not prose
--------------------------

Inspect exact order identifiers, statuses, filled quantities, positions, and
trace artifacts. A successful process, an agent summary, and a submitted order
are different from a filled order. An unresolved order must be reconciled before
retrying. ``orders_wait_for_terminal`` is bounded and may advance simulated time.

Use the strategy's clock in historical research. Report missing data explicitly;
do not silently replace a requested source. Record the model, dates, data,
source revision, replay state, cost, and results. See :doc:`agents_observability`.

For integrations in an existing Python project, see :doc:`standalone_components`.
For additional tools and signatures, see :doc:`agents_builtin_tools` and
:doc:`strategy_api_overview`. The generated ``llms.txt`` index points to the same
documentation; it is not a separate API contract.
