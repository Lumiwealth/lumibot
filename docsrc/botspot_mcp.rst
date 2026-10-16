BotSpot MCP Integration
=======================

.. meta::
   :description: Connect your AI assistant to BotSpot to create LumiBot strategies, run historical backtests, inspect charts, and report on your account.

Use BotSpot MCP when you want your AI assistant to create and edit LumiBot
strategies, run hosted historical backtests, inspect results, and report on
your existing bots. A paid BotSpot plan is required; data access and usage
limits depend on your plan and the requested workflow.

Start with the `maintained setup guide <https://github.com/Lumiwealth/botspot-mcp#connect>`_.
It contains current Claude Code, Cursor, Codex and hosted-client instructions,
first-use prompts, pricing and connection troubleshooting.

.. image:: ../docs/assets/readme/cta_botspot_mcp.png
   :alt: Use BotSpot MCP
   :align: center
   :width: 520px
   :target: https://github.com/Lumiwealth/botspot-mcp#connect

Connect and try one workflow
----------------------------------------

1. Add the Streamable HTTP endpoint ``https://mcp.botspot.trade/mcp`` in your client.
2. Complete the client's normal BotSpot OAuth sign-in.
3. Ask the assistant to check account access, create a strategy, run a historical
   backtest, and inspect that exact run using the `starter prompts
   <https://github.com/Lumiwealth/botspot-mcp#your-first-useful-result>`_.

Strategy creation uses ``create_strategy`` followed by the saved-file and
revision tools. Every edit creates a new revision; start a backtest from the
newest saved revision. Keep the returned backtest ID for status, chart and
artifact requests. A queued backtest is not a completed result.

No brokerage connection is needed for the starter historical example when
your plan includes its required data. Backtests are simulations and do not
guarantee future returns.

Permissions and availability
----------------------------------------

The direct MCP connection includes trading and deployment actions, subject
to account permissions, plan entitlements and normal product approvals.
An account connection alone does not authorize a trade. Protect API keys;
never paste them into a chat or commit them to source control.

Custom MCP installation and public app-store approval are separate. The
planned restricted store connection is still awaiting qualification and
release. Check the `distribution tracker
<https://github.com/Lumiwealth/botspot-mcp/blob/main/docs/distribution-status.md>`_
for submitted, approved and installation-tested destinations.

To disconnect, use your client's connector controls. Revoke any separate
BotSpot API key through BotSpot Account Settings.

LumiBot strategy agents
----------------------

Create a **Research** key in BotSpot's API Keys settings. It gives your agents
public research and the Marketplace and Community information you may read.
It does not authorize trading, deployments, backtests or Community posting.
Store your own key in your environment or secret manager:

.. code-block:: bash

   export BOTSPOT_RESEARCH_MCP_URL="https://api.botspot.trade/research-mcp"
   export BOTSPOT_RESEARCH_MCP_TOKEN="botspot_YOUR_RESEARCH_KEY"

LumiBot attaches that connection when you create a strategy agent. Static keys
do not need a renewal URL. BotSpot-hosted runtimes supply their own short-lived
capability and renewal endpoint; never copy those runtime tokens elsewhere.

The connection discovers datasets through ``search_data_catalog`` and reads
structured observations with ``query_data`` or documents with
``search_documents`` and ``get_document``. It also reads permitted Marketplace
listings, code and accounts, and the Community feed. Protected code and private
positions retain the owner's sharing and purchase rules.

In a backtest, the strategy's actual UTC clock limits the research request.
Sources without original-version and publication-time proof return a visible
``unsupported_capability`` error. Calendar dates or today's revised economic
tables do not establish what was available historically. Record missing
evidence and the provider used; changing a news source requires a new backtest.

Choose a narrower Custom key when you need fewer capabilities. Manage expiry
and revoke keys in BotSpot. Give an agent a posting grant only when you intend
to publish; the default Research connection cannot post or reply.

