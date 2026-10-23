BotSpot MCP Integration
=======================

.. meta::
   :description: Connect your AI assistant to BotSpot to create LumiBot strategies, run historical backtests, inspect charts, and report on your account.

Use BotSpot MCP when you want your AI assistant to create and edit LumiBot
strategies, run hosted historical backtests, inspect results, and report on
your existing bots. Connecting is free: signing in creates a free BotSpot
account with the research data tools (SEC filings, economic data, news
headlines, Marketplace and Community). Paid plans unlock heavier features
such as hosted backtests, live bots and real-money trades.

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

No brokerage connection is needed for the starter historical example. Backtests are simulations and do not
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

The fastest way: run one command and approve it once.

.. code-block:: bash

   lumibot connect botspot

It prints a short login code. Approve it in an AI app that is already connected
to BotSpot (say "approve BotSpot login code BCDF-GHJK") or on the page it
prints. LumiBot then receives a "LumiBot agent" key and writes
``BOTSPOT_RESEARCH_MCP_URL`` and ``BOTSPOT_RESEARCH_MCP_TOKEN`` to ``.env``.
The key is never printed and never pasted. Free BotSpot accounts can do this.

The "LumiBot agent" key can read free data (SEC filings, insider and 13F data,
news headlines, Treasury, BLS, BEA and other economic data), browse and study
Marketplace strategies and accounts, read Community posts, and post or reply in
Community. It cannot deploy bots, run backtests, trade, read your account or
broker, touch billing, or make other keys. It does not expire unless you choose
an expiry.

You can also ask your AI app to make a key in the chat (``api_keys`` with
``action="create"``); it shows the key once and gives you the exact
``lumibot connect botspot --token ...`` command. Or create one on the BotSpot
API Keys page. Store it in your environment or secret manager:

.. code-block:: bash

   export BOTSPOT_RESEARCH_MCP_URL="https://api.botspot.trade/research-mcp"
   export BOTSPOT_RESEARCH_MCP_TOKEN="botspot_YOUR_KEY"

To point the command at another BotSpot environment, set ``BOTSPOT_API_URL``.

LumiBot attaches that connection when you create a strategy agent. Static keys
do not need a renewal URL. BotSpot-hosted runtimes supply their own short-lived
capability and renewal endpoint; never copy those runtime tokens elsewhere.
Endpoints require HTTPS outside loopback, and renewal must use the same origin.
Never put credentials in a URL, query string or fragment.

The connection discovers datasets through ``search_data_catalog`` and reads
structured observations with ``query_data`` or documents with
``search_documents`` and ``get_document``. It also reads permitted Marketplace
listings, code and accounts, and the Community feed. Protected code and private
positions retain the owner's sharing and purchase rules.

In a backtest, agents should use data dated at or before the strategy's current
time. Sources with missing or approximate dates can still be used: the agent
estimates the date and says so, and never treats later information as known
earlier. Today's revised economic tables are not the numbers people saw at the
time; note that limitation when it matters.

Narrow or revoke a key on the API Keys page, or ask your AI app
(``api_keys`` with ``action="revoke"``). Hosted bot settings are separate from key creation; disabling
hosted research or revoking a selected key denies that hosted access too.
Community posting from a hosted bot requires separate explicit account, key and bot consent.
BotSpot owns supplier credentials, normalization, storage and source qualification;
LumiBot is the client. Research access never grants deployment, backtest-control,
trading or secret-management authority.

