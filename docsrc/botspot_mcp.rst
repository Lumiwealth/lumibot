BotSpot MCP Integration
=======================

.. meta::
   :description: BotSpot exposes a public MCP server for strategy generation, backtesting, deployment monitoring, artifact analysis, and chart/visual retrieval.

BotSpot exposes a public MCP server for strategy generation, backtesting,
deployment monitoring, artifact analysis, and chart/visual retrieval.

This is the piece that makes BotSpot useful even when you already use Codex,
Claude Code, Cursor, or another coding agent. A generic agent can write Python,
but the BotSpot MCP gives that agent Lumibot-specific tools: launch hosted
backtests, inspect charts and artifacts, compare runs, work with strategies in
the marketplace, and prepare deployment without leaving the coding workflow.

This page is intentionally practical: endpoint URLs, auth, client setup,
and verification commands.

.. image:: ../docs/assets/readme/cta_botspot_mcp.png
   :alt: Use BotSpot MCP
   :align: center
   :width: 520px
   :target: https://botspot.trade/agents?utm_source=documentation&utm_medium=mcp_guide&utm_campaign=lumibot&utm_content=mcp_primary_button

Last verified
-------------

- This doc was updated for the current production MCP behavior on ``March 16, 2026``.
- Canonical host is ``https://mcp.botspot.trade``.

Canonical endpoints
-------------------

- Production MCP root (hosted connectors): ``https://mcp.botspot.trade``
- Production MCP path (explicit endpoint): ``https://mcp.botspot.trade/mcp``
- Production API (non-MCP REST): ``https://api.botspot.trade``

Important transport note
------------------------

BotSpot runs MCP over HTTP JSON-RPC (POST). ``GET /mcp`` now returns a small
capability JSON document for connector reachability checks.
Tool execution still uses ``POST /mcp``.

Authentication modes
--------------------

BotSpot supports two modes:

1. **OAuth 2.1 + PKCE** (recommended for hosted connector UIs like Claude app/web and ChatGPT custom apps)
2. **API key bearer token** (recommended for CLI/IDE clients like Claude Code, Cursor, Codex, and scripts)

Create an API key
-----------------

1. Sign in to BotSpot.
2. Go to **Account Settings**.
3. In **API Keys**, click **Create New API Key**.
4. Copy the key immediately (it begins with ``botspot_`` and is shown once).

Use the key as:

.. code-block:: text

   Authorization: Bearer botspot_YOUR_API_KEY

Client quickstarts
------------------

LumiBot strategy agents
^^^^^^^^^^^^^^^^^^^^^^^

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

Claude app (Desktop/Web)
^^^^^^^^^^^^^^^^^^^^^^^^

1. Open Claude.
2. Go to **Customize -> Connectors**.
3. Add custom connector URL ``https://mcp.botspot.trade``.
4. Click **Connect** and finish Auth0 sign-in + consent.

Claude Code (API key)
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export BOTSPOT_API_KEY="botspot_YOUR_API_KEY"
   claude mcp add --transport http botspot https://mcp.botspot.trade/mcp \
     --header "Authorization: Bearer $BOTSPOT_API_KEY"
   claude mcp list

Cursor (API key)
^^^^^^^^^^^^^^^^

Add this to your Cursor MCP config (commonly ``.cursor/mcp.json``):

.. code-block:: json

   {
     "mcpServers": {
       "botspot": {
         "url": "https://mcp.botspot.trade/mcp",
         "headers": {
           "Authorization": "Bearer botspot_YOUR_API_KEY"
         }
       }
     }
   }

Codex CLI (API key)
^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   export BOTSPOT_API_KEY="botspot_YOUR_API_KEY"
   codex mcp add botspot --url https://mcp.botspot.trade/mcp \
     --bearer-token-env-var BOTSPOT_API_KEY
   codex mcp list

ChatGPT custom app (OAuth)
^^^^^^^^^^^^^^^^^^^^^^^^^^

If your ChatGPT plan has custom apps/connectors enabled:

1. Open ChatGPT settings for apps/connectors.
2. Add BotSpot MCP URL ``https://mcp.botspot.trade``.
3. Complete OAuth sign-in/consent when prompted.

What BotSpot MCP enables
------------------------

- Generate/refine strategies
- Start/stop/status backtests
- List/sort/filter backtests with server-side ordering
- Query CSV artifacts using SQL
- Fetch strategy visuals and backtest visual artifacts
- Fetch chart-ready time series (equity, drawdown, returns, portfolio)
- Read account status and billing links

Known chaining requirement
--------------------------

``start_backtest`` and ``get_code`` workflows are revision-specific.
In practice, call ``list_revisions`` (or use revision IDs returned by other tools)
before calling tools that require ``revisionId``.

Validation checklist (copy/paste)
---------------------------------

1) Metadata endpoint must return 200:

.. code-block:: bash

   curl -i https://mcp.botspot.trade/.well-known/oauth-protected-resource

2) Unauthenticated MCP call must return 401 with ``WWW-Authenticate``:

.. code-block:: bash

   curl -i -X POST https://mcp.botspot.trade/mcp \
     -H "Content-Type: application/json" \
     --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

3) Authenticated MCP call (replace key) should return tool list/result:

.. code-block:: bash

   curl -i -X POST https://mcp.botspot.trade/mcp \
     -H "Authorization: Bearer botspot_YOUR_API_KEY" \
     -H "Content-Type: application/json" \
     --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'

Local debugging (recommended)
-----------------------------

- Local Node MCP endpoint: ``http://localhost:8082/mcp``
- Tunnel endpoint example: ``https://<your-stable-ngrok-domain>/mcp``

Keep one stable tunnel URL during a debugging cycle to avoid connector and callback drift.

Troubleshooting
---------------

- **"Error connecting to MCP server"**:
  - Verify exact URL.
  - Reconnect and complete OAuth consent.
  - For OAuth clients, verify Auth0 callback/origin settings.
- **Missing tools or stale schema in a chat session**:
  - Start a fresh chat/session so tool metadata reloads.
- **401 / missing scope errors**:
  - Verify token type and scope set.
  - Regenerate API key and re-test with a minimal tool call.

More resources
--------------

- BotSpot AI agent and MCP page: `https://botspot.trade/agents <https://botspot.trade/agents>`_
- BotSpot MCP server guide: `https://github.com/Lumiwealth/botspot_node/blob/production/docs/mcp/mcp-server-guide.md <https://github.com/Lumiwealth/botspot_node/blob/production/docs/mcp/mcp-server-guide.md>`_
