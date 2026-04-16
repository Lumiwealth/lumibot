BotSpot MCP Integration
=======================

BotSpot exposes a public MCP server for strategy generation, backtesting,
deployment monitoring, artifact analysis, and chart/visual retrieval.

This page is intentionally practical: endpoint URLs, auth, client setup,
and verification commands.

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

- BotSpot developer page: `https://botspot.trade/developers <https://botspot.trade/developers>`_
- BotSpot MCP server guide: `https://github.com/Lumiwealth/botspot_node/blob/production/docs/mcp/mcp-server-guide.md <https://github.com/Lumiwealth/botspot_node/blob/production/docs/mcp/mcp-server-guide.md>`_
