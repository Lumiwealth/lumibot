Give your LumiBot agents superpowers
====================================

.. meta::
   :description: Connect BotSpot MCP for free: give your LumiBot AI agents research data, SEC documents, Marketplace strategies and Community knowledge.

Your trading agent can do more than reason about prices. **Connect BotSpot MCP
for free** and give it research data, source documents and a community of
strategy builders—all through one connection.

.. image:: ../docs/assets/readme/botspot_mcp_superpowers.png
   :alt: Connect LumiBot to BotSpot MCP to add Research, Marketplace and Community tools. Free to connect.
   :width: 640px
   :align: center
   :class: lumibot-entry-hero

.. _why-botspot-mcp:

More evidence. Better-equipped agents.
--------------------------------------

* **Research without a pile of integrations.** Discover economic data, news
  headlines and SEC filings; query datasets and read the underlying documents.
* **Learn from the Marketplace.** Explore published strategies and inspect the
  code and account information their owners share.
* **Tap into the Community.** Read other builders' discussions and share
  questions and findings when you ask your agent to do so.

LumiBot makes those tools available to agents inside your strategy. BotSpot
handles the data connections; your agents decide how to use the evidence.

.. _lumibot-strategy-agents:

Ask your assistant to connect it
--------------------------------

Already connected BotSpot MCP to Codex, Claude Code, Cursor or another coding
assistant? Tell it:

.. code-block:: text

   Connect this LumiBot project to BotSpot MCP. Create a LumiBot agent API key,
   save the connection in this project's environment, and verify a research request.

The assistant can create the key with BotSpot's ``api_keys`` tool. The default
**LumiBot agent** preset includes research, Marketplace and Community access;
you do not need to choose individual permissions. It saves the connection
locally so LumiBot can attach it when your strategy creates an agent.

**Not connected yet?** `Connect your assistant free at botspot.trade/agents
<https://botspot.trade/agents>`_, then use the prompt above. Key creation uses
your signed-in BotSpot session. A connection authenticated only with an API key
must sign in as the account owner to create another key.

Prefer manual setup?
---------------------

Create an API key in BotSpot Account Settings using the **LumiBot agent**
preset. Save these values in your project's ``.env`` file and keep it out of Git:

.. code-block:: dotenv

   BOTSPOT_RESEARCH_MCP_URL=https://api.botspot.trade/research-mcp
   BOTSPOT_RESEARCH_MCP_TOKEN=your-api-key

The optional ``lumibot connect botspot`` command can also set up the connection.
See :doc:`cli` for its options. On BotSpot-hosted strategies, the runtime
supplies the connection automatically.

Your coding assistant gets superpowers too
------------------------------------------

The same BotSpot MCP connection gives your coding assistant free LumiBot
strategy-building skills, hosted backtests with charts and logs, and deployment
tools. Ask it to build a strategy, backtest it and explain the results.

The connection exposes the ``skills`` tool. Your assistant lists the guides
with ``action="list"``, loads the relevant guide with ``action="get"`` and its
name, then loads only the references it needs. Public guides use the same
canonical source text as BotSpot's internal guides; private guides are not
published. Improving that source updates its public distribution through the
normal publication process.

Connecting makes these guides discoverable; it does not automatically load
every guide into a conversation or install native skill files in Codex or
Claude Code. The assistant must retrieve and follow the relevant guide.
Loading skills needs no sign-in and grants no account or trading permission.

**Connecting is free.** Hosted compute and heavier usage depend on your plan.
Data availability depends on the source, and private Marketplace information
requires its owner's permission. In backtests, agents use simulated time and
explain historical-data gaps or approximations. Backtests do not guarantee
future returns.

LumiBot also works on its own. To disconnect, remove the project connection
settings and revoke its API key in BotSpot Account Settings.
