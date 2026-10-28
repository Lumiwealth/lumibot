BotSpot MCP Integration
=======================

.. meta::
   :description: Connect your AI assistant to BotSpot to create LumiBot strategies, run historical backtests, inspect charts, and report on your account.

Use BotSpot MCP when you want your AI assistant to create and edit LumiBot
strategies, run hosted historical backtests, inspect results, and report on
your existing bots. Connecting is free. Paid BotSpot plans exist only for some
heavier features, and usage limits depend on your plan.

.. _why-botspot-mcp:

Why connect the free BotSpot MCP
--------------------------------

We strongly recommend it if you build with an AI assistant. One free connection
gives Claude, ChatGPT, Codex, Cursor or VS Code what it needs to work on real trading
strategies instead of only writing code:

* **Strategy-building skills.** Your assistant learns how LumiBot strategies are
  built, so it writes code that runs.
* **Free data.** News, economic data and SEC filings, ready for research.
* **Backtests and charts.** Your assistant can run a historical backtest on
  BotSpot's servers and read the charts, trades and logs.
* **Cheap scheduled deployments.** Run a bot on BotSpot's servers on a schedule.
  For a bot that runs once a day, that is usually cheaper than keeping your own
  computer or server on all the time.
* **The community.** Read and post, and see what other people's bots and trades
  are doing.
* **The marketplace.** Research strategies other people published, and publish
  your own so others can use it.
* **Easy broker linking.** Connect a broker on the website instead of managing
  keys on a server.
* **More tools for LumiBot's own AI agents.** With ``lumibot connect botspot``,
  agents inside your strategies get BotSpot research tools (macro data, SEC
  research, marketplace and community reads).

Backtests are simulations. They do not promise future results.

`Connect it free at botspot.trade/agents <https://botspot.trade/agents?utm_source=documentation&utm_medium=mcp_page&utm_campaign=lumibot&utm_content=why_section>`__

Start with the `maintained setup guide <https://botspot.trade/agents>`_.
It contains current Claude Code, Cursor, Codex and hosted-client instructions,
first-use prompts, pricing and connection troubleshooting.

.. image:: ../docs/assets/readme/cta_botspot_mcp.png
   :alt: Use BotSpot MCP
   :align: center
   :width: 520px
   :target: https://botspot.trade/agents

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
-----------------------

Create a named key in BotSpot's API Keys settings. Enable the independent public
research, Marketplace and Community-read permissions you need. Leave trading,
deployment, backtest-control, secret-management and Community-publication
permissions off unless you separately intend those capabilities. Expiry defaults
to Never; choose an expiry if needed. Store your key in your environment or
secret manager:

.. code-block:: bash

   export BOTSPOT_RESEARCH_MCP_URL="https://api.botspot.trade/research-mcp"
   export BOTSPOT_RESEARCH_MCP_TOKEN="botspot_YOUR_RESEARCH_KEY"

Or skip the key page entirely. One command signs you in and writes both values
to ``.env``:

.. code-block:: bash

   lumibot connect botspot

It prints a short login code and opens the BotSpot page where you sign in (a
free account is created if you do not have one) and approve the code. You can
also approve it by asking an AI app that is already connected to BotSpot.
BotSpot then creates a key for you, the command checks it and saves it. The key
is never printed. This is the standard device login (OAuth 2.0 device
authorization grant, RFC 8628) used by tools such as the GitHub CLI. Use
``--no-browser`` to only print the link, ``--token`` to save a key you already
have, or ``--paste`` to paste one made on the website. If the BotSpot server
you point at does not offer code login yet, the command falls back to the key
page flow. Connecting is free.

LumiBot attaches the connection when you create a strategy agent. Static keys
need no renewal URL. The configured MCP transport supports HTTP, private/local
servers, username/password authentication and endpoint query parameters. Use
HTTPS when available to protect credentials in transit, keep secrets out of
source control and logs, and use the server's supported authentication settings.

What your strategy agent gains
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Discover data:** use ``search_data_catalog`` and dataset descriptions to find
  available economics, news and SEC research. Availability depends on the source.
* **Read evidence:** use ``query_data``, ``search_documents`` and ``get_document``
  for observations and source documents, without wiring each supplier into the strategy.
* **Learn from other builders:** inspect permitted Marketplace descriptions,
  shared code and account information, and read Community discussions. Private
  code and account data follow their owner's sharing and purchase permissions.

In backtests, LumiBot bounds requests to simulated time. A date range remains
optional; a supplied range is clamped rather than added to every request.
Missing timestamps, date-only values and imperfect coverage do not disable the
connection. Agents can use available evidence, explain reasonable date estimates
and disclose uncertainty. They must not use information known to be from the
future or present revised/current data as an exact historical snapshot. A source
may return ``unsupported_capability`` for a historical form it cannot provide;
that is a limitation of that request, not a reason to shut down all research.
Live research has no backtest publication-date rule.

The research connection adds tools to a running strategy. Your external coding
assistant can separately use BotSpot MCP for free strategy-building skills,
backtest submission/results and deployment workflows. Hosted compute and heavier
usage depend on your plan; connecting is free. See the setup instructions above.
LumiBot also works independently with its existing broker and research tools.

Frequently asked questions
--------------------------

What is BotSpot MCP?
~~~~~~~~~~~~~~~~~~~~

BotSpot MCP connects your AI assistant to BotSpot. Your assistant can then
create and edit LumiBot strategies, run hosted historical backtests, look at
the results and report on your existing bots.

How is BotSpot related to LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LumiBot is the open source Python trading framework. BotSpot is the hosted
service around it. It runs hosted backtests and handles broker connections,
results and scheduling for LumiBot strategies. See
`BotSpot for AI agents <https://botspot.trade/agents>`_.

Is BotSpot MCP free?
~~~~~~~~~~~~~~~~~~~~

Yes, connecting is free. Paid BotSpot plans exist only for some heavier
features, and usage limits depend on your plan. The
`setup guide <https://github.com/Lumiwealth/botspot-mcp#connect>`_ has current
pricing.

What is the BotSpot MCP server URL?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add the Streamable HTTP endpoint ``https://mcp.botspot.trade/mcp`` in your
client, then finish the normal BotSpot sign in. The
`BotSpot agents page <https://botspot.trade/agents>`_ lists the exact steps for
each client.

Which AI assistants work with BotSpot MCP?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `BotSpot agents page <https://botspot.trade/agents>`_ lists ChatGPT,
Claude, Claude Code, Codex, Gemini CLI and Cursor. Some sign in with OAuth and
some use a BotSpot API key.

Do I need a brokerage account to run a backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. The starter historical example needs no brokerage connection when your
plan includes the data it uses. Backtests are simulations and do not promise
future returns.

Can my AI assistant place trades through BotSpot MCP?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The direct MCP connection includes trading and deployment actions. They still
depend on your account permissions, your plan and the normal product
approvals. Connecting an account alone does not approve a trade.

What should I try first?
~~~~~~~~~~~~~~~~~~~~~~~~

Ask your assistant to check your account access, create a strategy, run a
historical backtest and then inspect that exact run. The
`starter prompts <https://github.com/Lumiwealth/botspot-mcp#your-first-useful-result>`_
show how. Keep the backtest ID it returns, and remember that a queued backtest
is not a finished result.

How do I disconnect BotSpot MCP?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use your AI client's connector controls. If you also made a BotSpot API key,
revoke it in BotSpot Account Settings.
