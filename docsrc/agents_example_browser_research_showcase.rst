Authenticated Browser Research Showcase
=======================================

.. image:: ../docs/assets/ai-agent-workflows/ai-browser-research-showcase.png
   :alt: Authenticated browser research AI trading team workflow
   :width: 100%

This example demonstrates the full handoff: an authenticated browser
researcher reads a JavaScript application, a dedicated trading/risk agent
decides whether to trade, and a separate publisher can post a truthful trade
receipt to an explicitly authorized account.

Install the optional browser runtime first::

   pip install "lumibot[browser]"
   patchright install chromium

Set ``research_url`` and a named host-scoped credential profile in the hosting
application. For sites whose login fields are not self-describing, provide
``research_login_selectors`` with ``username``, ``password``, and optional
``submit`` selectors. Never commit passwords or pass them in an agent prompt.

Publishing is off by default: ``publish_enabled=False``. Enable it only for an
owned test account or an account you are authorized to automate, provide ``publish_url``
and its separate credential profile, and validate the site terms. The publisher
must report observed order status truthfully, use a stable idempotency key, and
capture a screenshot/action receipt. Provide ``publish_form_selectors`` for the
``idempotency_key``, ``receipt``, and ``submit`` fields when publishing through a
form. A submitted order is not a filled order.

Verified execution
------------------

The committed regression test runs this Strategy through
``PandasDataBacktesting`` against an owned JavaScript fixture. The browser
researcher performs a real login through the built-in browser tools and captures
a screenshot/trace; the risk agent fills one simulated SHOW share; the separate
publisher posts an idempotent receipt keyed by the order ID and captures a
second screenshot. No third-party account is touched.

The agent currently reasons from rendered visible text, DOM extraction, browser
state, and action results. Screenshots are durable audit evidence. They are not
yet supplied to the model as native multimodal image inputs.

`Inspect the execution receipt <https://github.com/Lumiwealth/lumibot/blob/version/4.5.92/docs/research/2026-09-20_AGENT_STRATEGY_EXECUTION_PROOF.md>`_.

.. literalinclude:: ../lumibot/example_strategies/ai_browser_research_showcase.py
   :language: python
   :linenos:
