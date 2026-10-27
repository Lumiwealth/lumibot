# Documentation connection cleanup

Explain LumiBot before hosted setup and use one shared connection destination.

Last Updated: 2026-10-26
Status: Source validated; documentation publication pending
Audience: Documentation maintainers

## Overview

The homepage explains the framework, its benefits and the AI strategy workflow
before the hosted connection panel. Broker and exchange pages reuse the existing
shared callout with seven short benefits, including separate community and
marketplace entries. The coding-assistant guide includes that panel once, in
its connection step. Obsolete client install badges and copied connector setup
instructions are removed; the shared button opens https://botspot.trade/agents.
The README and integration guide use the same setup destination.

The existing Sphinx pages, Furo theme and shared course-button styles remain
the rendering owners. A compact button variant fits narrow screens and honors
the selected theme. No new routes, runtime behavior or package version changes.

## Validation

- Regression baseline: four failures captured before editing the documentation.
- Documentation checks: 736 passed across the coding-assistant, broker FAQ,
  exchange, growth, community, tracking, discovery and docs SEO test modules.
- Sphinx 8.1.3 HTML build succeeded. Seven existing warnings concern unchanged
  broker/backtesting heading underlines, list formatting and an unlisted page.
- Shared Firefox manual QA: homepage, coding-assistant guide and broker guide
  at 1440x1100 and 390x844, in light and dark themes. Raw captures inspected.
- Connection button clicked through to the live setup page. One panel and one
  button verified on the assistant page. Phone button bounds fit the panel;
  no horizontal page overflow observed.
- Public-file review: original documentation changes only; no customer-supplied
  strategy code, private operational information or credentials added.

## Source compatibility

Validation started from `5bfbb71d`; documentation implementation is `b62b997f`.
The subsequent `4f992daa` account-mode repair changes broker runtime/tests only,
with no overlap in the changed documentation owners or documentation assertions.
Those compatible documentation passes and raw captures remain valid after merge.

## Publication

This receipt covers source and local rendering. No package release, tag or docs
publication was triggered. Publish through the normal documentation workflow,
then verify the homepage, assistant guide and a broker page on the public site.

## Strategy explanation correction (October 26)

The homepage and README now distinguish Python, AI-native and hybrid strategies
before examples. The single-agent example exposes `system_prompt`, creates one
trading agent in `initialize()` and invokes it every trading iteration. Its
source is shared between the README and Sphinx via `docsrc/_snippets/ai_strategy.py`.
It is an API/lifecycle example, not a profitability or completed-model-run claim.
The lifecycle contract is exercised without external model or broker actions.

Assistant connection is a separate heading after strategy examples and broker
execution, with an open-ended list of MCP-compatible clients. Repeated generic
AI disclaimers and duplicated walkthroughs/comparison tables were replaced by
links to existing detailed guides. No new page or runtime capability was added.
This improves every user's choice of strategy architecture independently of any
particular strategy, indicator, creator or customer.

All 737 documentation checks passed after the correction. The full local
Sphinx build completed, with pre-existing formatting diagnostics outside the
changed pages. Complete desktop screenshots include the entire page through its
FAQ and footer, rather than stopping at the introduction. Phone viewport capture
is tracked separately and must not be inferred from a desktop screenshot.
