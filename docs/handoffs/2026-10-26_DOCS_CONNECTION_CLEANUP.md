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
  API docstrings, heading underlines and a duplicate reference in API content.
- Shared Firefox manual QA: homepage, coding-assistant guide and broker guide
  at 1440x1100 and 390x844, in light and dark themes. Raw captures inspected.
- Connection button clicked through to the live setup page. One panel and one
  button verified on the assistant page. Phone button bounds fit the panel;
  no horizontal page overflow observed.
- Public-file review: original documentation changes only; no customer-supplied
  strategy code, private operational information or credentials added.

## Publication

This receipt covers source and local rendering. No package release, tag or docs
publication was triggered. Publish through the normal documentation workflow,
then verify the homepage, assistant guide and a broker page on the public site.
