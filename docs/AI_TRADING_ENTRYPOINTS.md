# AI trading entry points

The README and documentation lead with AI trading in Python, a runnable
backtest, and access to the complete strategy source. The number of agents is
an example architecture, not the library's product positioning.

## Page ownership

- README.md: AI install/key setup, Python runner, complete source link, examples,
  then an optional free-challenge invitation. Preserve the deeper reference
  content and conventional Strategy interface.
- docsrc/index.rst: three task routes and compact brand image, with grouped
  navigation for onboarding, AI, strategy/data, execution, and community.
- docsrc/agents_quickstart.rst: canonical Strategy literalinclude, exact model
  credentials, backtest commands, and interpretation of decisions/orders.
- docsrc/getting_started.rst: AI versus conventional Python routes. The optional
  synthetic installation check must explicitly say it is not an AI strategy.
- docsrc/agents_examples.rst: direct routes to starter, debate, and options
  examples. Each detailed page owns its code, requirements, and evidence.
- docsrc/_includes/learn_with_rob.rst: a compact free-challenge invitation after
  useful material. The bootcamp is secondary.

## Evidence contract

Do not call an engine-completed run a successful AI run if the agents were
skipped after provider errors. Inspect model traces, submitted orders, fill
records, and terminal status. An intentional hold can be correct; a skipped
model call is not a hold decision. Keep those outcomes distinct.

Provide code and run commands alongside every featured result. Record source,
model, data, dates, and whether the run used fresh inference or cached decisions.
A fresh inference run may choose different trades. Replay requires the original
matching cache/data/engine context; it does not guarantee general profitability.

## Visual checks

Inspect README and docs on desktop and phone. Confirm images stay compact,
code can be copied, page width does not overflow, navigation groups are readable,
and every primary route reaches the promised page. Marketing images must be
unaltered approved-generator outputs; evidence screenshots must show real runs.

## Competitor patterns used, September 13, 2026

GitHub API snapshots: TradingAgents 104,853 stars; OpenBB 72,926; AI Hedge Fund
63,355; LumiBot 2,056. These are total-star snapshots, not growth rates or causal
proof that a particular design produced the stars.

| Observed pattern | LumiBot implementation | Why and measurement |
| --- | --- | --- |
| [TradingAgents](https://github.com/TauricResearch/TradingAgents) explains specialized agents, gives CLI/Python entry points, and documents nondeterminism. | AI trading positioning; stock-team gallery; canonical Python quickstart; source/run pairing. | Make the product understandable and shareable; measure quickstart visits and source clicks. |
| [AI Hedge Fund](https://github.com/virattt/ai-hedge-fund) leads into a short install/run flow and explains API keys. | Copyable install, Gemini key, complete runner, direct editable source. | Reduce setup abandonment; observe successful first runs through voluntary feedback and reproducible issue reports. |
| [OpenBB](https://github.com/OpenBB-finance/OpenBB) gives a short Python import example, multiple consumption surfaces, and a separate commercial workspace. | Reusable-component page and coding-agent entry point; optional hosted/education links after useful code. | Reach developers embedding components and preserve an understandable commercial path. |
| OpenBB includes contributor links and a star invitation. | Existing contribution guide, issue forms and PR template; a short star invitation after the runnable example. | Turn successful use into advocacy; compare rolling net stars and contributor participation. |

These are observed patterns, not permission to copy third-party artwork, code or
claims. Current competitor capabilities must be checked before comparison copy.
In particular, AI Hedge Fund's roadmap and current installation differ from
older descriptions; a roadmap does not prove deployed capabilities.

## Growth measurement and next distribution work

The requested outcome is faster legitimate GitHub-star growth. The initial
working target is 10–15 net new stars/day over a rolling 28-day window, against
Rob's approximate five/day baseline. Establish actual daily total-star snapshots
before treating that baseline as measured. A 105,000-star outcome needs repeated
successful distribution and adoption; this page redesign alone cannot promise it.

Track three separate layers: discovery (repository visitors and tutorial
visits), activation (example source clicks and voluntarily reported successful
runs), and advocacy (net stars, forks, contributions and shared strategies).
GitHub stars cannot be reliably attributed to individual documentation clicks;
use dated release/distribution annotations and aggregate trends, not invented
person-level attribution. Do not add hidden telemetry to installed strategies.

The next distribution asset is a short real screen recording titled
“Build an AI trading strategy in Python with LumiBot,” showing setup, the actual
research and trader decisions, and the generated order report. Link one permanent
tutorial with source and requirements. Adapt that evidence for a developer post,
a trading-community tutorial, and an agent-tool integration example. Drafts are
separate from publication; use the authorized channel for any public send.

Keep the free challenge measurable through existing attributed links. Monitor
registrations, attended qualified calls, settled bootcamp payments, refunds and
fulfillment costs separately from stars. Do not interrupt existing experiments.

Repository hygiene remains a response/decision workflow, not a mass-closure
campaign. The existing digest and templates are implemented; maintainer decisions,
external announcements and hosted-bot repairs are separate outstanding actions.
The compatibility-preserving Strategy API is retained. A callback API rewrite
is not required for this entry-point work and has no demonstrated growth benefit.
