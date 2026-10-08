# LumiBot: what to learn from AI trading competitors

Research date: September 13, 2026. This is a proposed product plan, not an announcement of implemented features. Repository observations use the then-current default branches; LumiBot source review uses `9f77692f`.

## The decision

LumiBot should make its existing engine much easier to experience, compose, and trust. A wholesale replacement of `Strategy` is not supported by this research. The strongest opportunity is a short path from an AI trading idea to a working, inspectable strategy that can use supported brokers, while retaining an equally clear path for ordinary Python strategies.

The competition has substance. Its advantages include packaged first-run experiences, reusable data interfaces, persistent research workspaces, and agent-readable integration contracts. These are product choices, not merely attractive README graphics. However, popularity does not establish superior trading performance, reliability, or suitability for existing LumiBot users. No public evidence reviewed establishes that a particular API style caused a particular star count.

## The comparison set

GitHub API snapshots on the research date:

| Project | Stars | Job it presents to users |
|---|---:|---|
| TradingAgents | 105,407 | Run a team of financial research agents |
| OpenBB | 72,965 | Bring financial data into tools and applications |
| AI Hedge Fund | 63,368 | Configure and explore an AI fund |
| AI-Trader | 22,311 | Connect agents to a trading community/platform |
| OpenAlice | 7,061 | Maintain a local research and trading workspace |
| LumiBot | 2,060 | Build, backtest, and run Python trading strategies |

These are deliberately not five interchangeable backtesting libraries. OpenAlice is included for direct product relevance despite its smaller following. FinGPT (21,247) and FinRL (16,278) are adjacent finance-LLM and reinforcement-learning projects, respectively; matching their research scope would distract from this plan. Counts are attention snapshots, not active-user measurements. Sources: repository endpoints listed in the source inventory.

## What each competitor actually teaches us

### TradingAgents: an understandable research product

Its LangGraph workflow makes the roles and debate understandable. A user can run `TradingAgentsGraph(...).propagate(...)` or use its interactive CLI without first designing a strategy lifecycle. Its current implementation direction includes persistent decision memory and opt-in checkpoints, so those cannot honestly be advertised as capabilities absent from TradingAgents. Its README also acknowledges historical-data and nondeterminism limitations. [1]

**Our interpretation:** people can understand the experiment before understanding the framework. The intellectual output itself is interesting, even without a profitable trade. LumiBot should offer a complete research-to-trade example, plus a standalone research entry point for people who do not yet want a broker or a scheduler. Neither requires replacing existing strategies. We should also accept externally produced research, rather than demanding that every user adopt our agent orchestration.

### AI Hedge Fund: a configured thing you can run

The current README has moved beyond the older persona-demo presentation: install `aihf`, launch an interactive terminal app, build a fund, and backtest a saved mandate. Noninteractive execution emits JSON. The README still says it does not actually trade; persistent live operation is described as an evolving direction, not something to assume is already delivered. [2]

**Our interpretation:** a saved, understandable configuration and a single launch command reduce first-run decisions. Copy the convenience, not a second trading engine. LumiBot can generate an ordinary editable `Strategy` file and launch that file. Preserve the distinction between existing functionality and the competitor's roadmap when writing comparison pages.

### OpenBB: useful before adopting an entire framework

Its initial Python example fetches historical prices and converts the result into a dataframe. Its architecture exposes financial data through Python, REST, MCP, and analyst-facing surfaces. This is a reusable data layer with multiple consumers. [3]

**Our interpretation:** this is the clearest response to the concern about rigidity. Someone should be able to benefit from LumiBot without migrating their whole program. Start with documented public components that already work independently. Where components require hidden strategy state, introduce a deliberate public session contract only after an executable example demonstrates the need. Do not turn private helper imports into a promised stable API.

### AI-Trader: agent adoption is a product workflow

Its current homepage centers on an instruction that an existing agent can read to register and integrate. The repository describes a service with skills, API specifications, a frontend, signals, and community interaction. It is no longer accurate to characterize the current repository only as an autonomous-trading benchmark. Its advertised market and copy-trading capabilities were not independently execution-tested in this audit. [4]

**Our interpretation:** a coding agent needs a clear starting document, exact commands, machine-readable outcomes, and an obvious next action. LumiBot should offer a maintained agent integration recipe with a real backtest acceptance check. Copying a social trading platform, rewards system, or automatic copy-trading product is a much larger business decision and is not recommended for this library sprint.

### OpenAlice: continuity makes research useful repeatedly

OpenAlice presents a local desktop workspace with persistent files, native agents, scheduled research, and an inbox. Its broker workflow stages operations for user approval, and execution is explicitly beta. Thus, broker connectivity is not an exclusive LumiBot differentiator across this comparison set. [5]

**Our interpretation:** continuity is a stronger retention proposition than a one-off impressive answer. LumiBot should make run history and results easy to reopen and compare. A full desktop workspace would duplicate substantial hosted-product responsibilities. Begin with a portable run record and a readable local report; evaluate demand before building another application.

## What people say: evidence and limits

A Reddit developer who built a TradingAgents GUI praised the research output but disliked retrieving reports from terminal logs and files. Their fork added visible progress, a report reader, and follow-up conversations. Replies requested local data integrations and use outside US stocks; another commenter wanted to add it to a home assistant. These are concrete indications of interest in accessibility and composability. The author promotes their own fork, and explicitly had not yet established trading results. This is useful qualitative evidence, not an independent performance study. [6]

TradingAgents issue #1204 reports a provider-specific hang involving reasoning-token budgets and an OpenAI-compatible gateway. It is now **closed**. It supports testing provider failure behavior, not claiming the current project generally hangs. [7]

AI Hedge Fund issue #618 reports missing timeouts, but its body refers to TradingAgents and another gateway. That ambiguity makes it weak evidence about the current AI Hedge Fund implementation. Do not repeat it as a confirmed current defect. [8]

Grok X and web searches were used for discovery. X results included recommendations, skepticism, and observations about historical leakage, but did not provide a defensible representative sample of firsthand users. Stack Overflow and Medium searches likewise did not establish a reliable comparative satisfaction dataset. Tutorials, affiliate promotion, fork promotion, and repeated posts are not independent votes. This report therefore does not invent percentages of happy users or assert that social chatter proves why stars grew.

The defensible conclusion is narrower: readable research, easier access, local/provider choice, and composability have visible appeal; setup failures and inaccessible results create friction. The strength of each observation differs. We still need direct first-run testing with intended LumiBot users.

## What LumiBot already has

The audited tree already contains an agent runtime and manager, tools, skills, memory-related support, replay caching, and broker order tools. In particular, `lumibot/components/agents/builtins.py` includes `orders_get_status` and `orders_wait_for_terminal`. Do not create duplicate wait tools because an earlier plan suggested them.

`lumibot/components/agents/replay_cache.py` stores and loads keyed results. This is not proof of crash-safe live execution resumption. `runtime.py` contains model-call budget hooks with provider-specific constraints; that is not yet evidence of a universal user-facing cost control. Existing tests include agent runtime backtests, execution status, provider errors, account context, and managed option execution. Build upon them.

Our proposed differentiation is therefore: **Bring your Python logic or AI research; use one strategy engine to backtest it and connect supported brokers.** Prove asset/broker combinations individually. Do not claim every broker supports every asset, that all historical inputs are point-in-time, or that an agent guarantees better execution.

## Prioritized implementation handoff

The following commands and new filenames are design proposals, not currently supported interfaces. Every implementation starts by checking the current tree and adapting to existing ownership.

### 1. Make the first successful run a product

**Why:** addresses the strongest observable convenience gap with comparatively low architectural risk. This is the first adoption investment.

Add `lumibot/cli.py` and `lumibot/__main__.py`, using the existing package configuration in `setup.py`/`pyproject.toml` for one entry point. Proposed commands:

- `lumibot init my_strategy --template python` generates an ordinary Strategy subclass.
- `lumibot init my_strategy --template ai` generates a complete AI strategy with explicit provider configuration.
- `lumibot doctor my_strategy` checks Python/package compatibility, configuration presence, and selected data requirements without placing orders or silently spending on model calls.
- `lumibot backtest my_strategy` delegates to the existing backtest lifecycle.

Keep generated code small and editable. Never replace the Strategy scheduler, event loop, or broker ownership. Missing keys should identify the missing configuration and documentation link. A fresh AI run must plainly require its actual model credentials; an offline installation check is a separate command and is never called an AI run.

Update `docsrc/getting_started.rst`, `docsrc/agents_quickstart.rst`, `docsrc/agent_start_here.rst`, and the short README entry section. Retain direct Python code for users who do not want a CLI.

**Acceptance:** installation from the built package in a clean environment; both templates import; conventional backtest completes through the real engine; AI integration completes using an approved model budget; failures return useful exit codes; interrupts clean up threads. Run the existing affected backtest tests and compare an unchanged legacy Strategy's orders and results before/after. A CLI-only smoke test is insufficient.

### 2. Make every run easy to understand and reopen

**Why:** the Reddit evidence points to result access as a concrete pain. A screenshot of returns without the matching code and run context is a weak demonstration.

Inventory existing reporting in `lumibot/tools` and `docsrc/agents_observability.rst` before introducing a new renderer. Add a versioned run manifest under `lumibot/components/agents/schemas.py` or a dedicated reporting module if schemas have a different owner. Include strategy revision/hash, effective config, data window and provenance, model identifiers, decisions, tool outcomes, order/fill links, cost availability, and output paths. Redact secrets and private prompts where required.

Extend the existing report to show decisions beside actual order outcomes, with a direct path to code and data requirements. Save partial failure results too. Label a fresh stochastic run separately from recorded replay. Replay can support regression; it does not establish that fresh AI will reproduce the same returns.

**Acceptance:** success, no-trade, rejected order, partial fill, provider failure, and interrupted-run reports remain readable. No invented trades or metrics. Report rendering must not execute scripts embedded in model output. Add coverage adjacent to `tests/test_agent_execution_status.py` and existing reporting tests.

### 3. Let other research systems use LumiBot

**Why:** an integration can turn competitors' audiences into users without asking them to abandon their preferred research framework.

First publish a complete optional TradingAgents-to-LumiBot example. Define a small versioned research proposal containing asset identity, observation timestamp, source provenance, thesis, and intended action. Distinguish that proposal from an executable order. Feed it to a dedicated LumiBot trading agent with current account context; it may reject, resize, submit, and verify using existing tools. Keep broker/data integrity checks in their existing owners.

Proposed files: `lumibot/example_strategies/ai_external_research.py`, `docsrc/agents_external_research.rst`, and focused schema tests. Avoid a mandatory TradingAgents dependency or importing it at LumiBot package import time. A provider-neutral proposal schema belongs in the public package only after two consumers demonstrate it is general.

**Acceptance:** stale research, missing symbols, invalid quantities, market mismatch, and duplicate proposals are explicit outcomes. Repeat the same proposal through a real-engine integration test to expose duplicate-order behavior. Test an unchanged traditional strategy in the same release. Historical examples must not fetch present-day research and label it historical.

### 4. Productize bounded model execution

**Why:** expensive or opaque failure discourages repeat use. This improves reliability and contribution margin for anyone operating these strategies.

Extend existing ownership in `runtime.py`, `manager.py`, and relevant usage-accounting modules rather than writing a second gateway. Specify a user-facing total-run budget with provider pricing coverage, unknown-price behavior, retries, cancellation, and a durable usage ledger. Review `tests/test_agent_eval_call_budget.py`, `tests/test_agent_runtime_errors.py`, and provider accounting tests first.

Do not announce a dollar cap unless concurrent calls and retries reserve against that cap correctly. Unknown pricing must be visible. A model timeout must not trigger blind order resubmission: reconcile the order identifier and broker state first. Research checkpointing and order execution recovery are separate contracts.

**Acceptance:** interrupted calls, retries, concurrent reservations, missing usage, and exhausted budget have tests; targeted real-provider checks are cost-bounded. This is higher-risk work than the CLI and should follow it, not delay the first improvement indefinitely.

### 5. Make reuse discoverable before adding another API style

**Why:** follows the composability lesson without speculative lifecycle changes.

Audit the public research/data examples and `docsrc/imports_and_startup.rst`. Add three tested recipes: obtain supported historical data in an existing program; run research without a broker; backtest an existing Strategy from another Python application. Each recipe states what initializes threads, what requires credentials, and how resources are closed.

If standalone agent research currently requires a Strategy manager, document the real constraint and design a public session/context owner; do not instantiate internal dummy strategies. Add a session API only after its resource and tool-context contract is tested. Do not add a callback-based strategy API merely because it is shorter in a screenshot.

### 6. Build distribution around working integrations and examples

**Why:** improved activation alone will not create enough new arrivals to reach the star objective.

Publish a launch package for each completed workflow: a short real screen recording, exact runnable source, a result artifact, limitations, and a direct getting-started link. Prioritize search intent such as using external AI research in a backtest, running a Python strategy with a supported broker, and examining AI decisions alongside fills. Update `docsrc/lumibot_vs_tradingagents.rst` to reflect current capabilities and distinguish roadmap from shipping features.

Preserve creator challenge promotions and their tracked destinations. Measure their contribution separately from open-source activation; more banners are not evidence of more library adoption. Do not build a second hosted platform inside LumiBot. Hosted operation and deeper training remain separate next steps after the free library delivers value.

### 7. Maintain contributions as an acquisition channel

**Why:** contributors can add integrations, fixes, and distribution; unattended contributions waste that opportunity. Raw open-PR count alone does not prove poor maintenance.

Use `CONTRIBUTING.md` and `.github/ISSUE_TEMPLATE/` to request a runnable reproduction, version, provider, and expected versus actual behavior. Create a triage inventory with one explicit disposition per old PR: ready to review, needs author input, superseded with a linked replacement, or out of scope with an explanation. Do not bulk-close on age alone. Do not auto-merge broker or runtime changes.

Automate duplicate suggestions and missing-reproduction checks as maintainer assistance, with accountable human decisions. Measure response and decision time, merged contribution survival, and repeat contributors. No new recurring job on a personal machine is needed.

## Sequencing, economics, and measurement

Implement 1 and 2 first, then the narrow integration in 3; perform provider-budget work in 4 before broad expensive model experimentation. Use 5 to establish demand for a standalone API. Ship distribution with each completed increment, and triage contributions continuously through normal repository workflows.

This order is an expected-value judgment, not measured ROI. The revenue path is useful free workflow → engaged developer → optional hosted use or qualified training lead → settled payment → retained value after refunds and fulfillment. We do not yet have conversion and fulfillment data to rank these numerically by collected cash. CLI/report/integration work has moderate engineering cost and little required creator time; real-provider checks have explicit bounded cost. A short creator demonstration needs a bounded recording/review session. A new desktop/social platform has high engineering and support cost and is deferred.

For the first release, recruit a small mix of Python users and coding-agent users. Record clean-install completion, first valid backtest, time and interventions to completion, second-run usage, and error category. A proposed usability gate is at least 8 of 10 first-run participants completing the intended path without a maintainer editing their environment. That is a chosen target, not existing evidence.

Track GitHub net stars over comparable 7- and 28-day windows, referral traffic, example downloads where observable, contributor retention, and opt-in activation evidence. Do not silently install telemetry in the library. Link website campaigns with UTMs; GitHub stars do not provide reliable per-campaign attribution. Challenge registrations and booked calls are intermediate events; settled payments and retained customers establish commercial value.

Doubling a five-star/day baseline means roughly 10/day, tripling means 15/day. Reaching 105,000 from this snapshot would still take about 28 years at 10/day or 14 years at 20/day, ignoring churn. The long-term objective requires repeated distribution breakthroughs and compounding adoption, not a single README redesign. These arithmetic scenarios are not forecasts.

## Source inventory and verification boundary

1. [TradingAgents repository and current README](https://github.com/TauricResearch/TradingAgents). Primary architecture, setup, and current-feature claims.
2. [AI Hedge Fund repository and current README](https://github.com/virattt/ai-hedge-fund). Primary CLI, mandate, and execution-boundary claims.
3. [OpenBB repository and current README](https://github.com/OpenBB-finance/OpenBB). Primary data-interface and consumer-surface claims.
4. [AI-Trader repository and current README](https://github.com/HKUDS/AI-Trader). Primary current positioning and integration claims; advertised execution capabilities not independently exercised.
5. [OpenAlice repository and current README](https://github.com/TraderAlice/OpenAlice). Primary workspace and beta execution claims.
6. [TradingAgents GUI author and discussion on Reddit](https://www.reddit.com/r/LocalLLaMA/comments/1tm2ct0/i_built_a_local_gui_for_the_tradingagents/). First-person fork experience, with promotional and selection bias.
7. [TradingAgents issue 1204](https://github.com/TauricResearch/TradingAgents/issues/1204). Specific reported provider failure; closed when checked.
8. [AI Hedge Fund issue 618](https://github.com/virattt/ai-hedge-fund/issues/618). Ambiguous issue attribution; excluded as a confirmed current defect.
9. [LumiBot source snapshot](https://github.com/Lumiwealth/lumibot/tree/9f77692f864c910c3422d7e526f8275f9b8329b2). Local source inspection, not a new runtime qualification.
10. [GitHub repository API](https://docs.github.com/en/rest/repos/repos#get-a-repository): snapshots obtained from `repos/<owner>/<repo>` for the named repositories. Counts change continuously.

No competitor was independently installed or funded in this audit. Public documentation and selected source files establish architecture and advertised interfaces; they do not establish comparative execution reliability. No controlled user study establishes the cause of star growth. Recommendations above are hypotheses with explicit acceptance tests rather than promises of popularity or investment returns.
