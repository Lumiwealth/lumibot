# Restore useful BotSpot MCP research connections

Remove the added endpoint policy and blanket hosted-backtest denial requested by Rob.

Last Updated: October 22, 2026
Status: Source repair; package and production deployment are separate
Audience: LumiBot maintainers and the BotSpot release owner

## Overview

Rob rejected two restrictions introduced during the research-connection effort:
a BotSpot-specific URL policy in LumiBot and a Node gate rejecting hosted-backtest
research until a new server-attested clock existed. They disabled otherwise valid
connections or reads without delivering the requested capability. This repair
removes both additions rather than introducing another optional mode or bypass.
The earlier server-clock prerequisite is superseded by Rob's explicit instruction.

The active item is this restriction removal and a review of the adjacent connection
path. It does not resume the earlier package release, website qualification,
supplier integration, customer strategy work or an unrelated platform redesign.

## Starting sources and changes

- LumiBot: active `version/4.6.25`, starting source
  `b5429af91775397c391c289a116e0fe067ecff27`. The shared checkout was on an older
  version and was left untouched. An isolated detached worktree was created from
  the fetched active branch. No version number, release marker or tag changed.
- Node: current GitHub Main was fetched into an owned worktree. The repair
  `fix(mcp): restore granted research reads in hosted backtests` is commit
  `c8b48dd84`; the normal local admission publisher read back Main at
  `0f681ee705dbe4db8d26b7a6508ff6cc052441be` after reconciliation.
- LumiBot removes the extra URL parsing loop from the automatic connection.
  Private HTTP, URL authentication, query parameters and fragments are handed to
  the normal MCP transport. Malformed addresses may still fail in that transport;
  accepting configuration is not a claim that an unreachable server works.
- Node removes only the workload-kind check from the common tool-access owner.
  A valid hosted backtest no longer receives an unconditional capability error
  before its data, document, Marketplace or Community handler can run.
- Public connection documentation and the Unreleased changelog describe the
  restored behavior. Historical changelog entries remain unchanged.

## Adjacent restriction review

| Existing owner inspected | Finding and disposition |
| --- | --- |
| LumiBot automatic MCP attachment in `manager.py` | Removed the additional BotSpot URL policy. Missing URL/key still produces a configuration notice because no usable connection is configured. Ordinary tools remain available. |
| Generic `MCPServer` in `schemas.py` | Retained normal transport construction and optional token-renewal checks. Same-origin renewal prevents an automatic refresh capability being sent to another service; it is not a ban on ordinary research URLs or a required renewal endpoint for static keys. |
| Remote MCP transport and errors in `runtime.py` | Retained visible server errors, expired-token renewal and private-token redaction. These avoid a denied operation being falsely reported as a successful research result. |
| LumiBot simulation-date binding | Retained ordinary simulation-time bounds for data/document requests. This is the existing look-ahead behavior, not the removed requirement for an isolated trusted replay controller. |
| Node common `toolAccess.ts` | Removed the blanket hosted-backtest block. Missing permission errors remain. |
| Node research router, workload validation and hosted grant owner | Retained identity, current lifecycle, ownership, selected key, consent, generation, expiry and revocation checks. No trade/deploy/secret permission is silently added. |
| Node data-catalog handlers | Existing per-source availability/version errors remain with the actual source owner. Research is no longer rejected solely because the request came from a backtest. |
| Optional CLI and connection notice | No duplicate URL ban was found. The unlinked notice is informational and ordinary strategy tools stay available. CLI account setup was not invoked and no environment file was edited. |

This is an audit of the connection work and its adjacent owners, not a certification
that every restriction anywhere in LumiBot or BotSpot is necessary. The existing
Research preset is an explicit tool inventory, with a separate general MCP
configuration path; this repair does not add a tool filter, phrase classifier,
strategy/customer exception or parallel authorization owner.

## Reproduction and verification

Node tests use signed synthetic workload tokens, the real HTTP MCP adapter and
registered handlers. Persistence and suppliers are explicit fixtures. Before the
source repair the new common access regression failed and four HTTP cases failed.
The expanded complete owners passed 134 tests afterward, including permitted reads
in every research family, denied writes and propagation of source-specific errors.
Normal Main admission then passed 11,369 tests with 22 existing skips, type checks,
quality checks, unchanged MCP schema and the release-backtest routing contract.
There is no website change; the replaced guard only received hosted runtime context.

LumiBot's five accepted-endpoint regressions all failed before removal. All 98
connection-owner tests passed after removal. New local HTTP tests then exercised
automatic attachment with query parameters and fragments through an actual MCP
server and returned the exact expected data. The first HTTP assertions incorrectly
assumed `structuredContent` was always used; MCP also supports JSON text results.
The assertions now check the identical exact payload from either supported form.
The complete transport owner passed all six tests after that fixture correction.

The copied Python environment initially had older ADK/LiteLLM dependencies. It was
updated only inside the owned worktree using `requirements_agent_evals.txt`, with
MCP 1.30.0 and HTTPX 0.28.1. Final deterministic checks use ADK 2.11.0, GenAI 2.29.0
and LiteLLM 1.105.0. No model was invoked. The cumulative inference journal and its
reservations were not modified; no new budget, supplier account or purchase exists.

Final supported-tuple qualification passed **272 tests**, with two existing dependency
warnings, across these complete specifications:

```text
PYTHONPATH=. .venv/bin/python -m pytest -q
  tests/test_agent_botspot_research_mcp.py
  tests/test_agent_runtime_mcp_transports.py
  tests/test_agent_runtime_remote_mcp.py
  tests/test_botspot_research_hint.py
  tests/test_agent_tool_permissions.py
  tests/test_agent_runtime_provider_keys.py
```

The command was guarded by `safe-timeout 240`. Its real local MCP subprocesses
and ephemeral listeners were shut down by the existing fixture `finally` paths.
There were zero model calls, broker requests, provider requests or deployments.

## Evidence and release limits

Registered private artifact runs: Node `7f15af4d-1266-4386-b39f-d8728832953d` and
LumiBot `62b030f3-b3ac-436f-b32b-6cdff17322cd`. Node's normal author proof store
retains its source-bound qualification; the completed copy and branch were removed
through `agent-worktree finish`. The run owner removes disposable working output
at completion, with this small source/test receipt preserved in Git.

This work proves the machine connection/permission contracts locally. It does not
claim new native actor/judge qualification, supplier historical snapshots, an AWS
backtest, a newly published LumiBot package, or served production repair. The
existing production evaluation gates remain intact. Release owners must identify
the exact installed/served candidate before making a production completion claim.
No customer account, running workload, production service or secret was changed.
