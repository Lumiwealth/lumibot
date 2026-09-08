# AI AGENT EVALS

> Real-model release gates for LumiBot's built-in trading-agent behavior.

**Last Updated:** 2026-09-08
**Status:** Active
**Audience:** Both

---

## Overview

LumiBot agent evals call the real trading model and a separate real LLM judge.
They verify model behavior through the real Strategy, AgentManager and built-in
tool bindings, with fixture OHLCV and broker responses underneath those APIs, while
preventing broker writes, customer access, and unnecessary historical-data cost.
Passing evidence is valid for 90 days only when the case content, runtime skills,
tool descriptions, rules contract, acting model, and judge model have the same
fingerprint.

The harness no longer defines simplified account, market, pricing, or order
functions. It uses real Data, Asset, Position and Order objects and the actual
BacktestingBroker to settle orders. Research uses a local stdio MCP fixture,
including real tools/list discovery and the manager's historical-date binding.
This is not a claim of hosted authorization or provider-data integration.
The judge sees broker-observed order states separately from the model's claims.
Execution cases fail if no simulated fill exists; provider/runtime failures
cannot pass as completed decisions.

Each repetition receives a unique strategy identity and local replay directory;
remote replay reads/writes are disabled at the cache transport boundary. Live
hosted gateway and broker credentials never enter the fixture process. The CLI
imports only the approved Gemini key, disables LumiBot dotenv discovery before
importing Strategy, and rejects non-inference HTTP requests at the transport
boundary. This prevents a developer's default broker from starting during an
import. No customer account or external broker writes are needed.

---

## Contract

- Every file under `agent_eval_cases/` calls the real LumiBot agent runtime.
- Deterministic checks cover exact tool contracts, order count, ordering, IDs,
  safety boundaries, and artifact availability.
- The LLM judge scores strategy meaning, tool-result interpretation, contract
  correctness, and the final answer.
- External writes are fixture-backed unless a case is explicitly designated as
  a safe integration gate.
- New or materially changed cases must preserve an honest failing baseline, then
  pass three consecutive targeted repetitions.
- A case passes the release gate only when every required repetition passes.
- The runner reserves budget in a durable per-model-call ledger before every
  actor, judge, continuation and outer retry. It also records each repetition
  and resumes only missing or failed work.

## Initial Catalog

| Case | Release behavior |
| --- | --- |
| `options_iron_condor_atomic_open` | Loads options guidance, verifies four contracts, prices the package, and submits one atomic iron condor. |
| `options_credit_spread_close_signed_quantities` | Maps signed positions to correct closing sides and prevents duplicate or escalating closes. |
| `options_single_leg_chain_and_quote` | Retrieves a chain, verifies the exact contract, and checks current option market evidence. |
| `stock_price_before_order` | Retrieves current stock price evidence before any stock order. |
| `stock_pending_exit_no_duplicate` | Inspects an existing pending exit and refuses to submit a duplicate position change. |
| `stock_orb_completed_bars` | Uses a completed 09:30 ET opening range and a completed breakout bar before ordering. |
| `rules_active_override_strategy_prompt` | Applies only active canonical rules and gives them precedence over conflicting strategy prose. |

The preserved credit-spread red baseline lives under `agent_eval_baselines/`.
It records the real historical reversing sides, repeated closes, and quantity
escalation that the repaired catalog protects against.

## Local Commands

Validate schemas, actual tool bindings, MCP discovery and freshness without
reserving budget or calling a model:

```bash
python scripts/run_agent_evals.py --preflight-only --gate --max-cost-usd 4
```

This is a deterministic preflight, not a real-model pass or credential-acceptance
check. It does not write a freshness receipt or create a spending ledger.

Run deterministic preflight and the stale catalog:

```bash
python scripts/run_agent_evals.py \
  --gate \
  --repeat 3 \
  --max-workers 3 \
  --max-cost-usd 10
```

Run one changed case three times:

```bash
python scripts/run_agent_evals.py \
  --case-id options_credit_spread_close_signed_quantities \
  --repeat 3 \
  --max-cost-usd 2
```

The runner fails before paid calls when case schemas, fixtures, credentials,
model pricing, artifacts, or the requested total cost budget are invalid.

## Durable Evidence

Each run writes:

- append-only `ledger.jsonl` with one fsynced record per repetition;
- append-only `model_calls.jsonl` with fsynced reservations and settlements;
- incremental `progress.json` after each repetition and terminal `summary.json`;
- model, judge, token, timing, and estimated-cost totals;
- fixture versus real external-write classification;
- case and runtime fingerprints used by the freshness gate.

The call ledger owns the cap across process resumes and concurrent workers.
Native eval calls also use the provider's count-only endpoint for the exact
system instruction, tools and continuation history. A separate durable input
window paces actor and judge calls per model at 200,000 input tokens per minute.
This is an eval-worker throttle, not a customer quota or a provider-account cap.
It addresses the observed 250,000-token free-tier limit without serializing
entire cases, resetting spending, or enabling hidden SDK retries.
Unknown usage (including timeout or process death) retains the entire reserved
maximum; resuming cannot treat it as free. A corrupt ledger or changed cap/pricing
fails closed. An older run without a call ledger cannot safely resume inference
until its spending is reconciled. Do not start a new output directory to reset
an approved release-attempt cap. CI and local qualification must carry the same
attempt ledger or an explicitly reconciled remaining allocation.

Budgeted native Gemini requests disable SDK-level retries; any outer retry gets
a new reservation. The optional budget does not change ordinary agent runs or
provider account limits. No prompt or credential is written to the call ledger.
Cost with unsettled calls is a conservative committed amount, not a claim of
provider-settled billing. Native model context/output limits bound reservations.

Fingerprints include indicator and broker code plus installed ADK, GenAI,
LiteLLM, pandas-ta, pandas, NumPy and Alpaca SDK versions. Compatible passes retain
their original timestamp rather than being renewed when an unchanged gate skips
them. The current shared fingerprint is conservative: a shared dependency change
can invalidate more than one case.

Do not commit API keys, prompts containing secrets, or customer data in eval
cases or artifacts.

## Release Gate

The PyPI release workflow requires the `agent-evals` job alongside build, unit,
and backtest jobs. It restores the last passing freshness state, reruns only stale
cases, saves refreshed state, and uploads the ledger and summary. Publishing
cannot begin if the eval gate fails or lacks valid fresh evidence.

The manual `LumiBot Agent Evals` workflow uses the same runner and contracts. A
manual pass is useful for qualification, but release publication still verifies
the gate for the exact tagged candidate.
