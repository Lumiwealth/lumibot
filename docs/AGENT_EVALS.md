# AI AGENT EVALS

> Real-model release gates for LumiBot's built-in trading-agent behavior.

**Last Updated:** 2026-08-11
**Status:** Active
**Audience:** Both

---

## Overview

LumiBot agent evals call the real trading model and a separate real LLM judge.
They verify model behavior against fixture-backed production tool contracts while
preventing broker writes, customer access, and unnecessary historical-data cost.
Passing evidence is valid for 90 days only when the case content, runtime skills,
tool descriptions, rules contract, acting model, and judge model have the same
fingerprint.

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
- The runner appends one durable ledger row after every repetition and supports
  resuming only missing or failed work.

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
  --force \
  --max-cost-usd 2
```

The runner fails before paid calls when case schemas, fixtures, credentials,
model pricing, artifacts, or the requested total cost budget are invalid.

## Durable Evidence

Each run writes:

- append-only `ledger.jsonl` with one fsynced record per repetition;
- incremental `summary.json` with pass, fail, missing, skipped, and resumed counts;
- model, judge, token, timing, and estimated-cost totals;
- fixture versus real external-write classification;
- case and runtime fingerprints used by the freshness gate.

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
