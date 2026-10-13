# AI AGENT EVALS

> Real-model release gates for LumiBot's built-in trading-agent behavior.

**Last Updated:** 2026-10-12
**Status:** Active
**Audience:** Both

---

## Overview

LumiBot agent evals call the real trading model and a separate real LLM judge.
Both use the explicitly selected ChatGPT subscription connection, defaulting to
`chatgpt/gpt-6-luna` on medium reasoning. API-funded model selections are rejected
for both roles, including when ordinary provider API keys are present. Connect
your own account with `python -m lumibot.components.agents.chatgpt_auth login`.
Review your usage policy in ChatGPT Usage: the connection can use included
allowance and an authorized existing ChatGPT credit balance. Zero remaining
included allowance does not prohibit inference when credit use is enabled.
LumiBot does not change the account-wide connected-app credit setting; OAuth
authentication alone cannot establish that setting. API-key billing, credit
purchases and automatic reload are not performed by the eval runner.
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
retains only the explicitly selected protected subscription profile and essential
OS configuration, disables LumiBot dotenv discovery before importing Strategy,
and rejects ordinary provider credentials and external broker requests.
Only the public Responses endpoint, subscription model discovery and rotating
refresh endpoint are reachable through the fixture HTTP boundary. No API key,
Codex credential extraction, customer account or external broker write is needed.

---

## Contract

The `options_expiration_with_data` case lists an unpriced nearer expiration and
a later expiration with complete contract data. Options agents must inspect
other listed expirations within the user's constraints rather than treating
one unavailable expiration as evidence that the entire chain is unusable.
They still verify every leg and price one atomic package at one expiration.

The `stock_pending_exit_no_duplicate` case protects the existing order's
ownership of a due exit. A due condition or nonmarketable limit does not by
itself authorize canceling and replacing that pending order. The case requires
inspection of the exact pending exit and a no-mutation decision.
The `stock_price_before_order` judge also requires the reported lookback
condition to match the latest completed-bar window and its tool-computed value.

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
| `options_iron_condor_limit_between_bid_ask` | Submits one iron condor whose explicit package price sits between the bid and ask, without the prompt saying to use a limit. |
| `options_expiration_with_data` | Opens the iron condor on the listed expiration that has quotes when a nearer listed expiration has none. The prompt does not name that fallback. |
| `congress_public_filings_only` | Uses the House disclosure tool and trades only filings already public at the simulated clock. |
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
python scripts/run_agent_evals.py --preflight-only --gate --max-cost-usd 0
```

This is a deterministic preflight, not a real-model pass or credential-acceptance
check. It verifies the renewable plan permission and subscription model catalog,
without inference. It does not write freshness or create a call ledger.

Run deterministic preflight and the stale catalog:

```bash
python scripts/run_agent_evals.py \
  --gate \
  --repeat 3 \
  --max-workers 3 \
  --max-cost-usd 0 --max-model-calls 500
```

Run one changed case three times:

```bash
python scripts/run_agent_evals.py \
  --case-id options_credit_spread_close_signed_quantities \
  --repeat 3 \
  --max-cost-usd 0 --max-model-calls 80
```

The runner fails before inference when schemas, fixtures, plan permissions,
model availability or artifacts are invalid. `--max-cost-usd` must be zero; the
call cap covers actors, judges, continuations and bounded outer retries.

## Durable Evidence

Each run writes:

- append-only `ledger.jsonl` with one fsynced record per repetition;
- append-only `model_calls.jsonl` with fsynced reservations and settlements;
- incremental `progress.json` after each repetition and terminal `summary.json`;
- model, judge, token, timing, and estimated-cost totals;
- fixture versus real external-write classification;
- case and runtime fingerprints used by the freshness gate.

The call ledger owns a finite request allowance across resumes and concurrent
workers. Unknown usage after timeout or process death retains that request's
reservation; it is never treated as an unused call. The ledger records actual
input, cached-input, output and reasoning usage when the provider supplies it.
Zero API spend is not proof that ChatGPT's separate paid-credit setting is off.

SDK retries are disabled; outer retries require another ledger reservation.
No paid token-counting endpoint is used. Subscription quota and revoked grants
produce visible failures, not successful decisions. A corrupt ledger or changed
call cap fails closed. Do not switch output directories to reset an approved
release-attempt allowance. CI and local qualification need the same attempt
ledger or an explicitly reconciled remaining allowance. No prompt or credential
is written to the per-call ledger.

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

Both workflows use an authorized private evaluator selected by the repository
variable `LUMIBOT_SUBSCRIPTION_EVAL_RUNNER`, with the `subscription-evals`
environment and a shared non-canceling concurrency group. Register the worker
for this repository only, restrict the environment to trusted maintained
revisions, and connect a dedicated protected SIWC profile outside checkout.
Do not place rotating refresh tokens in public workflows or fork artifacts.
Without that worker/profile arrangement, the default runner fails authentication
visibly; an environment name alone does not establish its protections.

The manual `LumiBot Agent Evals` workflow uses the same runner and contracts. A
manual pass is useful for qualification, but release publication still verifies
the gate for the exact tagged candidate.

### Interrupted completion receipts

If a process stops after appending completed repetitions but before writing the
freshness file, resume reconstructs the receipt from the matching case/runtime
fingerprint in the durable ledger. It preserves the last matching run's original
passing timestamp and still requires the full consecutive pass count. Unrelated
fingerprints cannot renew the receipt, and an expired or incompatible receipt
cannot qualify a different release candidate. This recovery does not invoke the
model again for the already completed matching repetitions.
