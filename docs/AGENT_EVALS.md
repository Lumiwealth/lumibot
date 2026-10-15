# AI AGENT EVALS

> Real-model release gates for LumiBot's built-in trading-agent behavior.

**Last Updated:** 2026-10-12
**Status:** Active
**Audience:** Both

---

## Overview

LumiBot agent evals call the real trading model and a separate real LLM judge.
Both use the explicitly selected ChatGPT subscription connection. Actors default
to `chatgpt/gpt-6-luna` on medium reasoning; semantic judges use
`chatgpt/gpt-6.1-sol` on high reasoning after observed arithmetic/effect errors in
the lighter judge. API-funded model selections are rejected
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
python scripts/run_agent_evals.py --preflight-only --gate --max-cost-usd 0 \
  --output-root <owned-output>
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
  --max-cost-usd 0 --max-model-calls 500 --output-root <owned-output>
```

Run a changed, failed, or high-risk case five times in the same release-attempt ledger:

```bash
python scripts/run_agent_evals.py \
  --case-id options_credit_spread_close_signed_quantities \
  --repeat 5 \
  --max-cost-usd 0 --max-model-calls 500 --output-root <owned-output>
```

The runner fails before inference when schemas, fixtures, plan permissions,
model availability or artifacts are invalid. `--max-cost-usd` must be zero; the
call cap covers actors, judges, continuations and bounded outer retries.

Calibrate affected research rubrics with real judges and labeled safe/unsafe
controls under that same initialized allowance:

```bash
python scripts/check_agent_judge_calibration.py \
  --case-id research_macro_point_in_time \
  --model-call-ledger <owned-output>/model_calls.jsonl \
  --output-root <owned-calibration-output> --max-model-calls 500 --max-cost-usd 0
```

Calibration controls are not agent passes and never create case freshness. Both
runners persist an incomplete attempt before inference, then its completed or
failed result. Resume preserves completed unique repetitions and recovers older
native-only interruptions without forgetting their consumed calls.

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
release-attempt allowance. Actor, judge and rubric calibration need the same attempt
ledger. CI verifies the completed evidence without inference. No prompt or credential
is written to the per-call ledger.

Fingerprints include indicator and broker code plus installed ADK, GenAI,
LiteLLM, pandas-ta, pandas, NumPy, Alpaca, HTTPX, OpenAI, PyJWT, FileLock and Pydantic
versions. Install `requirements_dev.txt` with `-c requirements_agent_evals.txt`
for the authenticated qualification. Compatible passes retain
their original timestamp rather than being renewed when an unchanged gate skips
them. The current shared fingerprint is conservative: a shared dependency change
can invalidate more than one case.

Do not commit API keys, prompts containing secrets, or customer data in eval
cases or artifacts.

## Release Gate

The PyPI release workflow requires the `agent-evals` job alongside build, unit,
and backtest jobs. Run real actors and judges on the authenticated qualification
host, review their private native artifacts, then export a compact public-safe
receipt for the complete catalog:

```bash
python scripts/qualify_agent_evals.py create \
  --output-root <owned-actor-output> \
  --calibration-output-root <owned-calibration-output> \
  --model-call-ledger <owned-actor-output>/model_calls.jsonl
python scripts/qualify_agent_evals.py verify
```

Before exporting, resume the full actor catalog and judge controls using the
same owned directories and immutable call allowance. `--gate --repeat 1` still
requires every new or changed case's configured three or five consecutive passes;
it preserves completed matching repetitions and only schedules missing or failed
work. Avoid `--force` when resuming a qualification. Set `TMPDIR` to a dedicated
task-owned directory so production fixture memory is removed by its normal close
path, including failures.

```bash
python scripts/run_agent_evals.py --preflight-only --gate --repeat 1 \
  --max-cost-usd 0 --max-model-calls <existing-call-cap> \
  --output-root <owned-actor-output> --freshness-state <owned-freshness-file>
python scripts/run_agent_evals.py --gate --repeat 1 --max-workers 5 \
  --max-cost-usd 0 --max-model-calls <existing-call-cap> \
  --output-root <owned-actor-output> --freshness-state <owned-freshness-file>
python scripts/check_agent_judge_calibration.py \
  --model-call-ledger <owned-actor-output>/model_calls.jsonl \
  --output-root <owned-calibration-output> \
  --max-model-calls <existing-call-cap> --max-cost-usd 0
```

Normal release qualification runs on the version branch's pull request into
`dev`, LumiBot's default branch. After that reviewed merge and its required
checks pass, tag that exact `dev` merge for the publishing workflow. Read back
the published package with an isolated installation before advancing the next
version branch or downstream runtime pin. LumiBot has no `main` branch.

Commit `ci/agent-evals/qualification.json` with the tested candidate. The manual
`LumiBot Agent Evals` workflow and the PyPI release job both verify that receipt
on ordinary GitHub workers using the exact qualified SDK constraints. They make
no model calls and contain no developer login. A missing case, mismatched source
or SDK, stale or future timestamp, duplicate repetition, failed machine/judge
result, missing actor/judge usage, or incomplete native call blocks publication.
The receipt binds each settlement to its original attempt and source fingerprint;
an older attempt cannot donate model usage to a new passing row.
Starting a new matching eval attempt invalidates the existing exported receipt
before inference. A failed or interrupted attempt therefore cannot leave an old
green file publishable. Only exporting the complete recovered qualification clears
that invalidation; failed export also preserves the invalid marker.

The release owner must review the real native evidence before committing the
receipt. Hashes bind evidence and do not cryptographically prove inference. The
normal repository review and release authority remain the trust boundary. Full
prompts, transcripts and private artifacts stay out of the public receipt.

This replaces the old successful-workflow freshness ZIP import, which could
accept a freshness-only archive without native actor/judge evidence. Its helper
and normal workflow path are removed. Existing native ledgers stay recoverable;
old unbound call records are retained for cumulative usage but cannot qualify the
new receipt format. Rollback requires the normal reviewed source revert and
requalification; a cache or environment name cannot bypass the gate.

### Interrupted completion receipts

If a process stops after appending completed repetitions but before writing the
freshness file, resume reconstructs the receipt from the matching case/runtime
fingerprint in the durable ledger. It preserves the last matching run's original
passing timestamp and still requires the full consecutive pass count. Unrelated
fingerprints cannot renew the receipt, and an expired or incompatible receipt
cannot qualify a different release candidate. This recovery does not invoke the
model again for the already completed matching repetitions.
Newer incomplete native attempts supersede older passes. Export rejects them;
resume reruns the affected repetition, including failed repetitions above a
smaller newly requested repeat count. Fixture memory databases and exports belong
to their exact temporary fixture and are removed in its final cleanup.


### Qualification model policy

New qualification uses `chatgpt/gpt-6-luna` for both roles. Actors start at Low.
Low and Medium judges each passed 61 of 62 labeled control calls but falsely
rejected one repetition of the preserved safe historical-disclosure control.
Low misread the disclosed option value range; Medium incorrectly required an
extra UTC conversion for an aware timestamp comparison. The candidate Luna
judge therefore uses High and must pass the unchanged complete calibration set
before qualification. This is evidence-backed reasoning escalation within Luna;
it does not authorize a Sol, Astra or API-funded call.
The subscription transport explicitly requests the default service tier and
rejects priority/Fast. The runner rejects unapproved model escalation before
inference. Preserve older receipts with their original model names; they do not
become Luna qualifications when defaults change. Calibrate Luna against the
existing labeled safe/unsafe controls before actor qualification. Retain the
rubric and critical arithmetic, financial, privacy and historical controls.
Any stronger profile requires evidence of the cheaper profile's limitation;
an expensive-model exception additionally requires explicit human authorization.

The generic judge instructions distinguish binary floating-point residue from
meaningful price differences, and relational eligibility filtering from the
rows returned by a SQL query. A typed timestamp comparison normalizes instants;
an additional formatting operation is not required. Future-document access,
lexical timestamp filtering and unproved historical availability still fail the
unchanged negative controls. These are grading corrections, not actor policy,
fixture-label changes or substitutes for real-model qualification.
