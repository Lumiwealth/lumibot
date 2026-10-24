# Agent replay compatibility and portfolio reconstruction

Backtest replay must represent the runtime that could actually execute the decision.

**Last Updated:** 2026-10-24
**Status:** Cache and handoff checks pass; research and basket execution qualification incomplete
**Audience:** Framework contributors

## Overview

Previously the agent replay identity covered prompts, skills, model and tool declarations but omitted installed dependencies and callable implementation code. Installing a missing optional dependency could therefore leave a backtest replaying the old failed lookup without running a fresh model decision.

Replay identity now includes public package versions, Python version and source hashes for bound callables and the runtime. Local paths and environment variables are excluded. Successful compatible runs still replay. Entries containing explicit failed tool results remain inspectable evidence but are not reused as decisions; this also permits recovery after a transient external failure without requiring a package change.

The portfolio examples retain one researcher and one trading agent, called every iteration. Their prompts distinguish the snapshot valuation date from the filing date, preserve a complete ledger, handle partial sales and exercises, and describe uncertainty. Both agents receive the strategy timestamp and the trader receives the previous research separately. No filing parsing, portfolio calculation or conditional AI skipping is added to strategy Python.

## Verification

The regression tests first reproduced stale failed-result reuse and unchanged identities after dependency/implementation changes. The existing compatible-success control passed before the fix. Date-handoff regressions first failed because the examples omitted their explicit strategy timestamp.

The initial cache/handoff checks passed **276 tests**. After the research revision, the focused skill, instruction, example, SDK-rendering, evaluation-harness and public-hygiene checks pass **552 tests**. These are deterministic checks, not a complete real-model qualification. The wider 1,368-case agent inventory exceeded the bounded run window before completion; it is not a full-suite pass. Its observed documentation wording failure was repaired without weakening the assertion. A payload-only custom-runtime compatibility failure was corrected by allowing a runtime without an executable `run` method when constructing cache identity; both native and programmatic payload contracts now pass.

A fresh subscription-model stock backtest first read an annual report and subsequent disclosures but withheld estimated weights, leaving cash unchanged. The shared research policy has since been revised to permit requested estimates without a complete archive or original-version hashes. It preserves explicit publication-time comparisons, conservative date-only availability and exclusion of known later versions. For a timestamped index, eligible links are selected before reading its reports; an undated source remains usable with a disclosed estimate. Direct publisher-index hints remain inside the examples' own prompts.

Native controls caught attempts to follow known future links during an intermediate revision. The revised workflow keeps selected rows separate from excluded diagnostics and prohibits opening a known later page merely to confirm its metadata. Other controls caught incorrect optional sensitivity calculations and a mislabeled timestamp; these are failed evidence, not accepted qualification. The judge continues to reject incorrect amounts, incorrect timestamps and future access. It does not demand a particular SQL syntax or a narration of every excluded candidate when the evidence and actual exclusions are correct.

A later one-session stock run produced estimated weights and **one actual broker-simulated fill**, but it omitted an earlier eligible disclosure and did not execute the basket. The first stock order was accepted; subsequent submissions were rejected by account-readiness checks and were not recovered. This is a reproduced execution-guidance issue, not a successful portfolio backtest. No readiness check is removed by this research change.

A publisher URL hint initially used a brace-delimited year token, which the native SDK interpreted as a missing session variable before inference. The hint now uses angle brackets. A regression exercises the actual SDK instruction renderer without a provider call; both examples must render unchanged with empty session state.

Unit tests with a scripted model prove cache and handoff contracts, not real research or portfolio accuracy. Research-only stage checks are labeled separately from full backtests. No profitable backtest, complete holdings reconstruction, complete basket execution or options qualification is claimed.

## Sources and limits

- [House financial disclosure guidance](https://ethics.house.gov/financial-disclosure/): reporting and public disclosure rules.
- [House disclosure instructions](https://ethics.house.gov/financial-disclosure-instruction-guide/): annual valuation and transaction reporting context.
- Existing portfolio reconstruction audit: `docs/research/2026-09-29-pelosi-bots/2026-09-30_SWITCH_WINDOW_GRADE_V1.md`.

Disclosure delays, value ranges, gifts and unreported changes prevent exact knowledge of a household's current holdings. This implementation is not a claim of exact replication or investment performance. No package publication or deployment is part of this source change.
