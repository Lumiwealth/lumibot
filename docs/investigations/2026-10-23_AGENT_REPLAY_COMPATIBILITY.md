# Agent replay compatibility and portfolio reconstruction

Backtest replay must represent the runtime that could actually execute the decision.

**Last Updated:** 2026-10-23
**Status:** Cache and handoff checks pass; full portfolio qualification incomplete
**Audience:** Framework contributors

## Overview

Previously the agent replay identity covered prompts, skills, model and tool declarations but omitted installed dependencies and callable implementation code. Installing a missing optional dependency could therefore leave a backtest replaying the old failed lookup without running a fresh model decision.

Replay identity now includes public package versions, Python version and source hashes for bound callables and the runtime. Local paths and environment variables are excluded. Successful compatible runs still replay. Entries containing explicit failed tool results remain inspectable evidence but are not reused as decisions; this also permits recovery after a transient external failure without requiring a package change.

The portfolio examples retain one researcher and one trading agent, called every iteration. Their prompts distinguish the snapshot valuation date from the filing date, preserve a complete ledger, handle partial sales and exercises, and describe uncertainty. Both agents receive the strategy timestamp and the trader receives the previous research separately. No filing parsing, portfolio calculation or conditional AI skipping is added to strategy Python.

## Verification

The regression tests first reproduced stale failed-result reuse and unchanged identities after dependency/implementation changes. The existing compatible-success control passed before the fix. Date-handoff regressions first failed because the examples omitted their explicit strategy timestamp.

Affected local checks: **276 passed**. This covers replay compatibility, tool permissions, example structure and handoffs, synthetic stock fills, documentation contracts and public instruction hygiene. The wider 1,368-case agent inventory exceeded the bounded run window before completion; it is not a full-suite pass. Its observed documentation wording failure was repaired without weakening the assertion. A payload-only custom-runtime compatibility failure was corrected by allowing a runtime without an executable `run` method when constructing cache identity; both native and programmatic payload contracts now pass.

A fresh subscription-model stock backtest read an annual report and subsequent disclosures, but the researcher returned an incomplete reconstruction rather than weights, and the trader retained cash. The run was stopped after preserving that evidence. The shared web-document skill still requires exhaustive index manifests and original-version evidence before answering. That is a separate research-policy issue; it is not resolved by the cache repair and is not patched in this change.

Unit tests with a scripted model prove cache and handoff contracts, not real research or portfolio accuracy. No profitable backtest, complete holdings reconstruction or options qualification is claimed.

## Sources and limits

- [House financial disclosure guidance](https://ethics.house.gov/financial-disclosure/): reporting and public disclosure rules.
- [House disclosure instructions](https://ethics.house.gov/financial-disclosure-instruction-guide/): annual valuation and transaction reporting context.
- Existing portfolio reconstruction audit: `docs/research/2026-09-29-pelosi-bots/2026-09-30_SWITCH_WINDOW_GRADE_V1.md`.

Disclosure delays, value ranges, gifts and unreported changes prevent exact knowledge of a household's current holdings. This implementation is not a claim of exact replication or investment performance. No package publication or deployment is part of this source change.
