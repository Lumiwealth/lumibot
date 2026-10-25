# Generic agent replay and historical research compatibility

**Updated:** 2026-10-25
**Status:** Targeted qualification in progress; no release or deployment claim.

## Platform scope

These changes apply to every research-driven strategy. They would be useful
without any particular portfolio, publisher or user. Source-specific discovery
hints and investment rules belong in the strategy's own prompts.

Replay identity includes installed public package versions, Python version and
callable/runtime source hashes. It excludes environment values and local paths.
Explicit failed tool results remain inspectable but are not reused as decisions;
compatible successful decisions still replay. Installing a missing optional
dependency or repairing a failed transport therefore permits a fresh decision.

Historical research may use disclosed date, quantity and valuation estimates.
Missing original-version hashes or an exhaustive archive does not prohibit a
supported approximation. Known future publications and revisions remain excluded.
Snapshot dates, transaction dates and publication dates have different meanings.
Apply post-snapshot changes once and reconcile separately identified instruments.
A failed lookup does not establish a liquidation or empty portfolio.

Subscription qualification retains append-only usage and unknown reservations.
API-equivalent estimates are informational, never charges or admission limits.
Call reporting windows replenish automatically. API-key fallback and unapproved
model escalation remain prohibited. Qualification actors and judges explicitly
disable elapsed turn limits through the native SDK transport; functional errors,
authorization, trading and source-integrity assertions remain required.

## Verification

Cache tests reproduce stale failed-result reuse and changes in dependency or
callable identity; compatible-success controls preserve replay behavior.
Examples retain exactly one trading agent, call all configured agents each
iteration, and pass simulation time and previous research without Python data
fetching, parsing or portfolio calculations between agents.

The targeted deterministic suite passes 634 tests. Subscription reporting and
transport checks pass 88 tests, including retained unknown usage, atomic parallel
reservations, zero API billing, real SDK streaming shape, and unchanged model/auth
boundaries. A static qualification policy rejects positive, aliased and nested
turn deadlines. These counts are focused repair evidence, not a complete package
release gate.

The neutral approximation case passed five native actor/judge trials, including
partial sales, repeated pre-snapshot transactions, separate stock/option rows and
usable midpoint weights. Another historical-source case exposed a blocked future
document attempt despite a correct final answer; its decision remained incomplete.
The research instructions were clarified. Subsequent five-trial checks each
passed three trials and failed two: the remaining failures include omitted
eligible updates, an invented duplicate instrument, incorrect timestamp prose,
and a blocked future-document attempt. None qualifies the shared research path
for release. The most recent targeted deterministic guidance/example/harness
check passes 416 tests. Ten targeted real-model judge calibration checks pass,
including three repetitions of the new recorded positive control. The judge now
receives the actor's actual system and task instructions, so an empty rules-tool
response does not erase an explicit research-only assignment.

Inspection also found an existing callback truncating every tool result larger
than 4,000 characters irrespective of actual conversation size. A failed trial
received a pruned discovery index. That behavior remains unchanged pending
separate authorization for its generic repair; these findings do not establish
that truncation caused every model error.
No failed or interrupted run is counted as a pass. Case, source, model, usage and
verdict identities remain attached to their actual runs.
