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
The research instructions were clarified. Earlier five-trial checks passed
three and failed two, including omitted updates, a duplicate instrument,
incorrect timestamp prose and a blocked future-document attempt. These failures
remain preserved. After compact selections, complete current tool results and
explicit SQL-produced UTC timestamps, the targeted historical reconstruction
case passed all five native Luna actor/judge trials. One regression each for
approximation, live undated sources, estimated historical dates, date-only
publication and known-future exclusion also passed. These are scoped source
checks, not a complete package-release qualification.
The approximation benchmark also passes against the earlier product policy;
it remains compatibility coverage outside the release-gated case catalog.

Inspection found a callback truncating every tool result larger than 4,000
characters irrespective of conversation size. A failed trial received a pruned
discovery index. The authorized generic repair removes that callback and retains
the existing before-model pruning of older history only when the conversation
exceeds its context budget. Nine cross-provider row-retention tests failed
before repair while three skill controls passed. All 50 focused tests and the
agent/subscription owners pass, with 1,220 passing and three integration tests
outside the unit scope. This does not establish truncation as every model
error's cause.
No failed or interrupted run is counted as a pass. Case, source, model, usage and
verdict identities remain attached to their actual runs.
