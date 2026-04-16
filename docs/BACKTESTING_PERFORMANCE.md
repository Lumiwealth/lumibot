# BACKTESTING_PERFORMANCE.md — Speed, Parity, and Cost (Without Breaking Accuracy)

> A practical, evidence-driven guide to **measuring**, **debugging**, and **improving** backtesting performance end‑to‑end (strategy → data → cache → artifacts → UI), while preserving broker‑like correctness.

**Last Updated:** 2026-01-26  
**Status:** Active  
**Audience:** Developers, AI Agents (engineering docs)  

---

## Backtesting Definitions (Accuracy + Speed)

**Accuracy (gold standard):** if we can replay a period that was traded live and reproduce the broker’s realized behavior (fills + PnL) within defined tolerances (tick size, fees model). Vendor parity (e.g., DataBento artifacts) is a regression signal, not “truth”.

### Accuracy validation ladder (Tier 3 is the real gold standard)

- **Tier 1 (regression):** vendor parity / stored artifact baselines (e.g., DataBento-era runs) to detect drift.
- **Tier 2 (audit):** manual reviews around known hard edges (session gaps, holidays/early closes, rolls, rounding).
- **Tier 3 (gold):** **live replay baseline** — replay an interval that was traded live and reproduce broker fills + realized PnL within tolerances.

**Speed:** warm-cache runs are queue-free and complete in bounded wall time, with evidence (request counts, cache hit rate, iterations/sec, and wall-time split: data wait vs compute vs artifacts).

**Resilience:** backtests should not “fail” solely because post-processing (stats/tearsheets/plots) crashed. When post-processing fails, the run should still:
- preserve the trade stream (`trades.csv`) and portfolio stats (`stats.csv`) when available,
- classify the failure (simulation vs postprocess vs upload),
- and emit actionable diagnostics rather than silently omitting artifacts.

## Overview

Backtesting performance problems in LumiBot rarely have a single cause. “Slow backtests” usually come from one (or more) of:

1) **Startup latency** (task scheduling, container boot, python import time, dotenv scanning, first progress write)  
2) **Data hydration** (remote downloader/Theta queue waits, request fanout, cache misses)  
3) **Cache IO** (S3/local cache design: many small objects vs fewer larger chunks)  
4) **Compute** (pandas transforms, per-bar strategy logic, option pricing, portfolio valuation)  
5) **Artifacts** (tearsheets, indicators, plots; QuantStats/pandas/seaborn costs)  
6) **Progress + logging** (progress heartbeat, DB update frequency, logfile generation)  

This document is a **brain dump** of what we’ve learned while improving performance across option-heavy backtests (NVDA/SPX/Strategy Library demos), and it codifies the workflows, measurement discipline, and architectural principles that keep speed work from turning into correctness regressions.

> **Core principle:** Accuracy / realism comes first. Speed work must not change strategy semantics silently.

---

## How to use this doc

- If you’re new: read **Sections 1–5** (system model + how to measure).
- If a backtest is “hours”: jump to **Section 6** (pattern recognition) and **Section 7** (Theta/options fixes).
- If production is slower than local: read **Section 10** (parity methodology) and **Section 11** (startup).
- If costs are out of control: read **Section 12** (scaling + idle fleets).

When you learn something new:
- add it here (canonical playbook),
- add a date-first investigation under `docs/investigations/` when it’s a deep dive,
- update public docs (`docsrc/`) for anything user-facing.

---

## Table of contents

1. Goals and non-goals  
2. Performance is a system: phase model  
3. Evidence checklist (what to record in every investigation)  
4. The #1 rule for speed work: measure twice (cold vs warm)  
5. Cache semantics and how to tell if you’re “warm”  
6. Common slow patterns (what the logs look like)  
7. Common root causes and fix patterns (ThetaData/options focus)  
8. Profiling with YAPPI (how to attribute time)  
9. Performance baselines and history (what we track automatically)  
10. Production vs local parity (apples-to-apples comparisons)  
11. Startup latency (submit → first progress row)  
12. Cost + scaling (avoiding expensive idle fleets)  
13. Accuracy audits and telemetry (MELI-style, bulletproof)  
14. Documentation + security rules (public library hygiene)  
15. Performance PR checklist  
16. Appendix: command snippets (sanitized)  
17. Appendix: case studies (what we’ve already fixed)  

---

## 1) Goals and non-goals

### Goals

- Make backtests **fast** (especially options strategies), with bounded request volume on cold caches and near-zero downloader usage on warm caches.
- Make production backtests **explainable**:
  - if a backtest is waiting on data, show **what** it’s waiting on (`download_status`)
  - if it’s computing, show progress/heartbeat so it doesn’t look frozen
- Reduce production cost by eliminating **unnecessary idle instances** while preserving acceptable startup latency.
- Close the gap between **local warm runs** and **production warm runs** (parity).
- Preserve correctness:
  - pricing model remains broker-like
  - no silent drops of trades or “fast but wrong” fills

### Non-goals

- “Make it fast at any cost.” We do not accept performance improvements that introduce:
  - lookahead bias
  - changed fill semantics
  - missing/incorrect trades
  - incorrect option marks / MTM
- “Solve everything with bigger boxes.” Hardware helps, but the hardest slowdowns are usually request fanout + cache design, not raw CPU.

---

## 2) Performance is a system: phase model

When a user clicks “Backtest”, the system goes through phases. A useful mental model:

### Phase A — Submit + scheduling

- UI submits to a backend (e.g., Node service → bot backtest service).
- A task is scheduled on ECS (or equivalent).
- Capacity provider decides whether to reuse an existing instance or scale out.

**Symptoms when slow**
- “Nothing happens” for several seconds
- no logs yet, no progress rows

**Primary levers**
- capacity provider / warm capacity
- image pull behavior (safe only with immutable images)
- “write an initial progress row immediately” (perceived latency)

### Phase B — Container boot + python import

- container starts
- python imports LumiBot and dependencies
- dotenv scanning may run (if not disabled)
- logging/progress initialization happens

**Symptoms when slow**
- first log line delayed even after task start
- first progress row delayed

**Primary levers**
- disable recursive dotenv scanning in production (`LUMIBOT_DISABLE_DOTENV=1`)
- reduce work done before first progress write
- avoid expensive imports at module import time

### Phase C — Data hydration (remote downloader + caching)

This is the dominant cost for many option strategies on cold caches:

- building chains (expirations/strikes)
- fetching quote history / EOD / OHLC
- waiting on remote queue
- writing cache objects (local + S3)

**Symptoms when slow**
- many “Submitted to queue …” lines
- simulation datetime may not advance (waiting on data)
- ETA grows dramatically

**Primary levers**
- reduce request fanout (algorithmic)
- cache chunking (fewer objects; larger slices)
- memoize repeated requests within a run

### Phase D — Simulation compute

- per-bar strategy logic runs
- backtesting broker simulates orders/fills
- portfolio MTM runs per iteration (or per day)

**Symptoms when slow**
- little/no downloader usage, but wall time high
- yappi shows pandas-heavy call stacks

**Primary levers**
- reduce per-bar overhead (memoization, avoid repeated chain rebuilds)
- reduce expensive option pricing computations
- prefer vectorized transforms where safe

### Phase E — Artifacts (finalization)

- generate trades/logs/indicators files
- generate tearsheet/plots
- upload artifacts to storage

**Symptoms when slow**
- backtest completes simulation but “hangs at the end”
- heavy memory usage during report generation

**Primary levers**
- guard against pathological report generation (degenerate returns, huge dataframes)
- write incremental artifacts where possible

---

## 3) Evidence checklist (record this every time)

Speed work without evidence becomes cargo cult. Every investigation should capture:

### Run identity
- strategy name (and whether it’s a demo vs customer code)
- date window (start/end)
- data source (`BACKTESTING_DATA_SOURCE`)
- cadence (daily vs minute/second)

### Cache configuration
- cache backend (`local` vs `s3`)
- cache mode (`readonly` vs `readwrite`)
- cache namespace/version (how you define “cold” without deleting anything)

### Outputs
- wall time (start → end)
- success/failure (and exit reason if failed)
- artifacts produced (trades/logs/settings/tearsheet)

### Downloader telemetry
- count of “Submitted to queue” lines (or structured counter if present)
- dominant request types:
  - `option/list/strikes`
  - `option/list/expirations`
  - `option/history/quote`
  - `option/history/eod`
  - `ohlc` / `index/history/price`, etc.

### Attribution
- yappi profile (when needed): top functions by total time
- memory footprint (when relevant): peak RSS if accessible

> **Rule:** If a run is slow, the first question is always: *am I downloading a lot, or computing a lot?*

---

## 3.1a) Seconds-mode performance notes (futures-first)

Seconds-level backtesting can mean either:
- seconds for fills only (strategy cadence stays on minutes), or
- true seconds strategy loops (strategy runs each second timestamp).

True seconds loops change the economics:
- the iteration count can increase by ~60× (or more) vs minute,
- any per-iteration overhead that was “fine” at minute scale becomes catastrophic,
- one stray per-tick downloader request can turn a run from minutes into hours.

Recommended posture:
- Use an **event-driven clock** (advance on timestamps that exist in the dataset), not “every integer second”.
- Treat “prefetch once → slice forever” as a hard invariant.
- Avoid per-tick pandas churn (`pd.to_datetime`, `DataFrame.copy`, merges).

If you are implementing seconds-mode, read:
- `docs/BACKTESTING_SECOND_LEVEL_ROADMAP.md`
- `docs/investigations/bot_manager.md` (current implementation plan; futures-first)

---

## 3.1) Investigation report template (date-first, shareable, sanitized)

When a performance issue takes more than ~30 minutes to diagnose, write it up as an investigation.

Location:
- `docs/investigations/YYYY-MM-DD_TOPIC.md`

Why:
- prevents rediscovery across sessions/agents
- creates a permanent record of “what we tried” and “what worked”
- makes future regressions easier to detect

### Recommended structure

Use this template (adapt as needed):

```markdown
# <TOPIC>

> One-line description of what is slow and why this matters.

Last Updated: YYYY-MM-DD
Status: [Draft | Active | Fix Implemented | Closed]
Audience: [Contributors | AI Agents | Both]

## Overview
- What is slow?
- What are the success criteria?
- What is the current best hypothesis?

## Repro
- Strategy / window / provider
- “Cold” definition (cache version + fresh disk)
- “Warm” definition (same version + fresh disk)

## Evidence
- Wall time (cold/warm)
- Queue submits count + request type breakdown
- Cache hit/miss notes
- yappi attribution (if used)

## Root cause
- What specifically is generating the time cost?
- Why does this happen (code path explanation)?

## Fix options (ranked)
1) Best long-term fix
2) Acceptable short-term mitigation
3) What we should NOT do (anti-patterns)

## Correctness risks
- What could this break?
- What audits/tests are required?

## Validation
- What we ran after the fix
- Artifacts produced
- Audit results

## Follow-ups
- Docs to update
- Tests to add
- Cache re-warm required?
```

### Sanitization rules (because LumiBot is public)

Investigation docs should never include:
- API keys / credentials
- full internal URLs that are not meant to be public
- proprietary customer strategy code (only refer to it by “strategy A” unless explicitly permitted)
- raw proprietary datasets

Instead:
- describe env vars by name only
- use placeholders (`<API_KEY>`, `<S3_BUCKET>`)
- summarize request patterns quantitatively (counts and endpoint families)

---

## 4) The #1 rule for speed work: measure twice (cold vs warm)

For caching systems, a single run is ambiguous. You must measure at least:

### Cold run
- new local cache folder (fresh container simulation)
- new S3 namespace/version (fresh S3 simulation) **OR** a known-empty namespace

Expected: downloader usage can be non-zero, but request volume must be bounded.

### Warm run
- new local cache folder (fresh container)
- same S3 namespace/version as the cold run (S3 now “warm”)

Expected: near-zero downloader submits, large speedup.

If warm still downloads heavily:
- caching coverage is missing for some request type, or
- cache keying is unstable (namespace mismatch, different relative paths), or
- the strategy is generating new unique requests (algorithmic fanout).

---

## 5) Cache semantics and how to tell if you’re “warm”

### 5.1 What “warm” means in practice

If you use an S3 cache backend:

- **Warm** means the S3 namespace already contains every object the backtest needs.
- A warm run should show **near-zero “Submitted to queue”**.

### 5.2 Why “warm local” is not enough

Production containers are ephemeral. Even if S3 is warm:
- local disk cache starts empty in a fresh task
- so S3 must be the source of warmth

### 5.3 Common cache pitfalls

- Running from a directory with nested `.env` files causing the wrong cache settings to load.
- Changing cache folder paths such that the relative key layout changes (S3 key includes relative path).
- Incomplete cache coverage: you cached EOD but not quote snapshots, or vice versa.
- Many small objects: even “warm” runs are slow because S3 has to read thousands of tiny files.

### 5.4 Cache design principle: fewer, larger objects

If a workflow produces:
- tens of thousands of cache objects per backtest window

then “warm” still isn’t truly fast, because you’re IO-bound on object fetch overhead.

Performance work often means:
- chunking requests (per day / per contract / per week)
- caching the chunk
- serving intra-chunk lookups locally without additional IO

### 5.5 “Why is it still downloading on a warm run?”

There are only a few root causes:

1) Wrong effective env vars (namespace mismatch)
2) Cache is read-only (can’t persist new objects)
3) Cache coverage missing for a request type
4) The strategy asks for new data that was never cached (algorithmic fanout)

The fastest way to diagnose is to compare:
- the request type distribution of cold run #1 vs warm run #2

If warm run #2 still submits a lot of the same request types:
- caching is missing for those types or keying is unstable

---

## 6) Common slow patterns (what the logs look like)

### Pattern A — Downloader storm

**Symptoms**
- hundreds or thousands of “Submitted to queue” within minutes
- queue position grows
- ETA grows dramatically

**Likely causes**
- option strike scanning (probing many strikes repeatedly)
- chain building repeated per bar or per symbol
- quote history requested in tiny windows (per minute/per strike)

**First action**
- stop early; count submits; classify request types

### Pattern B — Warm run still downloads

**Symptoms**
- warm run has many submits

**Likely causes**
- cache namespace mismatch (wrong version/prefix/folder)
- missing cache coverage for a request type (e.g., quote snapshots newly required)

**First action**
- confirm effective env vars (settings.json / logs)
- identify which request types are missing from S3

### Pattern B2 — IBKR historical futures “conid registry missing” thrash (cont_future)

**Symptoms**
- backtest window is historical (contract months are now expired)
- repeated errors like “IBKR cont_future requires conids for explicit contract months …”
- frequent submits to `ibkr/trsrv/futures` even though the backtest never makes progress on data
- wall time explodes (minutes → hours) and logs become extremely repetitive

**Likely cause**
- `ibkr/conids.json` is missing in the active S3 cache namespace (fresh cache version/prefix), but
  IBKR Client Portal cannot discover conids for expired contracts.

**First action**
- check that the conid registry exists in S3 for the active cache namespace/version
- if running a fresh cache version, ensure the registry is seeded (see
  `docs/investigations/2026-01-27_ROUTER_IBKR_SPEED.md`)

### Pattern C — Low submits but still slow

**Symptoms**
- near-zero downloader submits
- wall time still high

**Likely causes**
- compute-heavy simulation (pandas, option pricing)
- S3 IO overhead (many small reads)
- artifact generation (tearsheet/plots)

**First action**
- yappi profile; separate S3 IO vs compute vs artifacts

### Pattern D — Looks stuck (UI), but it’s actually waiting on data

**Symptoms**
- simulation datetime not advancing
- progress row not updating
- user sees a frozen UI

**Likely causes**
- downloader wait loop without a heartbeat update
- `download_status` not persisted to DB (so UI can’t explain what it’s waiting on)

**First action**
- add/verify progress heartbeat and persist `download_status`

### Pattern E — “Fast locally, slow in prod”

**Symptoms**
- local run is fast, prod is slow even with warm S3

**Likely causes**
- you accidentally measured “warm local disk” locally (not prod-faithful)
- prod is IO-bound on S3 small-object reads
- prod is slower CPU (instance type)

**First action**
- enforce “warm S3 + cold local” on both sides; then profile

### Decision tree (fast triage)

Use this when a backtest is “painfully slow” and you need to decide what to do *before* you start changing code.

1) **Is the backtest actively downloading?**

- Look for “Submitted to queue” (Theta) or frequent HTTP fetches (other providers).

If YES:
- You are hydration-bound.
- The only two levers that matter are:
  - reduce request fanout, and/or
  - improve caching coverage + chunking.
- Bigger boxes rarely help.

If NO (near-zero submits):
- You are compute/IO/artifact-bound.
- Bigger boxes *might* help, but first prove it with yappi.

2) **Is the backtest “stuck” because simulation datetime is not advancing?**

If YES:
- It is almost always waiting on data (queue polling) or blocked on IO.
- The fix is **observability** first:
  - `download_status` must be populated
  - progress heartbeat must update the UI while waiting

3) **Does run #2 get dramatically faster than run #1?**

If YES:
- Cache is working; first-run hydration dominates.
- Focus on:
  - shrinking the amount of data requested
  - reducing the number of distinct requests (chunking)
  - pre-warming canonical windows if UX requires it

If NO:
- Either the cache isn’t actually warm (namespace mismatch / missing coverage), or the algorithm is still generating unique requests each run.

4) **Are the slow requests dominated by a single endpoint family?**

Typical endpoint families in option-heavy work:
- `option/list/expirations` → expiration fanout
- `option/list/strikes` → strike fanout
- `option/history/quote` → quote-history fanout (often the biggest killer)
- `option/history/eod` → daily/eod history
- `index/history/price` → index series dependencies (SPX/NDX)

If YES:
- Focus fixes on that family first; don’t “optimize everything”.

5) **Is this a correctness symptom masquerading as slowness?**

Examples:
- missing option marks → positions can’t be valued/exited → strategy loops “trying again”
- forward-fill storms → strategy keeps requesting prices that don’t exist

If YES:
- fix correctness first; performance usually improves once the loop stops.

---

## 7) Root causes + fix patterns (ThetaData + options heavy)

This section captures known painful classes of slowdowns and how we typically fix them.

### 7.1 Chain building: don’t build more chain than the strategy needs

Many strategies only need:
- one expiry (or a few)
- a narrow strike neighborhood near ATM or target delta

But naive chain building can fetch:
- every expiration
- every strike list
- repeatedly

**Fix patterns**
- **Bound default expirations** by days-out (different defaults for equities vs indices).
- Use **lazy chain resolution**:
  - fetch expirations once per day per underlying
  - fetch strikes only for the specific expiry needed
- Memoize chain results per `(symbol, trading_day)`.

### 7.2 Delta-to-strike selection: avoid per-strike quote probing

The most expensive options helpers often do:
- choose a delta target (e.g., 0.30, 0.50)
- then probe strikes to find the strike whose delta matches

If implemented as:
- “probe every strike and fetch quotes/greeks to compute delta”

it explodes request volume.

**Fix patterns**
- Use **bounded search**:
  - binary search or neighborhood scan
  - probe only a handful of candidates (not hundreds)
- Use chain greeks if available (avoid quote probes entirely).
- Use **Black–Scholes delta inversion** as a starting guess (then probe only nearby strikes).
- Memoize per `(symbol, day, expiry, right, target_delta)`.

> Note: The goal is not a hard “cap”; it’s to make the algorithm naturally need only a handful of probes.

### 7.3 Quote-history fanout: cache bigger slices

Slow full-year option strategies are frequently dominated by:
- `option/history/quote` calls

If those calls are requested as tiny windows (or per bar), you get:
- too many unique requests
- too many cached objects
- too much time waiting on the downloader

**Fix patterns**
- Request quote history in **chunked windows** (e.g., per trading day per contract).
- Cache the chunk once.
- Serve intra-day lookups from the cached chunk without new downloader submits.

### 7.4 EOD gaps vs intraday quotes: correctness + performance interaction

Some option series can lack day/EOD history even when intraday quote history exists.

**Correctness requirement**
- daily cadence strategies still need an option mark to MTM and to exit realistically

**Fix pattern**
- fall back to an intraday NBBO quote snapshot for daily MTM marks when EOD is missing

**Performance implication**
- acceptance caches must include the additional required objects, otherwise “warm cache invariant” fails (CI tripwire fires).

See:
- `docs/investigations/2026-01-06_THETADATA_OPTION_EOD_GAPS_DAILY_MTM.md`

### 7.5 Corporate actions storms (splits/dividends)

Repeated corporate action fetches can create:
- redundant requests
- slowdowns

**Fix patterns**
- memoize within a run
- add failure TTL (negative caching) to avoid retry storms

### 7.6 Report generation (tearsheet) crash + slowness

Tearsheet generation can:
- be expensive
- crash on degenerate return series

**Fix patterns**
- guard against degenerate/flat returns
- produce a placeholder tearsheet instead of crashing the whole backtest

### 7.7 “Forward-filled price storms”

If you see repeated warnings about forward-filled prices:
- the strategy is requesting prices at times where the current data frame has gaps

This can indicate:
- missing bars in a range
- an alignment bug between timestamps and bars
- requesting minute bars outside market hours

Forward-filling may keep the backtest running, but it can also mask missing data and distort fills.

**Fix patterns**
- ensure the underlying minute OHLC coverage is correct for the requested trading calendar
- for Theta index minute OHLC (RTH-bounded), treat **session close** as “complete coverage” (do not require 23:59/UTC-midnight bars)
- ensure option quotes are requested at times where quotes exist
- add negative caching for known-missing slices to avoid retry storms

See also:
- `docs/investigations/2026-01-13_SPX_INTRADAY_STALE_LOOP_FIX.md` (production “ETA days” incident, root cause, fix, and regression tests)

### 7.8 Case study: “ETA days” SPX intraday STALE loop (10×–100× lever)

If you ever see a backtest that appears “stuck” and the logs show:
- `[THETA][CACHE][STALE] prefetch_complete but coverage insufficient` repeating
- extremely high `Submitted to queue` rates for `v3/index/history/ohlc` (SPX minute OHLC)

...the fastest path to a 10× win is almost always to restore the warm invariant:
- **warm means** `queue_submits == 0` (same S3 namespace, fresh local disk cache)

This specific failure mode was fixed by clamping the intraday “coverage required” end timestamp for index assets to the **last trading session close at or before** the end requirement (holiday/weekend/early-close safe). See:
- `docs/investigations/2026-01-13_SPX_INTRADAY_STALE_LOOP_FIX.md`

#### Why this matters for “S3 vs EBS/EFS” discussions

Before pursuing storage changes, validate whether warm runs are actually warm and where time goes:
- If “warm” still submits to the downloader, storage changes do not help (you’re dominated by queue work).
- If “warm” has `queue_submits≈0`, profile with yappi to see if the run is CPU-bound or S3-IO-bound.

In a prod-like warm baseline for the client benchmark `SPX0DTEHybridStrangle`, yappi showed `s3_io` was ~1% and `pandas_numpy` dominated. That implies EBS/EFS would not produce an order-of-magnitude speedup for warm runs of that strategy (CPU/artifacts are the next levers).

---

## 8) Profiling with YAPPI (how to attribute time)

### 8.1 When to use yappi

Use yappi when:
- downloader submits are near-zero but runs are still slow
- production is slower than local on warm runs (parity work)
- you need proof of “where time goes”

### 8.2 How to enable

Set:

- `BACKTESTING_PROFILE=yappi`

Expected artifact:
- `*_profile_yappi.csv`

### 8.3 How to analyze

Use:
- `scripts/analyze_yappi_csv.py <profile.csv>`

Look for buckets:
- S3 IO (boto3, download_file, list/get object)
- downloader wait (HTTP polling, sleep loops)
- pandas transforms (merge, concat, tz_convert)
- strategy logic (your strategy methods)
- artifacts (QuantStats, matplotlib)
- progress/log overhead (CSV writes, DB updates)

### 8.4 Profiling gotchas

- Profiles can be skewed by excessive logging.
- Keep flags consistent when comparing runs (prod vs local):
  - `SHOW_TEARSHEET`, `SHOW_INDICATORS`, `SHOW_PLOT`
- Avoid profiling “cold downloader storms” when you’re trying to measure compute; warm the cache first.

---

## 9) Performance baselines and history (what we track automatically)

We maintain an automated history of acceptance backtest execution times:

- `tests/backtest/backtest_performance_history.csv`

This file is appended automatically by the acceptance harness and provides:
- timestamp
- test name
- execution time seconds
- git commit hash + version (from `setup.py`)

### How to use it

- to detect regressions (“this test used to take 40s, now it’s 90s”)
- to confirm speed work improved runtimes over time

### What it does *not* tell you

- whether a run was cold vs warm for S3 (it assumes acceptance invariants)
- attribution (you still need yappi/telemetry)
- production vs local parity

If you’re doing major performance work, consider adding an investigation doc summarizing:
- before/after acceptance runtimes
- queue submit counters
- yappi attribution

---

## 10) Production vs local parity (apples-to-apples)

Parity means:
- same code
- same flags
- same cache backend + namespace
- same “warm S3, cold local” condition

### 10.1 Recommended parity protocol

For a short window (1–3 trading days):

1) Run in prod with:
   - warm S3 namespace
   - fresh local cache (implicit)
2) Run again in prod immediately
3) Run locally with:
   - same S3 namespace
   - fresh local cache folder
4) Run again locally immediately

Record:
- wall time
- downloader submits
- yappi totals if enabled

### 10.2 Interpreting gaps

If prod is slower and submits are near-zero:
- likely CPU (instance type) or S3 IO

If prod is slower and submits are high:
- likely downloader queue contention or request volume

If local is faster because it reused local disk cache:
- you are not measuring prod-faithful conditions; fix the methodology

### 10.3 Typical drivers of prod vs local gaps

- CPU clock differences (Graviton vs laptop cores)
- S3 network latency and per-object overhead
- container-level overhead (cgroups, IO)
- artifacts cost differences (headless rendering, fonts)

---

## 11) Startup latency (submit → first progress row)

Startup latency is a combination of:
- scheduling delay
- container boot
- python boot
- first progress write

### 11.1 Measure it as a timeline

For each run, capture:
- submit timestamp
- task startedAt
- first log line timestamp
- first progress row timestamp

Bucket it:
- delay before task startedAt → capacity/scheduling
- delay between startedAt and first log → container boot
- delay between first log and first progress row → python boot/progress init

### 11.2 Reduce perceived latency immediately

Write a “queued/starting” progress row as soon as the job is accepted.

Even if the task takes seconds to schedule, the UI is no longer dead.

### 11.3 Reduce real latency

- prefer cached images (only safe with immutable image references)
- keep at least one warm instance if UX matters more than cost
- disable recursive dotenv scanning in prod

---

## 12) Cost + scaling (avoid expensive idle fleets)

### 12.1 The common failure mode

If instances are “protected from scale in” and never unprotected, the ASG cannot scale down even if:
- desired capacity drops
- ECS has idle container instances

This is a cost leak.

### 12.2 What to measure

At any moment:
- number of ACTIVE container instances
- number of RUNNING tasks
- number of idle instances (ACTIVE with 0 running tasks)
- ASG InService count vs desired
- whether instances are `ProtectedFromScaleIn=true`

### 12.3 Practical strategy

- keep a small warm baseline (e.g., 1 instance) for UX
- scale out to handle load
- ensure scale-in protection is cleared for idle instances

### 12.4 Burst concurrency (future)

If you want “10 backtests at once” cheaply:
- use scale-to-zero + spot capacity providers
- but only after request fanout is controlled, otherwise you DDoS your own downloader

### 12.5 Parallelism assessment (2026-03-28)

Backtest speed work often gets framed as “the engine needs parallelism.” That is only partly true.

What is true:

- a single backtest can be compute-heavy,
- the main strategy loop is a real hotspot,
- and aggregate throughput across many independent runs can benefit enormously from parallel execution.

What is **not** true:

- that LumiBot cannot run anything in parallel today,
- or that generic parallelization of `on_trading_iteration()` is the safest path to big wins.

Important distinctions:

1. **Single backtest**
- The core execution model is serial by design: strategy logic, order processing, and clock advancement are path-dependent.
- Generic loop parallelization risks changing fills, timestamps, and PnL.

2. **Many independent backtests**
- This is the best place for big concurrency wins.
- Parameter sweeps, window sweeps, and strategy comparisons should use process/container-level parallelism with bounded downloader concurrency.

3. **Data hydration**
- Some provider work is already parallelized today (chunk downloads, thread-pool fanout, async prefetch).
- For cold-cache option-heavy runs, hydration/request fanout often dominates before core loop parallelism does.

4. **Artifacts**
- Plots, tearsheets, indicators, and uploads can be a meaningful part of wall time.
- If users need core results first, defer or decouple post-processing.

Small local benchmark note (synthetic in-memory minute data):

- empty 20-day minute loop: ~7,800 iterations in ~6.0s
- same loop with one `get_last_price()` per bar: ~7,800 iterations in ~10.4s

Interpretation:

- single-run compute overhead is real,
- but 10x single-run gains are more likely to come from batching, event-driven skipping, cache reuse, or artifact deferral than from directly parallelizing the core loop.

Recommended order of operations:

1. measure cold vs warm,
2. profile a real slow strategy,
3. parallelize **independent runs** first,
4. batch/skip work inside one run,
5. only then consider intra-loop parallelism with parity safeguards.

Detailed reasoning and code anchors:

- `docs/investigations/2026-03-28_BACKTEST_PARALLELISM_ASSESSMENT.md`

---

## 13) Accuracy audits (MELI-style, bulletproof)

### 13.1 Why audits matter

Speed improvements can silently change:
- which contract gets chosen
- which strike/expiry is selected
- fill prices
- trade timing

Every major speed fix to options logic must be validated with a full audit.

### 13.2 Audit data

Enable:
- `LUMIBOT_BACKTEST_AUDIT=1`

Prefer storing:
- a canonical CSV (full table) + a markdown summary linking it

Audit table should include as much as possible:
- underlying price
- option symbol (expiry/strike/right)
- bid/ask snapshots at submit + fill
- derived mark
- fill price and size
- strategy reason/tag (if available)

### 13.3 Where to put audit reports

- Engineering reports: `docs/investigations/YYYY-MM-DD_TOPIC.md`
- Do not commit raw secrets or internal API keys.

---

## 14) Documentation + security rules (public library hygiene)

LumiBot is a public library. Treat docs as public by default:

### 14.1 Do not commit secrets

Never commit:
- API keys
- AWS secret keys
- passwords
- private hostnames/endpoints that are not intended to be public

When documenting env vars:
- document **names and semantics**
- use placeholders for values

### 14.2 Internal vs public docs

- `docs/` (this folder): engineering notes for contributors/agents
- `docsrc/`: public Sphinx docs (user-facing)
- `docs/handoffs/`: local/private coordination (gitignored going forward)
- `docs/investigations/`: shareable deep dives (sanitize)

### 14.3 Documentation discipline

If you add/change:
- env vars → update `docsrc/environment_variables.rst` and `docs/ENV_VARS.md`
- caching semantics → update `docs/REMOTE_CACHE.md` and relevant Sphinx pages
- profiling workflows → update public + internal docs (`docsrc/backtesting.performance.rst` and this file)

---

## 15) Performance PR checklist

Before merging any speed-related change:

### Evidence
- [ ] cold vs warm measurements recorded
- [ ] queue submit counts recorded and categorized
- [ ] yappi profile collected when needed

### Correctness
- [ ] no strategy code edits (unless explicitly required)
- [ ] full audit produced for option strategies touched
- [ ] regression test added for the bug/perf pathology

### Docs
- [ ] `docs/BACKTESTING_PERFORMANCE.md` updated if the work adds a new pattern or workflow
- [ ] public docs updated if user-facing behavior changed (`docsrc/`)

### Ops
- [ ] no cache deletions (use versioning for cold simulations)
- [ ] long runs use timeouts and small windows first

---

## 16) Appendix: command snippets (sanitized)

### 16.1 Count downloader submits in a log

```bash
rg -n "Submitted to queue" /path/to/logs.csv | wc -l
```

### 16.2 Identify dominant request types

```bash
rg -n "Submitted to queue" /path/to/logs.csv | rg -o "path=[^ ]+" | sort | uniq -c | sort -nr | head
```

### 16.3 Run a short local prod-like backtest (no secrets)

Use the repo’s prod-like runner and inject env vars via your shell/environment manager.

Always wrap long runs with a timeout:

```bash
bin/safe-timeout 1200s python3 scripts/run_backtest_prodlike.py ...
```

### 16.4 YAPPI analyze

```bash
python3 scripts/analyze_yappi_csv.py /path/to/profile_yappi.csv
```

### 16.5 AWS: measure “idle instances” (conceptual)

In ECS:
- list container instances
- describe them to find `runningTasksCount=0`

In ASG:
- compare `InService` vs `DesiredCapacity`
- check `ProtectedFromScaleIn`

Example commands (placeholders; do not paste secrets):

```bash
# 1) How many container instances are ACTIVE?
aws ecs list-container-instances \
  --profile <PROFILE> \
  --region <REGION> \
  --cluster <CLUSTER> \
  --status ACTIVE

# 2) For each container instance, get runningTasksCount and ec2InstanceId
aws ecs describe-container-instances \
  --profile <PROFILE> \
  --region <REGION> \
  --cluster <CLUSTER> \
  --container-instances <CONTAINER_INSTANCE_ARN> \
  --query 'containerInstances[0].{ec2:ec2InstanceId,running:runningTasksCount,pending:pendingTasksCount,agentConnected:agentConnected}'

# 3) Check ASG desired vs InService and whether instances are protected
aws autoscaling describe-auto-scaling-groups \
  --profile <PROFILE> \
  --region <REGION> \
  --auto-scaling-group-names <ASG_NAME> \
  --query 'AutoScalingGroups[0].{Desired:DesiredCapacity,InService:length(Instances[?LifecycleState==`InService`]),Protected:length(Instances[?ProtectedFromScaleIn==`true`])}'

# 4) If scale-in is blocked by protection, clear protection for known-idle instances
#    (only do this for instances that have runningTasksCount=0)
aws autoscaling set-instance-protection \
  --profile <PROFILE> \
  --region <REGION> \
  --auto-scaling-group-name <ASG_NAME> \
  --instance-ids <INSTANCE_ID_1> <INSTANCE_ID_2> \
  --no-protected-from-scale-in
```

> Safety note: instance protection is an infra lever; clearing it is safe only when you are confident the instance is idle.

### 16.6 AWS: quantify downloader fanout from logs (conceptual)

When diagnosing “hours-long” runs, you usually want:
- total “Submitted to queue” count
- breakdown by endpoint family (quote history vs strikes vs expirations)

Example (CloudWatch Logs Insights pseudocode; adjust to your log group):

```text
fields @timestamp, @message
| filter @message like /Submitted to queue/
| parse @message /path=(?<path>[^ ]+)/
| stats count() as submits by path
| sort submits desc
```

---

## 17) Appendix: case studies (what we’ve already fixed)

This section is intentionally high-level and avoids secrets/IDs. For deep dives, prefer date-first investigation docs.

### 17.1 “Daily options MTM goes flat” (ThetaData EOD gaps)

- Problem: day/EOD option history can be missing even when intraday quotes exist → daily cadence can’t mark options → flat equity curves.
- Fix: daily-cadence MTM falls back to an intraday NBBO quote snapshot when EOD/day mark is unavailable.
- Follow-up: acceptance caches must be warmed for the new required quote snapshot objects.

See:
- `docs/investigations/2026-01-06_THETADATA_OPTION_EOD_GAPS_DAILY_MTM.md`

### 17.2 “Tearsheet crashes at the end”

- Problem: report generation can crash on degenerate return series.
- Fix: guard tearsheet generation and produce a placeholder instead of crashing the whole backtest.

### 17.3 “OptionsHelper delta selection explodes request volume”

- Problem: naive delta-to-strike selection can probe many strikes and flood the downloader (for example: building a large candidate strike list and calling `get_greeks()` per strike to “hunt” a 20Δ contract).
- Fix direction:
  - Prefer `OptionsHelper.find_strike_for_delta(...)` for delta-based strike selection (bounded probing + caching; avoids scanning many strikes).
  - Cache selected expiry/strikes in `self.vars` for the trading day so intraday retries don’t recompute unless the underlying has moved materially.
- Observed impact (cold-cache example, 1 day window; numbers vary by symbol/interval):
  - `get_greeks()` calls: ~94 → ~13
  - downloader queue submissions: ~560 → ~212
  - wall time: ~75s → ~32s

See:
- `docs/investigations/2026-01-09_PRODLIKE_LOCAL_BENCHMARK_RUNS_WEEK_WINDOW.md`

### 17.4 “Corporate actions fetch storms”

- Problem: repeated split/dividend fetches can thrash remote services.
- Fix: memoize and add negative TTL to avoid retry storms.
