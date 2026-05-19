# BACKTESTING_SPEED_PLAYBOOK — How to Improve Backtest Speed (Router-Mode, Evidence-Driven)

> A practical, repeatable SOP for improving LumiBot backtesting performance **without changing strategy code**, using **production routing (router JSON)** and preserving **acceptance/accuracy invariants**.

**Last Updated:** 2026-01-27  
**Status:** Active  
**Audience:** Developers + AI Agents  

---

## Overview

This document is a **how-to playbook** for speeding up backtests in a way that is:

- **Production-realistic:** always benchmark with **router-mode** (the way production runs).
- **Evidence-driven:** every change must be backed by measured runs and profiler evidence.
- **Safe:** never leak secrets in docs; never “fix” by editing strategies; never break sacred legacy/acceptance behavior.

If you want deeper conceptual background (phase model, pattern catalog, cost notes), start with:
- `docs/BACKTESTING_PERFORMANCE.md`
- `docs/BACKTESTING_ARCHITECTURE.md`
- `docs/REMOTE_CACHE.md`
- `docs/ACCEPTANCE_BACKTESTS.md`

---

## 0) Non‑negotiables (read first)

### 0.1 Do not “cheat” by editing strategy code

Performance improvements must come from:
- LumiBot code (preferred), or
- the platform prompt system that generates strategies (only if the bottleneck is fundamentally user-code patterns).

**Do not modify Strategy Library demo strategies** to make benchmarks look faster. That invalidates the benchmark.

### 0.2 Router-mode is mandatory for performance work

Production uses router-mode, so performance work must use router-mode too.

**Canonical production routing JSON:**

```json
{"default":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}
```

This is set via:
- `BACKTESTING_DATA_SOURCE='{"default":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}'`

### 0.3 Repo + process safety (multi-agent rules)

This environment often has multiple AI/human sessions touching the same repo. To avoid lost work:

- Before changing anything, check:
  - `git status --porcelain=v1`
  - `git branch --show-current`
- Never use destructive git operations:
  - **Never run `git checkout`** (hard ban)
  - avoid `git reset --hard`, `git clean -f`, and `git stash`
- Do not switch branches unless explicitly instructed.
- Do not open PRs unless explicitly requested.

### 0.4 Long-running commands must have a timeout

Performance runs can hang (queue waits, network stalls). Always wrap long commands with a timeout, e.g.:
- `/Users/robertgrzesik/bin/safe-timeout …`

### 0.5 Theta safety: never steal the licensed session

For ThetaData work:
- Backtests should use the **Data Downloader** via `DATADOWNLOADER_BASE_URL` (do not launch ThetaTerminal locally).
- Never start ThetaTerminal locally using production credentials (single-session vendor constraint).

### 0.6 Acceptance / legacy backtests are sacred

Backtest correctness regressions are unacceptable. The acceptance suite is a regression firewall:
- `tests/backtest/test_acceptance_backtests_ci.py`

Speed work must not break:
- correctness metrics,
- determinism,
- warm-cache queue-free invariants,
- or “legacy” tests (see `tests/AGENTS.md` for legacy policy).

### 0.7 Public repo hygiene: no secrets in docs, code, tests, logs

This repo is committed to GitHub. Do not paste:
- real API keys / credentials
- production downloader hostnames/URLs
- secrets from `.env` files

Safe patterns:
- use placeholders (e.g., `https://<your-downloader-host>:8080`)
- reference env var names instead of values
- include only sanitized snippets (no headers/keys)

S3 bucket/prefix names are acceptable examples, but **never include access keys**.

---

## 1) Definitions: “accuracy”, “speed”, “warm”, “cold”

### 1.1 Accuracy (gold standard)

Accuracy means: replay an interval that was traded live and reproduce broker fills + realized PnL within tolerances.
Acceptance runs are a deterministic regression firewall, not a complete proof of “truth”.

See `docs/BACKTESTING_ARCHITECTURE.md` and `docs/BACKTESTING_ACCURACY_VALIDATION.md`.

### 1.2 Speed (what we optimize for)

A backtest is “fast” when warm-cache runs are:
- **queue-free** (0 downloader submits), and
- complete within a bounded wall time budget (CI + production expectations).

We treat “warm-cache but still submitting to queue” as a **cache regression** (or key instability) until proven otherwise.

### 1.3 Cold vs warm (two different axes)

Backtests have two cache layers:

1) **Local disk cache** (inside the backtest container / your machine)
2) **Remote cache** (S3) mirrored via the backtest cache manager

Therefore “cold/warm” must always be stated on both axes:

- **S3 warm** vs **S3 cold**:
  - Warm = required objects already exist under the active cache namespace/version.
  - Cold = objects are missing (allowed to submit to downloader if in readwrite mode).
- **Local disk warm** vs **local disk cold**:
  - Warm = objects already exist in `LUMIBOT_CACHE_FOLDER`.
  - Cold = fresh/empty local cache folder (simulates a fresh ECS task).

### 1.4 Time measurements

Always record both:
- **Wall time**: end-to-end clock time you experience.
- **Simulation/backtest time**: `backtest_time_seconds` recorded in `*_settings.json`.

Also track perceived UX latency:
- **submit → first progress row** (important for user perception; see `docs/investigations/2026-01-05_STARTUP_LATENCY.md`).

---

## 2) System map: where backtest time goes (phase model)

Backtest slowness is usually one (or more) of:

1) **Startup**: container scheduling, import time, dotenv scanning, first progress row
2) **Data hydration**: downloader queue waits, request fanout, cache misses
3) **Cache IO**: S3 roundtrips, parquet reads/slices, too many small objects
4) **Compute**: pandas transforms, per-bar strategy loop overhead, pricing
5) **Artifacts**: tearsheets, plots, indicators, CSV export
6) **Progress/logging**: progress heartbeat, logging volume, UI helpers

If you learn a new recurring pattern, add it to:
- `docs/BACKTESTING_PERFORMANCE.md`

---

## 3) Router-mode: how production routing works (and why it matters)

### 3.1 How routing is selected

LumiBot treats `BACKTESTING_DATA_SOURCE` specially:
- If it is a JSON object string, LumiBot switches to the router datasource (`RoutedBacktestingPandas`) and uses the dict as the routing map.

Implementation references:
- `lumibot/strategies/_strategy.py` (parses env var and selects datasource)
- `lumibot/backtesting/routed_backtesting.py` (provider registry + routing adapters)

### 3.2 Canonical production routing JSON

Use this for all benchmarks unless explicitly investigating a single-provider path:

```bash
export BACKTESTING_DATA_SOURCE='{"default":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}'
```

Notes:
- Router aliases `"futures"` → `"future"` for user convenience, but **does not imply** `"cont_future"`.
- `crypto_future` is explicit for crypto perps/futures; if omitted, the router falls back to `crypto`.
- When routing is wrong, speed “fixes” can be fake (you may be measuring a different provider than production).

---

## 4) Benchmark selection (standardized; don’t bikeshed)

### 4.1 Primary benchmark suite: acceptance backtests (CI gate)

Use the acceptance suite as the **non-negotiable baseline** for correctness and speed budgets:
- `tests/backtest/test_acceptance_backtests_ci.py`
- Baselines + budgets live in: `tests/backtest/acceptance_backtests_baselines.json`

Current acceptance cases include:
- ThetaData (options + equities + index/options): multiple Strategy Library demos
- IBKR (crypto + futures): dedicated acceptance scripts

Key invariants (CI):
- Deterministic metrics (centipercent strict)
- Warm-cache invariant: `thetadata_queue_telemetry.submit_requests == 0`
- Speed cap: `backtest_time_seconds <= max_backtest_time_seconds`

### 4.2 Fast-iteration benchmarks (1-week windows)

For iterative performance work, prefer short windows that still produce trades.
Canonical windows + measurement rules live in:
- `docs/investigations/2026-01-09_PROD_SPEED_BENCHMARK_PROTOCOL.md`

Rule of thumb:
- Always run a benchmark **twice back-to-back**:
  - Run #1: cold local disk (fresh cache folder)
  - Run #2: immediate rerun (warm local disk)

### 4.3 Customer “slow family” reproduction (optional but recommended)

When you have a real-world slow backtest, add it as a Tier C benchmark:
- do not edit strategy code
- capture the “should trade” interval
- use router-mode and prod-like flags
- record a minimal sanitized repro in `docs/investigations/YYYY-MM-DD_<TOPIC>.md`

### 4.4 Seconds-mode benchmarks (futures-first)

Seconds-level backtesting has two meanings:
- seconds for fills only (bar magnifier), and
- true seconds strategy loops (strategy runs on seconds).

When working on true seconds-mode (futures-first), treat it as a separate benchmark family because:
- iteration counts can increase by ~60× vs minute,
- naïve implementations can become unusable quickly, and
- the “warm-cache must be queue-free” invariant becomes even more important (one stray per-tick download kills everything).

Required reading before starting seconds perf work:
- `docs/BACKTESTING_SECOND_LEVEL_ROADMAP.md`
- `docs/investigations/bot_manager.md` (implementation plan; futures-first)

Minimum benchmark suite for seconds work:
- a no-op/low-compute seconds strategy (measures engine overhead)
- a deterministic “order heavy” seconds strategy (validates fill semantics and hot-loop overhead)
- at least one real futures strategy (customer reproduction)

Windows:
- 1 day (iteration)
- 1 week (gate)

Evidence requirements are unchanged:
- cold vs warm (prove warm is queue-free)
- median-of-3 timings
- YAPPI attribution when slow

---

## 5) Measurement protocol (required for every performance change)

### 5.1 The run matrix (what to run, in what order)

For each benchmark, run:

1) **S3 warm + local cold**
   - New empty local cache folder
   - Expected: minimal startup + S3 hydration reads; **no queue submits** if S3 truly warm
2) **S3 warm + local warm**
   - Same cache folder, immediate rerun
   - Expected: fastest run; still **no queue submits**
3) (Optional) **S3 cold + local cold**
   - New S3 cache namespace/version
   - Allowed: queue submits (readwrite mode)
   - Goal: characterize cold cost and request fanout

Default mode recommendation (prod parity):
- Use `LUMIBOT_CACHE_MODE=readwrite` unless you have a specific reason not to.
- Use `readonly` only when you explicitly want to:
  - prove the run is warm and would be queue-free even if writes were disabled, or
  - avoid mutating shared S3 during experiments.

### 5.2 Don’t delete caches; isolate them

Do **not** delete your global local cache to simulate production. Instead:
- For performance work, set `LUMIBOT_CACHE_FOLDER` to a dedicated folder under `~/Documents/Development/`
- For “local cold”, use a **new empty folder** per run (no deletion required)

Why:
- avoids destroying valuable caches
- prevents cross-test contamination
- makes experiments repeatable and auditable

### 5.3 Required evidence to capture (every run)

Minimum required run metadata:
- timestamp (local + UTC if available)
- git SHA (short) + `lumibot_version`
- benchmark name + date window
- router JSON string used
- cache config:
  - `LUMIBOT_CACHE_BACKEND`, `LUMIBOT_CACHE_MODE`, `LUMIBOT_CACHE_S3_BUCKET`, `LUMIBOT_CACHE_S3_PREFIX`, `LUMIBOT_CACHE_S3_VERSION`
  - `LUMIBOT_CACHE_FOLDER` (local)
- timings:
  - wall time
  - `backtest_time_seconds`
  - submit → first progress row (if measuring UX)
- downloader telemetry:
  - queue submits total
  - top paths (top 3–10)
- profiler evidence (when needed):
  - yappi top functions by total time

Where to find it:
- `logs/*_settings.json` includes:
  - `lumibot_version`
  - `backtesting_data_sources` (router JSON string)
  - `remote_cache` and `remote_cache_stats`
  - `thetadata_queue_telemetry`

### 5.4 Benchmark record template (copy/paste)

Use this template in your investigation ledger:

```markdown
## <BENCHMARK> — <ENV> — <YYYY-MM-DD>

- git: <shortsha>
- lumibot_version: <x.y.z>
- window: <YYYY-MM-DD → YYYY-MM-DD>
- BACKTESTING_DATA_SOURCE (router JSON): <...>
- cache:
  - backend/mode: <s3/readwrite|readonly>
  - s3 bucket/prefix/version: <bucket>/<prefix>/<version>
  - local cache folder: <path>

### Timings
- wall time: <...s>
- backtest_time_seconds (settings.json): <...s>
- submit→first progress row (optional): <...s>

### Downloader / Queue
- submit_requests: <N> (from thetadata_queue_telemetry)
- top paths:
  - <path>: <count>
  - ...

### Profiler (if enabled)
- yappi artifact: <path>
- top hotspots:
  - <func>: <ttot_s>
  - ...

### Notes
- <observations + hypothesis>
```

---

## 6) Tooling: sanctioned ways to run “prod-like” benchmarks locally

### 6.1 Preferred runner: `scripts/run_backtest_prodlike.py`

Use this for repeatable, production-like runs:
- isolates the working directory (avoids accidental `.env` discovery)
- writes artifacts to a clean `workdir/logs/`
- can override cache folder + S3 version safely
- can optionally enable profiling

File: `scripts/run_backtest_prodlike.py`

Key flags:
- `--data-source '<router JSON>'`
- `--cache-folder <path>`
- `--cache-version <version>` (simulate cold S3 without deleting anything)
- `--profile yappi`

### 6.2 Acceptance warm/fill runner (outside CI)

When acceptance fails due to missing warm caches (tripwire / queue-free invariant), warm S3 outside CI:
- `scripts/warm_acceptance_backtests_cache.py`

Then re-run:
- `pytest -q tests/backtest/test_acceptance_backtests_ci.py`

### 6.3 YAPPI analysis helper

YAPPI artifacts (`*_profile_yappi.csv`) can be summarized via:
- `scripts/analyze_yappi_csv.py`

Important:
- YAPPI adds overhead; treat it as a hotspot ranking tool, not a precise wall-time measurement.

### 6.4 Router-mode suites (recommended pattern)

For repeatability and “append-only” logging, prefer suite runners that:
- run a benchmark set
- write artifacts into a run directory
- append results to a ledger `.md` and `.csv`

Example (IBKR router work):
- `docs/investigations/2026-01-27_ROUTER_IBKR_SPEED.md`
- `scripts/bench_router_ibkr_speed_suite.py`

For ThetaData router performance, follow the same pattern (create a new dated investigation ledger + suite runner).

---

## 7) Local vs production realism (how to avoid misleading conclusions)

### 7.1 Why local timings can lie

Your machine differs from production:
- CPU: your laptop may be faster than typical AWS backtest instances
- Network: your laptop is far from S3; production tasks are in the same region
- Local disk cache persistence: your machine keeps caches across runs unless isolated

Therefore:
- use **queue submits** + **remote cache stats** as primary “warmness” evidence
- treat wall time as environment-specific; compare relative deltas (before/after) in the same environment

### 7.2 How to simulate “fresh ECS task disk” locally (without deleting global caches)

Always run with a dedicated local cache folder:
- `LUMIBOT_CACHE_FOLDER=/Users/<you>/Documents/Development/backtest_cache/<topic>/<run>`

Then:
- “local cold” = new empty folder
- “local warm” = rerun with the same folder

### 7.3 How to compare to production

Preferred:
- validate the final candidate change by running the same benchmark in production (BotManager/BotSpot pipeline).

In local-only investigations:
- focus on request fanout, cache hit rates, and profiler hotspots that should generalize to production.

---

## 8) Deciding what to optimize (attribution playbook)

The first question for any slow run:

> Is it slow because we are **waiting on data** (downloader/queue/cache IO), or because we are **computing** (python/pandas/strategy loop), or because we are slow in **artifacts** (tearsheet/plots)?

### 8.1 “Waiting” signature (downloader dominated)

Common indicators:
- logs contain many `Submitted to queue` lines
- settings show non-zero `thetadata_queue_telemetry.submit_requests`
- yappi shows a lot of:
  - `threading.Condition.wait`, `queue.Queue.get`, or queue client polling functions

Fix direction:
- reduce request fanout
- stabilize cache keying / coverage so warm runs do not submit
- address downloader health when endpoints hang (see Section 11)

### 8.2 “Compute” signature (CPU/pandas dominated)

Indicators:
- near-zero queue submits
- yappi shows pandas transforms, merges, groupbys, sorting, or per-bar loops

Fix direction:
- avoid repeated dataframe scanning
- reduce per-iteration work (memoize derived series, precompute lookups)
- prefer coarse prefetch windows (one fetch + slice) rather than repeated fetches

### 8.3 “Artifacts” signature (end-of-run slow)

Indicators:
- simulation completes quickly, but the run hangs at tearsheet/plots/exports
- high memory usage during reporting

Fix direction:
- guard against pathological report generation
- make artifact generation best-effort and non-fatal

---

## 9) Fix patterns that preserve semantics (generic, cross-provider)

This section is a catalog of safe speed wins that should not change strategy behavior.

### 9.1 Reduce request fanout (biggest lever for options/index strategies)

Patterns:
- Replace “one request per bar” with “prefetch a window once, then slice”.
- Memoize repeated requests within a run (cache in-memory by key).
- Add **negative caching** for confirmed “no data” intervals to avoid infinite refetch loops.

Required evidence:
- queue submits drop materially
- warm runs have 0 submits
- acceptance metrics unchanged

### 9.2 Stabilize cache semantics (make warm truly warm)

Patterns:
- Use stable cache keys (avoid embedding tiny window bounds unless necessary).
- Align session bounds deterministically (regular session vs extended; day close alignment).
- Ensure coverage checks match how data is timestamped (UTC vs market timezone).

Required evidence:
- no “Submitted to queue” on warm S3
- cache coverage logs do not show repeated “STALE” on identical windows

### 9.3 Reduce S3 roundtrips

Patterns:
- prefer fewer larger objects over many tiny objects
- avoid redundant downloads (parquet + sidecar) when sidecar is optional
- batch/hydrate only when needed

Required evidence:
- remote cache stats show fewer downloads
- wall time improves in production-like runs

### 9.4 Reduce per-bar compute overhead

Patterns:
- avoid scanning large data stores every call (use metadata flags / coverage caches)
- avoid repeatedly converting timezones / parsing datetimes in hot loops
- avoid re-deriving the same indicators or chain filters per iteration

Required evidence:
- yappi hotspots shift away from repeated utility work

---

## 10) ThetaData-focused speed work (what usually matters)

ThetaData backtests are often dominated by:
- option chain building (expirations/strikes fanout)
- option quote history / snapshot calls
- index minute data coverage edge cases

Common targets:
- `lumibot/tools/thetadata_helper.py`
- `lumibot/backtesting/thetadata_backtesting_pandas.py`
- `lumibot/tools/data_downloader_queue_client.py`

Key invariants:
- do not alter trade/quote semantics silently
- preserve acceptance results and tiered accuracy validation

Useful reference investigations:
- `docs/investigations/2026-01-03_PROD_SPEED_PARITY_YAPPI.md` (waiting dominated → request fanout)
- `docs/investigations/2026-01-06_THETADATA_OPTION_EOD_GAPS_DAILY_MTM.md` (correctness fix with cache implications)
- `docs/investigations/2026-01-21_NDX_ALL_ZERO_OHLC.md` (placeholder data must converge; no infinite loops)

---

## 11) Data Downloader health (when slowness isn’t a LumiBot bug)

Sometimes “slow backtests” are caused by downloader degradation:
- queue endpoints timing out
- queue DB growth causing slow SQLite queries/locks
- service “up” but queue subsystem unhealthy

If logs show repeated submit timeouts or connection errors, consult the downloader docs:
- `botspot_data_downloader/README.md`
- `botspot_data_downloader/docs/investigations/2026-01-19_data-downloader-queue-db-incident.md`
- `botspot_data_downloader/docs/runbooks/data-downloader-alerting-and-self-heal.md`

Important:
- do not hardcode internal URLs in this repo
- treat downloader health checks as required for stable backtest SLAs

---

## 12) Testing requirements for performance work

### 12.1 Unit tests: required for any behavioral or caching change

Rule:
- If you change behavior or cache semantics, add/update unit tests so the improvement sticks.

Follow legacy test policy:
- `tests/AGENTS.md` (fix the code, not legacy tests, unless clearly justified)

### 12.2 Acceptance backtests: required before considering a change “done”

Always run:
- `pytest -q tests/backtest/test_acceptance_backtests_ci.py`

Acceptance asserts:
- metrics match baseline JSON strictly
- warm-cache invariant (no downloader submits)
- runtime budget respected

### 12.3 Speed budgets: how to update them safely

Acceptance speed budgets live in:
- `tests/backtest/acceptance_backtests_baselines.json` (`max_backtest_time_seconds`)

Guidelines:
- Do not set budgets based on your laptop runtime.
- Budgets should reflect CI reality (GitHub Actions is slower and more variable).
- If a change makes a benchmark significantly faster, you may tighten budgets, but:
  - confirm via CI runs (or keep conservative slack)
  - avoid flakiness (don’t set budgets too close to the mean)

### 12.4 “Performance regression tests” (recommended)

When a fix eliminates a known hot-loop failure mode, add a deterministic test that would fail if the loop returns.
Examples:
- “warm cache must have 0 queue submits”
- “history fetches must be O(1) per symbol/timeframe, not O(bars)”

---

## 13) Documentation + code-comment requirements (make it stick)

### 13.1 Every performance change must be documented (two places)

1) **Investigation ledger (append-only)**: record before/after runs with evidence.
   - `docs/investigations/YYYY-MM-DD_<TOPIC>.md` (+ optional `.csv`)
2) **Code comments (why/invariants)**: annotate the change at the point of implementation.

### 13.2 Code comment style for perf fixes (recommended)

When you land a perf fix, add a comment that answers:
- What was slow (symptom + evidence)?
- Why this fix works (mechanism)?
- What invariant must remain true (correctness + cache semantics)?
- What tests protect it?

Example pattern (adapt; do not paste secrets):

```python
# PERF: <one-line summary of bottleneck>
# WHY: <evidence: profiler/log signature>
# INVARIANT: <correctness constraint + warm-cache invariant>
# TESTS: <test paths that prevent regression>
```

### 13.3 Where to log run history (recommended standard)

Use:
- **One ledger per initiative** under `docs/investigations/` (date-first).
  - Example: `docs/investigations/2026-01-27_ROUTER_IBKR_SPEED.md` + `.csv`

Why:
- prevents an unbounded “mega ledger” file
- keeps each initiative auditable and easy to close out

Optionally (when a topic becomes evergreen):
- summarize stable learnings into `docs/BACKTESTING_PERFORMANCE.md`

Private/internal IDs/links/log queries belong in:
- `docs/handoffs/` (gitignored)

---

## 14) Cache format/layout changes (avoid unless necessary)

Cache changes are expensive to operationalize because large S3 caches already exist.

Policy:
1) Prefer **backwards-compatible** cache changes when possible (read old + write new).
2) If compatibility is too complex or unsafe, use a **version bump**:
   - set a new `LUMIBOT_CACHE_S3_VERSION`
   - warm the new namespace intentionally
   - keep the old namespace intact (no mass deletion)

If a cache version bump is required:
- document the migration plan (what warms, how long, cost implications)
- ensure acceptance backtests are warmed for the new namespace before CI expects queue-free runs

See also:
- `docs/REMOTE_CACHE.md`
- `docs/THETADATA_CACHE_VALIDATION.md`

---

## Appendix A) Canonical env vars (sanitized examples)

Router-mode (production-like):

```bash
export IS_BACKTESTING=True
export BACKTESTING_DATA_SOURCE='{"default":"thetadata","crypto":"ibkr","future":"ibkr","cont_future":"ibkr"}'

export DATADOWNLOADER_BASE_URL='https://<your-downloader-host>:8080'
export DATADOWNLOADER_API_KEY='<redacted>'
export DATADOWNLOADER_API_KEY_HEADER='X-Downloader-Key'
export DATADOWNLOADER_SKIP_LOCAL_START='true'
export THETADATA_USE_QUEUE='true'

export LUMIBOT_CACHE_BACKEND='s3'
export LUMIBOT_CACHE_MODE='readwrite'
export LUMIBOT_CACHE_S3_BUCKET='lumibot-cache-dev'
export LUMIBOT_CACHE_S3_PREFIX='dev/cache'
export LUMIBOT_CACHE_S3_REGION='us-east-1'
export LUMIBOT_CACHE_S3_VERSION='v44'

# Recommended for prod-like runs. BotSpot/BotManager should rely on injected env vars,
# not local .env or .env.local files.
export LUMIBOT_DISABLE_DOTENV='1'

# Optional when you want to load .env but block developer-local overrides.
export LUMIBOT_DISABLE_DOTENV_LOCAL='1'

# Recommended for repeated perf runs (avoid browser/UI spam; artifacts are still written to logs/)
export LUMIBOT_DISABLE_UI='1'
```

Local isolation (simulate fresh container disk without deleting anything):

```bash
export LUMIBOT_CACHE_FOLDER='/Users/<you>/Documents/Development/backtest_cache/theta_router_perf/run1'
```

Profiling:

```bash
export BACKTESTING_PROFILE='yappi'
```

---

## Appendix B) Common failure signatures (and what to do)

### B.1 “Submitted to queue” appears on a warm-cache run

Interpretation:
- S3 is not actually warm for the requested objects, OR
- cache keying/coverage is unstable, OR
- the strategy is generating unique requests per iteration (often via windowed snapshot keys).

Action:
- find top queue paths
- identify which cache objects are missing/unstable
- fix cache semantics or prefetch windowing so warm runs become queue-free

### B.2 Backtest “looks stuck” (progress not moving)

Possible causes:
- waiting on downloader queue (simulation datetime not advancing)
- progress heartbeat disabled/misconfigured
- BotManager status sync issues (out of scope for LumiBot, but affects UX)

Action:
- inspect `download_status` and `thetadata_queue_telemetry` in settings
- check downloader health if submit timeouts appear

---

End of playbook.
