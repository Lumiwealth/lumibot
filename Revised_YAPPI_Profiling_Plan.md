# Revised Plan — YAPPI Wall-Time Profiling for LumiBot Backtests

## Objectives
- Use **YAPPI wall time** to identify slow paths in LumiBot backtests.
- Keep runtime behavior identical to current tests (no features disabled).
- Eliminate randomness in sampling windows by predefining **all date ranges** up front.
- Store **the plan and results in a single config file** that acts as a queue and run log.

---

## Invariants (do **not** change during profiling)
- `plot_price = true`
- `show_live_equity_curve = true`
- File logging **enabled**
- Strategy lookbacks / parameters **unchanged**
- Same symbols, same data providers (DataBento), same environment

---

## Phase 0 — Cache-Impact Probe (run first)
Purpose: quantify the impact of first-time DataBento downloads vs cached runs.

1. Run the **same 1-month backtest twice in a row** with YAPPI enabled:
   - **Cold run** (no cache): `2024-09-01` → `2024-09-30`
   - **Warm run** (immediately after, with cache): `2024-09-01` → `2024-09-30`
2. Persist both runs’ YAPPI outputs and wall-time summaries.
3. Compute deltas (total wall time, top function self time, and cumulative time).
4. Outcome handling (to be decided after results):
   - If cold vs warm delta is **material**, we’ll either (a) add an explicit **preload step** for each 2025 window before profiling, or (b) run each 2025 window **twice** and only use the second (warm) profile.
   - If delta is **minor**, proceed with single profiles for 2025 windows.

---

## Phase 1 — Deterministic 2025 Backtest Schedule
Date universe: **2025-01-01 → 2025-10-31**.  
These windows are **fixed** (no per-run randomness). They’re spread across the period and avoid overlap with one another where practical.

### Four 1-week windows
- `2025-01-20` → `2025-01-26`
- `2025-03-10` → `2025-03-16`
- `2025-07-14` → `2025-07-20`
- `2025-10-13` → `2025-10-19`

### Three 1-month windows
- `2025-02-01` → `2025-02-28`
- `2025-05-01` → `2025-05-31`
- `2025-09-01` → `2025-09-30`

### One 3-month window
- `2025-04-01` → `2025-06-30`

### One full-range window (~10 months)
- `2025-01-01` → `2025-10-31`

> If Phase 0 shows a large cold/warm delta, insert a **cache-preload** pass for each window (same dates, no profiling) immediately before the profiled run, or run each entry twice and keep only the second profile.

---

## Single Source of Truth — Config + Queue File
Create `profiling_plan.json` at repo root. This file defines the plan **and** acts as a results log and run queue.

```json
{
  "meta": {
    "profiler": "yappi",
    "clock_type": "WALL",
    "output_dir": "profiling",
    "random_seed_for_selection": 20251101,
    "created_utc": "2025-11-01T00:00:00Z"
  },
  "invariants": {
    "plot_price": true,
    "show_live_equity_curve": true,
    "file_logging": true,
    "lookbacks_unchanged": true
  },
  "runs": [
    { "id": "phase0-cold-2024-09", "start": "2024-09-01", "end": "2024-09-30", "label": "cache_probe_cold", "status": "pending" },
    { "id": "phase0-warm-2024-09", "start": "2024-09-01", "end": "2024-09-30", "label": "cache_probe_warm", "status": "pending" },

    { "id": "wk-2025-01-20_01-26", "start": "2025-01-20", "end": "2025-01-26", "label": "week", "status": "pending" },
    { "id": "wk-2025-03-10_03-16", "start": "2025-03-10", "end": "2025-03-16", "label": "week", "status": "pending" },
    { "id": "wk-2025-07-14_07-20", "start": "2025-07-14", "end": "2025-07-20", "label": "week", "status": "pending" },
    { "id": "wk-2025-10-13_10-19", "start": "2025-10-13", "end": "2025-10-19", "label": "week", "status": "pending" },

    { "id": "mo-2025-02", "start": "2025-02-01", "end": "2025-02-28", "label": "month", "status": "pending" },
    { "id": "mo-2025-05", "start": "2025-05-01", "end": "2025-05-31", "label": "month", "status": "pending" },
    { "id": "mo-2025-09", "start": "2025-09-01", "end": "2025-09-30", "label": "month", "status": "pending" },

    { "id": "qtr-2025-Q2like", "start": "2025-04-01", "end": "2025-06-30", "label": "three_months", "status": "pending" },
    { "id": "full-2025-01-01_10-31", "start": "2025-01-01", "end": "2025-10-31", "label": "full_range", "status": "pending" }
  ],
  "results": {
    "_schema": {
      "wall_seconds_total": "float",
      "wall_seconds_no_download": "float|nullable",
      "cpu_seconds": "float|nullable",
      "top_functions_csv": "path",
      "top_threads_csv": "path",
      "git_rev": "string",
      "timestamp_utc": "iso8601",
      "notes": "string"
    }
  }
}
```

**Queue semantics**
- Each run starts with `status: "pending"`.
- Runner sets `status: "running"`; upon completion sets `status: "done"` or `"error"`.
- Results for each `id` are appended under `results[id]`.

---

## Execution Protocol (each run)
1. Ensure invariants are set (`plot_price`, `show_live_equity_curve`, logging).
2. YAPPI setup (wall clock), start immediately before the backtest begins; stop right after it ends.
3. Save outputs:
   - `profiling/<id>.funcs.wall.csv`
   - `profiling/<id>.threads.wall.csv`
   - Optional pstats: `profiling/<id>.wall.pstat`
4. Persist a per-run summary into `profiling_plan.json → results[id]`:
   - `wall_seconds_total`
   - `wall_seconds_no_download` (leave `null` for now; populate later if we add a separate prefetch timer)
   - `git_rev`
   - `timestamp_utc`
   - `notes` (e.g., “cache-warm pass”, “preloaded”, or error info)

**YAPPI instrumentation (minimal snippet to add around the backtest call)**
```python
# new
import yappi, time

# new — wall time
yappi.set_clock_type("wall")
yappi.start()

t0 = time.time()
run_backtest(start_date, end_date)  # your existing entry point
t1 = time.time()

yappi.stop()

func_stats = yappi.get_func_stats()
thread_stats = yappi.get_thread_stats()
func_stats.save(f"profiling/{run_id}.funcs.wall.csv", type="csv")
thread_stats.save(f"profiling/{run_id}.threads.wall.csv", type="csv")

wall_seconds_total = t1 - t0
# write wall_seconds_total and file paths back into profiling_plan.json under results[run_id]
```

> If Phase 0 shows a large cold/warm gap and you choose the **preload** approach, surround your data-load routine with a simple timer and record that as `wall_seconds_no_download = wall_seconds_total - preload_seconds`. Keep the actual backtest profile as the second (warm) pass.

---

## Reporting
- After Phase 0, compute and store:
  - `delta_wall_seconds = warm.wall_seconds_total - cold.wall_seconds_total`
  - Top offenders in `*.funcs.wall.csv` (rank by cumulative time).
- Proceed with Phase 1 using either single runs or the preload/two-pass approach based on the Phase 0 delta.

---

## Acceptance
- All date ranges are predetermined and recorded in `profiling_plan.json`.
- Invariants hold across every run (no features disabled).
- Each run logs YAPPI outputs and a JSON summary into the same file.
- The plan is reproducible (seed and dates fixed).
