# Early Exit Profiling

## Overview

Early exit profiling allows you to detect O(n²) performance degradation **without running full long-duration backtests**. This is especially useful when:

- 10-month runs take 1+ hour
- 3-month runs take 20+ minutes
- You want to quickly identify performance issues before investing time in full runs

## How It Works

1. **Baseline**: Use Phase 0 results (1-month completed run) as baseline
2. **Early Exit**: Run long backtest with timeout (e.g., 2 minutes)
3. **Compare**: Analyze if functions are taking longer per call in early exit vs baseline

### Key Insight

If a function has O(n²) complexity:
- Its per-call execution time increases as the backtest progresses
- In the first 2 minutes of a 10-month run, if functions are already slower per call than in Phase 0, this indicates non-linear scaling
- We can detect the problem early without waiting for the full hour-long run

## Usage

### Step 1: Run with Timeout

Run any Phase 1 profiling session with a timeout:

```bash
# Run 10-month backtest with 2-minute timeout
python -m profiler.runner --run full-2025-01-01_10-31 --timeout 120

# Run 3-month backtest with 2-minute timeout
python -m profiler.runner --run qtr-2025-Q2like --timeout 120
```

The profiler will:
- Start YAPPI profiling
- Run the backtest for up to 120 seconds
- Stop automatically when timeout is reached
- Save partial profiling data with `.early` suffix

### Step 2: Compare Against Baseline

Compare the early exit data against Phase 0 baseline:

```bash
python -m profiler.compare_early_exit --baseline phase0-cold-2025-01 --early-exit full-2025-01-01_10-31
```

### Expected Output

```
================================================================================
EARLY EXIT COMPARISON ANALYSIS
================================================================================

Baseline Run: phase0-cold-2025-01
  Time: 1m 28.2s
  Iterations: 6,120
  Rate: 69.4 iter/sec

Early Exit Run: full-2025-01-01_10-31
  Time: 2m 0.0s
  Timeout: 120s
  Iterations: 4,500

PERFORMANCE ANALYSIS
--------------------------------------------------------------------------------
Expected iterations in 120s: 8,328
Actual iterations: 4,500
Deficit: 3,828 iterations
Performance ratio: 54.02%

⚠️  DEGRADATION DETECTED: Performance is significantly slower than baseline
    This suggests non-linear scaling (possible O(n²) behavior)

TOP FUNCTIONS COMPARISON
--------------------------------------------------------------------------------
Function                                            Baseline    Early Exit     Ratio
                                                   (avg ms)     (avg ms)
--------------------------------------------------------------------------------
⚠️  pd.concat                                        0.0234        0.0512     2.19x
⚠️  GCFuturesOptimized.on_trading_iteration          1.2450        2.1123     1.70x
   Strategy.get_historical_prices                   0.4521        0.4623     1.02x
   DataBentoDataBacktesting.get_historical_prices   0.3201        0.3145     0.98x

Functions with warnings: 2
(⚠️  = function is 50%+ slower per call in early exit)
```

## Interpretation

### ✅ No Degradation (Good)
```
Performance ratio: 95.12%
✅ NO DEGRADATION: Performance is consistent with baseline
```
This means the backtest is scaling linearly - safe to run full duration.

### ⚠️ Degradation Detected (Problem)
```
Performance ratio: 54.02%
⚠️  DEGRADATION DETECTED: Performance is significantly slower than baseline
```
This indicates O(n²) or worse scaling - **DO NOT run full backtest**. Fix the issue first!

### Function Warnings

Functions marked with ⚠️ are taking 50%+ longer per call in the early exit vs baseline:
- **Ratio > 1.5**: Function is slowing down (possible O(n) growth in an O(n) loop = O(n²))
- **Ratio > 2.0**: Strong evidence of quadratic behavior
- **Ratio ≈ 1.0**: Function is stable (good)

## Time Savings

Without early exit:
- 10-month run: 60+ minutes
- 3-month run: 20+ minutes
- **Total for 2 runs**: 80+ minutes

With early exit:
- 10-month run: 2 minutes
- 3-month run: 2 minutes
- **Total for 2 runs**: 4 minutes

**Savings: 76 minutes (95% time reduction)**

## Recommended Workflow

1. **Complete Phase 0** (cache probe) - this is your baseline
2. **Pre-cache data** for Phase 1 date ranges
3. **Run short Phase 1 tests** (1-week, 1-month) normally - these are fast
4. **Use early exit for long tests** (3-month, 10-month):
   ```bash
   python -m profiler.runner --run qtr-2025-Q2like --timeout 120
   python -m profiler.runner --run full-2025-01-01_10-31 --timeout 120
   ```
5. **Compare early exit results**:
   ```bash
   python -m profiler.compare_early_exit --early-exit qtr-2025-Q2like
   python -m profiler.compare_early_exit --early-exit full-2025-01-01_10-31
   ```
6. **If degradation detected**: Fix O(n²) code before running full tests
7. **If no degradation**: Safe to run full-duration tests

## Files Generated

Early exit profiling creates files with `.early` suffix:

```
profiling/
  full-2025-01-01_10-31.early.funcs.wall.csv    # Function stats
  full-2025-01-01_10-31.early.threads.wall.csv  # Thread stats
  full-2025-01-01_10-31.early.wall.pstat        # pstat format
```

Results are also saved to `profiling_plan.json` with:
```json
{
  "full-2025-01-01_10-31": {
    "wall_seconds_total": 120.0,
    "early_exit": true,
    "timeout_seconds": 120,
    "notes": "Early exit profiling (timeout=120s) on 2025-11-01 ..."
  }
}
```

## Advanced: Custom Timeout Values

Choose timeout based on expected total runtime:

- **For 20-minute runs**: Use `--timeout 120` (2 minutes = 10% sample)
- **For 1-hour runs**: Use `--timeout 180` (3 minutes = 5% sample)
- **For 2+ hour runs**: Use `--timeout 300` (5 minutes = ~4% sample)

Recommended: **2-3 minutes is sufficient** for most cases.

## Limitations

- Early exit profiling captures data from the **start** of the backtest
- Some O(n²) issues may not be detectable if they only manifest after longer accumulation
- Iteration count may not be available for early exit runs (depends on when timeout occurs)
- Best used as a **screening tool** - if degradation is detected, run full test after fixing

## Example Session

```bash
# 1. Complete Phase 0 (if not already done)
python -m profiler.runner --phase 0

# 2. Pre-cache data
python precache_gc_futures.py

# 3. Run short tests normally
python -m profiler.runner --run wk-2025-01-20_01-26
python -m profiler.runner --run mo-2025-02

# 4. Run long tests with early exit
python -m profiler.runner --run qtr-2025-Q2like --timeout 120
python -m profiler.runner --run full-2025-01-01_10-31 --timeout 120

# 5. Compare results
python -m profiler.compare_early_exit --early-exit qtr-2025-Q2like
python -m profiler.compare_early_exit --early-exit full-2025-01-01_10-31

# 6. If no degradation detected, run full tests
python -m profiler.runner --run qtr-2025-Q2like
python -m profiler.runner --run full-2025-01-01_10-31
```
