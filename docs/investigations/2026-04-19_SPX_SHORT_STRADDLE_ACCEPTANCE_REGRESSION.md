# SPX Short Straddle Acceptance Test Regression — Investigation

**Last Updated:** 2026-04-19
**Status:** In progress — root cause candidates enumerated; final bisect requires CI (credentials not available locally)
**Audience:** lumibot maintainers, release engineers working the 4.5.0 PyPI cut

## Overview

`test_acceptance_spx_short_straddle` has been red on CI since 2026-04-01 (the merge of PR #984 / `v4.4.58`). Metrics drifted ~2.3% beyond the 0.15% centipercent tolerance. This investigation documents what we know, what we've ruled out, and the most efficient remaining bisect path. Do NOT rebaseline the test without landing on a specific root-cause commit — the user policy is "if the test fails, the code is wrong; we don't just fix tests assuming they're right."

## Scope

- **Test:** `tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_spx_short_straddle`
- **Script:** `tests/backtest/acceptance_strategies/SPX Short Straddle Intraday (Copy).py`
- **Window:** 2025-01-06 → 2025-12-25, ThetaData, 1-minute sleeptime, 0DTE SPXW short straddle, EOD close at 15:55 ET.
- **Baseline:** `4.4.36` frozen on 2026-01-23 (baseline_run_id `SPXShortStraddle_2026-01-23_23-19_zOT1cB`).
- **Failure signal:** last observed on CI run `24492945391` (2026-04-16) — metrics drift well past the ±0.15% centipercent tolerance.

## Observed Drift

| Metric | Expected (4.4.36) | Current (4.4.62+) | Delta (cps) | Delta (%) |
|---|---|---|---|---|
| `total_return` | -985 | -1214 | -229 | -2.29% worse |
| `cagr` | -1018 | -1253 | -235 | -2.35% worse |
| `max_drawdown` | -2771 | -2873 | -102 | -1.02% deeper |

Direction: **more negative returns, deeper drawdown**. Consistent signature of either additional realized costs (fees/slippage) or less-favorable portfolio valuation over the year.

## Timeline

| Date | Event |
|---|---|
| 2026-01-23 | `4.4.36` released; SPX straddle baseline captured. |
| 2026-02-24 | `cce1052a` — prefer snapshot mark for daily Theta option MTM. |
| 2026-03-03 | `728bfc0f` — guard backtest option MTM from off-session stale marks. |
| 2026-03-06 | `82aa9bea` + `29c81c02` — daily last-price optimization scoped by datasource. |
| 2026-03-08 | `721fab17` — apply per-contract trading fees in backtesting. |
| 2026-04-01 | `v4.4.58` merged into `dev` → test goes red on CI. |
| 2026-04-16 | Most recent failing CI run observed (`24492945391`). |

## Earlier Finding (Previous Session, Compacted)

A prior session pinned the "regression appeared" signal to the `v4.4.57 → v4.4.58` boundary, but subsequent diff inspection showed that specific boundary only contains logging changes (4 lines in `_strategy.py`, 3 lines in `trader.py`) — behaviorally a no-op. Conclusion from that session: the real change MUST be somewhere in the `v4.4.36 → v4.4.57` range, and the `4.4.58` merge is just when CI started observing it (possibly because `4.4.58` includes a merge from `dev` that pulled earlier commits onto the release path). That conclusion is carried over here.

## Candidate Commits (not yet bisected)

Listed in descending likelihood of touching the straddle's hot path.

### Highly likely (options MTM / fill pricing)

- **`cce1052a` — "fix: prefer snapshot mark for daily Theta option MTM"** (Feb 24).
  Changes `_strategy.py` option-quote fetch to prefer snapshot marks even when a day quote is present, plus a last-resort day-quote fallback when snapshot probing fails. Hot-path only for `timestep_hint == "day"`; **needs verification** that `_update_portfolio_value_prices` doesn't flow option valuation through that path at EOD on a 1-minute backtest.

- **`728bfc0f` — "Guard backtest option MTM from off-session stale marks"** (Mar 3).
  Blocks portfolio valuation from ingesting option marks outside 09:30–16:00 ET. The straddle closes at 15:55 ET so in theory has no open options outside hours — **but** if a close order fails or slips to 16:00, the guard would change the EOD valuation of a briefly-open residual position. Low-probability behavioral hit, but non-zero.

- **`00484d04` — "Fix backtest portfolio valuation on zero price"** — inspection pending; commit title directly names portfolio valuation.

- **`5a3ce88f` — "fix: use forward-fill when daily snapshot quote is unavailable"** — daily forward-fill. Same caveat as `cce1052a` about the day-cadence path being triggered.

### Likely (option lifecycle / settlement)

- **`205b8420` — "feat: model option assignment/exercise settlement and propagate lifecycle statuses"** — straddle closes at 15:55 so normally no assignment, but edge cases exist if close orders don't all fill by close.
- **`6fb3484f` — "feat: add opt-in early assignment model and refresh acceptance baselines"** — opt-in; straddle script doesn't appear to enable it.
- **`27b3d0dd` — "fix: resolve meli acceptance regression and update option DNE behavior"** — DNE ("Did Not Exercise") on expiring OTM options. Plausible for 0DTE positions that straddle the close.

### Ruled Out

- **`721fab17` — "apply per-contract trading fees in backtesting"**. Verified: the straddle script passes `TradingFee(percent_fee=0.0)` and never sets `per_contract_fee`. With `per_contract_fee=0.0`, the new `trade_cost += Decimal(str(order.quantity)) * trading_fee.per_contract_fee` term is zero. No behavioral impact.
- **`82aa9bea` / `29c81c02` — daily last-price optimization scoped by datasource**. These only gate `_should_use_daily_last_price_optimization()`; the straddle is a 1-minute backtest and `_should_use_daily_last_price(asset)` returns False for its ATM option queries.
- **`v4.4.57 → v4.4.58` boundary** (logging-only, per prior session).

## Reproduction Plan (what the next agent should do)

Local repro requires `THETADATA_USERNAME`, `THETADATA_PASSWORD`, `DATADOWNLOADER_BASE_URL`, `DATADOWNLOADER_API_KEY`, and the `LUMIBOT_CACHE_S3_*` credentials. The `.env` file in this repo already has them; source it before running:

```bash
set -a; source .env; set +a
pytest tests/backtest/test_acceptance_backtests_ci.py::test_acceptance_spx_short_straddle -xvs
```

Each run is ~9 min on CI, ~15–20 min locally. Parallelise if possible — each candidate commit can be tested on its own git worktree.

### Recommended Bisect Order (shortest expected path to root cause)

1. **Bisect bracket check:** confirm `4.4.36` (`ff1b5022`) matches baseline and HEAD reproduces current failing numbers. One run each — establishes the invariant.
2. **Check `728bfc0f^` vs `728bfc0f`** — the off-session MTM guard is the highest-probability single commit because it directly gates which option marks enter portfolio valuation. Two runs.
3. **If (2) flat, check `cce1052a^` vs `cce1052a`** — snapshot-mark preference. Two runs.
4. **If still flat, check `00484d04` and `5a3ce88f`** — zero-price and forward-fill patches.
5. **If still flat, binary-bisect the remaining 107-commit range** via `git bisect run` with a script that extracts `total_return` from the tearsheet and asserts `abs(total_return + 985) < 15`.

## Decision Framework Once Root Cause is Found

Once the specific commit is identified:

- **If the commit is a documented correctness fix** (e.g., "avoid stale mark poisoning"), and the new metric is MORE correct than the baseline: rebaseline `spx_short_straddle_repro` in `tests/backtest/acceptance_backtests_baselines.json` with a CHANGELOG entry citing the specific commit and explaining why the baseline moved. Record the new `baseline_run_id`. Do NOT lower the tolerance to paper over drift.
- **If the commit is a regression** (broke behavior that was previously correct): fix the commit, don't rebaseline.

## Why the Final Bisect is Not in This Doc

Local runs require live ThetaData credentials, a warm S3 cache namespace, and ~9 min per iteration — 5+ iterations at minimum to isolate. This doc gets the next session to within 2-3 runs of the root cause by pre-ruling-out obvious no-ops (fee change, daily-pricing scope changes) and pre-prioritising the highest-probability candidate (off-session MTM guard).

## Bisect log — 2026-04-19

Local reruns with identical baseline (`-985 / -1018 / -2771`) and fresh S3 cache:

| Commit | Date | Result | Notes |
|---|---|---|---|
| `a9cab9e4` ("start 4.4.53", just before `728bfc0f`) | 2026-03-03 | **PASS** | ~5:42 runtime; metrics match baseline. |
| `728bfc0f` ("Guard backtest option MTM from off-session stale marks") | 2026-03-03 | **PASS** | ~5:46 runtime. Rules out the off-session MTM guard — it does NOT cause the drift. (Contradicts earlier hypothesis in this doc's "Candidate Commits" section.) |
| `aa98d089` (4.4.58 merge) | 2026-04-01 | **FAIL** | ~6:06 runtime. Total Return -12.24% (baseline -9.85%), CAGR -12.64% (baseline -10.18%). Same ~2.3% drift magnitude as current HEAD. |
| `version/4.5.0` HEAD (`0306ebff`) | 2026-04-19 | **FAIL** | Total Return -12.14%, CAGR -12.53%, Max Drawdown -28.73%. |

**So the regression is in `(728bfc0f, aa98d089]` — commits from 2026-03-03 22:11 through 2026-04-01 02:02.**

### Bisect narrowed to a single commit: `af8df88b`

Subsequent bisect iterations against baseline `-985 / -1018 / -2771`:

| Commit | Date | Result | Metrics |
|---|---|---|---|
| `83f0056e` ("Fix day-timestep lookup regression and harden IBKR no-data cache reuse") | 2026-03-12 | **PASS** | matches baseline |
| `8c4c0913` ("backtest: fix console print settings being overwritten") | 2026-03-17 | **PASS** | matches baseline |
| `489d34ff` (Merge PR #981 "backtest_console_logging") | 2026-03-27 | **PASS** | matches baseline |
| `af8df88b` ("WIP: checkpoint local work before creating version/4.4.57") | 2026-03-30 20:46 | **FAIL** | -12.24% / -12.64% — identical to HEAD |
| `0a9db3ce` ("deploy 4.4.57") | 2026-03-30 21:28 | **FAIL** | -12.24% / -12.64% |
| `aa98d089` (4.4.58 merge) | 2026-04-01 | **FAIL** | -12.24% / -12.64% |

The ~2.3% drift is introduced in a single commit: **`af8df88b`**.

### What's in `af8df88b`

Commit message says: *"Includes: AI agents module, cash events, broker updates, docs, tests."* The `lumibot/` diff alone is huge — key files:

| File | Lines | Role |
|---|---|---|
| `lumibot/strategies/_strategy.py` | 801 | core strategy + portfolio valuation |
| `lumibot/tools/indicators.py` | 451 | tearsheet metrics computation |
| `lumibot/entities/cash_event.py` | 214 (new) | **Cash events subsystem** |
| `lumibot/tools/ibkr_helper.py` | 123 | IBKR data fetching |
| `lumibot/strategies/strategy.py` | 110 | Strategy public API |
| `lumibot/backtesting/routed_backtesting.py` | 45 | Routed backtesting adapter |
| `lumibot/brokers/{broker,alpaca,tradier}.py` | 491 | Broker layer |
| `lumibot/components/agents/*` | ~2800 (new) | AI agents module |

### Prime suspect: cash events subsystem

For a short-straddle P&L drift of this exact shape (~-2.3% on Total Return AND CAGR, -1% on Max Drawdown) the most plausible cause is a change in how option premium credit / debit / margin is accounted. That's exactly what the new `cash_event.py` + its wiring into `_strategy.py` introduces. A 2026-03-25 architecture doc (`docs/handoffs/2026-03-25_cash-events-and-financing-architecture.md`) is added in this same WIP, confirming cash-accounting semantics were actively being redesigned.

Secondary suspect: `indicators.py` 451-line change (tearsheet metrics). Inspected: primarily adds custom-metrics hooks and a metrics JSON artifact, not a redefinition of total_return / cagr / max_drawdown. Less likely to be the cause but not ruled out.

### Next steps for the owner of cash-events

1. Diff `af8df88b^..af8df88b -- lumibot/entities/cash_event.py lumibot/strategies/_strategy.py` and the cash-events handoff doc.
2. Decide: is the new cash-events accounting the correct one (the 2026-01-23 baseline was wrong), or does the new subsystem introduce a subtle bug (the baseline was correct)?
3. If new is correct → rebaseline the `spx_short_straddle_repro` baseline with a changelog citation to `af8df88b` + the cash-events architecture doc.
4. If new has a bug → fix it in `cash_event.py` / `_strategy.py` cash wiring.

Local bisect requires ~6 min per iteration; 6 total iterations were used here (728bfc0f^, 728bfc0f, aa98d089, 0a9db3ce, 83f0056e, 8c4c0913, 489d34ff, af8df88b). Worktrees cleaned up. The `.env` file used is the repo-root `.env` with ThetaData + S3 + downloader credentials.

## Data Collected

- CI failure artifacts from run `24492945391` (shard 2/4, 2026-04-16): raw assertion output only; the tearsheet CSV was not preserved as a build artifact. For full trades.csv / logs.csv access, the failing tearsheet would need to be pulled from a fresh CI run (add `actions/upload-artifact` step scoped to `_acceptance_runs/**` before bisecting).
- `git log ff1b5022..aa98d089 -- lumibot/strategies/ lumibot/brokers/ lumibot/backtesting/ lumibot/tools/smart_limit_utils.py lumibot/entities/order.py` — 107 commits, 16 files, 4,426 insertions, 837 deletions.
