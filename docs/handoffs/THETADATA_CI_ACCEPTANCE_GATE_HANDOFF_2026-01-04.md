# ThetaData CI Acceptance Gate — Handoff (2026-01-04)

This handoff is for a *new agent/session* whose only job is:

1) Make the **manual Strategy Library acceptance backtests** run in **GitHub CI**
2) Make them run **the same way we run them locally** (production-like flags)
3) Make them **block merges/releases** when they fail
4) Record and enforce:
   - **CAGR (annual return)**, Total Return, Max Drawdown
   - **wall-clock runtime**
   - **cache behavior** (no accidental ThetaTerminal hits in a “warm cache” run)

This file is intentionally long and explicit because we have repeatedly lost time
to “almost acceptance tests” that were not faithful to the real system.

---

## 0) Ground Rules (non-negotiable)

### 0.1 Absolute bans / constraints

- **Never run `git checkout`** (use `git switch`, `git restore`).
- **Do not modify Strategy Library demo strategies** and do not modify customer strategies.
  - Fix LumiBot (and only data-downloader if it is the proven root cause).
  - If CI needs strategy code, **copy the demos into test fixtures as frozen snapshots**
    and treat them as read-only (no “fixes” in the strategy logic).
- **Long-running commands must be wrapped**:
  - `/Users/robertgrzesik/bin/safe-timeout 1200s …` (use longer only for explicit full-window runs).
- Never start ThetaTerminal locally with prod credentials (it kills prod access).

### 0.2 CI acceptance tests must be faithful

The CI gate must run the same acceptance backtests we run locally:

- Same 7 strategies (same code, same windows)
- Same env vars (production-like flags)
- Same data source behavior:
  - `BACKTESTING_DATA_SOURCE=thetadata`
  - `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
  - use dev S3 cache credentials
  - allow downloader creds to be present (so the run behaves like prod)

**Do not invent “fake” smokes**.
If you must add “smoke-only” short-window tests, they can exist, but they do not
replace the acceptance gate.

---

## 1) Repo Map (where everything lives)

### 1.1 LumiBot repo root

`/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot`

### 1.2 Canonical docs

- Acceptance gate (human-facing): `docs/ACCEPTANCE_BACKTESTS.md`
- Architecture: `docs/BACKTESTING_ARCHITECTURE.md`
- Remote cache: `docs/remote_cache.md`
- Cache validation: `docs/THETADATA_CACHE_VALIDATION.md`
- Investigations: `docs/investigations/`
- Handoffs: `docs/handoffs/`

### 1.3 CI tests location

- All tests live under `tests/`
- Backtest tests live under `tests/backtest/`
- The acceptance gate should live under `tests/backtest/` as well.

Important: tests are subject to `tests/AGENTS.md` legacy policy:
- If a test predates 2025-01-01, fix code, not test.
- For new tests (this acceptance gate), you can iterate the expectations, but
  document why you updated them.

---

## 2) The Acceptance Suite (the “official 7”)

These are the **only** strategies the acceptance gate cares about.
They are defined in `docs/ACCEPTANCE_BACKTESTS.md`.

List (Strategy Library demo filenames; do not modify demo code):

1) `Demos/AAPL Deep Dip Calls (Copy 4).py`
2) `Demos/Leaps Buy Hold (Alpha Picks).py`
3) `Demos/TQQQ 200-Day MA.py`
4) `Demos/Backdoor Butterfly 0 DTE (Copy).py`
5) `Demos/Meli Deep Drawdown Calls.py`
6) `Demos/Backdoor Butterfly 0 DTE (Copy) - with SMART LIMITS.py`
7) `Demos/SPX Short Straddle Intraday (Copy).py`

**CI requirement:** run these same 7 strategies with the same windows.

---

## 3) CI Problem Statement (why this exists)

We repeatedly ship regressions because:

- Acceptance runs are expensive and humans forget to run all 7
- “Smoke tests” are not representative of real behavior
- Prod vs local parity drifts without being detected
- Metrics drift (CAGR/MaxDD) without explanation
- Performance drifts (wall time) without being detected
- Cache regressions cause hidden downloader/ThetaTerminal hits

The CI acceptance gate is meant to stop all of that.

---

## 4) Key Requirements for Agent A (what you must deliver)

### 4.1 A single CI gate in the normal test workflow

We want acceptance tests to be a required job in the existing CI workflow
(not a separate “optional workflow” that nobody runs).

If runtime is too long:
- shard across jobs in a matrix
- or shard at the pytest layer

But the outcome must still be:
- “CI is green” means acceptance passed.
- “CI is red” blocks merges/releases.

### 4.2 Runs must be “warm-cache” by assumption

The dev S3 cache is expected to already contain the needed data.
Therefore:

- If an acceptance backtest hits ThetaTerminal due to cache miss, that is a failure.
- If it needs to hydrate new data, that is a failure (because it means the cache set is incomplete).

### 4.3 Validate correctness and speed

For each strategy run:

- Extract and assert:
  - Strategy CAGR (annual return)
  - Strategy Total Return
  - Strategy Max Drawdown
- Compare to “expected baseline values” with tolerances
- Record and assert a wall-clock ceiling (CI-specific; likely higher than local)

### 4.4 Capture artifacts for debugging

When CI fails, we need artifacts to debug quickly:

- logs CSV
- trades CSV
- stats CSV
- tearsheet CSV/HTML
- settings JSON (contains backtest runtime and env details)

Upload these as GitHub Actions artifacts per strategy job.

---

## 5) What “same as local” actually means (env vars)

The acceptance gate must run using the production-style knobs.

### 5.1 Core knobs (must match local acceptance)

- `IS_BACKTESTING=True`
- `BACKTESTING_DATA_SOURCE=thetadata`
- `DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080`
- `DATADOWNLOADER_API_KEY=<secret>` (do not print)

### 5.2 Artifacts / output knobs (prod-like)

User expectation: production runs do **not** use quiet logs and do produce artifacts.

- `SHOW_PLOT=True`
- `SHOW_INDICATORS=True`
- `SHOW_TEARSHEET=True`
- `BACKTESTING_QUIET_LOGS=false`
- `BACKTESTING_SHOW_PROGRESS_BAR=true` (ok to disable in CI if it spam-logs)

If CI cannot support full plotting reliably:
- do not silently disable; document the difference and get explicit approval.

### 5.3 Cache knobs (must use dev S3 cache)

The exact knobs depend on LumiBot’s cache implementation, but in principle CI must set:

- AWS creds for the dev cache bucket (read/write)
- any `BACKTEST_CACHE_*` vars used by LumiBot/BacktestCache (see `lumibot/tools/backtest_cache.py`)

Important: **Do not embed secrets in the repo**. Use GitHub Actions secrets.

---

## 6) Key “cache correctness” assertions (how to detect ThetaTerminal hits)

Acceptance runs should be “warm”.

We treat these patterns in the backtest logs as **failures**:

- `ThetaData cache MISS ... fetching ... from ThetaTerminal`
- any explicit “fetching from ThetaTerminal” wording
- repeated “Submitted to queue” *might* still happen for cached S3 reads (data-downloader is also the cache gateway),
  so do **not** use “Submitted to queue” as a hard failure by itself.

Preferred failure signal:
- detect explicit `cache MISS` lines that mention ThetaTerminal.

Secondary signals:
- extremely high request volume compared to known-good (optional).

---

## 7) Baselines and Expected Metrics (where they come from)

### 7.1 The single source of truth

`docs/ACCEPTANCE_BACKTESTS.md` is the canonical record of:

- each strategy’s canonical window(s)
- baseline run IDs (artifact prefixes)
- expected values (CAGR/Total Return/MaxDD)
- speed history (wall-clock, machine spec)

CI acceptance gate should use values from that doc.

### 7.2 What to do when baselines drift

Baselines can drift for legitimate reasons (bug fix, fill model change, data quality).

Process:

1) If acceptance backtest fails due to metrics drift:
   - do not blindly update numbers
   - first confirm whether it is a bug or an expected behavior change
2) If it’s a real correctness fix:
   - update `docs/ACCEPTANCE_BACKTESTS.md` baseline section with:
     - old baseline
     - new baseline
     - reason for change
     - version bump and date
3) Update CI expected values accordingly (with a comment referencing the docs update).

---

## 8) Practical CI design (recommended implementation)

This is the recommended architecture for the CI gate.
It’s built to be:
- faithful
- debuggable
- shardable
- tolerant to minor metric noise (but not to correctness regressions)

### 8.1 Strategy code in CI (the hardest part)

GitHub CI runners do not have the Strategy Library repo.
Therefore we need one of these approaches:

#### Option A (recommended): frozen strategy snapshots in repo

- Create `tests/backtest/acceptance_strategies/` in LumiBot.
- Copy each demo file into it verbatim (no changes).
- Add a `manifest.json` that maps:
  - strategy name -> filename -> canonical windows -> baseline expectations.
- CI runs those scripts exactly as a user would.

Pros:
- deterministic
- visible and reviewable in PRs
- doesn’t require cross-repo access

Cons:
- duplication (but acceptable if we treat these as “fixtures”)

#### Option B: download strategy zips from S3

- Use S3 to store canonical code.zip for each acceptance strategy.
- CI downloads the zip, unpacks, runs it.

Pros:
- can be made identical to “what prod ran”

Cons:
- more moving parts, harder to review changes
- needs careful versioning of the zip pointers

**Recommendation:** start with Option A unless the user explicitly insists on S3-zips.

### 8.2 How to run each strategy in CI

Use a subprocess per strategy to avoid cross-test contamination:

- create a temp workdir per strategy run (pytest tmp_path)
- set env vars exactly as local acceptance
- run:
  - `python3 tests/backtest/acceptance_strategies/<strategy>.py`
- enforce a timeout (CI-specific; likely 45–60 minutes per strategy depending on sharding)

### 8.3 Sharding

Preferred:
- GitHub Actions matrix with 7 jobs (one per strategy)

Alternate:
- run them sequentially but that will be too slow and will time out.

### 8.4 Assertions (per strategy)

1) Ensure run exited 0
2) Ensure artifacts exist:
   - `*_logs.csv`
   - `*_trades.csv`
   - `*_stats.csv`
   - `*_tearsheet.html` and/or `*_tearsheet.csv`
   - `*_settings.json`
3) Extract metrics (CAGR/Total Return/MaxDD):
   - from tearsheet CSV if present
   - else parse HTML (less ideal)
4) Compare to baseline with tolerance
5) Scan logs for cache miss signals (ThetaTerminal hit)
6) Enforce runtime ceiling

### 8.5 Artifact upload

In GitHub Actions:
- Upload the strategy’s output logs folder as an artifact, even on success if size is reasonable.
- Always upload on failure.

---

## 9) Implementation Checklist (agent A TODOs)

### 9.1 Prep work

- [ ] Read `docs/ACCEPTANCE_BACKTESTS.md` and confirm the 7 strategies and windows.
- [ ] Identify the baseline artifacts in `Strategy Library/logs` that correspond to each (run_id prefixes).
- [ ] Extract baseline metrics from those artifacts and update the doc if missing or stale.

### 9.2 Create the strategy fixtures (Option A)

- [ ] Create `tests/backtest/acceptance_strategies/` folder.
- [ ] Copy each demo script into this folder with the same filename (or a sanitized stable name).
- [ ] Add a `manifest.json` describing:
  - id (stable key)
  - filename
  - start/end window(s)
  - expected metrics
  - allowed tolerance
  - CI runtime ceiling

### 9.3 Implement the runner + validator

- [ ] Implement `tests/backtest/test_acceptance_backtests_ci.py`:
  - parameterized by the manifest
  - runs each strategy in a temp directory
  - enforces timeout
  - parses artifacts
  - asserts metrics + cache behavior + runtime

### 9.4 Wire into GitHub Actions

- [ ] Update the existing CI workflow (do not create a separate “optional workflow”).
- [ ] Add a job matrix (7 shards) that runs only acceptance gate tests.
- [ ] Add required secrets:
  - dev S3 cache creds
  - downloader API key
  - any other cache env vars used by LumiBot
- [ ] Upload artifacts per shard.

### 9.5 Local verification

- [ ] Run one acceptance strategy locally via the new CI harness:
  - `pytest -q tests/backtest/test_acceptance_backtests_ci.py -k <one strategy>`
- [ ] Ensure it runs and produces the same artifacts as Strategy Library runs.

---

## 10) “Gotchas” (things that have bitten us)

### 10.1 Bool env vars are strings

In multiple places, env vars are parsed as strings.
Prefer `true/false` lower-case to avoid accidental truthiness.

### 10.2 GitHub runners are slower than your M3 Max

Do not set CI thresholds equal to local thresholds.
Instead:
- record local wall times in `docs/ACCEPTANCE_BACKTESTS.md`
- set CI ceilings with a conservative multiplier
- if CI is consistently too slow, shard more (matrix) rather than loosening thresholds indefinitely

### 10.3 Cache “warm” ≠ “no downloader traffic”

LumiBot + downloader architecture can still call the downloader even when cached,
because the downloader can be the gateway to cached objects.

What matters is:
- no ThetaTerminal hits (no cache MISS -> ThetaTerminal)

### 10.4 Tearsheet can fail on degenerate returns

QuantStats/Seaborn has failure modes when returns are flat or too short.
LumiBot should avoid crashing backtests in those cases (see `lumibot/tools/indicators.py`).

### 10.5 Do not “fix” acceptance drift by updating numbers blindly

If CAGR/MaxDD drift:
- investigate first
- confirm correctness
- document baseline update in `docs/ACCEPTANCE_BACKTESTS.md`

---

## 11) Suggested file templates

### 11.1 `tests/backtest/acceptance_strategies/manifest.json` (example)

```json
{
  "strategies": [
    {
      "id": "tqqq_sma200_thetadata",
      "file": "TQQQ 200-Day MA.py",
      "start": "2013-01-01",
      "end": "2025-12-01",
      "expected": {
        "cagr": 0.41,
        "total_return": 20.0,
        "max_drawdown": -0.75
      },
      "tolerance": {
        "cagr_abs": 0.05,
        "total_return_abs": 5.0,
        "max_drawdown_abs": 0.05
      },
      "ci_timeout_s": 3600
    }
  ]
}
```

### 11.2 `pytest` runner outline (pseudo)

```py
def run_strategy(path, env, timeout_s) -> RunResult:
    t0 = time.monotonic()
    proc = subprocess.run([sys.executable, path], env=env, cwd=tmpdir, timeout=timeout_s, capture_output=True, text=True)
    dt = time.monotonic() - t0
    return RunResult(rc=proc.returncode, stdout=proc.stdout, stderr=proc.stderr, wall_s=dt)

def parse_tearsheet(tearsheet_csv) -> dict:
    # extract CAGR, MaxDD, Total Return
    ...

def assert_no_thetaterminal_hits(log_csv):
    assert \"cache MISS\" not in text or \"ThetaTerminal\" not in text
```

---

## 12) Success Criteria (definition of done)

Agent A is done when:

- CI runs the 7 acceptance backtests on every PR (or at least on every PR to dev)
- Failures block merges
- CI uploads artifacts for debugging
- Each run validates:
  - CAGR, Total Return, MaxDD (with documented tolerances)
  - runtime ceilings
  - cache behavior (no ThetaTerminal hits in “warm cache” assumption)

---

## 13) Notes for the next human handoff

When handing this off again:

- Include the exact CI job logs and artifact links for one full run
- Include the baseline metrics table you used and where they came from
- Note any environment variable subtlety you discovered (string booleans, required secrets, etc.)

---

## 14) Baseline extraction runbook (local → doc → CI)

The acceptance gate is only as good as its baselines.
This is a practical, repeatable way to refresh baselines when needed.

### 14.1 Where baseline artifacts live (local)

Strategy Library artifacts (human runs) typically land in:

- `/Users/robertgrzesik/Documents/Development/Strategy Library/logs`

Each run produces a common set of files, usually:

- `*_settings.json` (contains runtime and env echo)
- `*_stats.csv` (often includes summary stats)
- `*_tearsheet.csv` and `*_tearsheet.html` (QuantStats summary)
- `*_trades.csv` and `*_trades.html`
- `*_logs.csv`

### 14.2 What to record as the baseline

For each acceptance strategy/window, record:

- `run_id` (the unique suffix in the filenames)
- `lumibot_version` (from settings or from the console “Lumibot version …” line)
- `window` (start/end)
- `CAGR` (annual return headline)
- `Max Drawdown`
- `Total Return`
- `wall_time_s`

Then copy those into:
- `docs/ACCEPTANCE_BACKTESTS.md` (human gate doc)
- the CI manifest (machine gate)

### 14.3 How to compute CAGR/MaxDD/TotalReturn reliably

Prefer `*_tearsheet.csv` when available (it is structured and stable).

If only HTML exists:
- parse the table entries, but document the parsing approach in code (brittle).

If both are missing:
- treat as a failure; acceptance backtests must produce artifacts.

---

## 15) GitHub Actions wiring (example patterns)

### 15.1 Matrix sharding example (7 strategies)

This is the conceptual shape (do not paste blindly; adapt to LumiBot CI workflow structure):

```yaml
jobs:
  acceptance_gate:
    strategy:
      fail-fast: false
      matrix:
        acceptance_id:
          - aapl_deep_dip_calls
          - leaps_alpha_picks
          - tqqq_sma200
          - backdoor_0dte
          - meli_deep_drawdown
          - backdoor_0dte_smartlimit
          - spx_short_straddle_intraday
    env:
      IS_BACKTESTING: \"True\"
      BACKTESTING_DATA_SOURCE: thetadata
      DATADOWNLOADER_BASE_URL: http://data-downloader.lumiwealth.com:8080
      DATADOWNLOADER_API_KEY: ${{ secrets.DATADOWNLOADER_API_KEY }}
      AWS_ACCESS_KEY_ID: ${{ secrets.DEV_CACHE_AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.DEV_CACHE_AWS_SECRET_ACCESS_KEY }}
      AWS_DEFAULT_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.12' }
      - run: pip install -e .[dev]
      - run: pytest -q tests/backtest/test_acceptance_backtests_ci.py -k ${{ matrix.acceptance_id }}
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: acceptance-${{ matrix.acceptance_id }}
          path: path/to/output/logs
```

Key points:
- `fail-fast: false` so one failure does not cancel the other shards (we want full signal).
- Upload artifacts on every shard even if it fails.

### 15.2 Secrets naming conventions (recommended)

Use explicit names so future maintainers can understand intent:

- `DEV_CACHE_AWS_ACCESS_KEY_ID`
- `DEV_CACHE_AWS_SECRET_ACCESS_KEY`
- `DATADOWNLOADER_API_KEY`

Avoid ambiguous “AWS_KEY” names.

---

## 16) Security / hygiene notes (important)

- Never print secrets in CI logs.
  - Do not `echo $DATADOWNLOADER_API_KEY`.
  - Avoid `set -x` in shell steps.
- Avoid writing secrets into artifacts.
  - If a log file includes headers/keys, scrub before upload.
- Keep the acceptance fixture strategies free of credentials.
  - They should rely on env vars only.

---

## 17) Troubleshooting checklist (when CI is red)

### 17.1 Cache misses

If CI fails due to cache misses:
- confirm CI is using the correct dev cache bucket/credentials
- confirm the cache set actually contains the window (might require a one-time pre-warm job)
- do not “paper over” by allowing ThetaTerminal downloads; that defeats the warm-cache gate

### 17.2 Metrics drift

If CI fails due to CAGR/MaxDD drift:
- open the uploaded `*_tearsheet.csv` and compare to baseline
- check for:
  - data source changes (theta vs yahoo)
  - fill model changes (market vs smartlimit)
  - corporate action handling changes (splits/dividends)
- only after confirming correctness:
  - update `docs/ACCEPTANCE_BACKTESTS.md`
  - update CI manifest expected values

### 17.3 Runtime regressions

If CI exceeds time ceilings:
- confirm the run is not downloading from ThetaTerminal
- inspect “Submitted to queue” volume and cache miss logs
- shard more aggressively if needed
- treat repeated regressions as a real performance bug, not “CI is slow”

