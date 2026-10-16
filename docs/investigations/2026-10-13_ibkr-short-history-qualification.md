# IBKR declared history minimum

`length` controls the fetch/return window. `minimum_bars` optionally declares the
latest completed lookback actually required by the strategy's decision. The
default remains `length`, preserving existing strict MGC/other requirements.
This does not infer a custom indicator's requirements or rewrite strategy rules.

Example: `get_historical_prices(asset, 500, timestep="day", minimum_bars=51)`
fetches the same 500-bar window and returns the same prices. Required-data
validation examines the last 51 returned rows, before filtering absence/null
markers. A hole inside those 51 rows still invalidates the result; an irrelevant
older incomplete prefetch does not. A strategy declaring this minimum must
actually operate on this lookback. Framework input evidence retains the full
returned frame independently of validation.

Source baseline: `1d5025571b9ff010b961a731662996df720736da`.
Red regression: seven API/validation cases failed before the patch; a second
focused run reproduced five failures, including both real direct/routed engine
cases and missing-row validation with enough older rows to hide the required gap.
After the fix, all 102 cases in the indicator-prefetch, IBKR history-health and
frozen-engine modules passed (24.46 seconds). Existing strict-request and
genuinely incomplete/no-trade tests were retained.

Two actual frozen-input backtests request 500 bars while consuming three;
their bars, signals, real fills, cash, holdings, equity and input hashes equal
the normal three-bar-request baseline exactly. No network access is allowed.
Provider/candidate live acceptance and the full release gates are separate.

The release is not published merely because these focused checks pass. The
latest version branch must pass normal cloud gates and required review before
tagging/publication and downstream consumption. Platform deployment belongs
to the dedicated deployment task under Rob's October 13 ownership rule.

## Input-observation cost qualification

An additional red regression demonstrated repeated scalar conversion of identical
overlapping input rows. The existing recorder now reuses bounded row serialization
and canonical JSON bytes. Tests preserve original frame fingerprints, signed zero,
scalar types, corrected values, missing rows and explicit capacity-limit reporting.
No observation, required-data check or financial calculation is disabled.
All 109 focused tests pass after both fixes (23.96 seconds).

Sixteen serial actual-price-corpus replays cover routed TQQQ SMA200, AAPL daily,
SPY intraday and MGC across a roll: SDK18 and candidate, each twice. Every compared
bar, signal, fill, cash balance, holding and equity value matches exactly, and each
run has complete required data. Provider access is blocked. These are saved-input
engine runs, not actual Manager Dev execution or S3 warm-repeat broker counts.

Latest two-sample engine medians in seconds (SDK18 / candidate): TQQQ1.896/2.090,
AAPL0.895/1.054, SPY intraday0.806/0.872, MGC0.188/0.183. The earlier TQQQ candidate
was approximately2.9s. Sampling and concurrent Mac activity limit timing claims;
some observation overhead remains. Do not claim whole-backtest speed qualification
or a universal speedup from these figures. Exact-source hosted integration timing
and actual cold/warm Manager runs remain mandatory before claiming final rollout.

SDK19's earlier real-model cost was$0.24479. The remaining gate is capped at$0.45;
combined maximum$0.69479 stays below the established$0.70 SDK19 allowance. No provider
account cap or customer traffic control is changed.

## Gate timeout and recovery defect

Full source CI38480592343 passed. Model run38480595682 timed out after39
passing repetitions but left freshness.json on its earlier runtime fingerprint.
Its durable per-call ledger commits$0.3190006, including one unsettled call's
full reservation; the last progress snapshot's$0.2042535 was not final spending.
Resume38483994008 failed budget admission after unnecessarily repeating seven
passing repetitions, costing$0.03455691. Neither failed run is a passing gate.

The partial-artifact restorer previously required the stale freshness record
to already contain the new fingerprint. A red regression reproduced the loss.
Recovery now also derives complete native records from distinct passing
repetitions, model identities and aware timestamps in the actual ledger.
Latest failed fingerprints and duplicate repetitions remain unqualified.
All82 owning harness tests pass; replaying the actual timeout artifact recovers
13 complete cases/39 repetitions with zero model calls. This helper does not
change the actor/judge runtime fingerprint or any model input/scoring policy.
