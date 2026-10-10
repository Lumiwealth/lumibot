# IBKR qualification: fixed-data comparisons and remaining deployment proof

Status at 2026-10-10 02:40 UTC: candidate source work, not production acceptance.
LumiBot 4.6.17 has not been published by this qualification attempt.

## Method

Run 4.6.6, 4.6.11 and the candidate with identical saved prices and strategy
settings through the actual strategy executor, backtesting broker, fills and
accounting. Replace only the external historical-data boundary and reject network
access. Preserve exact floating-point values in private comparison artifacts;
rounded values below are presentation only. Compare input bars, decisions, fills,
cash, holdings and equity. Separately exercise raw provider decoding, cache reuse,
session aggregation and contract resolution, which an engine-boundary replay
cannot prove.

Private immutable S3 captures retain object version IDs and SHA-256 hashes.
Provider market-data files remain private and are not distributed in this repo.
Portable deterministic equivalents are release blockers in
`tests/backtest/test_ibkr_frozen_replay.py`; the helper is
`tests/backtest/ibkr_replay_support.py`. Real-data scripts/results are retained
under the qualification workspace's ignored `logs/qualification/`.

## Fixed-data comparison

| Scenario | 4.6.6 equity | 4.6.11 equity | Candidate equity | Explanation | Engine seconds: 6 / 11 / candidate |
|---|---:|---:|---:|---|---|
| SPY, 250 daily decisions | 100004.05 | 100004.05 | 100004.05 | Bars, decisions, 41 fills, cash and equity identical | .769 / .699 / .573 |
| AAPL, 250 daily decisions | 100074.85 | 100074.85 | 100074.85 | Bars, decisions, 23 fills, cash and equity identical | .511 / .621 / .573 |
| TQQQ SMA200 | 100012.78 | 100012.78 | 100012.78 | Same 200 decisions, 9 fills and complete comparison | 1.628 / 1.340 / 1.363 |
| SPY intraday, direct SDK | 100017.07 | 100017.07 | 100017.07 | Same 65 decisions and 29 fills; helper loads 66 / 66 / 2 | 3.872 / 3.747 / .665 |
| MES daily, verified saved minute mark | 99733.75 | 99712.50 | 99712.50 | 4.6.6 daily candle includes the next session; 11/candidate match | .191 / .155 / .234 |
| GC held physical contract across history roll | 90000.00 | 90000.00 | 100970.00 | Older versions omit the position mark at session openings; candidate matches every independent held-contract decision mark | measured in private artifact |
| NG held physical contract across history roll | 97000.00 | 97000.00 | 103150.00 | Candidate matches all four independent decision marks; no required-data failures with captured close/reopen prices | .092 / .079 / .149 |
| MGC held physical contract across history roll | 98800.00 | 98800.00 | 100084.00 | Same valuation defect; candidate matches every independent held-contract decision mark | measured in private artifact |

These are bounded replay timings, not end-to-end production speed claims.
The production routed SPY path already needed only two helper loads: .713s on
4.6.11 versus .717s on the candidate, with identical bars through equity.
The direct SDK improvement must not be advertised as a production-wide 6x gain.

Raw TWS stock decoding used 251 SPY plus 251 AAPL daily bars. Version 4.6.6
shifted every trading date one day early; 4.6.11 and the candidate preserved the
provider's session dates and OHLCV. Normalized stock replay equality does not
excuse the old raw-decoding defect.

An independent session-open-inclusive/session-close-exclusive oracle checked
28 MES/GC/MGC hourly-derived daily candles: 23 mismatches each on 4.6.6, none
on 4.6.11 or the candidate. IBKR daily settlement and last intraday trade are
not interchangeable price definitions; these tests intentionally compare the
same hourly-derived trade-price semantics.

Real GC/MGC hourly stitching matched all 207 expected rows on 11/candidate;
4.6.6 had 87 differing rows after the corrected September 21 roll. NG matched
206 rows on 11/candidate; the 4.6.6 run requested expiries absent from the fixed
NG corpus because its schedule was wrong. The separate held-NG engine replay uses the same fixed continuous input for all
versions, plus saved physical-contract minute prices at every valuation event.
It completes the ledger comparison above; it does not claim the old resolver could
fetch the corrected contracts or that sparse mark captures prove full minute coverage.

## Newly protected accuracy failures

- Continuous history is a chart, not a physical held contract. Two flat contracts
  at 100 and 110 previously generated artificial roll-spread P&L. Orders now bind
  to their submission-time expiry; held positions and protective children retain
  it. Explicit closes target that held contract. Strategy-directed rolls create
  separate close/open fills with observed prices and configured fees. Long,
  short, hold, close, roll and fee cases are permanent engine tests.
- Multiple opposite expiries and reversals spanning expiries require explicit
  contracts; the simulator refuses to silently choose financial semantics.
- Required missing warmup invalidates an otherwise completed zero-trade run.
  Valid zero-trade outcomes remain valid. Optional prefetch gaps are separate.
- Missing marks in an open session remain invalid even when a previous price
  keeps the diagnostic ledger inspectable. Verified maintenance/weekend marks
  use the last completed close. No price is manufactured across unknown gaps.
- Expired NG/CL/MCL TWS identity requests use exact last-trade dates. Completed
  empty identity results receive a 15-minute retry cooldown, never an absent-price
  cache marker. Transient gateway failures remain retryable.
- The delayed-feed boundary is frozen per backtest. Repeated decisions cannot
  chase a moving wall-clock tail; a decision requiring newer data is incomplete.

## Production observations and limits

The qualified downloader source was
`20bcc96bc0c224cfd84ae3fd06e8feff90ee0220`. Its SDK16 priority round recorded 168/168 complete,
bulk refresh 188 complete with seven partial, and current-context deeper history
96 complete with 13 partial. This is real progress, not complete universe coverage.

Two inspected production stock runs made 182 and 152 distinct downloader requests
(the apparent roughly 300 counts included duplicate log entries). In the second,
S3 reads consumed 1.354s, S3 uploads 10.182s, and summed broker-request time 803.1s.
A deployed candidate rerun must
prove those requests disappear for complete, immutable historical windows.

Downloader source tests pass on 382e298cfe3cc3ee6a30f2890e75c5b305f1e508,
including priority/bulk fairness, lower-tier periodic refresh, recent breadth
before deep history, known listing dates, gateway image digest pins and TWS
identity absence classification. Source success is not deployed proof.

Manager required-data invalidation is on main at
fd729105577f3f30353c0b11e03ea59f5cd191e9. Node's existing failed-state behavior
has a new cloud-qualified contract on main at
7c8a7dc652b1834184441b846da8a46daee6204e. A real SDK artifact/uploader boundary
probe produced BACKTEST_REQUIRED_DATA_UNAVAILABLE with all output families
present. Hosted Dev/UI/production acceptance is still outstanding.

## Separate release scope

The release branch also contains other agents' legacy Asset backup restoration,
FRED diagnostic redaction, transparent docs favicon and Alpaca cancel/fill-race
reconciliation. These are separate changes, not IBKR speed fixes. Their focused
tests pass; the final combined source must pass the full normal gates.

Agent gate 38015216000 failed one of 51 repetitions due to an inaccurate stock
order explanation. The failure, bounded spend and separate correction are
recorded in `2026-10-10_stock-decision-evidence-accuracy.md`. No unchanged rerun
counts as recovery. Five targeted repetitions passed in 38017744552. Full deterministic cloud CI
38017692733 passed all shards on e177626f. The normal publication gate remains
required. Prior model/example/200-character-reason changes were not
silently reverted by this qualification work.

## Remaining acceptance

1. Complete final source CI, targeted agent recovery and the normal publication
   gate; release through version/4.6.17 -> dev PR -> tested merge tag.
2. Verify PyPI installation bytes, pin the exact package in the warmer and
   Manager, then follow their normal Dev and production promotions.
3. Run real synthetic Dev stock-daily, stock-intraday and MGC scenarios with
   data-health artifacts, visible failure/valid-zero-trade UI evidence and
   cold/warm end-to-end timings. Customer accounts remain read-only.
4. Verify native cache objects for all six priority cadences using the actual
   deployed candidate, and observe lower-tier progress/freshness over time.
5. Finish isolated reconnect/replacement survival proof.
   Do not treat a healthy container or shared-gateway login as that proof.

No claim of universal accuracy, completed deployment, unlimited provider history,
or a weeks-to-completion estimate is supported by these bounded results alone.
