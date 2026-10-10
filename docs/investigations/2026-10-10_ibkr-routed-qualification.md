# Routed IBKR qualification corrections

The deployed integration qualification exposed gaps not covered by a direct-adapter
replay. A saved-data fixture that returned its entire corpus also masked insufficient
request windows. The fixture now enforces start/end bounds, and the stock and futures
ledger oracles run against both direct IBKR and the routed adapter.

Confirmed failures before correction:

- A three-day daily stock lookback beginning after a weekend returned one trading bar.
- The routed slow path applied ThetaData calendar-day reshaping to native IBKR daily
  closes. MGC returned 50 of 51 requested completed bars at a futures session opening,
  while a subsequent fast read could return 51. A daily stock fill following a holiday
  could use the prior session's open. Neither behavior is a provider entitlement limit.
- Prefetched simulation coverage prevented a later, larger indicator lookback from
  extending the series.
- Routed futures valuation used a prior daily candle instead of the current intraday
  price; direct final reporting could ask for a mark after the simulation endpoint.
- A no-benchmark backtest with a real fill omitted its trades artifact when plotting
  was enabled. Artifact verification correctly rejected the incomplete result.

The repair keeps IBKR native daily rows on the normal Data slicing path, counts
equity lookback bars through cached exchange-session bounds, and preserves original
simulation endpoints. Futures position valuation uses native minute data and known
closed-session marks, never unknown-gap forward fills as proof of complete data.
The existing required-data failure remains sticky when any actual decision lacks
required history.

Permanent qualification includes bounded provider fixtures, 250-session SPY/AAPL
and TQQQ SMA200 independent signal/fill/cash/equity checks, MES/GC/MGC/NG futures
accounting, long/short held-contract roll tests with fees, MGC 51-close cold/warm
parity, and no-benchmark artifact export. The prior replay results remain evidence
for the direct adapter; they did not establish routed-adapter equivalence.

A historical five-minute regression previously allowed an underfilled 250-bar
lookback with two initial provider probes. Its stronger assertion now requires all
250 bars, permits the necessary extra initial history page, and requires zero new
provider calls on every subsequent simulated step. This is an accuracy correction,
not permission to repeat downloads.

Licensed live-cache and end-to-end integration artifacts remain in the private
deployment evidence store. Passing portable tests alone does not establish a
successful deployed customer path.

The bounded real intraday comparison also exposed a direct-IBKR fast-fill bug:
Trades cache files include bid/ask derived from a candle's close. The optimized
broker path used those values while the minute was still forming, unlike the
normal quote path, which uses its open. The regression now supplies that actual
cache schema to both direct and routed engines and asserts every fill against the
source minute open. The fast path follows the existing Data quote convention;
non-derived bid/ask and timestamp freshness checks are retained.

Additional explicit boundaries: final futures valuation is capped at the actual
simulation endpoint; Friday's known-closed mark expires at the exact Sunday
reopening. Complete stock corporate-action frames can be reused across the whole
simulation, but a missing action column is not evidence of no actions. Future
dividends are indexed once and credited only on their actual ex-date.

The stock intraday oracle checks equity at every decision as well as fills. This
caught a separate routed snapshot leak: Data snapshots exposed the forming
minute's final close to portfolio valuation. Routed IBKR stock/index snapshots
now use the point-in-time last-price reader. Direct IBKR intraday marks also
retain a completed close through verified market closures and expire at reopening;
an unknown missing tail remains unavailable. Earlier hypotheses about the
final-equity difference are superseded by this recorded first-divergence trace.

Exact daily session counting also replaces generic calendar-day padding rather
than retaining the earlier padded start. A bounded 250-session regression supplies
only the required warmup and fails if the same complete frame reloads. On the
saved TQQQ SMA200 corpus this reduces routed history helper reads from 19 to one
(and direct reads from two to one). All sixteen saved-data replays retain identical
bars, decisions, fills, cash, quantities and equity after this optimization. These
counts measure eliminated history reads, not a claimed engine CPU speedup.
