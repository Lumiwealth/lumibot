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
