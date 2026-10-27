# Perpetual backtest financial accounting

This generic accounting capability serves strategies on any exchange with
explicit contract terms, independently of any particular strategy or user.

The perpetual owner records FIFO lots by strategy, full asset identity and hedge
leg selectors. Opening contracts reserves initial margin without spending the
notional or losing collateral from equity. Reductions realize only the closed
lots; reversals open a new opposite basis. Linear and inverse P&L use the shared
instrument terms. Fees use actual quote notional, convert to settlement currency,
and record `order.trade_cost_currency`. Settlement wallets stay distinct from
the strategy reporting currency, using historical currency-pair data for FX.

Explicit long/short legs retain separate positions, average basis and collateral
ownership. Reduce-only over-sized exits record the actual reduction and cancel
the remainder. Insufficient opening collateral rejects the order before changing
cash, positions or lots. Strategy cash updates now select the owning strategy as
well as the currency; a regression reproduced one strategy replacing another's
cash position during construction.

Backtests can use `Strategy.create_order(..., reduce_only=True)`; the existing
order serialization preserves this instruction. Pandas instrument lookup uses
the contract quote currency independently of settlement/reporting currency.

Qualification includes hand-checked linear/inverse long/short P&L, fees, margin,
FIFO partial exits, reversals, independent hedge legs, multiple collateral
wallets and two strategies sharing one broker. Normal `Strategy.run_backtest`
round trips cover spot, USDT and USDC contracts, inverse coin settlement and
base-coin native quantities with actual fill callbacks, final positions and NAV.
These use synthetic price histories and the actual strategy/broker path, not
exchange order execution. The initial financial regression reproduced seven
failures; the independent collateral-ownership regression also failed before
its repair.

The complete affected accounting/CCXT/Bitunix, futures, cash, asset, serialization,
Pandas lookup and candle-execution selection passed 1,507 tests and 22 subtests,
with two existing skips and 17 API/downloader tests excluded, using Python 3.10
and CCXT 4.5.64. This is local source qualification, not exchange execution proof.

Historical funding, maintenance tiers, mark-price liquidation and matching an
actual exchange-traded interval remain separate qualification work. Initial
margin and correct P&L alone do not establish a complete perpetual simulation.
