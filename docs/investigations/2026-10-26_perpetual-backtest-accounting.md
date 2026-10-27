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

## Funding and public-data qualification

The subsequent lifecycle implementation settles funding at returned timestamps,
once per instrument/strategy/event, preserving hedge legs and settlement units.
Clock jumps do not charge positions for intervals before ownership or use future
marks. Funding and liquidation records are available on the broker and in
analyzed results; funding is part of trading return, not an external deposit.

Data sources accept explicit `funding_rates`, `mark_prices` and `perpetual_risk`
inputs. Cross and isolated liquidation use the supplied maintenance/fee rates.
When data is missing, causal candles and collateral exhaustion are disclosed
approximations. Isolated gap losses beyond allocated collateral receive an
explicit modeled insurance credit rather than spending unrelated collateral.

Real public Phemex checks exposed three adapter defects: CCXT 4.5.64 leaves
USDC contract size missing although V2 metadata identifies base-coin quantities;
its legacy spot/inverse candle path discards historical `since`; and inverse
funding uses a retired V1 route. The shared provider compatibility owner repairs
V2 metadata and uses the bounded candle-list endpoint with CCXT's native scale
parser. Funding uses the current V2 route and the actual `fundingRate8hSymbol`
metadata, not the distinct predicted-funding symbol. Pagination advances from
observed event timestamps without assuming eight-hour payment intervals.

Final local qualification on the reconciled candidate passed **1,524 tests and
22 subtests**, with two existing skips and 24 external API/downloader cases
excluded. The separate public API qualification passed four WebSocket family
checks (ticker, book, trades, candles for spot, USDT, USDC and inverse) and four
normal-strategy historical round trips using real downloaded candles, funding
and isolated DuckDB caches. The historical interval was October 26, 2026,
07:58-08:06 UTC; perpetual positions crossed the 08:00 funding event. Simulated
fills, funding amounts and final collateral/NAV matched independent contract
calculations. Temporary databases were closed and removed by their owning tests.
The isolated-loss and analyzed-result additions do not change these exercised
non-liquidating public-data paths. The final docs build succeeded with existing
unrelated docstring warnings and was inspected at desktop and phone widths.

This proves useful simulation behavior and public exchange data access. It does
not prove authenticated exchange execution, private-stream recovery during real
fills, exact historical risk tiers, intrabar liquidation, exchange insurance
rules or matching a live-traded interval. Those remain explicit qualification
items; this model must not be described as exchange-exact.
