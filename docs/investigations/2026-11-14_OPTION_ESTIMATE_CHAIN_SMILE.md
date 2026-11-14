# Option estimate from the expiration's volatility smile

How estimated option fills are priced when other contracts of the expiration are loaded, how it was validated, and what is still open.

**Last Updated:** 2026-11-14
**Status:** Shipped in 4.6.61
**Audience:** Developers + AI Agents

---

## Overview

An option order in a backtest fills at a price that belongs to the fill bar: a real quote, else
a real trade, else an estimate marked `price_source=estimated`
(`BacktestingBroker._option_fill_price_at_bar`). Until 4.6.60 the estimate always came from the
contract's own last trade. Since 4.6.61 it comes from a volatility smile fitted across the
expiration when enough of the expiration is in memory.

## Code

- `lumibot/tools/option_price_estimate.py`: pure numpy, no broker or data source objects.
  `estimate_option_prices` takes the contracts of one expiration at one instant (each one's
  latest real trade, the underlying then and now) and returns a price, the volatility used and
  a source label per contract. `parity_forward_factor` gives the forward/spot ratio from calls
  and puts at the same strikes. Tests: `tests/test_option_price_estimate.py`.
- `BacktestingBroker._chain_option_estimate_now`: collects the inputs and calls it.
  Tests: `tests/backtest/test_option_chain_estimate_fill.py`.

## Rules the broker follows (invariants)

1. Real quote and real trade on the fill bar always win. The estimate is only the third choice.
2. Nothing is fetched. Contracts are read from the data source's in-memory store
   (`_data_store`), minute data only. A backtest that loaded only its own legs keeps the
   own-trade model, to the cent (`test_estimate_is_unchanged_when_the_chain_is_not_loaded`).
3. The smile is used only when at least `OPTION_CHAIN_MIN_OTHER_CONTRACTS` (5) other contracts
   of the same underlying and expiration traded within the last 390 minutes, and only when the
   module answers from a smile or from neighbouring strikes. If it would answer from the
   contract's own older trade, the previous code path runs instead, so that case is unchanged.
4. No look-ahead: only trade bars that started before the current bar are read
   (`test_estimate_never_reads_trades_after_the_fill_bar`).
5. No invented trades: calendar padding rows (`bar_is_observed` false), placeholder rows
   (`missing`) and bars reporting zero volume are not trades
   (`test_bars_with_zero_volume_are_not_trades`).
6. Calls and puts are fitted separately, so a wrong interest or dividend assumption cancels.
   Interest and dividend yield are 0, as before.
7. Put-call parity forward only when the underlying is an index (European exercise). It needs
   at least 4 strikes where the call and the put traded in the same minute within the last 15
   minutes; otherwise no forward is used.
8. Daily-bar backtests keep the own-trade model (ages are measured in minutes).

## Validation

Method: for every contract and every fifth minute that had a real bid and ask, build the
estimate only from trades that happened earlier and compare with the quote midpoint. Sample:
691,306 contract-minutes, 64 trading days in 2025 (47 calm, 17 volatile), 0 to 45 days to
expiry, 1,024,662 real trade bars as inputs.

Strikes from 5 delta to at the money (147,293 contract-minutes), absolute error in cents:

| Price used | Median | 90th percentile | Inside real bid/ask |
|---|---|---|---|
| Last trade price, unchanged | 25.0 | 390.0 | 36.8% |
| Own last trade (model before 4.6.61) | 17.7 | 113.1 | 33.8% |
| Smile across the expiration | 12.1 | 67.0 | 44.8% |

Real half-spread in the same sample: 10.0 cents median.

All rows, by time since the contract's own last trade (median cents, own-trade model versus
smile with the parity forward): 0 to 1 minute 5.4 versus 4.8; 2 to 5 minutes 8.0 versus 5.8;
6 to 15 minutes 9.2 versus 5.9; 16 to 60 minutes 10.0 versus 5.6; over 60 minutes 7.0 versus
4.3. By days to expiry: same day 4.9 versus 4.9; 1 to 7 days 12.1 versus 8.1; 8 to 45 days
43.9 versus 19.7.

Speed: about 1.8 ms per expiration-minute when volatility is solved inside the call, about
0.6 ms when solved once per trade bar and passed in (`trade_volatility`). The broker solves
inside the call; an estimate is only needed when an order fills.

## Limits

- 97.6% of the sample is one family of cash-settled index options. The rest is calls on one
  large ETF. Single stocks and American-style puts were not measured.
- Truth is the quote midpoint at the start of the minute. A real fill is not the midpoint.
- Only every fifth minute was scored; neighbouring minutes are not independent.
- Same-day, at-the-money contracts on volatile days were slightly worse with the smile (22.0
  versus 20.8 cents median).
- An American exercise model changed nothing measurable where it could be tested and cost
  about five times the time, so none is used.

## Open

- The gain only appears when the expiration's trade bars are in memory. Loading neighbours on
  demand costs data requests and is a separate decision.
- A real interest rate and dividend yield instead of 0.
- The module's own source label (`smile`, `smile_parity`, `neighbor`, ...) is not written to
  the trades file yet; every model price is `estimated`.
