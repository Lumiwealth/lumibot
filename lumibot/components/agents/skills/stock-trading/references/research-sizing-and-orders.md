# Stock Research, Sizing, and Orders

## Evidence

Check the current price and recent history for every serious candidate. Use
technical indicators when the strategy depends on them. For broad discretionary
decisions, use relevant news, macroeconomic, and company evidence when available
and date-bound every source during backtests.

## Sizing

Use portfolio value, cash, current exposure, a current quote, and the user's risk rules.
Use a valid ask to budget an immediate buy and the intended price for a limit
order. A current last-price fallback is usable when quote sides are missing;
a historical close is an analysis input, not the execution-cost estimate.
When a stop level exists, estimate per-share risk and size from that distance. Do
not buy token positions merely to be active, and do not create accidental leverage
because cash is invested in an existing or defensive position.

## Position changes

Manage current exposure before adding another position that would violate limits.
When rotating, close or reduce the old position first, verify the result, then size
the replacement from the updated account state.

For a requested basket plan the full inventory jointly and finish every
feasible target difference. Preserve the specified capital basis and allocation
caps. Refresh actual account state after fills without restarting all unexecuted
sizing calculations. Already-correct positions need no duplicate purchases.
One unavailable constituent does not prevent completing the supported remainder.

## Verification

After submission, inspect the exact order identifier and reread positions and open
orders. In a backtest, use one short bounded `orders_wait_for_terminal` after your
own market-order submission so the simulator can process a fill. Do not claim a
fill from a submission response.

An existing pending order owns that position change. Inspect it by identifier and
do not submit another entry or exit for the same exposure while it remains open.
Never create a stream of replacement exits merely because the position has not
changed yet.
