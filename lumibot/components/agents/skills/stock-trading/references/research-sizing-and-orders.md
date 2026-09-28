# Stock Research, Sizing, and Orders

## Evidence

Check the current price and recent history for every serious candidate. Use
technical indicators when the strategy depends on them. For broad discretionary
decisions, use relevant news, macroeconomic, and company evidence when available
and date-bound every source during backtests.

## Sizing

Use portfolio value, cash, current exposure, price, and the user's risk rules.
When a stop level exists, estimate per-share risk and size from that distance. Do
not buy token positions merely to be active, and do not create accidental leverage
because cash is invested in an existing or defensive position.

## Position changes

Manage current exposure before adding another position that would violate limits.
When rotating, close or reduce the old position first, verify the result, then size
the replacement from the updated account state.

## Verification

After submission, inspect the exact order identifier and reread positions and open
orders. In a backtest, use one short bounded `orders_wait_for_terminal` after your
own market-order submission so the simulator can process a fill. Do not claim a
fill from a submission response.

An existing pending order owns that position change. Inspect it by identifier and
do not submit another entry or exit for the same exposure while it remains open.
Never create a stream of replacement exits merely because the position has not
changed yet.
