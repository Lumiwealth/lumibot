# Option Position Management

## Reconstruct exposure

Group positions by underlying, expiration, right, and strike. Use signed quantities
to identify long and short legs. Do not infer current exposure from memory or from
the order originally requested.

For every exact contract:

- quantity greater than zero: long, close with `sell_to_close`
- quantity less than zero: short, close with `buy_to_close`
- quantity equal to zero: flat, do not send another close

The inverse mappings are invalid: `buy_to_close` does not close a positive long
position, and `sell_to_close` does not close a negative short position. Check the
side of every proposed closing leg against the latest signed quantity before
pricing or submission.

Closing quantity is `abs(current_quantity)`. Reread positions after every close
attempt. Never double, aggregate repeatedly, or use a previously remembered
quantity after the account state changes.

## Manage before opening

When any leg of an intended structure remains open or any related order remains
pending, manage that state before opening another package. A partial structure is
not flat. An unverified submission is not permission to submit again.

Use strategy rules for profit targets, loss limits, time exits, delta exits, and
roll decisions. Write the relevant condition against current evidence before
acting. If the rule cannot be evaluated, do not invent a trigger.
