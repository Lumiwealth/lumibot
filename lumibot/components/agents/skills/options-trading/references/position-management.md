# Option Position Management

## Reconstruct exposure

Group positions by underlying, expiration, right, and strike. Use signed quantities
to identify long and short legs. Do not infer current exposure from memory or from
the order originally requested.

For every exact contract:

- quantity greater than zero: long, close with `sell_to_close`
- quantity less than zero: short, close with `buy_to_close`
- quantity equal to zero: flat, do not send another close

Close a single held option through `orders_submit_order` using the closing side
above and the absolute current quantity. For multi-leg positions, do not map
package sides by hand: use `action='close'`
on `options_calculate_multileg_price` and `orders_submit_multileg`, naming only
the held contracts; LumiBot derives each side and the full held quantity from
the current signed position.

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

## Cash collateral and stock delivery

For physically settled short puts, reserve the full strike times actual
deliverable/multiplier times quantity plus costs against available cash. The
premium received does not remove the assignment obligation. For covered calls,
verify enough unencumbered deliverable shares after related pending orders.
Do not assume every contract represents 100 ordinary shares.

Assignment can occur before expiry. Reconcile the actual option removal,
underlying delivery, cash and acquisition/disposal basis before the next action.
A short put becoming shares permits a covered-call decision; a covered call
being assigned returns its shares to cash. An expired option becoming flat does
not itself prove either stock delivery or permission to repeat an unresolved
order. Manage changed share quantities and residual obligations using actual
positions, never only a remembered phase label. Preserve existing contracts'
symbols when a strategy's configured entry symbol changes.

For premium-capture exits, use actual entry fills and the cost to close the
same remaining contract, including costs. Cheap mark-based attention is not an
executable quote. Before submitting, inspect that contract's available bid/ask
and spread; distinguish a disclosed historical last-trade proxy from live
execution evidence. Option premium cash and whole-portfolio profit differ while
stock downside or an open short-option liability remains.
