# Multi-Leg Pricing and Orders

## Build the package

Express each leg with its exact symbol, expiration, strike, right, side, and
quantity. Preserve a common package quantity across equal-ratio structures. For
ratio structures, preserve the intended leg ratios explicitly.

Always call `options_calculate_multileg_price` after evaluating all exact legs
and immediately before submitting the package. Do this even though the submit
tool can calculate a price when `net_limit_price` is omitted:

- Calculate per-unit economics with one contract per leg.
- Positive signed price means debit.
- Negative signed price means credit.
- Compare the result with the maximum possible debit, credit, gain, and loss implied
  by the structure before ordering.

Use the returned per-leg signed prices and quantities when reconciling that
sum. Keep per-share package price distinct from dollars per contract and total
order value. Recheck any apparent arithmetic conflict with the pricing tool;
do not declare quotes inconsistent from an unsupported mental calculation.

Match the price style to the user's execution goal. `mid` estimates value;
`best` and `mid` can leave an order unfilled. For an authorized entry or exit
intended to execute now, calculate the current executable `fastest` price
(buy asks, sell bids), unless the user's price preference requires another
style. Closing held positions also requires `action='close'`. Submit the
returned signed price explicitly as `net_limit_price`.
Do not use an uncapped market order or override the user's price/risk limits.
If the quoted executable price breaches a limit, report the conflict without
submitting. An executable quote is still not a guarantee of a fill.

## Submit atomically

Use `orders_submit_multileg` once for the complete package. Pass the full intended
quantities only at submission. Do not submit separate child orders merely because
the package has several legs.

If the broker rejects or does not support atomic package submission, stop. Do
not switch to single-leg tools. Report the capability blocker and make a no-trade
decision for that package.

Before submission, confirm portfolio, positions, open orders, current underlying
price, chain membership, exact Greeks when relevant, every leg quote, package
price, and intended risk.

After submission:

1. Capture the returned package or order identifier.
2. Call `orders_get_status` for that exact identifier. Use a short
   `orders_wait_for_terminal` only when appropriate.
3. Reread positions and open orders.
4. Describe only the state proved by those results.

Never resubmit merely because the first response is uncertain. Resolve the current
order and position state first.
