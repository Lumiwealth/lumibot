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
