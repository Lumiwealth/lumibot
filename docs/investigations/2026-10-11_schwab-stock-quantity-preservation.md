# Schwab stock quantity preservation

The shared stock builder previously converted every order quantity with `int()`.
A positive sub-share close became zero and failed inside `schwab-py` with
`quantity must be positive`; a quantity such as `1.0436` silently became `1`.
The same builder also supplies replacements and advanced-order child legs.

The adapter now emits integers for whole shares and JSON numbers for fractional
shares, checking that decimal serialization preserves the requested quantity.
Nonfinite, nonpositive, and lossy values fail before submission. This replaces
an implicit rounding policy with one invariant across the existing shared path;
no position, side, session, order-type, option, or futures policy changes.

`tests/test_schwab_fractional_quantities.py` exercises the real adapter, SDK, and
HTTP serialization with an intercepted transport. Cases cover submits, replacements,
advanced legs, position cleanup batches, rejection state, invalid values, and
integral stock/option controls. The corrected frozen regression failed 30 cases
with 9 passing controls before the production change, then passed all 39.

## Provider capability limits

Current [Schwab account terms](https://www.schwab.com/legal/schwab-brokerage-account-agreement)
describe fractional trading with eligibility and order/session restrictions.
The [Advisor Services announcement](https://advisorservices.schwab.com/whats-new/trading-and-investment-management)
describes fractional support across advisor trading channels, including APIs.
Neither proves eligibility for every retail Trader API account or order.
The retail developer specification was unavailable during this investigation.

The inspected `schwab-py` 1.5.1 equity-leg builder accepts positive numeric
quantities without rounding. Its [reference](https://schwab-py.readthedocs.io/en/latest/order-builder.html#order-legs)
does not promise provider acceptance. LumiBot therefore preserves the request
and lets the provider's existing rejection path report eligibility failures.
No live order was placed, and transport tests do not establish execution or
customer recovery. Fractional limit-session eligibility remains a separate
provider-contract question; the existing session policy is unchanged.
