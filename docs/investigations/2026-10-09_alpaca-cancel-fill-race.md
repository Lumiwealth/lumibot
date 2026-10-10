# Alpaca smart-limit cancellation/fill race

A scheduled run timed out with one smart-limit order pending. During its final
hold expiry, Alpaca repeatedly rejected cancellation because the exact order
was already filled. Without the fill notification, the local tracker remained
active and the drain correctly refused to claim completion.

The Alpaca adapter now handles HTTP 422 cancellation failures with an exact
read-only order lookup. A confirmed filled response must match the identifier,
contain a finite positive fill price, and report the full requested quantity.
Normal broker fill processing accounts for the remaining quantity and clears
the active tracker. A second cancellation does not duplicate the transaction.
Confirmed canceled orders use normal cancellation processing. Uncertain states,
missing/invalid fill data, lookup failures and authentication failures remain
errors. No deadline, sizing, order intent or trading rules change.

The new fill-race regression failed against the original adapter (APIError),
while the uncertain-state cases passed. It now verifies real broker tracker,
transaction and strategy-callback effects using synthetic REST responses,
including a previous partial fill without double-counting quantity or its cost. Production
qualification still needs a managed test order and exact-artifact promotion;
successful production runs without submitting a new order are not fill-path
proof.
