# Order-list refresh must use the broker lifecycle reducer

The public `Strategy.get_order()` accessor refreshes via `Broker.sync_orders()`.
Previously, a known row in broad history received a direct status/quantity copy,
bypassing the Schwab reducer used for missing-order direct lookup and stream
observations. Setting FILLED first suppressed fills; canceled history could lose
late executions, rejection details were absent, and stale WORKING reopened a
confirmed canceled order.

Four synthetic transport tests failed before the change: empty transactions
instead of 40 plus 20, absent rejection detail, zero instead of 100 filled, and
canceled state regressed to NEW. These exercise the actual Strategy accessor,
Schwab parser, native tracker and event queue; all external requests are blocked.

Known broad observations now use the existing adapter observation hook. The
generic hook preserves existing field-copy behavior for other brokers; Schwab
keeps its single reducer. The accessor documentation describes broad refresh and
conditional direct lookup accurately. No order duration or trading policy changes.

Focused related contract inventory: 156 passing before the additional pending-
cancel/fill race assertion. Full SDK CI remains a separate required check. This
is SDK contract evidence, not qualification of AI-authored strategy repairs.
