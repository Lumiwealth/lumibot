# Phemex account reads and private balance snapshots

The generic CCXT integration must retain supported wallets and account-family
identity for any strategy. These fixes do not change strategy configuration,
contract quantities, permissions or account modes.

Phemex retains inactive legacy markets whose USD settlement is rejected by its
account endpoint with code `39108`, `Currency not supported.` Previously that
response aborted every combined balance/position read. Account discovery still
queries every settlement group, including inactive groups with supported wallets.
It skips only that provider error when every market in the group is explicitly
inactive. Active or unknown-status groups, authentication failures and other
provider errors continue to fail visibly.

A private spot `wo.subscribe` snapshot contains `wallets` and an `orders` object
with `open`, `closed` and `fills` lists. It may arrive while only the order watcher
is running. Retain the already-parsed balance in the managed stream so a later
balance subscriber can read it. Keep spot, legacy and V2 caches distinct rather
than exposing CCXT Pro's shared cross-family balance map. Native frames own
Phemex balance publication, as they already own order publication.

Regression checks reproduced both failures before the fixes. The broker tests
also preserve active-market/unknown-status errors and unrelated provider errors;
the stream tests preserve snapshot copying and same-currency wallet isolation.
Authenticated read-only checks then verified supported wallet aggregation,
position/equity reads, and a spot balance snapshot requested after the order
snapshot. No exchange write is needed for these checks.

Perpetual subscriptions were acknowledged but did not deliver initial account
frames during qualification. No empty snapshot is synthesized from an
acknowledgment. Private perpetual updates, live order execution and mode changes
remain unqualified. An inverse public ticker snapshot was unavailable during the
check; its book, trades and candles arrived, and the normal REST price fallback
returned a usable price.

Owners: `lumibot/brokers/ccxt.py`,
`lumibot/trading_builtins/ccxt_stream.py`,
`tests/test_ccxt_derivatives.py`, `tests/test_ccxt_stream.py`.
