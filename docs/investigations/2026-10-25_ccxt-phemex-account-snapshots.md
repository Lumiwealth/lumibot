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

## Per-collateral AOP snapshots

The [provider AOP specification](https://phemex-docs.github.io/#subscribe-account-order-position-aop)
allows zero or more initial snapshots, each describing one collateral account.
Treating an individual snapshot as the whole legacy or V2 family discarded
other currencies' balances and open orders. Spot wallet snapshots still replace
the entire spot wallet set; perpetual snapshots retain other collateral wallets.
Order reconciliation carries the collateral currency independently of the V2
transport selector, including an empty order list that clears only that wallet.
Incremental orders use market settlement metadata to keep the same ownership.

Four regression cases reproduced the balance and order loss across both native
account families before the fix. The affected stream, derivative, spot and
Bitunix compatibility groups passed afterward. This is generic account isolation
for every strategy and supported collateral currency, independent of any user.
The complete affected CCXT, Bitunix and derivative-position group passed 1,055
checks with two existing skips and 17 opt-in API/downloader cases deselected.
It does not turn an acknowledgment into a complete account snapshot.
