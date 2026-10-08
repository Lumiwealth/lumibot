# IBKR registry and roll coverage repair

An unrelated newly discovered futures contract could republish a stale local stock ID over a corrected remote
ID. The seed path also uploaded the complete local registry without merging. A missing continuous-futures
segment logged an error but was absent from history health, so a returned tail could appear complete.
Friday evening and a weekend crossing the autumn clock change also produced incorrect closure results.
Queue submission could retry indefinitely before the result timeout started.

The regression baseline was witnessed on the active 4.6.7 source: seven failures covering three registry
publication cases, missing-roll telemetry and three closure intervals. Expired-contract discovery then failed
seven additional identity/discovery cases before implementation. Two total-deadline tests failed before the
queue fix: queue-full submission exceeded its caller budget, and submission time was excluded from polling.

The repair uses conditional S3 writes, current-lookup ownership of replacements, fill-only namespace seeds,
safe partial-roll telemetry, local-wall-clock weekend reopening, bounded shared TWS discovery and a total
queue deadline isolated between threads. No broker login, order, strategy substitution, manufactured fill,
shared cache deletion or data-freshness relaxation is introduced. Publication and downstream deployment
still require their normal qualification and release controls.

An additional daily-cache regression was found during runtime verification.
Legacy repair markers included future retry timestamps without a confirmed
no-data outcome. The daily scan honored those timestamps after the provider
already had the missing completed session. Four red cases reproduced that
suppression. Daily repair now honors only confirmed-no-data markers, matching
the existing whole-window negative-cache contract. The complete daily gap
self-healing file passes 44 tests, including bounded actual-bar replacement,
preservation of prior real bars and confirmed-no-data retry delays.
