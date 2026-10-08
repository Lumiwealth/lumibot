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

Additional cache regressions were found during runtime verification. Ambiguous
legacy daily markers included future retry timestamps without a confirmed
no-data outcome. Older writers also labeled a successful page containing only
older bars as confirmed absence of newer bars. Provider reads subsequently
returned the missing completed session, while these persisted markers
suppressed repairs for 24 hours.

Four initial red cases reproduced ambiguous-marker suppression. Five more red
cases reproduced the inferred-tail marker, both history-reader publication
paths, and suppression of the bounded daily repair. Both readers now preserve
positive bars without inferring tail absence. Whole-window and daily checks
ignore the known legacy inferred-tail reason. Explicit no-data outcomes retain
their retry delays. Regression coverage also proves one bounded repair replaces
the affected marker, preserves prior real bars and creates no duplicate dates.

The complete release suite exposed an old regression that required the inferred
tail markers. Its replacement retains same-process request deduplication and
proves a subsequent worker can fetch the omitted real bar. During that check,
an unchanged provider page also exposed a non-advancing backward cursor. Two
bounded red cases, hourly and daily futures, reproduced repeated requests.
Paging now stops when its next cursor cannot move backwards, keeps real bars,
and records partial history without persisting a no-data verdict.

A later real-reader check exposed another diagnostic gap: a resolved futures
contract could have no intraday bars for a completed session, and its derived
daily series silently omitted that date. Two red cases cover a missing session
and wholly empty history. Daily history now reports expected and returned
completed-session counts and missing dates while retaining only provider data.
