# Agent evaluation database lifecycle

Repeated evaluations must release the native resources of completed fixtures
while other workers continue. Every `AgentManager` owns an independent in-memory
`DuckDBQueryLayer`. A strategy and its manager reference one another, and captured
tool bindings can retain completed fixtures. Garbage collection is therefore not
a deterministic database shutdown boundary.

The previous fixture cleanup stopped its broker stream, closed recorded web
clients and removed temporary files, but left its database connection open.
Repeated runs accumulated DuckDB scheduler threads until unrelated thread
creation could fail, including an authentication executor allocation before the
first model request. The allocation failure location alone did not identify the
resource owner.

`DuckDBQueryLayer.close()` now closes the owned database and clears registered
frames and cached table metadata. It is safe to call repeatedly. Metrics remain
available for post-run evidence. `ProductionFixture.close()` invokes this owner
cleanup through `ExitStack`, so a broker or web-client cleanup error cannot skip
the database and temporary-directory cleanup. It closes only the fixture's
connection; another active fixture remains queryable.

The regression tests retain completed fixtures deliberately, execute real SQL,
check that closed connections reject queries, verify registered data is released,
and observe native thread counts returning to the active-fixture baseline after
each repetition. The bounded wait in that test accommodates the existing broker
stream's stop-event polling; it is not a resource-admission check. Worker counts,
DuckDB parallelism and model behavior are unchanged. This change does not claim
to add a general shutdown API for all live strategy resources.
