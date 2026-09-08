# Backtest phase timing contract

Backtest progress CSV now carries `runtime_timings`, a bounded JSON object of
timezone-aware UTC wall-clock observations. The data source owns this state per
run. First observations are immutable, and heartbeat does not establish simulation
advance. Recording a milestone performs no provider import, request or file write;
the existing progress writer carries its snapshot.

The executor records initialization entry/completion and actual first trading
callback entry immediately before calling user code. Price lookup and first finite
returned price are separate observations. A missing or nonfinite price does not
establish data readiness and does not introduce a new failure condition. First
usable price describes a successful `get_last_price` call, not validated coverage
for every symbol in the run. Existing sim-time price safety remains unchanged.

Report entry/completion are separate from simulation. Settings are saved again
after reports to retain the completed phase. A report failure does not invent its
completion timestamp. Custom data sources without the optional timing methods
remain compatible. These diagnostics must never determine billing or terminal
success, and an outer runtime must keep its own trusted lifecycle timestamps.

Regression evidence: initial tests failed because phase recording was absent.
Tests cover per-source isolation, immutable observations, heartbeat/progress
separation, callback entry before a price wait, and missing/nonfinite prices.
Existing progress and sim-time safety tests remain part of the affected checks.
These synthetic contract tests do not measure cloud startup latency.
