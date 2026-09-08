# Indicator temporal safety

The previous implementation computed over the entire backtest store and only
indexed the output at strategy time. An SMA with `offset=-1` changed from 25 to
1510 when only the following day's close changed from 30 to 3000. A custom
function could also aggregate future rows or mutate the shared provider frame.

`Indicators._dispatch` now validates causal parameters, copies the prefix through
strategy time before calculation, and caches against an observed-value digest.
This deliberately removes the unsafe full-future-series compute-once optimization.
An incremental implementation would need equivalent temporal and correction
tests before replacing it. Custom functions that fetch other data themselves
remain responsible for their own temporal safety.

`tests/test_indicator_temporal_safety.py` exercises the actual accessor and Agent
tool bindings with fixture OHLCV underneath them. It covers the preserved
negative-offset defect, custom input isolation, future invariance, corrections,
time rewind and parameterized batch contracts. It is deterministic testing, not
a real-model eval or proof of provider/session completion semantics.

Agent batches accept `requests_json` with unique IDs, per-request parameters and
timeframes. The published list-of-names interface is retained for existing
strategies. Invalid envelopes fail before calculations; individual calculation
failures remain explicit in otherwise useful batches.

Remaining qualification: adapter-specific completed bars (including daily date
labels, sessions and timezones), independently calculated multi-indicator values,
real-model reasoning, and performance on long observed histories. Do not infer
those guarantees from the prefix regression alone.
