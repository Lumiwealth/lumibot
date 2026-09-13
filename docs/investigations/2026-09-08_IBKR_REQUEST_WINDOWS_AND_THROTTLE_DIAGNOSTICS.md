# IBKR request windows and throttle diagnostics

## Historical constraints

The April daily-history work established that universally small pages can
increase multi-year request counts and interact with coverage bugs to produce
flat prices. Preserve the five-year maximum, full-run prefetch, UTC pagination,
split normalization, missing-session repair and conditional cache merging.
The August history-integrity contract intentionally made diagnostics nonfatal.
This change does not make missing bars, no trades, optional lookup failures or
closed sessions into a new backtest failure condition.

## Changes and regression proof

- Daily stock/index fetches use the full caller-provided interval. At up to one
  year, the period is its calendar-day ceiling plus seven days of padding.
  Longer intervals and calls without explicit bounds retain `5y`. Indicator
  lookback is part of the requested interval, not the visible simulation span.
  Tests preserve two-page multi-year fetching and nonconstant historical prices.
  This is a candidate efficiency improvement; provider latency still needs a
  matched cloud measurement and the downloader's strict validation stays active.
- `HistoryPayloadError` retains allowlisted structured metadata separately from
  its concise public error. A typed 429 is transient, not an identity failure
  and never durable evidence that prices do not exist.
- Health records include a hash of instrument/source/session identity and the
  exact UTC requested window. Different intervals cannot overwrite each other.
  Duplicate event IDs for the same window are ignored. A later successful
  record for that window can replace its unresolved state.
- The outer failed-fetch handler continues after recording its cause, instead
  of recording a second `empty_history_payload` event for the same exception.
- Queue timeouts retain provider details. A known provider cooldown keeps the
  original correlation ID and does not reset HTTP sessions or force duplicate
  queue submissions. Existing best-effort timeout limits remain enforced;
  ordinary stuck-queue recovery is retained for other timeout types.
- Active download status exposes a small `provider_wait` object and clears it
  on recovery/new download/completion. It never increments simulation progress.

Preserved failing pre-fix tests: different required windows collapsed from two
records to one; the original `partial_history` cause became
`empty_history_payload`; repeated provider waits forced new correlation IDs;
and seven-day/330-day requests used five years. The long-range fixture already
passed before the sizing change and continues to pass.

The first expanded deterministic run passed 121 tests covering the changed
helpers, health, queue client, progress, routed daily prefetch, gap repair,
UTC request construction, index prices, crypto sessions and futures prefetch.
These tests use synthetic data and do not establish cloud startup performance.

No package version change, publication, provider fallback, cache purge, new
environment variable, or downstream deployment is part of this code change.
