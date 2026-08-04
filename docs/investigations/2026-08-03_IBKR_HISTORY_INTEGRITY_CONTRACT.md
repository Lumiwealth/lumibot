# IBKR History Integrity Contract

One-line description: Durable handling for incomplete IBKR history without slowing healthy warm backtests.

Last Updated: 2026-08-03
Status: Implemented, release validation pending
Audience: LumiBot maintainers and release operators

## Problem

IBKR history requests can return incomplete pages, empty pages, authentication
resets, timeouts, or a response for a stale contract identifier. Previous cache
logic could treat several ambiguous outcomes as permanent absence. That made one
temporary provider or downloader failure suppress retries in later backtests.

The opposite behavior is also unsafe. Blindly retrying every failed request can
make a long backtest repeatedly perform the same expensive downloader work.

## Contract

Every history outcome is one of:

- `complete`: requested completed sessions are present.
- `partial`: some usable history exists, but continuity is not proven.
- `confirmed_no_data`: a typed identity or availability lookup proves absence.
- `transient_failure`: a timeout, reset, malformed response, or other retryable
  failure prevented a complete answer.

Only `confirmed_no_data` may create a durable missing marker. Its marker has a
reason and retry timestamp. Partial and transient outcomes use an in-process
cooldown, preserving zero repeat calls within one backtest while allowing the
next process to retry.

## Lazy Repair

Daily US stock and index data is compared with completed exchange sessions even
when the cache contains only placeholders. Exact missing sessions are grouped,
split into segments of at most ten sessions, and repaired with at most four
small padded requests under one elapsed-time deadline. Real bars replace
placeholders through the existing conditional cache merge.

This keeps the normal five-year cold fetch unchanged. It also prevents a few
missing sessions from becoming another five-year request.

For stock and index history, one identity-related history failure clears only
the in-memory contract value, performs one fresh typed security-definition
lookup, and retries history once. The corrected mapping is persisted only after
the existing selection rules choose a valid contract.

## Evidence

`settings.json` receives a credential-free `data_health` object. It reports the
requested period, expected and returned sessions, up to 100 missing-session
dates, the full `missing_session_count`, repair attempts, transient failures,
contract refreshes, and whether incomplete data remains. It is diagnostic
metadata and does not make a backtest fail.

The read-only cache inventory command is:

```bash
LUMIBOT_CACHE_MODE=s3_readonly python scripts/audit_ibkr_cache_health.py --remote
```

It reads existing parquet objects and reports structural health. It does not
call IBKR, upload cache objects, or delete anything.

## Performance Invariants

- A complete warm cache performs zero downloader calls and zero contract
  lookups.
- A repeated partial or transient request in one process performs zero
  additional downloader calls.
- Daily repair performs at most four requests, each covering at most ten
  missing sessions plus padding.
- Contract recovery performs at most one forced lookup and one history retry.
- Partial data remains available and is reported. The health contract does not
  add a new exception path.

## Regression Coverage

Unit coverage verifies outcome classification, marker expiry, legacy marker
retry, all-placeholder repair, bounded repair segmentation, exact missing
session evidence, one-time contract refresh, same-process cooldown, and
read-only audit behavior. Production qualification must separately compare warm
and cold request counts, timestamps, OHLC values, trades, metrics, and wall time
against the preserved baseline.
