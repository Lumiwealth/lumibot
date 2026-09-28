# Titus Schwab Reliability: Token Expiry, Loop Blocking, Cancel Latency

Investigation and fixes for the 2026-08-20 support thread: repeated Schwab
token loss, a 584-second blocking iteration that delayed an order cancel by
~55 seconds, strategy-copy losing cadence/scan settings, and filled orders not
triggering the hedge leg.

- Last Updated: 2026-08-21
- Status: Fixes implemented locally + unit-tested; live Schwab verification pending (no Schwab connection available in this environment)
- Audience: Engineering / support

## Overview

The customer's live BotSpot strategies on Schwab exhibited four distinct
failure modes. This document maps each symptom to its root cause in code,
describes the fixes applied, and records before/after metrics. Per the
customer-bug-reproduction rule in `AGENTS.md`, none of this should be reported
to the customer as "fixed" until the exact customer path is re-verified in the
BotSpot test environment with a live Schwab connection.

## Symptom → Root Cause Map

### 1. "Cannot maintain its connection and token" (`token_expired`, TokenExpiredError)

Root causes found:

1. **Refresh was lazy-only.** The `OAuth2Session` refreshes inline inside
   whichever broker request first crosses the access-token expiry boundary
   (`lumibot/brokers/schwab.py`). Access tokens last ~30 minutes
   (`expires_in: 1800` default), which matches Rob's diagnosis of losing the
   connection ~30 minutes after connect. There was no background refresher.
2. **Missing `SCHWAB_APP_SECRET` makes refresh silently impossible.** The
   Basic-auth compliance hook is registered only when api_key AND secret exist;
   without it Schwab rejects the refresh exchange. Previously this produced
   only a soft warning at startup. See
   `docs/investigations/2026-06-01_schwab-local-token-refresh.md`.
3. **Destructive error handling:** any exception during client init deleted the
   token file - a transient network error could destroy a valid refresh token
   and force interactive re-auth. A 401 during account-number lookup also
   deleted the file, including in external-OAuth mode where the file is
   parent-managed.

Fixes (all in `lumibot/brokers/schwab.py`):

- `_start_schwab_proactive_token_refresh()` daemon thread rotates the token
  5 minutes before expiry, retrying every 60 s on failure; started automatically
  when a refresh token and `SCHWAB_APP_SECRET` are available.
- Missing-secret case now logs an explicit ERROR stating the connection will
  fail ~30 minutes after re-authentication.
- Init errors delete the token file only when it is genuinely corrupt
  (unreadable JSON / structurally missing token); transient errors keep it.
- The 401 token-file deletion no longer runs in external-refresh mode.

### 2. Iteration blocking: cancel deadline missed by ~55 s; one iteration took 584.33 s

Root cause (structural): `self.sleeptime = "1S"` only sets the *minimum spacing*
between iteration starts. APScheduler runs the iteration job with
`max_instances=1` (`strategy_executor.py`), so ticks are skipped while an
iteration runs and nothing preempts it. Any `_manage_pending_buy()`-style
deadline logic written inside `on_trading_iteration()` can only run when the
iteration regains control. The 584 s iteration spent its time on sequential
option-chain fetches, per-symbol quote calls, hedge-candidate precompute, and
unthrottled order-list refreshes (`get_order`/`get_orders` default
`broker_refresh_ttl_seconds=0.0` re-pulls the full 7-day order history each call).

Fixes:

- Executor now logs a loud warning whenever a live iteration exceeds 120 s,
  explaining the blocking behavior and remedies
  (`strategy_executor.py`, `_ITERATION_OVERRUN_WARN_SECONDS`).
- Fills are already detected independently of the loop (Schwab uses a 5 s
  `PollingStream`; fill events are dispatched to the strategy queue), so the
  correct pattern for the hedge leg is documented in
  `docs/investigations/2026-06-01_titus-schwab-strategy-validation.md`
  (exact-id polling window or native OTO/bracket). Strategy-code follow-up
  required - see Follow-ups.

### 3. Cancel latency (5.256 s server-side on one cancel)

Root causes in `cancel_order()` (`schwab.py`): every successful cancel made a
second full round trip (`_log_cancel_direct_read`), dispatched with
`wait_until_complete=True`, and no request in the session had a timeout, so a
stalled connection blocks indefinitely. Inline OAuth refresh (Issue 1) also
landed on cancels.

Fixes:

- Post-cancel diagnostic read is now opt-in via `SCHWAB_CANCEL_DIAGNOSTICS`.
- All requests through the Schwab OAuth session get a 30 s default timeout
  (`_apply_default_request_timeout`).
- With the proactive refresher, the OAuth handshake no longer lands inside a
  cancel.

### 4. Strategy copies lose cadence / continuous scan / settings

Root cause (botspot_node): `cloneRevisionMetadata()`
(`src/AiBotBuilder/utils/cloneMetadata.ts`) copied only five metadata fields;
the compiled schedule contract (`scheduleMetadata*`) was dropped, so clones
deployed without their cadence/repeated-scan schedule. Both clone paths
(web controller + MCP `clone_strategy`) spread this helper's output, so both
are fixed by extending it.

Fix: `cloneRevisionMetadata()` now carries `scheduleMetadata`,
`scheduleMetadataHash`, `scheduleMetadataStatus`, and `scheduleMetadataSource`.

## Before / After Metrics

Baseline evidence: customer thread (2026-08-17..20), prior telemetry docs
(`2026-06-04_schwab_cancel_telemetry.md`, `2026-06-05_TITUS_SCHWAB_CANCEL_EXACT_PATH.md`,
`2026-06-01_titus-schwab-strategy-validation.md`).

| Metric | Before | After (expected / measured) |
|---|---|---|
| Schwab connection lifetime | Failed ~30 min after reconnect (`token_expired`); no successful rotation recorded | Background refresh keeps the access token ≤5 min from expiry at all times; unit test proves rotation fires without any broker call |
| Where OAuth refresh cost lands | Inside an arbitrary broker call (quote/cancel) once per 30 min | Never on a broker call; happens up to 300 s early in background |
| Missing `SCHWAB_APP_SECRET` detection | Soft warning; silent failure 30 min later | Explicit startup ERROR naming the failure mode |
| Transient init error handling | Valid token file deleted → forced re-auth | File kept unless genuinely corrupt |
| Cancel hot path HTTP round trips | 2 (DELETE + diagnostic GET) | 1 (DELETE); GET only with `SCHWAB_CANCEL_DIAGNOSTICS=1` |
| Healthy cancel latency (baseline 305 ms DELETE, method total 1.5–1.7 s incl. read) | 305 ms + ~0.45 s broad-read + dispatch wait | Expected ≈ DELETE-only (~305 ms class); worst case previously absorbed multi-second refresh handshake — eliminated |
| Cancel request timeout | None (indefinite hang possible) | 30 s hard ceiling on all session requests |
| Slow-iteration visibility | Silent; discovered from logs post-hoc | Warning log when an iteration exceeds 120 s |
| Worst observed iteration | 584.33 s blocking (sequential chain scans, quotes, order refreshes) | Framework cannot preempt user code; warning added. Strategy-level fix = cached chains/batched quotes/event-driven hedge (Follow-up) |
| Cancel sent vs 2 s deadline | 55 s late (loop blocked) | Unchanged by framework alone; requires strategy pattern change (Follow-up) |
| Clone keeps cadence/continuous scan | Lost (`scheduleMetadata*` not copied) | Copied verbatim incl. hash; unit-tested |
| Unit tests (this change set) | — | lumibot `test_schwab_positions_unit.py` 47→53 passing (6 new); botspot_node controller suite 54 passing (2 new tests) |

## Verification Status And Limits

- All new behavior is covered by deterministic unit tests; no live Schwab
  account was available in this environment ("no schwab").
- Per the AGENTS.md reproduction rule, before telling the customer anything is
  fixed: deploy this build in the test environment with a live Schwab
  connection and verify over ≥35 minutes that (a) no `token_expired` occurs,
  (b) `[Schwab] Proactive background token refresh completed.` appears roughly
  every ~25 minutes, and (c) a cancel on the exact customer strategy revision
  completes with a single round trip.

## Follow-ups (not done here)

1. **Strategy-code hedge pattern**: move hedge submission into
   `on_filled_order` / bounded exact-id polling, or use native OTO/bracket so
   the hedge does not depend on the loop (see
   `2026-06-01_titus-schwab-strategy-validation.md` lines 146–157; proven
   0.661 s submit-after-fill).
2. Bound the cancel-deadline check independently of the loop (e.g., cooperative
   cancellation checked by data/broker helpers, hosted on the existing
   `check_queue` thread).
3. Cache option chains briefly and batch quotes in `schwab_data.py`; honor 429s.
4. Give exact-id `get_order` lookups a single-order GET instead of the 7-day
   list pull, and/or a nonzero default `broker_refresh_ttl_seconds`.
5. External-OAuth mode: alert when the parent's rotation is not observed
   (child currently loops on warnings only); complete the never-passed-live
   Schwab parent-refresh harness (`2026-07-02_external-oauth-managed-child-validation.md`).
6. botspot_node: consider carrying `SchedulePolicy.policyJson` cadence into the
   clone's first deployment draft, and cloning multi-file revision rows.
