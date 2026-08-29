# Fast Order Lifecycle Guide

Broker-neutral guidance for deadline-driven cancellation, callback races, reconciliation, hedges, and request budgets.

**Last Updated:** 2026-08-28  
**Status:** Active design and strategy-authoring guide  
**Audience:** LumiBot strategy authors, maintainers, broker-adapter engineers, and AI-agent prompt maintainers

## Overview

Fast order management is not one polling loop. It is a state machine with three
different clocks and three different policy layers:

1. **Local policy time:** when the strategy intends to dispatch a cancel.
2. **Transport time:** how long the cancel API request takes to return.
3. **Broker lifecycle time:** when the broker finally reports `FILLED`,
   `CANCELED`, `EXPIRED`, or an error/rejection state.

The reusable architecture is broker-neutral. Broker-specific behavior belongs
in a broker profile, and the strategy still owns its deadline, replacement,
hedge, and conflict policy. Do not turn one strategy's timeout or risk rule into
a global LumiBot default.

## The Core Model

Track each order under a stable **causal group key**. Depending on the strategy,
that key might identify one symbol, one entry-replacement chain, one spread, or
one entry-plus-hedge relationship.

Typical states are:

```text
IDLE
ENTRY_ACTIVE
CANCEL_PENDING
RECONCILE_REQUIRED
HEDGE_SUBMITTING
HEDGE_ACTIVE
HEDGE_FAILED
TERMINAL
```

All event sources feed the same idempotent reducer:

- the tracked local order object;
- `on_partially_filled_order`;
- `on_filled_order`;
- `on_canceled_order`;
- a cancel response or exception;
- a bounded exact-order reconciliation read after a missed callback, restart,
  reconnect, or ambiguous mutation result.

Callbacks and reconciliation may repeat or arrive in a surprising order. Key
side effects by the broker order identifier and causal group so a duplicate
event cannot submit the same hedge or replacement twice.

## Deadline Design

The strategy or order policy supplies `cancel_after_seconds`. LumiBot should not
invent a universal value. If the user defines a hard upper bound, derive a
dispatch target from that bound and an explicit local safety margin:

```python
cancel_deadline = time.monotonic() + cancel_after_seconds
dispatch_at = cancel_deadline - local_safety_margin_seconds
```

`time.monotonic()` is appropriate for elapsed-time deadlines because wall-clock
adjustments cannot move it backward. The safety margin covers local scheduling
overhead; it does not guarantee network latency or broker terminal time.

`self.sleeptime = "1S"` means the next trading iteration begins after the
current iteration finishes. It does not interrupt a slow scan. A deadline that
is checked only in a later `on_trading_iteration` can therefore be late by the
entire scan duration.

For a deadline-critical order:

1. Complete expensive universe, chain, quote, liquidity, and hedge-candidate
   work before submission.
2. Store the exact order returned by `submit_order` and arm the monotonic
   deadline immediately.
3. During the short deadline window, process local pending order events with
   `self.sleep(..., process_pending_orders=True)` and inspect the tracked object.
4. Dispatch `cancel_order` from the local clock. Do not place a broker-backed
   read immediately in front of that dispatch.
5. Yield to the event loop so queued callbacks can run.
6. If terminal state remains unknown, schedule a bounded exact-order
   reconciliation read with backoff.

Checking local state every 50 or 100 milliseconds does not consume broker API
quota. Calling `get_order` every 50, 250, or 500 milliseconds does.

## What Lifecycle Callback Methods Mean

LumiBot's `on_*` lifecycle methods are callbacks: the framework invokes them
after it observes an event. They are still methods on the strategy class, but
thinking of them as callbacks makes their ownership clear. A callback reports
an observation; it does not cause the broker event named in the method. Live
callbacks can be delayed, duplicated by reconnect/reconciliation paths, or
arrive while strategy iteration code is running, so their side effects must be
idempotent and should avoid long blocking broker reads.

### `on_filled_order`

This is the fastest normal transition for a full fill. The callback already
contains the filled order. If a hedge is required, pass that callback order
directly to an idempotent hedge helper instead of making the hedge wait for
another broker read. `quantity` is the quantity applied by this callback. It is
the whole fill when no partial callback preceded it, or the remaining delta
after earlier partial-fill callbacks.

### `on_partially_filled_order`

Use this when hedge or replacement quantity depends on partial fills. Track
cumulative filled quantity and hedge only the unprocessed delta. LumiBot passes
`(position, order, price, quantity, multiplier)` and `quantity` is the newly
observed fill delta, not the broker's cumulative filled quantity. A later full
fill callback must reuse the same idempotency state.

### `on_canceled_order`

This reports a terminal cancellation transition. It does **not** schedule or
initiate the cancel. `cancel_order()` can return before the strategy's queued
`on_canceled_order` callback runs, so code must not assume the callback executed
synchronously inside the cancel call.

These callbacks are complementary, not competing timers. Fill and cancel are
racing broker outcomes; both should enter one reducer. Whichever terminal fact
is accepted first wins the transition, and duplicate or stale events become
no-ops.

## Scope Blocking To Actual Risk

Unknown state is not terminal, but it also does not automatically justify a
strategy-wide freeze.

- A cancel-pending order must block a conflicting replacement or additional
  exposure for the **same causal group**.
- An entry-plus-required-hedge group remains unresolved until its hedge policy
  is reconciled.
- Independent symbols or groups may continue when the strategy's capital and
  risk policy permits.
- A strategy-wide blocker is correct only when the strategy truly has a global
  invariant, such as one shared capital slot or one-at-a-time execution.
- A missing read, rate limit, or cancel exception must not be treated as
  terminal. Defer only decisions that depend on the missing state unless a
  broader risk policy requires more.

This distinction prevents two opposite bugs: duplicate/conflicting exposure on
one side, and unnecessarily stopping unrelated trading on the other.

## Broker Request Budgets

Treat rate limits as a total request budget, not a `get_order` rule. Quotes,
option chains, broad order lists, exact order reads, account refreshes, submits,
cancels, and replaces may all contribute to broker throttling.

Prefer, in order:

1. local stream/callback state for the deadline hot path;
2. bulk market-data calls and bounded caches for scans;
3. one exact-order read for missed callbacks or ambiguous state;
4. broad account order reads outside the deadline-critical path;
5. bounded retry with exponential backoff and jitter after throttling.

Record a broker call budget per iteration and per endpoint family. A retry loop
without both per-call and per-iteration bounds is a production risk.

## Schwab Profile

Schwab developer applications expose an application-level order limit for
order-related requests per minute. The `schwab-py` setup guide states that
make, cancel, and replace requests beyond the configured limit are throttled
and rejected. Do not infer a universal market-data limit or requests-per-second
guarantee from that application setting.

An authorized local sample across two test dates measured 16 unique successful
Schwab cancel HTTP responses:

| Statistic | Client-observed cancel response time |
|---|---:|
| Minimum | 228 ms |
| Median | 302.5 ms |
| Mean | 315.6 ms |
| Population standard deviation | 69.9 ms |
| 90th percentile | 440.5 ms |
| 95th percentile | 444 ms |
| Maximum | 444 ms |

The individual values were `228, 236, 249, 261, 264, 272, 275, 295, 310,
311, 324, 343, 356, 437, 444, 444` milliseconds.

This small sample describes client-observed DELETE response latency for one
account, network path, and library version. It is not a Schwab SLA, it does not
measure every rate-limit condition, and it does not prove when the broker's
terminal callback becomes visible. Strategy deadlines must come from user
intent and risk policy, not by doubling this sample's maximum.

The Schwab adapter uses account-activity WebSocket messages as a wake-up signal,
then reconciles only the locally tracked active orders through exact-order REST
reads. WebSocket and REST observations feed the same serialized, idempotent
transition reducer. The adapter performs an active-order reconciliation after
login or reconnect and retains a 30-second broad order-history poll only as a
healing fallback. It does not increase broad polling to once per second.

A successful Schwab cancel HTTP response means the request was accepted. The
local order remains `CANCELLING`, which is active and non-terminal, until a
later broker observation says `CANCELED`, `FILLED`, `EXPIRED`, or rejected/error.
Schwab 429 responses suppress more reads in the same endpoint family for the
server's `Retry-After` interval, or bounded exponential backoff with jitter when
that header is absent. A throttled or missing read never invents terminal state.

References:

- [schwab-py application order limit](https://schwab-py.readthedocs.io/en/latest/getting-started.html#order-limit)
- [schwab-py client response and timeout behavior](https://schwab-py.readthedocs.io/en/stable/client.html)

## Telemetry Contract

Fast lifecycle telemetry should make the three clocks observable without
logging credentials, account numbers, raw broker payloads, or full order data.

Recommended structured events:

```text
order.submit.request
order.submit.accepted
order.deadline.armed
order.deadline.reached
order.cancel.request
order.cancel.response
order.lifecycle.callback
order.reconcile.request
order.reconcile.response
order.hedge.request
order.hedge.accepted
order.hedge.terminal
broker.rate_limited
strategy.iteration.summary
```

Recommended safe fields:

- schema version and UTC timestamp;
- run, deployment, and revision identifiers;
- broker name and endpoint family;
- a per-run HMAC or opaque `order_ref` and `causal_group_ref`;
- event name and local status before/after;
- broker status when safely normalized;
- `elapsed_ms`, `deadline_budget_ms`, `deadline_lateness_ms`, and
  `callback_queue_delay_ms`;
- HTTP status, normalized error class, retry-after duration, and bounded
  request-count window;
- result category such as accepted, terminal, ambiguous, throttled, or failed.

Never log authorization headers, access or refresh tokens, account numbers,
callback query strings, raw request/response bodies, or a complete order spec.
Symbols, quantities, prices, and broker order identifiers should be omitted or
pseudonymized in shared production logs unless a separately controlled support
surface requires them.

Useful histograms and counters include:

- submit response latency;
- submit-to-cancel-decision time;
- cancel request latency;
- cancel-response-to-terminal-callback time;
- submit-to-terminal time;
- fill-callback-to-hedge-submit time;
- iteration duration and deadline lateness;
- request and HTTP 429 counts by broker endpoint family.

## Testing Matrix

A reusable regression suite should cover:

- fill before the deadline;
- partial fill followed by cancel;
- fill while cancel is in flight;
- cancel response before queued callback;
- duplicate and out-of-order callbacks;
- slow or missing exact-order reads;
- rate limits across market-data and order endpoints;
- restart/reconnect with ambiguous state;
- rejected hedge and bounded retry;
- same-risk-group replacement blocked while unrelated groups continue;
- genuine strategy-wide risk policy that intentionally blocks all groups;
- broker profiles with different callback and polling capabilities.

Live broker tests are valuable for transport behavior, but deterministic unit
tests should own the full race matrix. Never require real-money mutations in a
production Agent eval.
