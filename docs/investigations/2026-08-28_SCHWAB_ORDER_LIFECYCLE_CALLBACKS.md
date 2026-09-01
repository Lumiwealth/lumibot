# Schwab Order Lifecycle Callback Investigation

Broker-safe lifecycle reducer, streaming wake-ups, cancellation truth, rate limiting, and callback quantity semantics.

**Last Updated:** 2026-08-31
**Status:** Implemented on the active version branch; deterministic and Rob-owned live stream/reconnect qualification complete; live partial-fill capture still pending
**Audience:** LumiBot maintainers, broker-adapter engineers, BotSpot runtime engineers, and Agent prompt/eval maintainers

## Overview

The Schwab adapter had two competing sources of local truth. Its five-second
REST poll treated every parsed order as new, while successful cancel HTTP
responses immediately marked orders terminal and emitted cancellation
callbacks. That combination could lose fill/partial/cancel/reject transitions,
duplicate callbacks after reconciliation, and release local strategy state
before Schwab had actually confirmed cancellation.

This change replaces those behaviors with one serialized transition reducer.
Account-activity WebSocket messages wake bounded exact-order reconciliation;
REST snapshots and stream-triggered reads enter the same reducer. Broad order
history remains a 30-second healing path. No strategy timeout, hedge rule,
symbol rule, or customer-specific blocking policy is embedded in the adapter.

## Preserved Red Baselines

The first deterministic lifecycle suite ran before product changes and produced
`9 failed, 45 passed`. The failures reproduced the shared contracts:

- a successful cancel DELETE emitted a cancellation callback and terminal local
  status before any terminal broker observation;
- `PARTIALLY_FILLED` degraded instead of producing an incremental fill event;
- polled status changes were sent through the new-order path rather than a
  transition reducer;
- duplicate, out-of-order, cancel/fill, reject, and expiration observations did
  not converge through one exactly-once lifecycle path.

Three streaming contract tests were then added before the stream integration.
They failed because the healing poll was still five seconds, the account
activity handler/subscription path did not exist, and stream activity could not
wake bounded reconciliation. A reconnect snapshot test separately failed until
successful login/subscription woke active-order reconciliation.

A rate-limit test failed because two immediate exact-order reads both called the
broker after the first response returned HTTP 429 with `Retry-After: 2`.

A deterministic concurrent test forced the REST and stream paths to observe the
same partial fill simultaneously. Before the reducer lock, both paths emitted
the callback. This established that dictionaries alone were not an exactly-once
contract.

## Implemented Contract

### Cancellation acceptance is non-terminal

`cancel_order()` always sends the explicit Schwab request when the adapter is
ready. A successful HTTP response changes a non-terminal order to
`CANCELLING`; it does not mark the order or child orders canceled and does not
emit `CANCELED_ORDER`. A later broker-observed `CANCELED`, `FILLED`, `EXPIRED`,
or rejection/error state owns the terminal transition.

`Order.is_canceled()` no longer treats `CANCELLING` as terminal. This is a
broker-neutral semantic correction: cancellation in progress is still exposed
to a possible fill/cancel race.

### One serialized transition reducer

Every observed Schwab snapshot is serialized through an `RLock` and keyed by
broker order identifier. The reducer maintains:

- a cumulative executed-quantity high-water mark;
- terminal observation keys;
- monotonic status handling so an old `NEW` snapshot cannot overwrite
  `CANCELLING`;
- delta-only partial/full fill callbacks;
- fill precedence while local cancellation is still pending;
- exactly-once cancel and error/expiration callbacks;
- no replay of unrelated terminal rows from the seven-day history response.

Schwab execution activities are reduced to cumulative quantity and a weighted
average fill price. The reducer converts cumulative broker values into the
newly observed delta required by LumiBot's callback contract.

### Streaming-first observation with bounded REST healing

The handler is registered before stream login and account-activity
subscription. An account-activity message is treated as an opaque wake-up, not
as trusted order data. The reconciler performs exact reads only for locally
tracked active orders. Successful login/reconnect also wakes reconciliation so
events missed while disconnected are repaired.

The raw account-activity payload is intentionally not parsed into order state in
this revision. That avoids inventing an unstable provider schema before a
sanitized Dev/local capture exists. The provider-authoritative REST response is
the observation fed to the reducer. The broad history poll remains at 30
seconds solely as slow healing.

### Rate-limit behavior

HTTP 429 on exact-order or order-history reads records a deadline per endpoint
family. `Retry-After` is honored when present; otherwise the adapter uses
bounded exponential backoff plus jitter, capped at 60 seconds. Reads inside the
backoff return no observation without calling Schwab. Missing or throttled reads
do not synthesize terminal state.

One-second broad polling was rejected. It would multiply seven-day order-history
requests without eliminating races, and it conflicts with the already observed
aggregate 429 pressure from scans, order reads, positions, submissions, cancels,
and market-data requests.

### Safe lifecycle telemetry

The adapter emits structured log fragments for cancel request/response,
lifecycle callback, stream health, and rate-limit events. Broker order ids are
represented by a per-process salted SHA-256 prefix. Logs omit account numbers,
symbols, quantities, prices, response bodies, tokens, and raw stream payloads.
The telemetry can measure HTTP response time and callback transitions but does
not claim to measure Schwab's internal terminal-commit time.

## Quantity and Callback Semantics

The public contract now calls `on_*` methods lifecycle callback methods:

- `on_partially_filled_order(position, order, price, quantity, multiplier)` is
  called with the newly observed fill delta;
- `on_filled_order(...)` receives the full fill when there were no partial
  callbacks, or the remaining delta after earlier partials;
- `on_canceled_order(order)` is terminal observation, not a command or timer;
- live callbacks may be delayed or reconstructed after reconciliation, so
  side effects must be idempotent and callback bodies should not require a
  redundant broker read before using the supplied event.

## Qualification Results

The targeted deterministic gate is green:

```text
pytest tests/test_schwab_positions_unit.py tests/test_order.py -q
83 passed
```

The suite covers non-terminal cancel acceptance, partial/final fill deltas,
duplicate and out-of-order observations, cancel/fill races, error/expiration,
concurrent REST/stream duplicates, handler registration order, reconnect
reconciliation, active-only exact reads, slow broad healing, opaque telemetry,
and `Retry-After` suppression.

## Live Stream Red Baseline and Repair

The published 4.5.87 wheel passed live REST account, quote, submit, cancel, and
exact-order reads, but failed before account-activity subscription. schwab-py's
`StreamClient.login()` reads `client.token_metadata.token`; LumiBot constructed
the REST `Client` around its manually managed `OAuth2Session` without attaching
the corresponding `TokenMetadata`, leaving `client.token_metadata=None`.

A new initialization test reproduced that exact contract failure before the
repair. The active 4.5.88 branch now constructs `TokenMetadata` from the wrapped
token and passes it to schwab-py's REST `Client`. The metadata references the
same token dictionary that LumiBot's refresh path updates in place, preserving
one token owner across REST, stream login, and reconnect.

Rob-owned after-hours validation then proved both initial connection and a
fresh-client reconnect without logging raw provider payloads or account ids:

- initial subscription received `OrderCreated` after a real broker-terminal
  rejected order;
- unsubscribe/logout followed by a fresh `StreamClient`, login, and
  subscription received a second `OrderCreated` event;
- the pass criterion excluded the `SUBSCRIBED` acknowledgement so an
  acknowledgement could not masquerade as order activity;
- automatic access-token refresh completed before the stream sessions and both
  sessions used the refreshed shared token metadata.

REST remains the authoritative parsed order snapshot. The remaining live
boundary is an intentionally controlled partial/full-fill capture while the
market is open; deterministic delta, duplicate, cancel/fill-race, and reconnect
coverage remains green.
