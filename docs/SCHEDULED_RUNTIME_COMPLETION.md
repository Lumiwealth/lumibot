# Scheduled Runtime Completion

Description: Completion-aware shutdown for scheduled and explicit run-once strategies.
Last Updated: 2026-10-01
Status: Implemented, unreleased
Audience: Strategy authors and runtime operators

## Overview

`Trader.run_all(run_once=True)` executes one trading iteration, then finishes
runtime-owned work before calling `on_strategy_end`. Work started by the end
hook is drained next. The runner then publishes its final cloud snapshot,
closes the broker connection, and backs up variables.

The drain runs without requiring `LUMIBOT_SCHEDULED_EXECUTION` or a nonzero
`LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS`. The latter still requests a minimum
post-iteration observation window for scheduled execution. It is not a deadline
that can truncate pending SmartLimit work.

## Work that keeps the runner alive

- Active native SmartLimit orders, including partially filled orders, through
  their configured repricing ladder and final hold, until filled, rejected,
  expired, or confirmed canceled.
- Broker submission queue tasks until the worker calls `task_done`, including
  a submission already removed from the queue while its broker call is running.
- Tracked unprocessed orders and cancellation/replacement transitions.
- Queued order callbacks. The runner processes callbacks and advances SmartLimit
  itself because run-once execution does not start the continuous live worker.

Synchronous strategy operations naturally finish before their lifecycle hook
returns. Arbitrary application-created threads or tasks are not automatically
registered as LumiBot work. Passive resting market/limit/GTC, stop and bracket
orders are not a reason to keep the process alive indefinitely. Broker-resident
orders can remain active after the process exits and be reconciled on the next
invocation.

## Failure and interruption

Pending work has a separate liveness guard: at least 300 seconds, extended to the
longest observed configured SmartLimit ladder plus final hold plus 300 seconds of
broker-response grace. Callback-created longer SmartLimit configurations can
extend that budget without resetting it on every poll. This is a total drain
budget, not permission for an unbounded chain of new orders.

On timeout, the timing record says `drain_failed` with pending-work counts,
`run_once()` returns `False` and stores the `TimeoutError` on the executor, and
the normal crash hook runs. An explicit stop produces `drain_interrupted` and
an `InterruptedError`. Neither path publishes a successful final snapshot or
claims all broker orders were canceled. Forced external termination cannot be
prevented by a library; the host must wait for normal process completion rather
than apply a shorter kill deadline.

## Verification

`tests/test_scheduled_run_once.py` covers zero/short post windows, native repricing,
confirmed cancellation, fill/error/cancel completion, dequeued broker work,
end-hook work, callback-created orders, long configurations, passive GTC orders,
and stop/timeout failures. `tests/test_scheduled_order_process_boundary.py` also
launches a fresh process using native strategy order creation, SmartLimit
submission, repricing, broker fill processing, the fill callback, and final cloud
serialization before process exit. Broker and HTTP transports are fixtures, not
claims of hosted delivery or actual broker fills.
