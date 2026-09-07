# Alpaca Await-Market-Close Crash Investigation

One-line description: Documents the live Alpaca end-of-day crash caused by calling a backtesting-only pending-order method.

Last Updated: 2026-08-27

Status: Fixed locally with credential-free regression coverage; Alpaca paper validation not run.

Audience: LumiBot contributors and operators diagnosing Alpaca live-strategy lifecycle failures.

## Overview

LumiBot issue [#1113](https://github.com/Lumiwealth/lumibot/issues/1113) reports that an Alpaca
paper strategy crashes after the market closes. The strategy reaches
`Strategy.await_market_to_close()`, which delegates to the broker and raises:

```text
AttributeError: 'Alpaca' object has no attribute 'process_pending_orders'
```

The failure is in LumiBot's live broker path, not in strategy code or Alpaca's order API. No broker
request is required to reproduce it.

## Root Cause

`Alpaca` had its own `_await_market_to_close()` override. The override was introduced in a May 2025
commit aimed at stuck backtests and copied this call from `BacktestingBroker`:

```python
self.process_pending_orders(strategy=strategy)
```

`process_pending_orders()` simulates fills and exists only on `BacktestingBroker`. Live Alpaca
orders are updated by the broker's WebSocket trade-event stream or OAuth polling stream, so an
Alpaca broker neither defines nor needs that method.

The override also bypassed the generic live broker's `is_market_open()` guard. After a session had
already closed, Alpaca's next-close timestamp could refer to a future session rather than a wait
that should be skipped.

## Fix and Invariants

The Alpaca override is removed so `Alpaca` inherits `Broker._await_market_to_close()`. The generic
implementation:

- waits only while the configured market is open;
- preserves the strategy's minutes-before-closing buffer;
- uses the broker sleep abstraction; and
- does not simulate pending-order fills in a live broker.

The change does not alter the public `Strategy.await_market_to_close()` contract, order submission,
fill processing, positions, cash accounting, or backtesting. `BacktestingBroker` retains its own
override and continues to process simulated pending orders before advancing simulated time.

## Regression Evidence

The credential-free regression test constructs an Alpaca instance without running its initializer,
replaces the market clock and sleep boundary with mocks, and calls the public strategy method.

Before the fix:

```text
2 failed
AttributeError: 'Alpaca' object has no attribute 'process_pending_orders'
```

After the fix, the test verifies both relevant live states:

1. An open session sleeps for `time_to_close - minutes_before_closing`.
2. A closed session does not request the next close and does not sleep.

The existing legacy backtesting await-close tests remain unchanged and continue to protect
backtesting pending-order processing.

## Live Validation Boundary

No Alpaca credentials, account, or live/paper broker connection were used for this fix. The issue
does not provide the reporter's saved strategy artifact, so the exact customer deployment cannot be
run locally. A maintainer or reporter with an isolated Alpaca paper account should confirm the full
strategy lifecycle through market close before describing the customer path as end-to-end proven.
