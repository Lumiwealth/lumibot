# Alpaca daily valuation and benchmark window

The general capability is correct, clock-aware daily valuation and comparable
benchmark reporting for every Alpaca backtest. It is useful independently of any
particular strategy or user. No strategy, model, allocation or trading policy is
embedded in the implementation or fixtures.

## Failure and repair

Daily `get_last_price` always returned the bar's opening price, including after
the session close. It also exposed that opening price before the exchange opened.
Daily account snapshots therefore mislabeled opening marks as closing values.
The corrected provider path uses the prior completed close before opening, the
current open while the daily bar is incomplete, and the current close once the
exchange session ends. Exchange calendars preserve early closes. Option first
prints remain unavailable before their timestamp; crypto does not close at the
equity exchange close. Intraday fill conventions remain unchanged.

The Alpaca benchmark branch separately computed close-to-close changes, omitting
the first session's move and accepting incomplete windows. It now uses the
existing generic daily benchmark calculation: first available open through the
last completed close, subsequent dividends when supplied, and visible missing
coverage. Intraday benchmarks retain their existing observation basis.

## Regression evidence

- Before repair: seven daily-mark failures and six benchmark-window failures.
- Provider regressions cover ordinary/early closes, preopen missing history,
  first option prints, crypto, first-session return, dividends and missing
  first/interior/final benchmark sessions.
- The real engine acceptance test verifies an actual filled order at the open,
  cash reconciliation and closing portfolio value. Only external SDK responses
  are fixtures; the broker, fills and accounting are real.
- The focused Alpaca inventory passed 93 tests locally in the pinned SDK environment.
- The native actor/judge verifier preserves all 23 existing real-model case
  qualifications and their original usage/timestamps. Fixtures construct
  PandasDataBacktesting, never AlpacaBacktesting. Compatibility excludes the
  Alpaca adapter and only the exact Alpaca benchmark dispatch body; the dispatch
  condition, other providers and shared accounting remain source-bound.
- The existing receipt's stale compatibility anchor was advanced to its actual
  original commit after verifying the immutable receipt bytes and rebuilding
  its original runtime fingerprint. The native receipt itself is unchanged.

These deterministic regression tests do not constitute new real-model runs.
Local passing checks are not evidence of a package publication or hosted rollout.

## Review follow-up

The SDK transport regression reproduced a missing first-session preopen mark:
the prior close existed before the loaded window, but the lookup suppressed
history extension. The lookup now permits the ordinary bounded provider history
path and resolves the earlier session calendar. Session-aware recursion remains
necessary when a timezone-shifted daily label initially selects an unstarted
session; returning that bar's close directly would reveal a future closing price.

Equity benchmarks now request an equity calendar even on a strategy source
configured for continuous trading. Non-session rows are excluded before daily
return calculation; crypto retains all calendar days. Native tests separately
verify the two asset classes and the actual requested provider calendar.

The combined affected provider, benchmark, accounting, real-engine and inference
compatibility inventory passed 154 checks locally. The existing 23-case native
actor/judge receipt remains source-compatible, with original records unchanged.
