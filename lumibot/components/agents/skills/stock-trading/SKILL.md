---
name: stock-trading
description: Use before researching, selecting, opening, modifying, or closing a stock or ETF position, including discretionary investing, rotation, breakout, momentum, mean-reversion, opening-range breakout, and VWAP trading. Also use when a broad mandate leads you to consider stocks or ETFs even if the user did not name an asset class initially.
---

# Stock Trading

Load this skill before using stocks or ETFs as part of a trading decision. If a
broad mandate leads you to a stock idea, load it before submitting an order.

## Core workflow

1. Read portfolio value, cash, current positions, and open orders.
2. Retrieve the current price and recent price history for every serious candidate.
3. Use batch tools for a universe. Do not loop one symbol at a time when a batch
   price or history tool can return the same evidence.
4. Evaluate the user's entry, exit, sizing, and frequency rules against current
   evidence. Write down the decisive condition and whether it is true before
   submitting an order. Do not invent missing signals.
5. Size from current portfolio value, available cash, current price, volatility or
   stop distance, and the user's risk rules. For a notional cap, calculate
   the maximum notional and call `risk_calculate_stock_quantity`; use its returned
   whole-share quantity unchanged. Verify its notional is at or below both the cap
   and available cash before submission. Submit only when the returned quantity is
   greater than zero; otherwise make a no-trade decision.
6. Submit the selected order once.
7. Capture the returned identifier, inspect that exact order, and reread positions
   and open orders. In backtests, a short bounded `orders_wait_for_terminal` is
   appropriate immediately after your own market-order submission because it
   lets the simulator process the pending fill. Do not use an unbounded wait.
8. If a related order is already open, inspect that exact order and do not submit
   another order for the same intended position change. A pending exit already
   owns the exit. Let it resolve or cancel it deliberately before replacing it.
9. Reconcile the final summary with the mutation tools and the final account
   reads. If an order tool returned a submitted identifier, never say that no
   order was entered. Report the exact observed status instead.

## Research depth

Match research to the strategy. A broad discretionary investment decision should
use relevant technical, news, macro, and company evidence when available. A
mechanical intraday strategy should prioritize the exact price, bar, volume, and
indicator evidence required by its rules. Do not force irrelevant research merely
to increase tool use.

## References

Load only the smallest relevant reference:

- `references/research-sizing-and-orders.md`: evidence, sizing, entries, exits,
  rotation, and order verification.
- `references/intraday-setups.md`: opening-range breakout and VWAP mechanics.

The user's active strategy rules decide whether a trade should happen. This skill
provides reusable stock-trading mechanics and does not invent a strategy.
