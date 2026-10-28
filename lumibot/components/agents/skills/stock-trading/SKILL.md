---
name: stock-trading
description: Use before researching, selecting, opening, modifying, or closing a stock or ETF position, including discretionary investing, rotation, breakout, momentum, mean-reversion, opening-range breakout, and VWAP trading. Also use when a broad mandate leads you to consider stocks or ETFs even if the user did not name an asset class initially.
---

# Stock Trading

Load this skill before using stocks or ETFs as part of a trading decision. If a
broad mandate leads you to a stock idea, load it before submitting an order.

## Core workflow

1. Read portfolio value, cash, current positions, and open orders.
2. Read `market_quote` before sizing; prefer `market_quotes` for a basket.
   For an immediate buy use a valid ask as its executable cost; for an
   immediate sell use a valid bid. For an explicit limit, size at that limit.
   When a quote side is unavailable, its valid current `price`, or a fresh
   `market_last_price` / `market_last_prices`, is a usable disclosed fallback.
   Missing bid/ask alone does not make a priced stock untradeable. Do not
   substitute an old historical close for the execution price.
   Retrieve recent price history for every serious candidate.
   Read that history with `market_historical_prices`, also for a single symbol;
   pass `table_name` to query it with `duckdb_query`.
   `market_load_history_table` does not replace it before a stock order.
   Compute averages and indicators with a tool (`get_indicator`, `get_indicators`,
   or `duckdb_query` over loaded bars), never by mental arithmetic, and quote
   the tool's value. A query returning individual closes has not computed their
   average. Request the aggregate or indicator and inspect its returned value
   before using it in a decision or describing it as tool-calculated.
   For lookback rules, select the requested count of the latest completed bars
   and match the reported indicator and date window to that tool result. When
   reporting a date span, retrieve the first and last timestamps of those same
   selected rows alongside the aggregate. Do not infer their dates from a bar
   count or an assumed weekday calendar. If the result does not give the window,
   omit that extra date claim. Do not discard an already-completed bar again or
   substitute an earlier indicator row.
   For a historical snapshot select the latest eligible row by timestamp and
   read that row's close. Selecting MAX(close) alongside MAX(datetime) pairs
   unrelated values and does not select the snapshot-date price.
3. Use batch tools for a universe. Do not loop one symbol at a time when a batch
   price or history tool can return the same evidence.
4. Evaluate the user's entry, exit, sizing, and frequency rules against current
   evidence. Write down the decisive condition and whether it is true before
   submitting an order. Do not invent missing signals. Keep the explanation's
   comparisons identical to the evaluated rule: a latest price exceeding an
   average does not establish that every constituent bar exceeded it. If a
   claim concerns all bars, their count, or a date range, compute and inspect
   that exact predicate from the returned data before asserting it.
5. Size from current portfolio value, available cash, current price, volatility or
   stop distance, and the user's risk rules. For a notional cap, calculate
   the maximum notional and call `risk_calculate_stock_quantity`; use its returned
   whole-share quantity unchanged. Verify its notional is at or below both the cap
   and available cash before submission. Submit only when the returned quantity is
   greater than zero; otherwise make a no-trade decision.
6. Submit the selected order once. Price a limit order from the current
   quote or current last-price fallback, never from a historical bar's close: a buy limit
   below the current price usually does not fill. Follow the order tool's
   guidance when the order must fill this session. This is for a new order; it
   is never a reason to modify an order that is already pending (see step 8).
7. Capture the returned identifier, inspect that exact order, and reread positions
   and open orders. In backtests, a short bounded `orders_wait_for_terminal` is
   appropriate immediately after your own market-order submission because it
   lets the simulator process the pending fill. Do not use an unbounded wait.
8. If a related order is already open, call `orders_get_status` with its returned
   identifier to inspect that exact order; listing open orders alone does not
   replace this status read. Do not submit another order for the same intended
   position change. A pending exit already
   owns the exit: leave it in place and report that it owns the position change.
   Modify or cancel and replace it only when the user's rules explicitly require
   changing an existing order. A due exit condition or a nonmarketable limit
   alone does not authorize changing an already-pending exit.
9. Reconcile the final summary with the mutation tools and the final account
   reads. If an order tool returned a submitted identifier, never say that no
   order was entered. Report the exact observed status instead.

## Complete portfolio work

For an authorized allocation or rebalance build one compact full-inventory
plan on the requested capital basis: target amounts/quantities, held and pending
quantities, funding sells, remaining buys and real exceptions. A prescribed
allocation is the trading mandate; do not require a separate discretionary
conviction thesis before replicating each requested constituent.

Calculate the capital basis and reserve before quantities: investable capital is
the authorized basis times the requested investment fraction (or basis minus the
requested reserve). Each allocation cap is investable capital times its target
weight. Verify multiplication/division with an available arithmetic tool. An
unavailable name keeps its allocation in cash, alongside the original reserve;
do not silently use the full account as the allocation basis or renormalize the
supported subset unless the user authorized that change.

Work through every feasible required change in this run. Prefer `orders_submit_orders`
for a jointly sized independent basket: pass explicit order rows, verify all returned
identifiers together, then reconcile actual account state. A batch is not atomic;
verify funding sells before submitting dependent buys. Preserve atomic option
packages through `orders_submit_multileg`. Use batch quotes and
history when the task needs historical analysis; an explicit allocation does not
need an invented historical entry signal. After each mutation refresh the account and complete position/open-order
pagination, then size only the next intended order against its allocation cap
and actual available cash. Do not restart all remaining sizing calculations
after each fill. An existing correct holding requires no new purchase; a pending
order owns only its own remaining quantity. Verify funding sells before buys.

Before finishing compare the full target set to actual positions and terminal
orders. A few confirmed trades or an acknowledgement are not full completion.
Continue supported independent trades when one constituent is unavailable.
Explain each actual missing price, unaffordable quantity or broker failure;
do not invent an exception or finish merely because the basket is long. Never
force a token trade, breach an allocation cap, short or overspend to satisfy a count.

Use this stopping rule for the execution loop: after each verified change,
remove only that completed quantity from the worklist and continue to the next
feasible item. There is no default cap on trades per iteration. An honest report
that only a few requested holdings were bought still leaves the task unfinished.
When the final comparison finds an affordable missing target with usable current
pricing and no conflicting pending order, call the next order tools instead of
ending the run. Stop only when the whole inventory is reconciled or every
remaining item has its own observed blocker.

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

## Explain consequential order actions

Supply an explicit `reason` before every submission, modification or cancellation.
Use at least 200 characters; aim for 600–2,500 when the decision needs it. Explain
the thesis, dated evidence, sizing, material risks and what would invalidate it.
State missing evidence honestly. This is a decision summary, not hidden internal
reasoning, and must contain no credentials or account identifiers. The order
service commits it before execution. Report only observed order status and fills;
reconcile an uncertain outcome before trying again.

When a separate `public_reason` is requested, write a qualitative trade thesis
and risk. Keep portfolio-relative sizing percentages, formulas, account value,
buying power, owner context and tool purposes in the private `reason` only. Do
not copy its sizing explanation into the public summary. Follow the order tool's
public-summary contract before submitting; later omission of an unsafe summary
does not make that proposed summary suitable for sharing.
