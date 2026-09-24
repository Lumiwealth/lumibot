---
name: options-trading
description: Use before researching, selecting, opening, modifying, or closing any option position, including single-leg options, vertical spreads, iron condors, butterflies, straddles, strangles, calendars, and other multi-leg structures. Also use when a broad trading mandate leads you to consider options even if the user did not mention options initially.
---

# Options Trading

Load this skill before using options as part of a trading decision. The user does
not need to have requested options explicitly. If options become relevant while
you are pursuing a broader mandate, load this skill before selecting contracts or
submitting an option order.

## Core workflow

1. Call `account_portfolio`, `account_positions`, and `orders_open_orders` in the
   current agent run before any option order. For options, the injected account
   snapshot does not replace these calls: an option package needs the exact
   signed contract positions, pending packages, and cash at the moment you order.
2. Read the underlying's current price. Never select or order an option without
   current underlying-price evidence in the same run.
3. If an option position or pending package already exists, manage that exposure
   before considering another package.
4. Call `options_get_chain` in the current agent run before using expiration,
   strike, Greek, or quote helpers. Use only expirations and strikes returned by
   tools.
   Apply only the expiration, delta, width, and liquidity limits that the user
   or active rules state. Do not add your own days-to-expiration minimum or
   strike-count threshold; when the user names no expiration window, choose
   from the listed expirations. A short strike list is not by itself a reason
   to decline. Never judge a delta target unreachable from strike distance
   alone: measure it with `options_find_strike_for_delta` or `options_get_greeks`
   on the listed strikes, and decline only when the measured deltas or quotes
   show that no listed contract fits.
5. Verify every selected contract individually. Candidate-selection helpers narrow
   the search but do not prove the exact contract's Greeks or quote quality.
6. Evaluate every leg. For every multi-leg order, explicitly call
   `options_calculate_multileg_price` after evaluating the exact legs and before
   submission, even when the submit tool can calculate a price automatically.
7. Submit related legs as one atomic multi-leg order. Never submit related legs
   independently, including entry, exit, adjustment, or cleanup. If atomic
   package submission is unavailable, make a no-trade decision and report that
   broker capability as the blocker.
8. Capture the returned identifier, inspect that exact order, and reread positions.
   Submission is not proof of a fill, and a fill response alone is not proof that
   the account has the intended final exposure.
   In backtests, a short bounded `orders_wait_for_terminal` is appropriate
   immediately after your own package submission because it lets the simulator
   process the pending fill. Do not use an unbounded wait. Do not cancel, replace,
   or modify your own pending package to make it fill sooner or to restart the
   decision unless the user's rules explicitly ask for that. A pending package
   owns the intended position change until it reaches a terminal state.

## Position truth

Treat current signed quantities as authoritative. Positive is long, negative
is short, zero is flat.

To close held option contracts, always pass `action='close'` to
`options_calculate_multileg_price` and `orders_submit_multileg`. List only
`symbol`, `expiration`, `strike`, and `right` for each held leg. LumiBot derives
each closing side from the current signed position (long becomes
`sell_to_close`, short becomes `buy_to_close`) and defaults the quantity to the
full held amount. Never write closing sides yourself. Pass `quantity` only for
a per-unit price check or an intended partial close.

- Never multiply a cleanup quantity or repeat a close without rereading positions.
- Do not report flatness until every relevant signed quantity is zero.

If a close does not produce flat positions, inspect its exact status and open
orders. Do not switch tools, change quantities, or submit another close until
the prior order's terminal state and the current signed positions prove what
remains.

## Pricing truth

- Evaluate each exact leg before calculating a package price.
- Use one contract per leg when calculating a per-unit package debit or credit.
- Use the intended package quantity only when submitting the order.
- A positive signed package price is a debit. A negative signed package price is
  a credit.
- Reject prices that contradict the structure's economic bounds.

## References

Load only the smallest relevant reference:

- `references/contracts-greeks-liquidity.md`: expiration, strike, Greek, and quote selection.
- `references/multileg-orders.md`: signed package pricing and atomic order submission.
- `references/position-management.md`: reconstructing, managing, and closing exposure.
- `references/common-option-structures.md`: standard leg topology for verticals,
  iron condors, butterflies, straddles, strangles, and calendars.

The user's active strategy rules decide whether a trade should happen. This skill
explains options mechanics and safe evidence use. It must not invent a strategy,
override user rules, or force a trade.
