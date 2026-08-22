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

1. Read current portfolio value, cash, signed positions, and open orders in the
   current agent run before any option order.
2. Read the underlying's current price. Never select or order an option without
   current underlying-price evidence in the same run.
3. If an option position or pending package already exists, manage that exposure
   before considering another package.
4. Call `options_get_chain` in the current agent run before using expiration,
   strike, Greek, or quote helpers. Use only expirations and strikes returned by
   tools.
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

## Position truth

Treat current signed quantities as authoritative:

- Positive quantity is long. Reduce it with `sell_to_close`.
- Negative quantity is short. Reduce it with `buy_to_close`.
- Never use `buy_to_close` for a positive quantity. Never use `sell_to_close`
  for a negative quantity. Those pairings do not close the observed position.
- Close exactly the absolute current quantity for each contract.
- Never multiply a cleanup quantity or repeat a close without rereading positions.
- Do not report flatness until every relevant signed quantity is zero.

Immediately before pricing or submitting a close, reconcile every exact contract
against the latest `account_positions` result:

| Observed signed quantity | Meaning | Closing side | Closing quantity |
| --- | --- | --- | --- |
| `+Q` | long | `sell_to_close` | `Q` |
| `-Q` | short | `buy_to_close` | `Q` |

Reject the proposed package yourself if any closing leg violates this table. If
a close does not produce flat positions, inspect its exact status and open orders.
Do not switch tools, reverse sides, change quantities, or submit another close
until the prior order's terminal state and the current signed positions prove
what remains.

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
