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

An instruction to open or close a bounded position when stated checks pass is
authorization to act once those checks pass. Do not require a second instruction
or add an unstated history length, signal, risk percentage, or sizing condition.
Use a permitted contract count within the user's stated cap. Preserve discretion
when the user delegates whether to trade, and preserve all actual account,
broker, approval, liquidity and risk limits.

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
   show that no listed contract fits. Missing Greeks or quotes for one listed
   expiration do not establish that all expirations lack data. Check other
   listed expirations that satisfy the user's constraints before declining for
   unavailable data. Re-select and verify every leg for the chosen structure.
   Keep one expiration when the structure requires it. Calendar legs may use
   distinct listed expirations only when the user or active rules allow them.
   Never relax the user's limits to find a trade.
   When no expiration is specified, choosing an eligible listed expiration is
   part of contract selection; unusable nearer contracts do not disqualify a
   fully verified later candidate. If a chosen wing has the wrong ordering,
   correct it from the listed contracts and verify its exact Greeks and quotes
   before repricing. A recoverable selection error is not proof that valid
   market evidence is unavailable.
   Build a bounded candidate comparison from listed strikes and exact Greeks.
   Choose short strikes by the requested signed or absolute delta. A strike
   near the underlying does not automatically match the delta target. When a
   checked candidate misses the target, use candidate helpers or other eligible
   listed strikes to search toward the target. Candidate helpers can narrow
   larger chains; compare relevant candidates directly for a small strike list.
   Compare verified deltas before selecting the closest eligible candidate.
5. Separate short-strike selection from protective-wing selection. For an iron
   condor with specified wing widths, calculate
   `long_put = short_put - put_width` and
   `long_call = short_call + call_width` from the requested widths.
   Check those exact wing strikes against the complete returned put and call lists.
   A contract previously considered as a wing can become a short when its verified
   delta is the appropriate match; rebuild both outer wings for the selected short pair.
   Unchecked outer strikes do not establish that protective wings are absent.
   Keep the complete listed-strike evidence separate from the subset whose
   Greeks or quotes you have inspected. If an exact wing is listed but has not
   been inspected yet, query that contract next. Before reporting a missing
   wing, reconcile its exact symbol, expiration, right and strike against the
   returned list; absence from your evaluated-candidate subset is not absence
   from the market.
   Verify every selected contract individually. Candidate-selection helpers narrow
   the search but do not prove the exact contract's Greeks or quote quality.
   Changing any leg's symbol, expiration, right or strike invalidates that leg's
   earlier checks. Re-read both Greeks and quote quality for its final identity
   before package pricing or submission; verifying a replaced candidate does not count.
6. Before constructing an opening multi-leg payload, read
   `references/common-option-structures.md` and verify each buy/sell side,
   strike ordering and ratio against the intended structure. For credit
   verticals, sell the higher-strike put and buy the lower-strike put, or sell
   the lower-strike call and buy the higher-strike call. Reversing one spread
   changes the structure. Closing sides instead come from
   current signed positions as described below. Evaluate every exact leg, then
   explicitly call `options_calculate_multileg_price` before submission, even
   when the submit tool can calculate a price automatically. An unexpected
   near-zero price or wrong debit/credit sign is a reason to recheck leg topology
   and returned quotes before concluding that market evidence is unavailable.
   Preserve the requested structure and constraints; never force a trade.
7. Submit related legs as one atomic multi-leg order. Never submit related legs
   independently, including entry, exit, adjustment, or cleanup. If atomic
   package submission is unavailable, make a no-trade decision and report that
   broker capability as the blocker.
   If checks pass for an already-authorized execution, continue from preview to
   `orders_submit_multileg` with the verified legs and signed limit, then
   complete the status and position checks below before your final answer.
   A successful price preview or an intention to submit is not completed execution.
   If a real condition remains unmet, state that condition and leave the position unchanged.
   For one authorized option contract, use `orders_submit_order` with the exact
   verified identity, quantity, side, limit price and required private purpose
   and order reason. Complete that call before claiming submission.
8. Capture the returned identifier, inspect that exact order, and reread positions.
   Submission is not proof of a fill, and a fill response alone is not proof that
   the account has the intended final exposure.
   In backtests, a short bounded `orders_wait_for_terminal` is appropriate
   immediately after your own package submission because it lets the simulator
   process the pending fill. Do not use an unbounded wait. Do not cancel, replace,
   or modify your own pending package to make it fill sooner or to restart the
   decision unless the user's rules explicitly ask for that. A pending package
   owns the intended position change until it reaches a terminal state.
   Verify every returned child identifier for a multi-leg package and reconcile
   every intended leg against fresh signed positions. One child's terminal fill
   does not prove the complete package filled. Report unresolved legs explicitly.

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
- Reconcile package arithmetic using the returned per-leg signed prices and
  quantities. Do not invent a conflicting credit from mental arithmetic. If a
  discrepancy appears, recheck the exact sides, ratios and price style with the
  pricing tool before deciding the evidence is inconsistent.
- Reject prices that contradict the structure's economic bounds.
- For a standard same-expiration credit iron condor with one contract per leg,
  compute `put_width = short_put - long_put`,
  `call_width = long_call - short_call`, and
  `short_gap = short_call - short_put` separately from the verified strikes.
  The short-strike gap is not a wing width. Check the complete strike ordering
  and each protective width against the intended structure.
  Reconcile `credit = -net_limit_price` with the returned signed leg prices.
  Independently compute `max_loss = max(put_width, call_width) - credit` in
  those same price units, then scale by the verified contract multiplier and
  matched package quantity to obtain monetary maximum loss. Compare that loss
  and the package cash impact against the actual strategy, account and broker
  limits. Other structures or ratios require their own verified payoff bounds.
- Choose the price style for the authorized execution goal before submitting.
  A midpoint is a valuation estimate, not an immediately executable quote;
  `best` and `mid` limits can remain pending. For an authorized entry or exit
  intended to execute now, calculate `price_style='fastest'` from current
  exact-leg quotes unless the user specifies a different price preference.
  Closing held positions also requires `action='close'`. Pass the returned
  signed price as `net_limit_price` so the order records the price you verified.
  This buys at the quoted asks and sells
  at the quoted bids, while retaining a limit. Preserve every user debit cap,
  minimum credit, risk bound and execution preference; if the executable price
  violates one, leave the position unchanged and explain the conflict.
  Quote movement can still prevent a fill, so verify actual status and positions.

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

## Explain consequential order actions

Supply an explicit `reason` before every submission, modification or cancellation.
Use at least 200 characters; aim for 600–2,500 when the decision needs it. Explain
the thesis, dated evidence, sizing, material risks and what would invalidate it.
State missing evidence honestly. This is a decision summary, not hidden internal
reasoning, and must contain no credentials or account identifiers. The order
service commits it before execution. Report only observed order status and fills;
reconcile an uncertain outcome before trying again.
