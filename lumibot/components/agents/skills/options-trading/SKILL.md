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

Apply the workflow to your assigned role. A non-trading researcher gathers
evidence needed for the research question and gives a compact dated assessment;
it need not repeat the trader's complete account/contract/order-readiness
checklist. The trading agent independently establishes execution readiness
immediately before each action and never treats an upstream answer as that
verification.

An instruction to open or close a bounded position when stated checks pass is
authorization to act once those checks pass. Do not require a second instruction
or add an unstated history length, signal, risk percentage, or sizing condition.
Use a permitted contract count within the user's stated cap. Preserve discretion
when the user delegates whether to trade, and preserve all actual account,
broker, approval, liquidity and risk limits.

1. Establish current cash, exact signed contract positions and pending packages.
   A complete, fresh Runtime Context account snapshot can supply that evidence;
   inspect its completeness, timestamps and exact identities. Call
   `account_portfolio`, `account_positions` or `orders_open_orders` for missing,
   truncated or stale portions, and reread affected state after an order action.
   Do not repeat unchanged complete reads merely to satisfy a ritual sequence.
2. Read the underlying's current price. Never select or order an option without
   current underlying-price evidence in the same run.
3. If an option position or pending package already exists, manage that exposure
   before considering another package.
4. For new contract selection, call `options_get_chain` in the current agent run
   before using expiration, strike, Greek or quote helpers. Use only expirations
   and strikes returned by tools. A risk-reducing close of verified exact held
   contracts does not require a broad new candidate search; use their returned
   identities and refresh their execution evidence.
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
   Interpret the returned Greeks for the complete intended exposure, rather than
   treating their retrieval as a checkbox. Weight each leg by its signed contract
   quantity and multiplier. Assess directional delta and how gamma can change it;
   account for time decay and volatility sensitivity when material to the holding
   policy. Offsetting current Greeks do not guarantee those offsets persist.
   Explain the consequential exposure/risk briefly in the order rationale using
   the actual evidence; do not invent missing Greeks or list every Greek by rote.
   Preserve each tool-returned contract Greek's sign. Distinguish that raw
   long-contract Greek from its signed position contribution: multiply by signed
   contract quantity and the actual multiplier once. A long put's negative delta
   is not made positive by calling it a protective leg; a short position reverses
   the raw contribution. Check the signed sum before reporting a net exposure and
   omit an unsupported numeric claim rather than guessing it.
   For a reported aggregate across multiple legs, use an available calculation
   tool, normally the existing `duckdb_query`, with one VALUES row per exact leg: its unchanged
   raw Greek, signed contract quantity and multiplier. Sum
   `quantity * raw_greek` for contract-weighted units, and
   `quantity * multiplier * raw_greek` for underlying-share-equivalent exposure.
   Calculate all relevant aggregates in one query. A VALUES calculation needs
   no previously loaded market table or extra price-history request. Inspect the
   returned rows before reporting the result; do not copy an upstream net value
   or reverse a raw put sign while transcribing inputs. A researcher that does
   not need an aggregate may report the actual raw observations instead.
   In an order explanation, report the calculated net value and its units;
   do not recopy a tuple that mixes raw Greeks with signed contributions or
   claim that raw contract Greeks themselves cancel. If a net value is not
   needed for the decision, omit it rather than add an unchecked calculation.
   If a configured toolset has no calculator, show the signed calculation and
   units and disclose that it was not independently tool-checked.
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
7. Before submitting a package with a short option, include its concrete
   assignment/settlement consequences and intended handling in the order
   rationale. Merely naming "assignment risk" does not explain the obligation.
   Assess exercise, assignment and settlement for the selected contracts too.
   A short option can create an obligation even when another leg bounds the
   package's expiry payoff. Identify the actual contract multiplier, deliverable,
   exercise style and cash or physical settlement where available. For a short
   physically settled call, explain the potential delivery or resulting short
   underlying position and how the held longs, account capacity and planned exit
   would address it. Do not assume an offsetting long exercises automatically or
   eliminates early-assignment, expiration mismatch or cash/position consequences.
   Apply this assessment to the actual instrument; do not assume every option is
   a standard equity contract. Disclose unavailable contract terms and decide
   whether they are material under the mandate.
   Submit related legs as one atomic multi-leg order. Never submit related legs
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
- Preserve the intended relative leg quantities when calculating a per-unit
  package debit or credit: one 2-long/1-short package contains two long contracts
  and one short, not one of each. Balanced 3/3 holdings represent three 1/1
  packages; 6/3 holdings represent three 2/1 packages. Distinguish package units,
  leg ratios and total absolute contract quantities; do not multiply them twice.
- A fixed user-requested contract count or package count is an exact mandate,
  not permission to scale that count up to a spending cap. Check both requested
  counts and the relative ratio before submission. If the user specifies only
  a ratio and a risk budget, choose the number of units under that policy;
  otherwise preserve the stated count. A budget ceiling does not require
  spending all of it. The trader owns this decision and submits the exact legs.
- Reconcile the pricing helper's result against ratio-weighted exact-leg prices
  and the contract multiplier before choosing the actual package quantity and
  submitting. A helper result that omits a ratio is inconsistent evidence, not
  permission to change the user's package or pretend it is affordable.
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

## Monitoring an operation

When attention tools are available, verify each condition's units, sign and
comparison against the exposure you actually hold. `basket_value` is the signed
marked value of its supplied quantities times multipliers, not profit/loss or
an unsigned cost to close. A short basket normally has negative value; a rising
repurchase cost makes that value more negative. Entry cash plus current signed
holdings value gives marked profit/loss before fees. Derive the boundary from
the intended risk rule and check that a worsening observation would trigger it.
Do not use a positive greater-than condition to detect an increasing negative
liability. For an allowed loss L, solve `entry_cash + basket_value = -L`, so the
loss-review boundary is `basket_value <= -L - entry_cash`. An opening debit D
means entry_cash=-D, so the boundary is D-L, still positive while L<D; an opening
credit C means entry_cash=+C, so the boundary is -C-L. Never subtract the debit a
second time or put a loss amount directly into the value field. Check the
proposed comparison against one unchanged and one worsening mark before saving
it. A chosen partial-debit loss limit L<D requires a positive D-L boundary and
should alert before the basket reaches zero. If the mandate deliberately allows
loss of the full bounded debit, L=D gives a zero boundary: that is a review at
full-premium loss, not a partial-loss stop or zero economic risk. Preserve any
earlier loss review the user requested or the trader promised; do not silently
replace it with a full-loss boundary. Retain the chosen thesis and time reviews.
For example, a $200 opening debit with a $50 loss review uses a $150 holdings
value boundary; a full $200 loss uses zero. A negative $200 holdings mark would
mean a $400 loss before fees, not loss of only that original premium. State the
entry cash, chosen loss and resulting holdings-value boundary before saving it.
For a credit example, $50 received and a $20 loss review require a -$70 signed
holdings boundary: $50 + (-$70) = -$20. A -$30 boundary instead gives a $20
profit and cannot represent that loss. Substitute your actual numbers back
into the equation and reconcile the result with the explanation before saving;
knowing the formula does not establish that your chosen number satisfies it.
Keep the chosen review loss distinct from maximum possible package loss.
Marks request attention; refresh executable prices before ordering.

While an exit is working, retain the chosen protection for still-held or
partially filled exposure. An order-status watch observes execution progress;
it does not replace a price/value risk condition. Do not remove exposure-risk
conditions merely because a close was submitted. When replacing protection,
verify that its replacement covers the stated policy before retiring the old
condition. This preserves the chosen policy; it does not impose an earlier
stop or require a duplicate monitor where verified native protection suffices.

After a confirmed close, inspect your attention conditions and remove those
belonging to the completed operation. Do this before a new entry reuses the same
contracts. A zero-quantity residual warning would otherwise flag the intended
new holding. Give a new operation its own identity and current watches; changing
the identity does not remove earlier conditions automatically.

Use meaningful events and sparse reviews by default. An unchanged pricing-tool
defect does not become new research merely because another minute passes.
Report the exact conflict and choose a useful new-data or recovery review while
retaining any held-position risk observations. Respect an explicitly requested
frequent-AI policy rather than imposing a universal call limit.

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
