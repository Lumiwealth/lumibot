---
name: options-trading
description: Use before researching, selecting, opening, modifying, or closing any option position, including single-leg options, vertical spreads, iron condors, butterflies, straddles, strangles, calendars, and other multi-leg structures. Also use when a broad trading mandate leads you to consider options even if the user did not mention options initially.
---

# Options Trading

Load this skill before using options as part of a trading decision. The user does
not need to have requested options explicitly. If options become relevant while
you are pursuing a broader mandate, load this skill before selecting contracts or
submitting an option order.

Before an option submission, make the decision explicit in its order rationale:
explain the exact package's executable quote quality, signed net exposure from
the returned Greeks, price and total risk, and any short-option obligation.
Assess whether the exact legs' bid/ask widths support the chosen package limit
and holding policy; merely saying that quotes were fetched is not an assessment.
Use available quote times and sizes, disclose missing evidence, and explain the
material execution uncertainty. There is no universal acceptable spread width.
For a short leg, state the actual deliverable quantity and the underlying
position or cash obligation assignment could create before a long is exercised,
plus any temporary funding/capacity requirement or unverified capacity. A generic
"assignment risk" label does not describe those concrete consequences.
Physical short-call assignment delivers shares and receives cash; short-put
assignment receives shares and pays cash. State actual share inventory and
available cash separately from unverified broker permissions. Cash is not stock
already held or a guarantee that a broker will exercise the long option. Do not
describe cash as absent or insufficient when only delivery capacity is unknown.
Keep returned exercise style, settlement and deliverable terms distinct from
assumptions about standard contracts. If those terms were not supplied, disclose
that limitation and label any standard-contract assumption used in the obligation
assessment. Missing optional terms do not automatically veto a permitted trade;
apply the mandate and retain a management plan for the actual uncertainty.
Reading raw Greeks without assessing their signed, quantity-weighted result is
not this decision. Calculate a net value before reporting it, include its units,
and distinguish an offset in the present snapshot from risks that can change
during the holding period. Use the workflow below for the supporting checks.

## Core workflow

Apply the workflow to your assigned role. A non-trading researcher gathers
evidence needed for the research question and gives a compact dated assessment;
it need not repeat the trader's complete account/contract/order-readiness
checklist. The trading agent independently establishes execution readiness
immediately before each action and never treats an upstream answer as that
verification.
Missing contract selection, quotes or package prices in a thesis research
answer are work for the trader through its own tools, not failed research.
For thesis research, omit account, order-status and P&L commentary unless the
research question needs it; the trader owns execution reconciliation.
Aim for roughly 100–200 words in a normal final packet: the decision-driving
evidence, conclusion and material uncertainty. The persisted tool record keeps
the full tables. Use more space when the user's question or unresolved exposure
needs it; brevity never replaces the required checks or disclosure.

Each role checks its own reported facts against its returned evidence. Keep
last price, midpoint, bid and ask labels distinct; a last price is not a quoted
bid. Preserve the observation's field and units when citing it. A correct later
trader assessment does not make an earlier research claim correct. Keep the
packet compact and omit unused numerical claims rather than recopying them.
Keep actual contract inventory separate from net payoff in a price region.
Changing the underlying price does not change the strikes or quantities held;
describe those exact legs before explaining hedges or assignment obligations.
State the reference for a comparison: premium, distance from the current
underlying, exposure and risk are different quantities. Check both exact
candidates on that same basis before saying cheaper, nearer, higher or lower.
Use `duckdb_query` with a plain `SELECT` calculation for decision-driving
distance, collateral capacity, weighted exposure, payoff and threshold arithmetic. No loaded table
is needed for arithmetic. Calculate with the observed inputs, then check that
the result supports the comparison you report; do not guess the comparison.
For prices, use the same pricing style and units and prefer returned package
totals. For distance, compare each actual strike or package center with the
same observed underlying price. If the comparison does not drive the decision,
omit it instead of adding an unchecked advantage to the rationale.
Use the verified signed package total when explaining an order. If expanding
the leg arithmetic, reconcile every sign with the action: buying to close pays
cash and selling to close receives cash. Check the expansion against the returned
total before reporting it; omit an unnecessary expansion rather than publishing
an unchecked formula beside a correct tool result.
For an ongoing risk watch, signed current holding value is not profit/loss:
calculate it from the exact signed quantities, multipliers and observed marks;
profit/loss adds the opening cash flow. Verify a claimed current basket value
before putting that number in a watch reason or report. A watch reason can simply
explain why the selected boundary needs attention without reciting an unverified
current measurement.

Keep expiration payoff separate from profit. Gross expiration payoff is the
signed sum of the legs' intrinsic values, with their quantities and multipliers.
Profit subtracts the opening net debit and applicable fees; an opening credit
is a negative debit. For N matched one-to-one debit verticals of width W and
multiplier M, the gross payoff ceiling is N*W*M. Maximum expiration loss is the
actual total debit paid plus fees; maximum profit is N*W*M minus that debit and
fees. Subtracting the debit from the width calculates profit, not loss. Calculate
and label these separately before describing a loss watch; a partial loss must
not be called full-premium loss. The premium budget is neither payoff nor profit.
Other ratios require their own signed payoff calculation. At an underlying
price exactly equal to its strike, an option is at the money with zero intrinsic
value. An out-of-the-money call has underlying below strike; an out-of-the-money
put has underlying above strike. Zero intrinsic value alone does not distinguish
at the money from out of the money.

Track entry opportunities by the actual entry-review or entry date under the
mandate's trading-session timezone, separately from expiration and management.
Closing or reviewing a position opened in an earlier session does not consume
today's entry allowance unless the mandate explicitly counts those actions.
Check the dated order and decision evidence before claiming an allowance was
used. Retain unresolved obligations across session changes; after verified
closure, schedule the next genuinely eligible opportunity, which may be later
that same session. Do not invent an extra skipped day or cooldown.
When a read rejects malformed tool arguments, correct the reported format and
retry that read before finishing. An argument error is not unavailable market
data. Keep correction bounded and report a persistent error as unresolved.
Do not repeat an order submission because a later read failed; reconcile the
exact order and remaining exposure first.

An instruction to open or close a bounded position when stated checks pass is
authorization to act once those checks pass. Do not require a second instruction
or add an unstated history length, signal, risk percentage, or sizing condition.
Use a permitted contract count within the user's stated cap. Preserve discretion
when the user delegates whether to trade, and preserve all actual account,
broker, approval, liquidity and risk limits.

Before an order, verify every mandatory policy check against the final exact
contract and current decision time. If a check fails, choose another eligible
contract or hold. Do not submit an order whose own reason acknowledges a failed
requirement. A missing eligible candidate never authorizes relaxing a limit.

1. Establish current cash, exact signed contract positions and pending packages.
   A complete, fresh Runtime Context account snapshot can supply that evidence;
   inspect its completeness, timestamps and exact identities. Call
   `account_portfolio`, `account_positions` or `orders_open_orders` for missing,
   truncated or stale portions, and reread affected state after an order action.
   Do not repeat unchanged complete reads merely to satisfy a ritual sequence.
2. Read the underlying's current price. Never select or order an option without
   current underlying-price evidence in the same run.
   A current underlying last price or mark can support contract selection even
   when that underlying's bid/ask is unavailable. Assess the exact option legs'
   execution quotes independently; missing stock quote sides do not establish
   that option quotes are missing. Preserve the actual price label.
   For several exact contracts, `market_quotes` reads their bid/ask evidence in
   one tool call using the native provider's batching where available. Keep
   different expirations, strikes and rights distinct, inspect missing rows,
   source times and sizes, and do not turn a last price into a bid or ask.
3. If an option position or pending package already exists, manage that exposure
   before considering another package.
4. For new contract selection, call `options_get_chain` in the current agent run
   before using expiration, strike, Greek or quote helpers. Use only expirations
   and strikes returned by tools. A risk-reducing close of verified exact held
   contracts does not require a broad new candidate search; use their returned
   identities and refresh their execution evidence.
   Request chain coverage through the mandate's expiration horizon and inspect
   returned range/pagination limits. A default shorter query window is not
   evidence that longer-dated listed contracts are unavailable.
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
   Assess the selected package's signed net exposure and the Greeks material to
   its thesis; listing raw leg values alone is not that assessment. Explain the
   supported directional or risk implication. Calculate a numeric aggregate
   before citing one, rather than filling the answer with unused Greek values.
   For a package, prefer `options_get_greeks(legs_json=...)` with the exact
   selected identities, sides and quantities. It reads every leg and computes
   all signed net Greeks in one model/tool round trip. `greeks` uses signed
   contract quantities; `underlying_equivalent_greeks` also applies each actual
   multiplier once. Inspect completeness and each returned leg before citing a
   total. A zero current net theta, gamma or vega is an offset in that snapshot,
   not evidence of negative decay or nonzero exposure; those risks may change.
   In an older configured toolset without package input, use `duckdb_query`
   with one VALUES row per exact leg's unchanged raw Greek, signed quantity and
   multiplier. Sum `quantity * raw_greek` or `quantity * multiplier * raw_greek`
   in one query. A researcher need not calculate an unused aggregate.
   In an order explanation, report the calculated net value and its units;
   do not recopy a tuple that mixes raw Greeks with signed contributions or
   claim that raw contract Greeks themselves cancel. If a net value is not
   needed for the decision, omit it rather than add an unchecked calculation.
   If a configured toolset has no calculator, show the signed calculation and
   units and disclose that it was not independently tool-checked.
6. For a single option leg, choose a bounded limit from its exact verified
   bid/ask, or a historical trade basis explicitly marked usable by the tool,
   and submit through `orders_submit_order`. `options_calculate_multileg_price`
   requires at least two legs; a one-leg preview error does not invalidate
   otherwise usable exact-contract quotes or require inventing a second leg.
   Before constructing an opening multi-leg payload, read
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
   would address it. State the temporary buying-power or funding obligation
   separately from the bounded expiry loss: the account may have to support
   the resulting stock position before a long is exercised or shares are closed.
   Cash alone does not establish available buying power. If broker capacity is
   unavailable, identify that uncertainty and the intended handling; do not
   invent a margin requirement or assume the small package debit covers it.
   Do not assume an offsetting long exercises automatically or
   eliminates early-assignment, expiration mismatch or cash/position consequences.
   Keep the per-contract deliverable distinct from the position total: multiply
   the actual multiplier by the assigned contract count once, not twice.
   Apply this assessment to the actual instrument; do not assume every option is
   a standard equity contract. Disclose unavailable contract terms and decide
   whether they are material under the mandate.
   Missing optional metadata is not automatically a reason to reject an
   otherwise authorized bounded package. In a permitted historical simulation,
   explain any ordinary contract convention used as an assumption and its risk
   implications; never claim the provider verified it. Verify unusual or
   adjusted deliverables before relying on them. A researcher need not supply
   the trader's assignment plan. The trader assesses the available terms,
   capacity, protection and exit policy and owns that plan.
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

## Short-option coverage and settlement

For a cash-secured put, reserve strike times deliverable multiplier times all
held and working short contracts, plus costs, against available cash. Opening
premium is not permission to reuse that collateral for another obligation.
Calculate any policy collateral cap in the same currency, then compare the
obligation plus costs against that cap and available cash separately before
claiming either is insufficient.
For a covered call, reserve the corresponding unencumbered deliverable shares;
reconcile other working calls before counting the same shares again. Obtain the
actual contract multiplier rather than assuming every contract delivers 100.
Assignment and exercise can change both shares and cash. Reconcile positions,
fills, pending orders and share acquisition cost after settlement before choosing
another contract. Retain each existing contract's original identity when future
entry policy changes symbol. Premium income alone is not total return: include
remaining option liabilities, share value and costs.

When the strategy uses attention monitoring, retain a signed
`position_quantity` change condition for each held option: use `comparison="ne"`
against its verified current signed quantity. Equality to zero detects only a
full close, missing partial fills or other quantity changes. Retain status conditions
for its working orders, including filled, partially filled, rejected and canceled
transitions. Reconcile a working order's actual fill before establishing entry-cost
profit monitoring; an acknowledgement supplies neither quantity nor cost basis.
Assignment, exercise or partial closing can change
exposure without a price boundary crossing. Reconcile the resulting shares,
cash and remaining quantity before replacing the cash basis or removing watches.

## Position truth

Treat current signed quantities as authoritative. Positive is long, negative
is short, zero is flat.

For one held option contract identity, inspect its exact quote and bid/ask
spread, then use `orders_submit_order` with `buy_to_close` for a short position
or `sell_to_close` for a long position. Use a positive quantity no greater than
the verified remaining holding, a price-bounded limit and the intended time in
force. A single-contract close does not use multi-leg pricing or submission.
Reconcile any pending close before replacing it.

For an atomic close of two or more option legs, pass `action='close'` to
`options_calculate_multileg_price` and `orders_submit_multileg`. List only
`symbol`, `expiration`, `strike`, and `right` for each held leg. LumiBot derives
each closing side from the current signed position (long becomes
`sell_to_close`, short becomes `buy_to_close`) and defaults the quantity to the
full held amount. Never write closing sides yourself. Pass `quantity` only for
a per-unit price check or an intended partial close.

Keep that contract-only shape for the final submission too. Do not copy
`side` or `closing_side` fields from position reads, pricing results, or an
opening-order payload into closing `legs_json`. For example, a closing leg is
`{"symbol":"XYZ","expiration":"2027-01-15","strike":100,"right":"put"}`;
the position determines its closing action.

- Never multiply a cleanup quantity or repeat a close without rereading positions.
- Do not report flatness until every relevant signed quantity is zero.

If a close does not produce flat positions, inspect its exact status and open
orders. Do not switch tools, change quantities, or submit another close until
the prior order's terminal state and the current signed positions prove what
remains.

After a terminal unfilled entry, diagnose its returned status and refresh the
selected contracts' quote activity and available source-bar times with
`market_quotes`. Compare eligible alternatives or the limit within the mandate;
do not simply repeat an unchanged unfilled package each day. A usable historical
last-trade pricing anchor does not prove all legs have execution data in the
same minute. Preserve the requested expiration/structure and report a remaining
data or liquidity limitation rather than manufacturing a fill.

## Pricing truth

- Keep exact asset identity and source time separate from a quote object's
  construction time. A failed read for another asset type does not invalidate
  a later successful quote for the correct asset. Compare available source
  quote/bar times with the decision cutoff; never replace them with wall time.
- Before ordering, evaluate each final exact leg; candidate price previews may
  precede that final evidence check.
- Bind every quote, Greek assessment and preview to its exact contract
  identities, sides and quantities.
  Before citing a candidate price, compare that complete returned leg list with
  the candidate you are describing. An extra leg or a changed strike/ratio is a
  different package; correct and reprice it rather than attaching its result to
  another candidate. This also applies to research comparisons and rejected
  candidates, not only the eventual submitted package.
  After comparing alternatives, finish the selected package's evidence with
  its exact-leg Greek result and price preview. Report that result's returned
  aggregate and multiplier-adjusted values, not the latest result for another
  candidate. If the selected leg list changes, refresh the selected view before
  ordering or describing its exposure.
  Use that final preview's per-leg `pricing.spread_pct` when citing selected-leg
  spread quality. It is a fraction of midpoint, not a percentage number; never
  copy a different candidate's quote-quality table onto the selected package.
- In a historical minute-bar simulation, use only the pricing evidence the data
  source actually provides. `options_evaluate_market` reports whether that
  evidence is `usable_for_limit_pricing` and identifies a last-trade/bar proxy
  when historical bid/ask is unavailable. If that approximation is permitted by
  the simulation mandate, it can support approximate lifecycle testing; disclose
  it and do not claim executable historical liquidity. Live execution needs
  actual quotes. Never fetch historical ticks or invent bid/ask to fill a gap.
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
- Include configured or explicitly supplied commission estimates in the risk
  comparison. A per-contract/per-side estimate scales with every absolute leg
  quantity on entry and expected closing, including ratio legs. Identify it as
  an assumption rather than the broker's actual fee schedule when unverified.
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
comparison against the exposure you actually hold. Prefer `basket_pnl` for a
currency profit/loss review. Supply the actual signed net entry cash (positive
received, negative paid), signed quantities and multipliers; the monitor adds
that cash to the current marked holdings. A chosen loss review of L then uses
`comparison='lte'`, `value_json=-L`, without converting loss into a holdings-value
boundary yourself. Include fees in the basis if your policy requires them, and
reconcile the remaining quantities and cash basis after partial execution.
The agent still chooses the loss allowance and every trade. Distinguish that
chosen allowance from the selected package's maximum possible loss.

Inspect `registration_observation` after saving a condition. It reports the
current observation and whether the condition is already true, without firing
or acknowledging a warning. Nonzero quantity on an intentionally held leg is
expected exposure, not an unexpected post-close residual; replace a mistakenly
registered residual warning before finishing the decision. A genuine current
breach can legitimately require attention. Unavailable feedback does not remove
the watch or establish that its condition is false.

For existing `basket_value` conditions, this metric is the signed
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
Use the selected package's modeled loss, rather than a larger portfolio or
spending cap, when describing a partial-loss review. A package with $50 maximum
expiration loss does not have an earlier partial-loss stop at $200. Either
choose a coherent earlier review or explicitly accept its full defined risk
with useful time/thesis reviews, if the mandate permits that choice. An alert
beyond an expiration-payoff bound may instead monitor anomalous marks or
liquidity; explain that purpose rather than calling it partial-loss protection.
Marks request attention; refresh executable prices before ordering.

While an exit is working, retain the chosen protection for still-held or
partially filled exposure. An order-status watch observes execution progress;
it does not replace a price/value risk condition. Do not remove exposure-risk
conditions merely because a close was submitted. When replacing protection,
verify that its replacement covers the stated policy before retiring the old
condition. This preserves the chosen policy; it does not impose an earlier
stop or require a duplicate monitor where verified native protection suffices.

A close-completion deadline is not an order-submission time. Plan the first
management review early enough to allow pricing, execution and recovery before
that deadline. A pending acknowledgment does not satisfy it. Avoiding a duplicate
exit does not mean leaving an unexecutable limit untouched: refresh exact-leg
prices and manage the existing package with supported modification, or cancel,
confirm terminal status, reread residual holdings and replace only what remains.
An order list can show a package and its child legs separately. Match each
`parent_identifier` and the package's nested leg identifiers before calling an
order independent or competing. Manage the existing package as a unit; a child
listed separately is not evidence of another exit.
Keep the user's price/risk limits and any explicit patient-order policy. Market
execution requires permission under the active strategy policy. Watch meaningful
partial/terminal status changes and risk boundaries, rather than the already
known `new` acknowledgment. A time review needs a concrete intervention or
deadline; repeatedly rearming five-minute checks to restate unchanged status
spends inference without recovering the order. Backtest waits advance simulation
time; allow for their minute-bar rounding before a completion deadline.

After a confirmed close, inspect your attention conditions and remove those
belonging to the completed operation. Do this before a new entry reuses the same
contracts. A zero-quantity residual warning would otherwise flag the intended
new holding. Give a new operation its own identity and current watches; changing
the identity does not remove earlier conditions automatically.
After completing a scheduled setup or management review, also remove its
consumed one-off time condition, even if the new position remains open. Its
acknowledged warning stays quiet, but retaining it does not schedule another
review. Keep current risk conditions and the actual next review; if you updated
the same watch to a new future time, preserve that replacement.

Use meaningful events and sparse reviews by default. An unchanged pricing-tool
defect does not become new research merely because another minute passes.
An unchanged optional-information gap likewise does not justify recreating a
daily research alert. For an unfilled setup, choose the next genuine entry
opportunity or expected evidence change; preserve urgent held-exposure reviews.
Report the exact conflict and choose a useful new-data or recovery review while
retaining any held-position risk observations. Respect an explicitly requested
frequent-AI policy rather than imposing a universal call limit.

Separate new-setup research from management. A new setup needs the relevant
dated research and the trader; urgent management may reuse still-relevant
research and refresh it when necessary. With a team attention dispatcher,
leave the appropriate `review_scope` on the next review; it supplies the dated
answers without nested synchronous agent calls. Schedule the next eligible
opportunity after closure or a no-entry decision, using the mandate's cadence.
Do not confuse a healthy holding's monthly review with pending-order recovery:
watch the exact returned parent/children for terminal and partial-fill changes,
reconcile residual exposure and obligations, then choose a suitable recovery
review. A working close prevents another close for that operation, while
unrelated working orders do not suspend its risk monitoring. Combine related
events in one decision and do not create a frequent AI heartbeat for an unchanged
warning. Account for model/tool continuations when measuring actual usage.

For independent orders, `orders_submit_orders` exposes the existing native batch
API. It is not atomic: reconcile every result and any uncertain partial outcome
before retrying. Related option legs instead use `orders_submit_multileg` so
each spread, condor, butterfly or ratio is submitted as a package. The trader
chooses every instrument, quantity, price and action; these tools carry out that
decision without selecting a strategy.

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
Use at least 200 characters. Keep it focused on the choice, verified net price
and risk, sizing, material obligations and management. Explain the thesis and
what would invalidate it without reproducing quote or Greek tables already
retained in tool results. Repeat only decision-driving numbers, with their exact
observed labels and units; do not rename a last price or midpoint as a bid/ask.
Provide a detailed audit when requested rather than making every order one.
State missing evidence honestly. This is a decision summary, not hidden internal
reasoning, and must contain no credentials or account identifiers. The order
service commits it before execution. Report only observed order status and fills;
reconcile an uncertain outcome before trying again.

For a single held option, `options_check_spread_profit` can read marked P&L
from one original-direction leg; no second leg or package price is needed.
