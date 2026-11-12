# AI wheel with inexpensive monitoring

SPY is the default; change `parameters["symbol"]` for future entries. One researcher
supplies compact dated evidence. Exactly one trader selects contracts, sizes,
places orders and manages risk. The 94-line Python file wires lifecycle and the
shared attention runtime; it does not calculate signals or place orders.

The policy sells cash-secured puts, accepts assignment, sells covered calls
against delivered shares, accepts call-away and considers puts again. Defaults:
30–45 calendar DTE, absolute delta near 0.25, at most one short contract and put
collateral within 90% of available cash including working obligations and costs.
Existing exposure keeps its actual symbol when the future-entry symbol changes.

Copy `main.py`, `rules.json` and `requirements.txt` together. This candidate uses
the next-version single-option repairs; a source branch is not an installed package.
Use a compatible installed runtime, a supported local
[Login with ChatGPT](../../../docsrc/agents_chatgpt.rst) connection, and an
options-capable broker/data connection. Both agents explicitly use
`chatgpt/gpt-6-luna`, Medium reasoning and Standard/default tier. No paid API-key
fallback is configured. A local connection does not establish hosted funding.

The first setup is in the final 15 minutes of the actual broker session. Free
capacity retains one daily opportunity. While holding, `attention.poll()` observes
saved conditions without inference and `dispatch_team()` groups relevant events.
Setup refreshes research; management can reuse its dated answer. The trader
establishes profit, quantity, order, routine and pre-expiry reviews. Qualitative
ownership risk is reconsidered at the routine review, within seven calendar days.
Unchanged expiration risk uses position/order watches and at most one settlement
follow-up, rather than repeated intraday clock reviews. Native state persistence
supports restart recovery; startup must reconcile the actual broker account.

The favorable review threshold is 90% of actual **gross opening premium**, using
signed marked value before commissions. It is a review condition, not an automatic
close or net-profit claim. The trader checks executable closing quotes, costs and
net proceeds before choosing an exit. Ordinary bounded DAY limits are the default;
smart-limit repricing is not enabled. Exact bid/ask and spread matter when available.

Local October 13–27, 2026 diagnostics started with $100,000: AI wheel and regular
wheel each ended at $100,069.35, versus $99,057.92 buy-and-hold. Wheel cash was
$100,612.35 less a $543 open-option liability. Three research and six trading
decisions completed in about 348 seconds. This short sample establishes no AI
advantage or long-term performance. Fees were $0.65 per option contract per side;
stock fees, additional slippage and cash interest were zero. Historical minute
trade bars approximate execution; historical executable NBBO was unavailable.
Historical chains are not strictly point-in-time, and comparator selection rules
are similar rather than identical to AI judgment.

Controlled native scenarios separately exercised put assignment, delivered shares,
covered calls, call-away, re-entry and checkpoint restoration. The retained prefix
and continuation are not an uninterrupted historical study. Synthetic prices do
not establish returns, broker liquidity or live fills. Premium is not portfolio
profit: share values, open liabilities, costs, assignment and capped upside matter.
The diagnostic policy review found delayed arming of a required profit watch;
that recorded failure is preserved for the shared monitoring repair. These
records do not establish a hosted deployment or Marketplace installation.
