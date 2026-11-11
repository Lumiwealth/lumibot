# Persistent attention and signed option accounting

Date: 2026-11-11
Status: source qualification; not a package release or hosted-runtime claim.

The generic contract benefits any event-driven strategy independently of an
example: persist agent-selected read-only conditions, group changes for one
decision owner, suppress unchanged conditions, retain pending evidence across
restart, and reuse dated research for management decisions. The monitor selects
no instruments, trade quantities, prices, policy or orders. This diff adds no
security, credential, sandbox, hosting or private infrastructure implementation.

The attention module and tool definitions are adopted from source
`b27433362e9dafe84d0e2981449d11a0561e0884`; manager integration adds only monitor
construction under its existing shared-state lock. Newer canonical agent detail
spooling, outcomes, providers and other runtime behavior are preserved.

`OptionsHelper.check_spread_profit` now nets buy and sell directions and divides
marked P&L by the absolute signed opening cost. It uses each asset multiplier by
default and retains an explicit override. Zero basis has no defined percentage.
For example, an initial cost of -395 and signed current value of -179.6875 have
54.5095% marked profit, rather than the previous -145.4905%. This is marked value,
not an executable liquidation quote. Fees must be reflected in the cash basis.
The agent tool preserves those units. Partial exposure requires a reconciled
remaining-position basis, not the original basis blindly reused.

Shared options guidance distinguishes one-contract-identity closes through the
single-order tool from atomic multi-leg closes. Coverage, deliverable multipliers,
settlement and quantity/order monitoring are generic operating knowledge. Strategy
entry windows, target delta, budgets and discretionary exits remain policy.

Canonical 4.6.51 already includes expiry processing on non-PANDAS backtesting
sources; this patch preserves that repair. A new neutral controlled recorded
market test verifies short-put assignment into shares, covered-call delivery back
to cash, worthless expiration and re-entry. It checks the ending 100,500 cash and
100,400 equity separately because one remaining short option is worth -100.
Those values are synthetic test accounting, never historical performance.

Focused local evidence: 31 distinct unit/contract cases passed, including two
compatible long-profit/missing-mark controls retained from the initial run.
The five reproduced arithmetic failures passed after repair. The 28-case monitor,
accounting, quote and closing check initially had two new fixture assertions
incorrectly reading an existing `(order, options)` tuple; both passed after those
assertions were corrected. The delivery test initially lacked quote columns and
passed after the controlled fixture supplied its intended executable prices.
Four binding/replay/close cases also passed under the pinned evaluation SDK tuple.
No full suite or catalog was launched locally.

One real Luna subscription actor/judge compatibility pilot successfully closed a
single held short option with exact quote, correct side/quantity and observed
flatness even under the old wording. It does not establish a new failing model
regression; coverage-only scenarios remain outside the production eval catalog.
Source tests, model decisions, published packages, installed runtimes and broker
fills remain distinct evidence. No new production model cases or CI behavior are
introduced here, and no package version or release tag is changed.

A longer controlled real-model management run exposed two additional generic
failures. The marked-profit read rejected a single leg, despite the underlying
math supporting it; the profit binding now accepts one or more exposure legs,
while multi-leg execution and closing still require their original package
shape. A failed decision could also lose its claimed event if an agent removed
the watch before another tool failed. Both delivery APIs now restore that exact
pending evidence on failure, without rolling back order effects or newer watches.
Three new focused cases reproduced these failures before repair and passed
afterward. Existing package-close and recovery controls remain enforced.

An ordinary unit test also attempted to verify the prior release author's
source-bound model receipt on every PR. It has been removed from unit execution:
that task belongs to the maintained release-workflow verification command.
Explicit scope/pass/source checks remain in the verifier and its isolated unit
contracts. This changes no production model assertion and adds no selector.

Additional shared guidance requires a final mandatory-policy check against the
exact selected contract and decision time. A model must not submit an order
whose own reason acknowledges a failed requirement. Working-order monitoring
covers partial fills and terminal failures as well as fills and cancellation;
an acknowledgement is not entry cost or filled exposure. These instructions
apply to ordinary single options and packages without embedding any strategy's
policy values. The separately labeled example contains its own entry policy.
