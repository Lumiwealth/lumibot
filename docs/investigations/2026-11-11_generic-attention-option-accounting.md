# Persistent attention and signed option accounting

## Next-version pricing and unit clarification

The ordinary single-option limit path is now explicit, alongside the unchanged
two-or-more-leg atomic preview contract. Quantity-change monitoring compares
against the verified signed quantity, rather than observing only flattening.
Collateral explanations compare obligations, policy capacity and cash in the
same currency. These are existing contracts, not new investment defaults.

P&L registration feedback now labels currency units and reports the chosen
threshold as a percentage of absolute opening cash. It does not rewrite the
threshold, select a policy, dispatch an agent or place an order. This diagnostic
remains available when current price observations are missing; a zero cash basis
has no defined percentage. Three missing-feedback regressions failed before
repair. Six focused native checks then passed, including currency observation,
zero/negative bases and read-only feedback. No model was needed for those checks.

Five single-option actor/judge trials and one unequal-ratio atomic regression
passed with subscription funding, default tier and the maintained Luna profiles.
The component scope, native receipt and explicit compatibility review are in
`ci/agent-evals/single-option-guidance-*.json`. They retain original inference
source and timestamps. Later P&L diagnostics cannot affect these six actors,
which never called attention tools; no repeat inference is claimed. This is
local component proof, not installed-package or live broker qualification.
Shared system-prompt growth is zero. Concise existing-mechanics guidance grew
because ambiguous single-leg routing and units caused observed tool/monitor
failures. No security, credential or private infrastructure implementation is added.

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
fills remain distinct evidence. The existing generic attention-registration case
is imported with its original five-pass requirement. No CI behavior, package
version or release tag is changed.

A longer controlled real-model management run exposed two additional generic
failures. The marked-profit read rejected a single leg, despite the underlying
math supporting it; the profit binding now accepts one or more exposure legs,
while multi-leg execution and closing still require their original package
shape. A failed decision could also lose its claimed event if an agent removed
the watch before another tool failed. Both delivery APIs now restore that exact
pending evidence on failure, without rolling back order effects or newer watches.
Three new focused cases reproduced these failures before repair and passed
afterward. Existing package-close and recovery controls remain enforced.

The combined release carries the maintained author-written receipt verification.
Its newer admission contracts are preserved unchanged; this task adds no selector.

Additional shared guidance requires a final mandatory-policy check against the
exact selected contract and decision time. A model must not submit an order
whose own reason acknowledges a failed requirement. Working-order monitoring
covers partial fills and terminal failures as well as fills and cancellation;
an acknowledgement is not entry cost or filled exposure. These instructions
apply to ordinary single options and packages without embedding any strategy's
policy values. The separately labeled example contains its own entry policy.

The final narrow model batch names only attention registration and signed credit
spread closing. Both passed five actor/judge trials. This preserves package
closing alongside the ordinary single-option close and proves actual saved
conditions rather than promises. Full release receipt creation, including the
existing judge-calibration requirement, remains separate from these case passes.

## Combined release reconciliation

The change now builds on the open version/4.6.52 carrier. The carrier owns the
attention implementation, optional research completion, package lifecycle,
quote/order improvements, and author-written qualification. Its implementation
is reused rather than copied into a second runtime. An overlapping attention
research-error draft was retired in favor of the carrier's Manager classification.
An optional disclosed research gap can complete; an incomplete trader or explicit
decision-critical research failure remains incomplete.

The remaining runtime delta repairs signed marked profit, accepts a single held
option in the profit-read tool, and restores the claimed attention packet after
a decision removes a watch and then fails. Generic coverage/settlement and
single-option closing guidance accompany the compact separately labeled example.
No security, credential handling, BotSpot infrastructure or private protocol is
added. These capabilities apply to ordinary strategies independent of this example.

Local code proof after integration: 21 explicitly selected tests passed in 4.68s
(signed profit, single close, package-close compatibility, pending packet recovery,
physical settlement and optional research completion). Zero new model eval attempts
were used for these plain runtime fixes. Earlier model observations retain their
original source and funding provenance; they are not relabeled as current-pool,
hosted, or broker proof. The earlier separate-login model batch is not qualification
through the required three-account pool.

This task adds zero shared system-prompt bytes relative to the carrier. The options
skill grows from 39,261 bytes / 563 lines to 41,653 bytes / 601 lines. The additions
cover final policy checking, collateral/share coverage, settlement, single-contract
closing and signed-profit use. The carrier's newer concise-account-read guidance
and all other qualified options instructions are retained. This note records local
source proof only; no package publication or installed runtime is asserted.
