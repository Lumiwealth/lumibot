# MetaMask council review

Incremental verification of the native broker and PR 1262.

Last Updated: 2026-11-05

Status: In progress; identity and execution approved, cash activity in verification

Audience: LumiBot maintainers and reviewers

## Overview

The review targets the dedicated MetaMask Agent Wallet broker, default-USDC
Hyperliquid perpetuals and the separately experimental exact-input swap path.
It does not authorize funded trading, provider-policy changes, publication or
deployment. User-selected council is GPT-6.1 Terra, GPT-6.1 Sol and GPT-6 Astra,
each at high reasoning effort. Reviewers work read-only; one integrator applies
accepted fixes. Approval applies to the named checkpoint, not pending work.

## Baseline and merge

PR: https://github.com/Lumiwealth/lumibot/pull/1262

Original PR head: `b37625fe9865ab7d65da71d9998c1972be509898`.
Current `dev` was merged locally in `0996002ae`, retaining newer swap recovery,
shared order semantics and unrelated release work. All 483 offline MetaMask
tests passed after resolution. No unresolved GitHub review threads were present.

## Checkpoint 1: market and wallet asset identity

- Resolve uppercase strategy symbols to unique canonical venue names for market
  metadata, marks, candles and symbol-filtered funding. DEX names preserve their
  native case. Ambiguous identities fail explicitly.
- Persist canonical symbols for new orders and retain original operation/client
  IDs for case-only retries. Other changed intent still raises `ID_CONFLICT`.
- Use explicit chain-specific native price identities instead of assuming every
  EVM native token is ETH. Unknown native identities refuse valuation.
- Reject duplicate chain/contract registrations, including aliases/conflicting
  decimals, and reject registry assets outside approved chains.
- Preserve global Asset casing, wallet custody, network separation, provider
  policy, no-replay behavior and unsupported HIP3 execution boundaries.

Evidence:

- First affected engine/venue/model/EVM/client/broker selection: 348 passed.
- Final changed-family selection, including DEX/funding identity: 194 passed.
- Ruff check, Ruff format and whitespace checks passed.
- Live public `KPEPE` reads resolved to `kPEPE` on testnet and mainnet with
  positive marks. Closed candles matched independent direct canonical requests:
  zero recent testnet rows, three mainnet rows. No account credentials or
  mutations were involved. Empty testnet history was preserved truthfully.
- Sphinx HTML build exited successfully; changed MetaMask page had no reported
  errors and rendered new guidance. Other pages retain diagnostics in unchanged
  Tradier docstrings and existing documentation sources. No claim that the
  entire documentation build is warning-free.

Negative controls cover ambiguous case-folded market names, duplicate token
aliases, conflicting decimals, unapproved chains, unknown native price identities
and changed content under an existing operation ID. Supported unique assets on
different chains remain distinct.

Native price identity sources:

- https://raw.githubusercontent.com/MetaMask/agent-skills/main/skills/metamask-agent-wallet/references/market-data.md
- https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/eip155-56.json
- https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/eip155-43114.json

## Checkpoint 2: allowance prerequisites and standard order fees

The frozen trade quote deadline no longer expires prerequisite bounded ERC20
allowances. Each unsubmitted allowance remains subject to the unchanged reviewed
grant, current provider policy, frozen transaction/gas limits and original total
fee reservation. Trade execution retains both quote-expiry checks and refreshes
after all allowances settle. Restart never releases a new unsubmitted stage.

The actual perpetual fee ledger now populates cumulative USDC `Order.trade_cost`
before standard fill events and on restored snapshots. Rebates remain negative.
Repeated or fee-only observations do not replay fill callbacks. Unsupported
currencies keep raw evidence and an unavailable standard cost; missing fee
fields do not erase known cost. Invalid fee evidence is rejected before the
adapter emits a new execution. Broker-reported cash is not debited again.

New regressions reproduce a zero-reset plus exact-input approval across two
20-minute finality waits, then a fresh trade quote, pending managed approval,
restart and final output/fee reconciliation. Negative controls cover revocation,
changed authority, higher total fees and restart before the next allowance.
Round 3 approved unanimously. Astra's nonblocking coverage suggestion was
accepted: explicit grant-expiry and provider-policy mutation cases both passed
without source changes, retaining the compatible 198-test evidence above.
The existing restart example also tests an unsettled prior exit after initialize
clears its pending ID, addressing the previous PR review nitpick without another
submission. Five new execution/fee cases failed before the fixes; all ten initial
checkpoint cases passed afterward. Final EVM, broker, example and native-fee
selection passed 198 tests. Ruff, formatting and whitespace checks passed.
The checks use real adapter/event paths with synthetic provider/RPC transports;
they do not qualify funded managed-wallet execution. No funded activity occurred.

## Checkpoint 3: cash activity and truthful coverage

The normal broker now supplies existing `CashEvent` and `CashEventSync`
contracts. It reads both ledger endpoints only after managed-wallet identity
verification. Receipts identify the network, wallet, account boundary and exact
half-open UTC window, defaulting to 60 days. Inclusive page cursors preserve
timestamp ties; conflicting identities, saturated pages and resource limits
refuse complete coverage. IDs distinguish funding coins and transaction legs.

Signed default-DEX USDC funding, deposits, zero-fee withdrawals and unambiguous
zero-fee USDC account/subaccount transfers normalize into standard events.
Known events survive unresolved categories; account-mode-dependent transfers,
fee-bearing capital movements, HIP3/non-USDC and unknown types remain partial.
No net/gross fee split or historical account boundary is invented. These are
explicit classification limits, not claims of fully qualified capital history.
Provider/identity failures and unexhausted pagination are unavailable. Spot
history is unsupported. A checked empty window is distinct from unread history.
The list-only API refuses incomplete results. Normal Strategy collection uses
receipts and never reapplies cash already reflected in account balances.

Evidence: the missing-method regression failed before implementation. The first
affected activity/venue/client/standard-cash selection passed 96 tests. Final
activity selection, after added foreign-asset/fee controls and latest-event
limit handling, passed 24 tests. Ruff/format and whitespace checks passed.
Public real HTTP reads on both testnet and mainnet returned zero funding and
non-funding records for an unfunded public address over a one-hour window.
This proves request/empty-response interoperability only. No managed identity,
authenticated provider capability or funded cash semantics were qualified.

Primary contracts inspected:

- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Official Python SDK non-funding/funding test cassettes, with only schema
  observations retained and synthetic identities used in regression fixtures.

Mobbin's Base Activity flow was consulted before implementation. Its empty
state and typed sent/received activity support the chosen distinction between
checked empty history and incomplete/unavailable reads; no UI was added.
https://mobbin.com/flows/07e1b5a9-a7a8-4b52-85ad-20ad8b1fd30e?tab=screens

## Remaining checkpoints

1. Council approval or correction of normalized cash activity.
2. Installation guidance, final artifact/PR evidence, full affected gate and final
   whole-PR council approval.

Fresh authenticated testnet, controlled maker partial cancellation, funded mainnet
and funded native-swap qualification remain separate gates. Five connected tests
previously skipped because private test configuration was not supplied. Retained
historical funded claims must not be relabeled as fresh evidence.

## Council results

Round 1, identity checkpoint at `a532366db`: GPT-6.1 Terra approved, 8.5/10,
with no P1/P2 findings. Terra checked canonical resolution, retry identity,
registry validation, tests and chain metadata. Its approval retains the stated
native-provider pricing and funded-execution limits. Sol completed its original
review before replacement, approving 9/10 with no P1/P2 findings. Sol independently
checked public mainnet native prices and also flagged testnet provenance as a
nonblocking limitation: the public provider omitted testnet prices. The integrator
accepted Astra's registry-consistency correction without claiming that registry
identity proves provider price availability. Round 1 did not receive unanimous
approval.

Astra returned 8/10, request changes, with one P2: the testnet entries incorrectly
inherited mainnet coin types despite the cited chain registry declaring
`slip44:1`. The integrator verified all seven listed testnets against their
registry documents, corrected the entries and added request/response-boundary
coverage through `EvmWallet.price()`. Mainnet values remain unchanged. The new
tests failed first on all seven identities and the Amoy price lookup before the
correction. Empty provider price responses remain explicit errors.
The corrected model/EVM selection passed 87 tests; lint and formatting passed.
Fresh council review of the corrected checkpoint follows with all three models.
Cancellation of the superseded Sol run returned completed; its result and score
are retained.

Round 2 at `5d7a14bdf` approved the corrected identity checkpoint unanimously.
All reviewers reconciled the registry correction with the distinct limitation
that provider testnet pricing remains unproven. No new P1/P2 findings.

Round 3 at `351fb8a4d` approved execution/fees unanimously, without P1/P2 findings.
Each reviewer inspected the connected authority, journal, fee, callback and cash
paths. They reviewed reported test evidence without rerunning it; synthetic
transport results do not prove authenticated or funded execution.

| Delegate | Round 1, identity at a532366db | Round 2, identity at 5d7a14bdf | Round 3, execution at 351fb8a4d |
| --- | --- | --- | --- |
| GPT-6.1 Terra | 8.5/10, approve | 8.5/10, approve | 8.5/10, approve |
| GPT-6.1 Sol | 9/10, approve | 9/10, approve | 9/10, approve |
| GPT-6 Astra | 8/10, request changes | 9/10, approve | 9/10, approve |

Nothing has been pushed by this review task yet.
