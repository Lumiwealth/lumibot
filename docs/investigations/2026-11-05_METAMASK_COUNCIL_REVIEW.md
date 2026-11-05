# MetaMask council review

Incremental verification of the native broker and PR 1262.

Last Updated: 2026-11-05

Status: In progress; identity checkpoint awaiting council review

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

## Remaining checkpoints

1. Council approval or correction of identity checkpoint.
2. Swap prerequisite-approval timing and standard execution-fee projection,
   including existing restart/callback behavior and outstanding review nitpick.
3. Normalized funding/non-funding cash activity and truthful completeness.
4. Installation guidance, final artifact/PR evidence, full affected gate and final
   whole-PR council approval.

Fresh authenticated testnet, controlled maker partial cancellation, funded mainnet
and funded native-swap qualification remain separate gates. Five connected tests
previously skipped because private test configuration was not supplied. Retained
historical funded claims must not be relabeled as fresh evidence.

## Council results

Round 1, identity checkpoint at `a532366db`: GPT-6.1 Terra approved, 8.5/10,
with no P1/P2 findings. Terra checked canonical resolution, retry identity,
registry validation, tests and chain metadata. Its approval retains the stated
native-provider pricing and funded-execution limits. Sol's original review is
pending; the checkpoint is not yet approved by the complete council.

Astra returned 8/10, request changes, with one P2: the testnet entries incorrectly
inherited mainnet coin types despite the cited chain registry declaring
`slip44:1`. The integrator verified all seven listed testnets against their
registry documents, corrected the entries and added request/response-boundary
coverage through `EvmWallet.price()`. Mainnet values remain unchanged. The new
tests failed first on all seven identities and the Amoy price lookup before the
correction. Empty provider price responses remain explicit errors.
The corrected model/EVM selection passed 87 tests; lint and formatting passed.
Fresh council review of the corrected checkpoint follows once the original Sol
review has returned, so its findings can be reconciled in the same next round.

Nothing has been pushed by this review task yet.
