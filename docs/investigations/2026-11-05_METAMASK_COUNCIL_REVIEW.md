# MetaMask council review

Incremental verification of the native broker and PR 1262.

Last Updated: 2026-11-05

Status: In progress; recovery-reporting correction verified, fresh council pending

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

The source installation guide now selects the actual PR branch and prints its
head, instead of pointing at `dev` while describing unmerged fixes. Published
PyPI metadata currently reports 4.6.47 with the MetaMask extra, but that does not
contain these unmerged changes. Both guides distinguish historical funded and
installed-wheel receipts from this candidate's unqualified execution paths.

## Final candidate evidence

Corrected source: `9ec0af245b74749461d081aa3aa56fbfa6f9e5f0`.
Latest fetched `origin/dev` remains `3540c7f6e2dcfd2e30ef2a253b66bb0fc076df3b`
and is already an ancestor. Working tree was clean after the correction commit.
Remote PR still points at its earlier head; local merge resolution and new
fixes have not been pushed. No unresolved GitHub review threads were present.

- Final affected gate: `tests/test_metamask*.py`, standard cash-event tests,
  dotenv isolation and package-manifest checks: **590 passed, 5 skipped**.
  All five connected skips report that private native testnet configuration is
  not supplied. Tests use Python 3.12; old remote Python 3.10 CI is not fresh
  evidence for this candidate.
- Complete MetaMask workflow Ruff check and format passed, 34 files formatted.
  Dependency check found all 343 installed packages compatible. Whitespace clean.
- Built wheel from an isolated archive of the corrected source, installed it
  into a task-local target with existing compatible dependencies, and confirmed
  that all **280 packaged Python files** exactly match the source archive.
  Optional MetaMask metadata includes SDK 0.24.0 and the documented dependencies.
  This was not a fresh dependency resolution or public PyPI artifact install.
- Wheel SHA256:
  `a97f708014a410c8bc040b538bfc8ab9da6ef1e7ad9e1c7412884c29390a2568`.
- Isolated installed-wheel imports resolved to that target. With JavaScript
  absent from PATH, real public testnet and mainnet requests each returned a
  positive canonical mixed-case mark and three valid closed BTC minute candles.
  These reads created no provider client or journal. Installed broker history
  also distinguished missing identity setup (`unavailable`) from unsupported
  onchain history (`unsupported`). No authenticated or funded proof is inferred.
- Full Sphinx build exited successfully. Its new heading-underline diagnostic
  was corrected, then incremental verification passed with no MetaMask warning
  or error. One unchanged toctree warning remained in the incremental build;
  full build logs also retain unrelated existing Tradier docstring diagnostics.
  Rendered HTML contains candidate installation, cash-coverage guidance and the
  historical-qualification notice. No claim of a globally warning-free build.

The final review is of the complete integration and corrected cash checkpoint.
No further suite expansion is required absent new code changes or findings.

## Round-5 correction and verification

Full integration review at `387d29149` returned Terra 8.5/10 approve, Sol
8.5/10 request changes and Astra 8/10 request changes. All approved the corrected
cash checkpoint. Sol and Astra independently reproduced one shared P2: native
qualification could truncate its config, session, journal or prior receipt via
`--evidence` before identity verification. The integrator confirmed all these
collisions, plus the legacy-session collision, using disposable synthetic files.
Astra reproduced a second P2: $269.50 of existing $300 rolling reservations left
room for test order and entry but not closing. Terra's lesser issue was also
confirmed: an unsupported row before a fractional-millisecond `since` could
mark the requested empty window partial.

Corrections:

- Filter exact cash-window timestamps before classification; an unsupported row
  inside the requested window still makes coverage partial.
- Reuse one evidence preflight across all three qualification entrypoints. It
  rejects existing files, dangling symlinks, config/session/journal aliases and
  future recovery sidecars/locks, then reserves a fresh private file exclusively.
  A concurrent creator cannot be overwritten. Native snapshots reuse the
  existing flushed, fsynced atomic writer; receipt schema remains unchanged.
- Reuse reviewed authority and rolling journal-budget checks before native
  signing, requiring $75 for the resting test, entry and close under existing
  $25 caps. Existing canceled reservations and fee-adjusted costs count; no
  authority, expiry or spending history is reset. The same six-minute minimum
  grant lifetime as the other harnesses is retained.
- Add native qualification script to both CI path triggers and lint/format
  coverage. Document the guard behavior and repository-module invocation.

Fifteen new regressions failed before correction: exact-window classification,
13 unsafe evidence destinations and the real-journal budget case. Final affected
cash/native qualification/partial qualification/example selection passed **123
tests**. This includes no-write controls across all three entrypoints, private
readable snapshots, concurrent evidence creation, and the exact $75 threshold.
Full workflow Ruff and format passed, now 35 files; whitespace clean. Native
module `--help` ran without wallet access. Compatible 590-pass evidence above
is retained for unchanged paths; no second broad suite was launched.

Corrected source commit: `708acfa9fd99f01a5f48738e5db4a4047c6dab8e`.
Rebuilt wheel from a fresh isolated archive; all 280 packaged Python files match
that source. Wheel SHA256:
`e778c645d10a88fdfb8af17ec7e2022dc5b719c670632e97aca9fe3ed9977d7e`.
Task-local installation retained compatible existing dependencies. Its isolated
runtime proved exact-window filtering, case-only transfer deduplication and
in-window unknown classification through real broker/client/venue code with
synthetic identity/HTTP transports. JavaScript was absent from PATH. This is
installed-artifact continuity, not authenticated or funded proof. Prior public
market reads remain compatible because those paths are unchanged.

Updated public guide built successfully with no MetaMask diagnostics and one
unchanged toctree warning. Funded/authenticated and remote CI gates remain open.

## Round-6 recovery-reporting correction

At `3b45613ed`, Terra approved 8.5/10 and Sol approved 9/10. Sol independently
passed 24 focused checks. Astra requested changes at 8.5/10 with one P2:
persistent receipt I/O failure after an entry could raise again in recovery
logging, preventing residual cleanup. Shared cleanup could stop before its
close; partial-run error logging could skip subsequent wallets. Astra also
identified a lesser missing future legacy `session.owner.sqlite` fence and its
rollback journal in destination protection. No new supported broker-execution
P1/P2 was reported.

Five regressions failed first: two migration-fence destinations, native entry
followed by persistent receipt failure, and shared cleanup through both partial
and strategy callers. Recovery reporting is now best-effort with failures
retained for the final unsuccessful verdict. Native recovery continues past
failed snapshots and records `complete=False`. Shared cleanup still requires
exact live ownership and successful journal-backed close/flat verification,
then raises when reporting failed. Partial-run recovery records errors without
skipping workers, later wallets or resource closure. Legacy fence paths are
protected before broker construction.

Additional controls exercise the actual partial-run recovery loop and Strategy
harness's settlement/cleanup/finally path with synthetic scheduling/provider
transports. They verify that first-wallet or reporting failure cannot skip the
second cleanup, and that producers settle before exact reduce-only closure and
client release. Separate negative controls retain ownership-mismatch refusal
and mandatory journal-write failure. These are offline failure-flow proofs,
not funded Strategy/Trader qualification.

The changed harness families passed 104 tests. Full workflow Ruff/format, 35
files, and whitespace checks passed. Existing 123/590-test evidence remains
compatible for unchanged paths. Packaged Python is unchanged from `708acfa9f`,
so the source-matched wheel and installed runtime evidence remain applicable;
no redundant wheel build or broad suite is required for script-only corrections.
Updated guide build passed with no MetaMask diagnostics and one unchanged
toctree warning. Git comparison confirms no packaged `lumibot/` change from
the source-matched artifact commit.

## Exhaustive coverage inventory

The initial audit traced all active native modules and connected callers; final
council review covers the complete integration as well as the incremental diff.

| Area | Files and behavior checked | Required evidence or limit |
| --- | --- | --- |
| Authentication and owner setup | `auth.py`, `owner.py`, `provider.py`: browser/headless identity, OAuth rotation, private sessions, Guard Mode, policy and managed signing | Offline transport/security tests; fresh authenticated access unqualified |
| Authority and recovery | `models.py`, `journal.py`, `engine.py`: explicit binding, reservations, effect identities, ambiguous writes, restart, revocation and cancellation | Native model/journal/engine regression family |
| Venue execution and data | `venue.py`, `metamask_client.py`, `metamask_data.py`: signing domains, canonical symbols, prices/candles, leverage, IOC/limit, fills, fees and account modes | Native venue/client/broker tests and public live reads |
| Swap execution | `evm.py`, `fees.py`: quotes, approvals, frozen transactions, gas/rollup costs, simulations, finalized receipts, replay and overrun holds | Offline EVM/fee regressions; funded swaps remain experimental |
| Broker lifecycle | `brokers/metamask.py`: ordinary orders/positions, synchronization, cumulative callbacks/costs, worker shutdown and shared client | Broker and registration tests; no extra cash debit |
| Cash activity | `activity.py`, venue/client/broker, standard Strategy collector and cash entities | Exact-window receipts, pagination, signed activity, stable IDs, incomplete states and collector regressions |
| Integration wiring | `credentials.py`, public exports, `setup.py`, MetaMask CI workflow | Eager/lazy selection, optional package dependencies, installed artifact and complete MetaMask family |
| User flow and operations | `metamask_perpetual.py`, three `qualify_metamask_*` scripts, both current setup guides and legacy migration reference | Normal Trader round trip/restart offline; funded receipts retained only as historical evidence |

Unsupported orders, HIP3 writes, portfolio margin, bridges, delegated/gasless
routes and unqualified swap cases remain explicit refusals. Retired JavaScript
runtime is outside active qualification. No feature is added merely to expand
this inventory.

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

Round 4 at `1ad7463ac`: Terra approved 8.5/10; Sol requested changes 8/10,
with one reproduced P2. Transfer identities hashed raw user/destination/vault
addresses while classification normalized them. Case-only representations could
report the same 12 USDC transfer twice and still mark coverage complete. The
integrator independently reproduced -24 USDC, added three failing regressions,
and normalized addresses and transaction hashes before both identity and
duplicate comparison. Amount and fee conflicts remain rejected; input records
remain unchanged. The affected activity/venue selection passed 62 tests.
Cancellation was requested for Astra's superseded round-4 run before a verdict
was delivered. No score is assigned. All three models receive a fresh corrected
full-integration review.

Round 5 results and accepted corrections are detailed above. No additional
P1/P2 was reported in the supported broker execution paths. The entire revised
solution, including shared harness guards, receives a fresh three-model review.

Round 6 results and the recovery-reporting correction are detailed above. A new
three-model pass reviews the full corrected outcome and affected failure paths.

| Delegate | Round 1 identity | Round 2 identity | Round 3 execution | Round 4 cash | Round 5 integration | Round 6 corrections |
| --- | --- | --- | --- | --- | --- | --- |
| GPT-6.1 Terra | 8.5/10 approve | 8.5/10 approve | 8.5/10 approve | 8.5/10 approve | 8.5/10 approve | 8.5/10 approve |
| GPT-6.1 Sol | 9/10 approve | 9/10 approve | 9/10 approve | 8/10 request changes | 8.5/10 request changes | 9/10 approve |
| GPT-6 Astra | 8/10 request changes | 9/10 approve | 9/10 approve | cancellation requested, no score | 8/10 request changes | 8.5/10 request changes |

Nothing has been pushed by this review task yet.
