# Native Python MetaMask broker

Replace the separately managed JavaScript service with native LumiBot broker execution.

Last Updated: 2026-11-01

Status: Native candidate prepared; funded qualification awaits explicit owner-grant renewal

Audience: LumiBot maintainers

## Overview

User code imports `MetaMask` from `lumibot.brokers` and uses ordinary LumiBot strategies, orders, positions and callbacks. Installation, authentication, reads, signing, reconciliation and order execution must work with Node, Bun, npm and embedded JavaScript engines absent. A JavaScript subprocess hidden behind a Python class does not meet this contract.

The existing Python broker and data source convert LumiBot entities but delegate all execution and market reads to a manually installed localhost Node service. Its SQLite journal and provider policy checks contain useful behavior; the service boundary and runtime requirement are rejected by the new requirement.

## Completion predicate

A clean Python installation can instantiate and operate the broker through the normal LumiBot API without any JavaScript runtime or localhost service. Offline protocol, cryptographic, accounting, recovery and migration tests pass. Actual connected Python reads establish the configured managed wallet and provider policy. Trading qualification uses the actual Python path and requires separately authorized testnet operations; mocked tests and prior JavaScript receipts cannot establish replacement trading correctness.

## Architecture decision

Astra compared a monolithic broker with an in-process execution engine. Choose the engine because it concentrates idempotency and signing/recovery transitions without exposing them to strategies. The broker owns LumiBot adaptation; the engine owns reservations and reconciliation; the MetaMask adapter owns provider credentials/jobs/policy; the venue adapter owns Hyperliquid action serialization, prices, account semantics and fills. The data source shares the same native read interfaces. There is no internal router pretending to call the retired localhost endpoints.

Use immutable typed bindings and explicit operation states. Monetary values stay decimal strings or Decimal. A provider approval, completed signature, venue acceptance and confirmed fill are separate events. Persist request IDs and exact action/nonce/context before an effect; never retry an uncertain mutation under a new identity. Recheck authorization after provider waits and before venue submission.

## Protocol and licensing boundaries

The installed Agent SDK 7.3.0 has a restrictive license; do not translate its implementation. Use the permissively licensed Fox SDK wire contracts and independently implemented public protocols, with required notices. Fox SDK 2.10.0 is dual MIT/Apache-2.0 and describes its interfaces as internal, so compatibility is pinned and experimental rather than a stable public REST guarantee.

Compose the official MIT-licensed Hyperliquid Python signing primitives with MetaMask's managed signer. Do not export a wallet key, substitute a local private-key account, fake a LocalAccount or patch installed SDK source. Send complete protocol context for provider policy evaluation. Verify the returned signature against the exact typed data and configured wallet before venue submission.

Authentication, refresh, policy read schemas and Hyperliquid protocol context require concrete permitted-source and runtime evidence before signing is enabled. Unsupported or malformed state fails explicitly. No production/mainnet fallback, wallet creation, policy broadening, funding or trading occurs during construction.

## State and security

Preserve existing journal tables and pending records: bindings, operations, policy_approvals, native_funding and collateral_transfers. Never reset uncertain work to make the new implementation start. Retain exclusive execution ownership across processes and immutable wallet/network binding. Future migration must be explicit and tested against nonterminal historical records.

Native in-process execution removes the old process credential boundary. Untrusted strategy code can access process credentials or bypass cooperative local limits. Dedicated wallet and provider-enforced Guard policy remain the real boundaries. Documentation must not represent a local token or Python check as an equivalent sandbox.

## Verification sequence

1. Capture current Python baseline and the source/journal contracts.
2. Establish native authentication, wallet identity and policy reads against permitted protocol sources.
3. Implement typed state and durable journal, then prove idempotency, exclusive ownership and interruption handling.
4. Implement native public venue reads and exact account/asset/decimal semantics; verify real public testnet reads.
5. Implement remote signing/action construction, then test cryptographic parity and all effect boundaries.
6. Connect normal broker/data-source methods, callbacks and credential selection. Remove localhost transport requirements.
7. Preserve owner approval/recovery/funding capabilities through native APIs or provider-owned setup surfaces; do not silently discard them.
8. Test installation and normal strategy execution with all JavaScript runtime execution prohibited, then qualify connected reads and explicitly authorized testnet execution.
9. Update engineering/public documentation, Python package dependencies, CI and changelog. Remove obsolete service paths only after callers converge and user-owned changes are preserved.

## Current evidence

The original Python broker tests passed 39 checks before changes. These mock its loopback client and establish a regression baseline only. No replacement implementation or connected native trading result is claimed by that baseline.


## Native candidate evidence

The candidate passes 285 focused and adjacent regressions covering provider context, OAuth rotation, SQLite ownership, journal compatibility, signing vectors, revocation boundaries, unknown outcomes, acknowledged-response failures, legacy venue IDs, cumulative callbacks, cancellation retries and EVM receipt accounting. Five connected read-only checks verify two actual testnet wallet identities and Guard-policy digests, account reads, closed candles, expired-grant denial and an ordinary strategy iteration without replaying historical fills.

A clean wheel installation resolves its Python MetaMask extra and passes dependency consistency checks. Its native market-data path reads real testnet marks and closed candles with Node, Bun, npm and npx absent from PATH and subprocess execution prohibited. Wheel modules match the prepared source. Sphinx builds successfully; changed documentation introduces no new warning. Python lint, formatting and public-repository hygiene pass.

Astra reviewed the architecture and recovery changes. This is a cross-model source review, not an independent security audit or a funded trading receipt. The prior JavaScript tests are retained as historical evidence, not relabeled as native qualification.

## Remaining qualification and capability limits

The configured testnet owner grants are expired. They remain expired until the owner explicitly renews them. Native order placement, actual partial fills, cancellation races and managed signing still need connected receipts through this exact candidate. No signing, trading, wallet creation, provider-policy mutation, funding, publication or deployment was performed by the native migration's connected read checks. OAuth migration/refresh is the only authenticated mutation completed.

Default-DEX USDC perpetuals are the bounded trading candidate. HIP3 writes remain held because collateral identity and effective DEX fees require separate accounting. EVM swaps are experimental; native output without trace proof, testnet routing, delegated/gasless routes and non-source-denominated quote fees are rejected before execution. Mainnet swaps are not funded-qualified. Private owner funding, policy and collateral records remain preserved and inspectable; old pending work is never replayed automatically.

The replacement is prepared in an isolated checkout. Integrate into the intended local version checkout only after its native trading qualification succeeds and unrelated dirty paths remain preserved. Remote publication is not part of this task.
