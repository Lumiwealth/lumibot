# Native Python MetaMask broker

MetaMask managed-wallet execution through ordinary LumiBot strategies, without a JavaScript runtime or localhost service.

Last Updated: 2026-11-05

Status: Native adapter available; execution hardening candidate under review

Audience: LumiBot users and maintainers

## Overview

`lumibot.brokers.MetaMask` runs in the Python strategy process. It calls MetaMask's managed-wallet API and Hyperliquid's HTTP API directly. Wallet keys remain with MetaMask. Node, Bun, npm, the MetaMask CLI and an embedded JavaScript engine are not required.

The published package includes the native adapter: `python -m pip install
'lumibot[metamask]'`. The execution and reporting fixes described here belong to
[PR 1262](https://github.com/Lumiwealth/lumibot/pull/1262), an unreleased source
candidate. Installing published releases or `dev` does not install unmerged PR
changes. To review this candidate in a clean Python environment:

```bash
git clone --branch fix/metamask-execution-recovery --single-branch https://github.com/Lumiwealth/lumibot.git lumibot-metamask
cd lumibot-metamask
git rev-parse HEAD
python -m pip install '.[metamask]'
```

Default-DEX Hyperliquid perpetuals use normal `Order`, `Position`, strategy methods and fill callbacks. Native same-chain swap code remains experimental and requires separate funded qualification. Existing JavaScript trading receipts do not qualify this replacement.

Match the printed commit to the reviewed PR head before testing. Package
availability and passing offline checks do not establish production qualification.

## Integration coverage

The integration spans installation, owner authentication, broker/data selection, execution and recovery. The native client, owner setup, dependencies and public exports already exist on `dev`; the current candidate completes execution recovery, ordinary strategy usage and shared-client environment selection. Review the complete implementation together:

| Capability | Implementation | Regression coverage |
| --- | --- | --- |
| Python optional dependencies and public registration | `setup.py`, `lumibot/credentials.py`, broker/data-source exports | `test_metamask_registration.py`; CI installs `.[metamask]` on Python 3.10 and 3.12 |
| Browser login, headless session, OAuth refresh and migration | `lumibot/tools/metamask/{auth,owner,provider}.py` | `test_metamask_native_{auth,owner,provider}.py` |
| Owner-reviewed authority, durable reservations and recovery | `lumibot/tools/metamask/{journal,engine}.py` | `test_metamask_native_{journal,engine}.py` |
| Perpetual orders, positions, cash, polling and cancellation | `lumibot/brokers/metamask.py`, `lumibot/tools/metamask/venue.py` | `test_metamask_{broker,native_client,native_venue}.py` |
| Bounded cash activity and explicit history coverage | `lumibot/tools/metamask/activity.py`, broker/client/venue | `test_metamask_cash_events.py`; standard Strategy collector |
| Public perpetual prices/candles and registered token prices | `lumibot/data_sources/metamask_data.py` | `test_metamask_broker.py`, `test_metamask_native_evm.py` |
| Experimental same-chain exact-input swaps and fee accounting | `lumibot/tools/metamask/{evm,fees}.py` | `test_metamask_native_{evm,fees}.py`; funded qualification pending |
| Normal Strategy/Trader round trip, callbacks and restart | `lumibot/example_strategies/metamask_perpetual.py` | `test_metamask_example_strategy.py`; funded testnet evidence below |

These paths are repository-relative; test names are under `tests/`. Brace groups abbreviate multiple filenames. The MetaMask CI workflow runs the entire `tests/test_metamask*.py` family, including implementation already on `dev`. Scope is MetaMask Agent Wallet with default-DEX Hyperliquid perpetuals and the experimental swap path described below; it does not cover every MetaMask product or venue.

## Authentication and explicit authority

Use the official [Agent Wallet sign-in page](https://developer.metamask.io/agentic/login), keeping the same Google/email method as the funded wallet. Changing sign-in methods can select a different wallet. Copy its browser-issued token into the secure Python prompt, never a strategy source file or chat:

```python
from lumibot.tools.metamask.owner import login

# Opens the official page, verifies token identity and saves a private session.
login("<private-directory>/session.json")
```

For headless use, supply `PROJECT_ID` and `ACCESS_TOKEN`, or an explicit private `SESSION_FILE`. Automatic OAuth refresh requires a durable session file. Token rotation is locked, saved atomically and verified through provider introspection. A lost rotation response requires reconnect, not an automatic retry. Reconnect into a new session file, then deliberately select it. Login never creates a wallet or changes provider policy.

Select an existing dedicated managed EVM wallet and configure Guard Mode and its policy in MetaMask. Before authorizing local limits, read and review the exact policy text:

```python
from lumibot.tools.metamask_client import MetaMaskClient

config = {
    "SESSION_FILE": "<private-directory>/session.json",
    "STATE_FILE": "<private-directory>/broker.sqlite",
    "AGENT_ID": "strategy-wallet",
    "WALLET_ADDRESS": "<dedicated-managed-wallet-address>",
    "NETWORK": "testnet",  # Explicit testnet or mainnet; no fallback.
    "MODE": "perp",
}

with MetaMaskClient(config) as owner:
    owner.provider.verify_wallet(config["WALLET_ADDRESS"])
    policy = owner.provider.policy(config["WALLET_ADDRESS"])
    print(policy.text)  # Review provider-enforced network, address and spending rules.
    # After reviewing the policy and choosing exact limits:
    owner.authorize({
        "id": config["AGENT_ID"], "wallet": config["WALLET_ADDRESS"],
        "network": config["NETWORK"], "enabled": True,
        "expiresAt": YOUR_REVIEWED_EXPIRY_UNIX_MILLISECONDS,
        "policyDigest": policy.digest, "markets": ["BTC", "ETH"],
        "chains": [421614], "assets": [],
        "maxOrderUsd": "25", "maxDailyUsd": "100",
        "maxLeverage": 1, "maxSlippageBps": 50,
    })
```

The limits above are examples, not recommendations. `authorize()` is an explicit owner action. Startup never renews expiry, clears revocation, broadens a registry or changes remote policy. Review and authorize changes deliberately. Keep the durable journal on a local owner-controlled filesystem. Do not run separate copies for the same wallet.

Local Python checks are cooperative controls. Strategy code in the same process can access its credentials or bypass those checks. Dedicated wallet funding and provider-enforced Guard policy are the actual security boundaries. Typed-data signatures are not a provider outflow cap; read MetaMask's [outflow policy](https://docs.metamask.io/agent-wallet/reference/outflow-policy/).

## Ordinary LumiBot usage

The runnable example is `lumibot/example_strategies/metamask_perpetual.py`.
With an existing authorized testnet session and journal, select
`TRADING_BROKER=metamask`, `METAMASK_NETWORK=testnet` and `METAMASK_MODE=perp`, then run:

```bash
python -m lumibot.example_strategies.metamask_perpetual
```

`MetaMaskPerpetualRoundTrip` uses normal `Strategy` order, position, price and cash
methods with continuous `Trader` scheduling. It buys 0.005 ETH at 1x, then closes
the actual executed quantity with a reduce-only order on a later iteration.
Check current price, minimum notional, lot precision and available authority
before running. A rejected or zero-fill entry is not a successful round trip.

Reuse the same strategy name and parameters to resume the journaled round trip
without a new entry. A different name intentionally starts a new cycle. The
strategy continues checking positions and late fills after reaching flat.
Unknown submission outcomes hold execution instead of creating replacement IDs.
Stop and reconcile a rejected close before submitting a new closing intent.
Older native canceled records regain missing no-release provenance only when
their intact persisted plan and effect checkpoints prove it. Missing or conflicting
history remains unknown; recovery never clears reconciliation or fee holds.

MetaMask-specific setup stays outside the strategy class. Use a dedicated managed
wallet, one journal owner and an explicitly reviewed grant. The launcher checks
venue open orders because journal-backed `get_orders()` cannot discover manual
or foreign orders. Grant expiry and remaining daily capacity must cover the
closing leg as well as entry; `get_cash()` reports broker-reported USDC cash, not remaining
policy budget. Startup does not renew authority. Stop and join `Trader` before
calling `broker.close()` to release polling, order workers and the native client.

Quantities are base units with multiplier 1. Supported orders are buy/sell market or limit, with canonical `reduce_only=True` for closing positions. `custom_params` may contain `leverage` and `reduce_only`; it cannot weaken canonical reduce-only intent. Limit orders require `gtc`, `ioc` or `alo`. Market orders become a frozen slippage-bounded IOC limit. Conditional, bracket, OCO and automatic cancel/replace orders are unsupported.

The broker reserves pre-trade notional plus the current effective fee envelope. Duplicate operation IDs are idempotent; changed intent under an existing ID is rejected. Uncertain submissions block new execution and reconcile the original request/client-order ID. Cancellation uses a separate durable effect and cannot overwrite the original order's fills or terminal status. Canceling a prepared, unreleased order never advances it into a trade.

Perpetual receipt fees populate standard `Order.trade_cost` in USDC, including
maker rebates. This is the cumulative order cost, matching the existing broker
convention; partial-fill event rows contain the cost observed at that time, not
independent fee deltas to sum. Restored orders retain fees without replaying fills.
Fee-only refreshes update the order without new execution callbacks. Unknown
currencies remain in raw receipts with an unavailable standard cost rather than
an invented conversion. Account cash already includes fees; reporting them does
not deduct them again.

Standard account cash uses actual withdrawable funds and margin. Unified account equity uses HyperCore's pooled spot USDC once, without adding perps PnL again. Portfolio-margin accounts are unsupported. HIP3 writes are explicitly held until collateral-aware valuation and DEX fee metadata are qualified; public market reads remain available. Do not assume non-USDC collateral values are USDC.

### Cash activity and history coverage

`broker.get_cash_event_sync(since=aware_datetime, limit=5000)` reads both funding
and non-funding ledgers for the verified managed wallet. The default window is
the last 60 days, not all-time history. Receipts identify network, wallet, mode
and the exact UTC window, with an inclusive start and exclusive end. Pagination
retains same-millisecond boundary rows and rejects conflicting identities or
pages that cannot advance. Reads above 100,000 distinct records require a
smaller window. Stable event IDs include network, wallet, ledger and transaction
leg; retrying a read does not create new activity identities.

Qualified normalized events are signed default-DEX USDC funding, deposits,
zero-fee withdrawals and zero-fee USDC account/subaccount transfers with an
unambiguous selected-wallet leg. Funding is operating activity; qualified
capital transfers are external to this broker portfolio. Broker cash already
includes these events and is never changed again by history collection.

Unknown/malformed categories, fee-bearing capital movements, non-USDC assets,
HIP3 funding, spot transfers, and spot/perp account-class transfers keep history
`partial`. Historical account-mode changes and capital net/gross fee semantics
require separate qualification; current account mode does not prove historical
classification. Known events remain available, without certified coverage.
Provider/identity failures or unexhausted pagination return `unavailable`;
onchain spot history is `unsupported`. A successful empty read is `complete`
only for its checked window. Limits also make the result `partial`.

Normal Strategy cloud collection uses these receipts and marks initial history
loaded only after a complete read. The list-only `get_cash_events()` refuses
incomplete results with `CASH_HISTORY_INCOMPLETE`. Raw non-funding records remain
readable through `client.cash_history(start_ms, end_ms)`; bounds there are
inclusive. This reporting contract does not qualify funded provider execution.

`MetaMaskData` reads marks and closed minute/hour/day candles from the selected Hyperliquid network. Public perpetual data needs only `NETWORK`; it does not need a wallet credential. Candle history is limited to the venue's latest 5,000 bars. Spot candles and historical account equity are not provided.

LumiBot's uppercase asset symbols resolve to the unique canonical venue name,
including mixed-case markets such as `kPEPE`. Orders retain that native name in
their journal; case-only retries reuse the same operation. Ambiguous names are
rejected rather than selecting another market.

Environment selection uses `TRADING_BROKER=metamask`, optional `DATA_SOURCE=metamask`, `METAMASK_SESSION_FILE`, `METAMASK_STATE_FILE`, `METAMASK_AGENT_ID`, `METAMASK_PROJECT_ID`, `METAMASK_ACCESS_TOKEN`, `METAMASK_REFRESH_TOKEN`, `METAMASK_WALLET_ADDRESS`, `METAMASK_NETWORK`, and `METAMASK_MODE`. Keep secrets in your own secret manager.

When both broker and data source select MetaMask, they share the same native client, session and journal owner in eager and lazy startup. With only `DATA_SOURCE=metamask`, public perpetual reads use an independent client and require only `METAMASK_NETWORK`. Broker shutdown releases the shared client; data-only callers close `data_source.client` when finished.

## Testnet and mainnet

Select the network explicitly. Preserve separate network bindings and journals;
startup never converts testnet authority into mainnet authority. Hyperliquid uses
different API origins and market inventories. Resolve asset IDs and precision
from that network's metadata. Perpetual signatures use EIP-712 chain `1337` on
both networks, with source `a` for mainnet and `b` for testnet. These are distinct
from HyperEVM chain IDs. See the [official signing implementation](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py).

Supported account valuation is checked before a fresh execution reservation and
again after approval before releasing a leverage or order action. Portfolio
margin, unknown account modes and unreadable account state cannot proceed to
new execution. Cancellation and original-order reconciliation remain available.

The supported perpetual inventory covers the default USDC DEX. Unified accounts
can share collateral across DEXs, so this inventory does not prove wallet-wide
flatness. Keep the managed wallet dedicated to this adapter, without manual
positions or orders on other DEXs. Cross-DEX exposure needs separate inventory
and collateral qualification. See [account modes](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes)
and [margining](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining).

Mainnet liquidity, fee tiers, funding and liquidation are venue state, not
testnet constants or local grant limits. Low leverage and a spending grant do
not cap liquidation loss or funding. Opening orders below the venue minimum
can receive an explicit rejection; never enlarge them automatically or apply an
unverified minimum to reduce-only residual cleanup. See [venue errors](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)
and [liquidations](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations).

REST rate limits are shared across the public IP. Keep aggregate polling within
the [documented request weights](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits).
The current adapter fails explicitly on read errors and has no global weighted
rate scheduler. Uncertain writes retain their original identity and are never
automatically retransmitted. A safe read retry is not permission to repeat an order.

Primary-source findings and remaining boundaries are recorded in
[the mainnet contract audit](investigations/2026-11-02_METAMASK_MAINNET_CONTRACT_AUDIT.md).
Public metadata checks and deterministic mainnet contract tests need no funded
account; they do not establish funded mainnet execution.

## Same-chain exact-input swaps

`MODE="spot"` exposes `broker.swap(input_asset, output_asset, input_amount, identifier=...)` and `broker.get_swap(identifier)`. It does not reinterpret an exact-input swap as an exact-output buy order.

Register exact chain IDs, contracts and decimals in the binding's `assets`. Native assets use `address="native"`. Configure `CHAIN_RPC_URLS` for every approved chain. Explicit per-chain `GAS_LIMITS` set the owner ceiling, including when a provider gas estimate is too low. Quotes estimating above that ceiling are rejected. Without an explicit ceiling, the quoted gas limit is retained; a quote without either is rejected. The native gas asset must be registered for valuation and balance checks.

Each chain/contract may appear only once in the asset registry, including aliases
and conflicting decimal declarations. Assets must belong to approved chains.
Native pricing uses chain-specific CAIP identities: ETH uses `slip44:60`, POL
`slip44:966`, BNB `slip44:714` and Avalanche C-Chain AVAX `slip44:9005`.
The native price registry currently covers Ethereum, Optimism, BNB Chain,
Polygon, zkSync Era, Base, Arbitrum, Avalanche and Linea, plus their explicitly
listed testnet identities in `models.py`. Unknown native identities refuse
valuation rather than assuming ETH. A known identity does not establish a
provider price or funded chain qualification; unpriced test tokens still fail.
Listed testnets use the registry's `slip44:1` test-coin identity, independently of
their parent mainnet coin type.

The Python path binds the provider quote ID, wallet, recipient, chains, token identities, fee-net input, slippage and exact transaction. ERC20 allowance grants are capped to exact input, with a separate zero reset when needed. Simulation runs after prerequisite allowances settle. Each managed request is persisted before MetaMask signs and broadcasts; the broker never rebroadcasts its signed transaction. It verifies mined transaction identity, nonce, gas ceiling, RPC-finalized canonical receipt and net ERC20 output transfers. Quote output is never reported as a received amount.

Allowance finality can outlast a trade quote. After all required allowances are
finalized, the native client obtains and journals a fresh trade quote before
submitting the trade. It preserves the original input, minimum output, authority
and total fee reservation. A changed spender needing another allowance, weaker
minimum output or higher total fees stops execution for owner review. Original
approval and quote records remain immutable; restart reconciles an already
submitted trade by its original request ID without signing again.

Bounded prerequisite allowances use the reviewed grant's expiry, not the trade
quote's short lifetime. Each new allowance still requires unchanged authority,
current Guard policy, the frozen transaction/gas ceiling and available original
fee reservation. This lets zero-reset and exact-input approvals settle in
sequence before refreshing the trade quote. A restart never releases a remaining
unsubmitted allowance; already submitted requests reconcile their original IDs.

The configured RPC must support `eth_getBlockByNumber("finalized", false)`.
One later block is not finality. Pending finality retains submitted state;
unsupported finality or contradictory block identity holds reconciliation.
Historical filled records without finality provenance are checked read-only.
Missing or conflicting evidence retains their recorded fills, fees and spending
history and blocks new execution; it never resubmits the old swap. Finality is
the selected chain/RPC's contract, not a bridge withdrawal guarantee. See the
[Ethereum RPC block tags](https://ethereum.org/developers/docs/apis/json-rpc/#the-block-parameter).

Quote fees may use the exact source or destination asset. Source fees count
toward exact input; destination fees are already reflected in the quoted net
output and never increase input. Fee assets with conflicting chains, contracts
or decimals are rejected. Minimum output may round down by less than one output
atomic unit; another whole unit below that bound is rejected. Completion still
uses actual net receipt transfers.

Current limits are explicit: testnet swap routing, native output without trace proof, gasless/delegated routes, bridges and fees in a third asset are unsupported. These refuse before execution. Spot balances value only the approved asset registry in USD using actual provider prices, with zero synthetic USD cash. Missing prices raise an error, including unpriced test tokens. Mainnet swaps have not been funded-qualified for this native candidate.

## Migrating the retired service

Stop the old service through its existing runtime owner. Preserve its session and complete journal, including the owner-operation tables. Do not copy an active database or change a wallet identity to get around pending state.

Use the existing `broker.sqlite` as `STATE_FILE` and its exact agent and wallet identity. To import credentials, add `LEGACY_SESSION_FILE` pointing to the existing service `session.json`, and select a distinct new `SESSION_FILE`. The native client holds the old session fence during use, imports once and thereafter reuses the rotated native credentials. It leaves the original session intact. Refresh rotations belong to the native file; do not restart the retired service with stale credentials.

`BROKER_URL` and `BROKER_TOKEN` configurations now raise migration guidance. The former agent-scoped token is not a MetaMask access token. Never put it in `ACCESS_TOKEN`.

Known legacy venue orders reconcile their recorded venue IDs and retained fills. Old unused signatures are never released after restart. Expired uncertain native leverage reconciles current venue settings and rejects its unsubmitted intent without replay; a fresh order ID is required. Legacy swaps without native transaction evidence remain held for explicit owner reconciliation. `client.owner_operations()` exposes preserved policy, native-funding and collateral-transfer receipts; these are not automatically replayed. Funding and policy changes belong in MetaMask's owner surfaces. The retired local funding UI and its old qualification receipts remain historical reference in `METAMASK_LEGACY_SERVICE_HISTORY.md`.

## Qualification

Funded testnet and installed-wheel results below were recorded before this
council review. They have not been rerun for the identity, allowance, fee and
cash-history changes. Fresh authenticated and funded qualification remains open.

Offline tests cover provider context, OAuth rotation, journal ownership and reservations, official signing vectors, remote-signature recovery, order replay prevention, cumulative callbacks, cancellation, swap quotes and receipts. Connected Python reads verify actual wallet identity, Guard policy, testnet account, candles and ordinary strategy startup without replaying historical fills. A dependency check reports a compatible Python environment.

Native testnet opening, reduce-only closing and cancellation have funded receipts. Fresh live partial-flow remains unqualified. Actual short-lived testnet grants have expired on both dedicated wallets: new intents returned `REVOKED` without signing or changing orders, execution effects or nonces. The ordinary connected expiry test still skips active grants; offline expiry regressions remain separate evidence. Managed swaps still require their own funded receipts. Broker startup never renews expired local grants; prior JavaScript test results are not carried forward. No production or mainnet trading qualification is claimed.

The explicit `scripts/qualify_metamask_partial.py` testnet harness checks live remaining size against `frontendOpenOrders` and the persisted native client order identity. It retains independent paired-fill, fee, callback and cleanup assertions. `--daily-caps MAKER TAKER` validates separately reviewed existing grants; it does not change authority or erase spending history. Defaults remain 300/150, and each wallet needs at least $50 of reservation room before signing so an entry and residual cleanup fit the $25 order limit.

Broker reconciliation and qualification require order-status evidence to match
the persisted `cloid`. Live `frontendOpenOrders`
records match by `oid` and may omit `cloid`; when supplied, it must agree.
Remaining-size and fill checks remain mandatory. See the [venue response schemas](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint).

Protocol sources: [MetaMask architecture](https://docs.metamask.io/agent-wallet/reference/architecture/), [browser authorization](https://docs.metamask.io/agent-wallet/cli-setup/), [Hyperliquid API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint). Remote-wallet interfaces were checked against permissively licensed Fox SDK 2.10.0 declarations; Hyperliquid action serialization uses the official MIT Python SDK 0.24.0. No restricted Agent SDK implementation is translated or bundled. MetaMask's remote interfaces remain an experimental compatibility contract.

## Execution and dependency review

Hyperliquid's signing domain uses chain ID 1337 for both networks; its signed `source` binds testnet or mainnet. Managed requests must match that domain rather than an Arbitrum bridge chain. Provider validation rejections and Shield failure codes remain durable and never cause automatic reposts or venue submission.

Owner revocation is serialized with the final external handoff after network preparation. An expired uncertain cancellation can receive a new cancellation identity only after a fresh venue read confirms the same original order remains live. Expired uncertain leverage actions reconcile current venue settings and reject the unsubmitted intent without replay; use a new order ID afterward. Public order refresh and polling share cumulative fills and callbacks.

Live partial-fill remainders come from identity-checked open-order snapshots; `orderStatus` can retain the original size while an order remains open. Terminal fill refresh is read-only, preserves closed lifecycle, and emits only new execution deltas. Inconsistent execution evidence blocks new reservations until reconciled.

An IOC order can report venue status `filled` after executing only part of its requested quantity. When the original intent, persisted wire action, venue time in force and exact fills prove this case, LumiBot records the actual execution and cancels the unfilled remainder. The raw venue status remains available for audit. A status label or remaining size alone cannot establish a fill; incomplete or contradictory evidence stays held for reconciliation.

A real testnet IOC partial execution has been recovered through the corrected broker with exact fills and fees, followed by an exact reduce-only close and flat-account verification. This proves that recovery path. Controlled maker partial-cancellation qualification remains inconclusive, and no production readiness is claimed.

Polling defaults to 10 seconds. Wallet sweeps share fill and live-order reads; unchanged closed orders avoid individual status requests. Increase `POLL_INTERVAL` for large histories or multiple brokers sharing an IP because Hyperliquid applies [weighted limits per IP](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits). Under the [unified-account balance contract](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes), cash reports unheld USDC collateral, not guaranteed order capacity or the value of unrelated spot assets.

Atomic token quantities and receipt output checks remain exact regardless of a strategy's decimal context. A managed swap cannot be canceled locally because MetaMask may still broadcast it. Reverted transactions and earlier approvals retain gas fees and conservative daily reservations.

OP Stack fee estimates include L1 data and operator charges from block-pinned oracles. Execution gas has a frozen ceiling; separate rollup fees are current estimates, not signer-enforced caps. Actual overruns preserve fills and costs, charge an adjusted USD reservation at the frozen native-token price, and block further spending. After reviewing the exact `feeBudgetOverrun`, the owner may call `client.acknowledge_fee_overrun(operation_id, expected_overrun)`. Acknowledgment never erases actual daily usage. Legacy overruns without verified USD valuation remain held.

Install in a clean environment, or remove the retired `polygon-api-client` before upgrading. Its old CA-store constraint prevents current certificate fixes. LumiBot now uses the maintained Massive SDK while retaining `POLYGON_API_KEY` and `api.polygon.io`. The MetaMask extra requires patched `urllib3` and `certifi`. Session locks reject unsafe ownership, symlinks and hardlinks before acquisition; private owner-controlled state directories remain required. This does not claim that every optional or base dependency advisory is resolved.

New signature requests use the same fixed provider-origin metadata as MetaMask's high-level wallet adapter, with LumiBot identified separately in the durable client provenance and readable intent. Recovery validates each request's original provider origin. There is no alternate-origin retry after a failure. Guard Mode and exact policy checks remain enabled.

Native broker qualification has exercised actual testnet ALO placement/cancellation and market opening/reduce-only closing through Python on two dedicated wallets. A real Strategy/Trader ETH round trip delivered unique fill callbacks and reconciled ending cash exactly, including fees. The installed wheel passed native regression and connected-history tests on Python 3.10 and 3.12 with JavaScript runtimes absent from PATH and child processes denied during connected checks. That earlier wheel also recovered an actual canceled-partial receipt with matching venue fills and fees and no replayed callbacks. Fresh live partial-flow attempts remain inconclusive; no production or mainnet-swap qualification is claimed. Earlier JavaScript receipts remain historical evidence only.

## Local cancellation and release evidence

A prepared native order can be canceled locally only when its persisted native plan and signing checkpoints positively prove that the order was not released. Its structured `orderRelease` receipt distinguishes this from a venue cancellation. Generic order tools report `confirmed_not_submitted`, with an incomplete, non-retryable submission result. This says only that the order was not sent: prior signatures, leverage changes and audit history remain recorded.

Legacy or malformed records without release evidence remain unresolved; missing effects do not establish that nothing happened. Cancellation cannot clear their existing execution hold. A historical canceled label without venue identity, executions or positively observed acceptance projects as unknown and requires operator reconciliation. This projection does not reconstruct a hold already lost in a historical journal. Actual venue cancellations, including zero-fill and partial-fill cancellations, retain confirmed acceptance; late executions are still reconciled.

Cancellation signing and pending-signature release require a matching live venue observation without a reconciliation error. An unresolved or contradictory observation holds the cancellation as well as the entry. If an explicit local cancellation retry later obtains valid no-release proof and settled leverage evidence, it clears the resolved reconciliation hold while retaining prior effects and fee-review flags.

## Recovery and startup identity

When both the broker and data source select MetaMask, environment startup shares
one native client and journal. Startup does not create or renew signing authority.
Live order observations may omit an optional client ID; the persisted venue order
ID and all available identity fields still have to match before reconciliation.
Strategy refresh continues reconciling late fills after its local round trip ends.

## Frequently asked questions

### Do I need Node or a local MetaMask service?

No. Install `lumibot[metamask]` and configure the native session, wallet and journal. Managed signing and venue requests run through Python.

### Who holds the wallet keys and enforces spending limits?

MetaMask retains wallet keys. Guard Mode enforces its reviewed provider policy; local notional limits are cooperative process controls. Use a dedicated wallet with bounded funding and keep Guard Mode enabled.

### Can I preserve the previous integration's journal?

Yes. Retain the exact state file, agent and wallet identity. Known venue orders can reconcile; ambiguous legacy effects remain explicit holds. Old service tokens are not MetaMask access tokens, and startup never renews authority.

### What should I do after an approval timeout or unknown outcome?

Inspect `order.submission_outcome`: `broker_state_certainty` distinguishes `confirmed_submitted`, `confirmed_rejected`, `confirmed_not_submitted` for proven pre-release cancellation, and `unknown`. `reconciliation_required` marks unresolved or conflicting evidence. Agent submission tools preserve this state and return incomplete, non-retryable results while uncertainty remains. An acknowledgement is not a fill.

Inspect the recorded operation with `broker.client.get_order(operation_id)`. Reuse that operation's identity and reconcile the original request before creating another trade. Do not bypass Guard Mode or increase permissions to clear a failure.

### Does testnet qualification establish production swap readiness?

No. Native perps have funded testnet evidence, with the remaining limits recorded above. Mainnet trading and funded native swaps remain unqualified; earlier JavaScript receipts do not establish Python readiness.
