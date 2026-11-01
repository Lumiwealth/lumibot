# Native Python MetaMask broker

MetaMask managed-wallet execution through ordinary LumiBot strategies, without a JavaScript runtime or localhost service.

Last Updated: 2026-11-01

Status: Native candidate; funded execution qualification in progress

Audience: LumiBot users and maintainers

## Overview

`lumibot.brokers.MetaMask` runs in the Python strategy process. It calls MetaMask's managed-wallet API and Hyperliquid's HTTP API directly. Wallet keys remain with MetaMask. Node, Bun, npm, the MetaMask CLI and an embedded JavaScript engine are not required.

This native candidate is unreleased. For source testing, install the shared version branch in a clean environment:

```bash
pip install 'lumibot[metamask] @ git+https://github.com/Lumiwealth/lumibot.git@version/4.6.40'
```

Default-DEX Hyperliquid perpetuals use normal `Order`, `Position`, strategy methods and fill callbacks. Native same-chain swap code remains experimental and requires separate funded qualification. Existing JavaScript trading receipts do not qualify this replacement.

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

```python
from lumibot.brokers import MetaMask
from lumibot.entities import Asset
from lumibot.strategies import Strategy

class PerpetualStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        # Your strategy chooses its price, quantity and when to trade.
        pass

broker = MetaMask(config)
strategy = PerpetualStrategy(
    broker=broker,
    quote_asset=Asset("USDC", asset_type="crypto"),
    benchmark_asset=None,
)

# Inside on_trading_iteration, use self.create_order()/self.submit_order():
# asset = Asset("BTC", asset_type="crypto_future", leverage=1)
# order = self.create_order(asset, quantity, "buy", order_type="limit",
#                          limit_price=chosen_price, time_in_force="gtc")
# self.submit_order(order)
```

Quantities are base units with multiplier 1. Supported orders are buy/sell market or limit, with canonical `reduce_only=True` for closing positions. `custom_params` may contain `leverage` and `reduce_only`; it cannot weaken canonical reduce-only intent. Limit orders require `gtc`, `ioc` or `alo`. Market orders become a frozen slippage-bounded IOC limit. Conditional, bracket, OCO and automatic cancel/replace orders are unsupported.

The broker reserves pre-trade notional plus the current effective fee envelope. Duplicate operation IDs are idempotent; changed intent under an existing ID is rejected. Uncertain submissions block new execution and reconcile the original request/client-order ID. Cancellation uses a separate durable effect and cannot overwrite the original order's fills or terminal status. Canceling a prepared, unreleased order never advances it into a trade.

Standard account cash uses actual withdrawable funds and margin. Unified account equity uses HyperCore's pooled spot USDC once, without adding perps PnL again. Portfolio-margin accounts are unsupported. HIP3 writes are explicitly held until collateral-aware valuation and DEX fee metadata are qualified; public market reads remain available. Do not assume non-USDC collateral values are USDC.

`MetaMaskData` reads marks and closed minute/hour/day candles from the selected Hyperliquid network. Public perpetual data needs only `NETWORK`; it does not need a wallet credential. Candle history is limited to the venue's latest 5,000 bars. Spot candles and historical account equity are not provided.

Environment selection uses `TRADING_BROKER=metamask`, optional `DATA_SOURCE=metamask`, `METAMASK_SESSION_FILE`, `METAMASK_STATE_FILE`, `METAMASK_AGENT_ID`, `METAMASK_PROJECT_ID`, `METAMASK_ACCESS_TOKEN`, `METAMASK_REFRESH_TOKEN`, `METAMASK_WALLET_ADDRESS`, `METAMASK_NETWORK`, and `METAMASK_MODE`. Keep secrets in your own secret manager.

## Same-chain exact-input swaps

`MODE="spot"` exposes `broker.swap(input_asset, output_asset, input_amount, identifier=...)` and `broker.get_swap(identifier)`. It does not reinterpret an exact-input swap as an exact-output buy order.

Register exact chain IDs, contracts and decimals in the binding's `assets`. Native assets use `address="native"`. Configure `CHAIN_RPC_URLS` for every approved chain. Quote transactions must contain gas ceilings; when absent, configure explicit per-chain `GAS_LIMITS`. The native gas asset must be registered for valuation and balance checks.

The Python path binds the provider quote ID, wallet, recipient, chains, token identities, fee-net input, slippage and exact transaction. ERC20 allowance grants are capped to exact input, with a separate zero reset when needed. Simulation runs after prerequisite allowances mine. Each managed request is persisted before MetaMask signs and broadcasts; the broker never rebroadcasts its signed transaction. It verifies mined transaction identity, nonce, gas ceiling, canonical receipt and net ERC20 output transfers. Quote output is never reported as a received amount.

Current limits are explicit: testnet swap routing, native output without trace proof, gasless/delegated routes, bridges and non-source-denominated quote fees are unsupported. These refuse before execution. Spot balances value only the approved asset registry in USD using actual provider prices, with zero synthetic USD cash. Missing prices raise an error, including unpriced test tokens. Mainnet swaps have not been funded-qualified for this native candidate.

## Migrating the retired service

Stop the old service through its existing runtime owner. Preserve its session and complete journal, including the owner-operation tables. Do not copy an active database or change a wallet identity to get around pending state.

Use the existing `broker.sqlite` as `STATE_FILE` and its exact agent and wallet identity. To import credentials, add `LEGACY_SESSION_FILE` pointing to the existing service `session.json`, and select a distinct new `SESSION_FILE`. The native client holds the old session fence during use, imports once and thereafter reuses the rotated native credentials. It leaves the original session intact. Refresh rotations belong to the native file; do not restart the retired service with stale credentials.

`BROKER_URL` and `BROKER_TOKEN` configurations now raise migration guidance. The former agent-scoped token is not a MetaMask access token. Never put it in `ACCESS_TOKEN`.

Known legacy venue orders reconcile their recorded venue IDs and retained fills. Old unused signatures are never released after restart. Expired uncertain native leverage reconciles current venue settings and rejects its unsubmitted intent without replay; a fresh order ID is required. Legacy swaps without native transaction evidence remain held for explicit owner reconciliation. `client.owner_operations()` exposes preserved policy, native-funding and collateral-transfer receipts; these are not automatically replayed. Funding and policy changes belong in MetaMask's owner surfaces. The retired local funding UI and its old qualification receipts remain historical reference in `METAMASK_LEGACY_SERVICE_HISTORY.md`.

## Qualification

Offline tests cover provider context, OAuth rotation, journal ownership and reservations, official signing vectors, remote-signature recovery, order replay prevention, cumulative callbacks, cancellation, swap quotes and receipts. Connected Python reads verify actual wallet identity, Guard policy, testnet account, candles and ordinary strategy startup without replaying historical fills. A dependency check reports a compatible Python environment.

Native testnet opening, reduce-only closing and cancellation have funded receipts. Fresh live partial-flow and connected expiry rejection remain unqualified. The connected expiry test explicitly skips active grants; offline expiry regressions remain separate evidence. Managed swaps still require their own funded receipts. Expired local grants remain expired; prior JavaScript test results are not carried forward. No production or mainnet trading qualification is claimed.

Protocol sources: [MetaMask architecture](https://docs.metamask.io/agent-wallet/reference/architecture/), [browser authorization](https://docs.metamask.io/agent-wallet/cli-setup/), [Hyperliquid API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint). Remote-wallet interfaces were checked against permissively licensed Fox SDK 2.10.0 declarations; Hyperliquid action serialization uses the official MIT Python SDK 0.24.0. No restricted Agent SDK implementation is translated or bundled. MetaMask's remote interfaces remain an experimental compatibility contract.

## Execution and dependency review

Hyperliquid's signing domain uses chain ID 1337 for both networks; its signed `source` binds testnet or mainnet. Managed requests must match that domain rather than an Arbitrum bridge chain. Provider validation rejections and Shield failure codes remain durable and never cause automatic reposts or venue submission.

Owner revocation is serialized with the final external handoff after network preparation. An expired uncertain cancellation can receive a new cancellation identity only after a fresh venue read confirms the same original order remains live. Expired uncertain leverage actions reconcile current venue settings and reject the unsubmitted intent without replay; use a new order ID afterward. Public order refresh and polling share cumulative fills and callbacks.

Live partial-fill remainders come from identity-checked open-order snapshots; `orderStatus` can retain the original size while an order remains open. Terminal fill refresh is read-only, preserves closed lifecycle, and emits only new execution deltas. Inconsistent execution evidence blocks new reservations until reconciled.

Polling defaults to 10 seconds. Wallet sweeps share fill and live-order reads; unchanged closed orders avoid individual status requests. Increase `POLL_INTERVAL` for large histories or multiple brokers sharing an IP because Hyperliquid applies [weighted limits per IP](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits). Under the [unified-account balance contract](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes), cash reports unheld USDC collateral, not guaranteed order capacity or the value of unrelated spot assets.

Atomic token quantities and receipt output checks remain exact regardless of a strategy's decimal context. A managed swap cannot be canceled locally because MetaMask may still broadcast it. Reverted transactions and earlier approvals retain gas fees and conservative daily reservations.

OP Stack fee estimates include L1 data and operator charges from block-pinned oracles. Execution gas has a frozen ceiling; separate rollup fees are current estimates, not signer-enforced caps. Actual overruns preserve fills and costs, charge an adjusted USD reservation at the frozen native-token price, and block further spending. After reviewing the exact `feeBudgetOverrun`, the owner may call `client.acknowledge_fee_overrun(operation_id, expected_overrun)`. Acknowledgment never erases actual daily usage. Legacy overruns without verified USD valuation remain held.

Install in a clean environment, or remove the retired `polygon-api-client` before upgrading. Its old CA-store constraint prevents current certificate fixes. LumiBot now uses the maintained Massive SDK while retaining `POLYGON_API_KEY` and `api.polygon.io`. The MetaMask extra requires patched `urllib3` and `certifi`. Session locks reject unsafe ownership, symlinks and hardlinks before acquisition; private owner-controlled state directories remain required. This does not claim that every optional or base dependency advisory is resolved.

New signature requests use the same fixed provider-origin metadata as MetaMask's high-level wallet adapter, with LumiBot identified separately in the durable client provenance and readable intent. Recovery validates each request's original provider origin. There is no alternate-origin retry after a failure. Guard Mode and exact policy checks remain enabled.

Native broker qualification has exercised actual testnet ALO placement/cancellation and market opening/reduce-only closing through Python on two dedicated wallets. A real Strategy/Trader ETH round trip delivered unique fill callbacks and reconciled ending cash exactly, including fees. The installed wheel passed native regression and connected-history tests on Python 3.10 and 3.12 with JavaScript runtimes absent from PATH and child processes denied during connected checks. The current wheel also recovered an actual canceled-partial receipt with matching venue fills and fees and no replayed callbacks. Fresh live partial-flow attempts remain inconclusive; no production or mainnet-swap qualification is claimed. Earlier JavaScript receipts remain historical evidence only.

## Frequently asked questions

### Do I need Node or a local MetaMask service?

No. Install `lumibot[metamask]` and configure the native session, wallet and journal. Managed signing and venue requests run through Python.

### Who holds the wallet keys and enforces spending limits?

MetaMask retains wallet keys. Guard Mode enforces its reviewed provider policy; local notional limits are cooperative process controls. Use a dedicated wallet with bounded funding and keep Guard Mode enabled.

### Can I preserve the previous integration's journal?

Yes. Retain the exact state file, agent and wallet identity. Known venue orders can reconcile; ambiguous legacy effects remain explicit holds. Old service tokens are not MetaMask access tokens, and startup never renews authority.

### What should I do after an approval timeout or unknown outcome?

Inspect the recorded operation with `broker.client.get_order(operation_id)`. Reuse its identity and reconcile the original request before creating another trade. Do not bypass Guard Mode or increase permissions to clear a failure.

### Does testnet qualification establish production swap readiness?

No. Native perps have funded testnet evidence, with the remaining limits recorded above. Mainnet trading and funded native swaps remain unqualified; earlier JavaScript receipts do not establish Python readiness.
