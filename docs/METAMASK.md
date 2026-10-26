# MetaMask broker (local beta)

LumiBot's `MetaMask` broker uses MetaMask Agent Wallet for unattended execution from a **dedicated, separately funded server wallet per agent**. Its signing keys stay with MetaMask's server-wallet infrastructure. Normal browser wallet connection uses **MetaMask Connect**, the maintained replacement for the older MetaMask SDK.

This is an optional integration. Python strategies call a local Node service; installing LumiBot alone does not install Node or create wallets. Connected wallet execution remains a qualification step, not something unit tests establish.

## Install and connect

Use the `integrations/metamask` directory from the **same LumiBot revision** as the Python adapter. Node 22 (22.18+) or Node 24 is required; Node 24 is recommended for the pinned dependency tree.

```sh
cd integrations/metamask
npm ci --ignore-scripts
npm run build
npm start
```

Open the localhost URL printed by the service. Unlock setup with its local `admin-token` file. In setup:

1. Open the official MetaMask Agent Wallet login page. Complete sign-in and import its CLI token **into the local page**, never into a strategy or chat.
2. Create a named server wallet for the agent. Review the provider's Guard Mode policy, including network/address allowlists and outflow limits. Policy changes that broaden access can require MetaMask approval. Reload the policy to confirm it applied.
3. Set allowed markets, chains, exact token contracts/decimals, trade-notional limits, leverage, slippage, and expiry. Defaults are testnet, $10 per order, $25 per rolling 24 hours, 1× leverage, 50 bps slippage, and 24-hour expiry. These are editable defaults, not trading advice.
4. Create a scoped token. Save the displayed broker configuration in your secret manager. The service stores only the token hash; its UI displays the raw token once.
5. Fund only the agent wallet. The normal MetaMask Connect flow can request a user-approved testnet ETH transfer. Hyperliquid testnet collateral is separate from native gas. A submitted funding hash is not confirmation.

The service binds to `127.0.0.1`. Keep it local or use an authenticated SSH tunnel. No public deployment, wallet creation, funding, or mainnet trade happens merely by starting it. `--state-dir` selects a dedicated private state directory; `--port` changes the port. Mainnet bindings require the explicit `--allow-mainnet` startup flag and owner configuration. The setup UI deliberately creates testnet bindings.

## Python usage

Pass the configuration displayed by setup to the broker. The agent-scoped token and expected wallet/network are required. Do not give strategies the setup admin token or MetaMask CLI session.

```python
from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order

broker = MetaMask({
    "BROKER_URL": "http://127.0.0.1:8766",
    "BROKER_TOKEN": "<agent-scoped-token>",
    "WALLET_ADDRESS": "<agent-wallet-address>",
    "NETWORK": "testnet",
    "MODE": "perp",
})

# Within a normal LumiBot strategy, submit through self.submit_order().
asset = Asset("BTC", asset_type="crypto_future", leverage=1)
order = Order(
    "strategy-name", asset, "0.0001", "buy",
    quote=Asset("USDC", asset_type="crypto"),
    order_type="limit", limit_price="<limit-price>", time_in_force="gtc",
    custom_params={"reduce_only": False},
)
```

Set the strategy constructor’s `quote_asset=Asset("USDC", asset_type="crypto")` so account valuation uses the venue settlement currency.

The quantity example is illustrative, not a testnet minimum or suggested trade. The venue and configured limits determine whether an order is accepted. Market orders persist a slippage-bounded execution price and submit an IOC limit against that price, rather than repricing the authority at signing time. Limit orders require explicit `gtc`, `ioc`, or `alo`; `day` is rejected rather than silently changed into a perpetual order. Only buy/sell, market/limit, and explicit reduce-only behavior are supported. No conditional, bracket, OCO, or automatic cancel/replace emulation.

Environment-based selection supports `TRADING_BROKER=metamask`, `DATA_SOURCE=metamask`, `METAMASK_BROKER_URL`, `METAMASK_BROKER_TOKEN`, `METAMASK_WALLET_ADDRESS`, `METAMASK_NETWORK`, and `METAMASK_MODE`. Explicit constructor configuration is useful when several agents run in the same process.

`MetaMaskData` supplies perpetual marks and closed minute/hour/day candles from the selected Hyperliquid network. Spot prices require explicit token identity and a reliable provider USD valuation. No substitute provider or synthetic test-token dollar peg is used. Unsupported spot candle history and account equity history raise explicit errors.

## Exact-input swaps

Set `MODE="spot"` and register both assets in the owner allowlist:

```python
from lumibot.entities import Asset

source = Asset("ETH", asset_type="crypto", chain_id=11155111,
               token_address="native", decimals=18)
target = Asset("TOKEN", asset_type="crypto", chain_id=11155111,
               token_address="<issuer-verified-token-contract>", decimals=6)

operation = broker.swap(source, target, "0.001", slippage_bps=50,
                        identifier="stable-operation-id")
status = broker.get_swap(operation["id"])
```

Use real contract metadata; placeholder contracts intentionally fail validation. `input_amount` is the amount spent, not an exact output quantity. `swap()` returns a durable operation rather than pretending an exact-input swap is an exact-output LumiBot buy order. Swaps require a same-chain provider route. Missing testnet liquidity or reliable USD pricing is a supported refusal, not permission to trade on mainnet.

Identity is `(chain_id, token_address)` for bound EVM assets; ticker aliases do not create another token. Decimals are validated metadata, not identity. Existing unbound assets retain their previous serialization shape and identity.

The service verifies quote wallet, recipient, chain, contracts, decimals, input amount and slippage. Sequential ERC-20 approvals are capped to the requested input amount, preserving zero-reset transactions. Raw transaction, arbitrary-message signing, bridge, withdrawal, and token-transfer endpoints are unavailable to bot tokens. Gasless EIP-7702 execution routes are currently refused before submission.

## Limits and trust boundary

The runtime token binds to one immutable wallet, network, and agent ID. Another agent cannot reuse that wallet binding. Before signing, the service verifies remote ownership, Guard Mode, and a digest of the owner-reviewed provider policy. Changes require owner review and a new binding token.

SQLite reservations make duplicate IDs idempotent, account for outstanding orders, and retain uncertain submissions across restarts. A new request with the same ID but different content is rejected. Per-order and rolling daily limits apply to **pre-trade notional valuation**; they do not cap gas, funding payments, liquidation losses, or portfolio drawdown. Canceled orders conservatively retain their daily reservation. Keep the wallet's actual funding and provider outflow limits small enough for the intended risk.

**This is a service boundary, not a cryptographic sandbox.** The provider CLI session can control multiple wallets. Strategies must not be able to read that session, the admin token, or the service database. Run the service under a separate OS identity/container when strategies are untrusted, exposing only the authenticated loopback connection or tunnel. Separate funded wallets limit cross-agent exposure; they are not provider-issued wallet-scoped credentials.

Guard Mode can ask for 2FA on out-of-policy requests. MetaMask's documented outflow accounting does not cover every signature/allowance risk; application limits do not replace provider policy or onchain checks. Browser ERC-7715 capabilities and grants can be inspected, but **this broker does not request or redeem browser-wallet delegations**. Its autonomous path uses the dedicated Agent Wallet.

## Receipts, approvals and recovery

- `prepared` is a durable reservation. `unknown` is persisted before external submission. Neither is a fill.
- `awaiting_approval` retains the MetaMask signing-job ID. Browser sign-in approvals arrive by email; QR sign-in approvals appear in MetaMask Mobile notifications. Do not resubmit while approval is pending.
- `submitted` means accepted/broadcast, not mined. Swaps require mined transfer evidence. Perpetual fills use paginated venue fill records, not quote estimates; partial fills emit only the new quantity.
- Allowance transactions and trade transactions are separate. When an interrupted allowance later confirms, setup offers **Resume swap with fresh quote**. It rechecks policy and valuation against the existing reservation. An uncertain trade is never automatically replayed.
- A timeout without a known job/hash/order ID stays unknown. Inspect the provider wallet and venue before any replacement. This version does not guess an order match or offer an owner override that declares an unknown trade failed.
- Revoking a binding stops future bot requests. It does not cancel already submitted orders, revoke ERC-20 allowances or browser grants, undo signed messages, or withdraw funds. Setup can cancel known perpetual orders after bot revocation; confirm venue status afterward. Review remaining approvals in MetaMask.
- Preserve the state directory across restarts. Deleting the journal also deletes idempotency and budget history; do not reset it to escape a pending operation.

Perpetual account equity already includes funding. Reported cash is the lesser of spendable collateral and equity minus used margin, floored at zero; this excludes extra spot buying power outside the equity snapshot. `positionsValue` is the equity residual, not leveraged position notional. `get_funding_payments(start_time, end_time, symbol=None)` exposes its ledger (Unix milliseconds) without posting the same cash flow twice. Fill fees remain available in the order's raw operation. Spot cost basis and unrealized PnL are left unavailable; they are not invented from current prices.

## Verification

```sh
# In integrations/metamask
npm test
npm run build
METAMASK_TEST_PUBLIC_TESTNET=1 npm test

# In repository root; dotenv disabled to avoid real account configuration
LUMIBOT_DISABLE_DOTENV=1 python -m pytest tests/test_metamask_broker.py tests/test_asset.py
```

Local tests cover authorization separation, Host/Origin validation, idempotency, racing submissions, limits, expiry/revocation, restart uncertainty, approval persistence, exact chain identity, fill deduplication, partial fills, cancellation, transport loss, and broker lifecycle integration. The opt-in public test queries real Hyperliquid testnet market/candle APIs and submits no transactions.

Before operational use, qualify a funded testnet wallet: login, create/list wallet, policy update/2FA, scoped-token issuance, balances, a small order and receipt, partial fills/cancel where available, explicit approval denial/expiry, service restart during pending work, and revocation. Test swaps only where MetaMask returns a supported route and reliable prices. Hyperliquid faucet eligibility is provider-specific; never fund mainnet just to make a test pass.

## Dependency qualification

The 2026-10-26 production dependency audit reports 119 affected package entries
(10 high, 90 moderate, 19 low; zero critical). Compatible updates were applied.
Remaining findings include nested dependencies pinned by Agent SDK / Fox SDK,
including `bigint-buffer`, old Zod, Axios, viem and ws. Counts include transitive
paths and are not evidence that every advisory is reachable through this broker.
Do not treat the integration as production-qualified until upstream fixes or a
reviewed reachability assessment resolves the relevant findings. Forced major
version overrides have not been used to conceal incompatible vendor pins.

Recheck with `npm audit --omit=dev`. See, for example, the
[bigint-buffer advisory](https://github.com/advisories/GHSA-3gc7-fjrx-p6mg) and
[Zod advisory](https://github.com/advisories/GHSA-mrhx-c7xc-m3w5).
Authenticated execution also remains unverified: mined swap transfer units,
native-token receipt conventions, approval denial/expiry and funded account
reconciliation need the connected testnet qualification described above.

## Sources checked 2026-10-26

- [Agent Wallet quickstart](https://metamask.io/news/metamask-agent-wallet-quickstart)
- [Guard Mode and policy behavior](https://docs.metamask.io/agent-wallet/reference/trading-modes/)
- [Approval recovery](https://docs.metamask.io/agent-wallet/approve-requests/)
- [Supported chains](https://docs.metamask.io/agent-wallet/reference/supported-chains/)
- [MetaMask Connect](https://www.npmjs.com/package/@metamask/connect-evm)
- [Agent SDK](https://www.npmjs.com/package/@metamask/agent-sdk)
- [Hyperliquid API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)

Pinned packages: Agent SDK 7.3.0, Connect EVM 2.1.1, Smart Accounts Kit 2.0.0, viem 2.57.11, Zod 4.6.7. The standalone Agent Wallet CLI is not installed by this integration. Supported APIs were checked against the installed package declarations and official docs. Mobbin finance catalog was consulted; full authenticated flows were not accessible, so no claim of inspecting those flows is made.
