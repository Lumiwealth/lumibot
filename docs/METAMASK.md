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

1. Open the official MetaMask Agent Wallet login page. Complete sign-in and import its CLI token **into the local page**, never into a strategy or chat. Keep the original Google, email, or QR method: each method creates a separate wallet identity even for the same email. Setup shows the current method, offers reconnect, and warns when existing agent wallets are absent from the session. Failed session refresh no longer appears connected.
2. Create a named server wallet for the agent. Review the provider's Guard Mode policy, including network/address allowlists and outflow limits. Policy changes that broaden access can require MetaMask approval. Use **Refresh policy approvals** to see pending, approved, denied or expired requests after a restart, then reload the policy to confirm it applied. An unknown submission remains blocked against duplicate widening requests.
3. Set allowed markets, chains, exact token contracts/decimals, trade-notional limits, leverage, slippage, and expiry. Defaults are testnet, BTC/ETH markets, $25 per order, $100 per rolling 24 hours, 1× leverage, 50 bps slippage, and 24-hour expiry. These are editable defaults, not trading advice.
4. Create a scoped token. Save the displayed broker configuration in your secret manager. The service stores only the token hash; its UI displays the raw token once.
5. Fund only the agent wallet. Use **Hyperliquid testnet collateral → Refresh balances** to inspect actual spot USDC and perps equity. If mock USDC is in spot, enter an amount and choose **Move USDC to perps**. This owner-only action moves collateral within the same bound testnet wallet; bot tokens cannot call it or choose another recipient. Unified accounts pool collateral and do not need this move. Native gas is separate: the optional MetaMask Connect flow requests a user-approved testnet ETH transfer. A submitted funding request is not confirmation; verify balances or a mined receipt.

The service binds to `127.0.0.1`. Keep it local or use an authenticated SSH tunnel. No public deployment, wallet creation, funding, or mainnet trade happens merely by starting it. `--state-dir` selects a dedicated private state directory; `--port` changes the port. Mainnet bindings require the explicit `--allow-mainnet` startup flag and owner configuration. The setup UI deliberately creates testnet bindings.

Optional native gas funding retains an owner-only record before opening the browser wallet confirmation. Setup verifies the selected testnet, sender, recipient and exact wei amount against public transaction data, then waits for a successful receipt and two block confirmations before showing **confirmed**. This is a testnet inclusion check, not parent-chain finality. A transaction hash or a wallet success message alone is insufficient. Receipts marked **reverted** did not deliver ETH. A wallet rejection before a hash is returned is shown separately.

If the selected testnet is missing from MetaMask, setup requests its addition with the documented public RPC, then verifies the wallet's active chain before preparing funding. Rejecting the network request leaves funding unsubmitted.

After a reload, select the agent wallet and refresh gas funding. Pending hashes resume read-only receipt checks. If the browser closed before saving a hash, enter it from MetaMask Activity under the existing funding record. Alternatively, **Review pending funding** opens the same transfer with its original persisted nonce and a fresh buffered fee estimate. It checks the connected sender and selected testnet again. A mined nonce must be reconciled before another wallet request. A mined same-nonce cancellation resolves as **replaced**, without claiming the expected ETH arrived. Unknown outcomes block a new funding intent; setup never automatically sends again. Bot tokens cannot prepare, link or refresh owner funding records. EVM native ETH and Hyperliquid USDC collateral remain separate.

If MetaMask speeds up or cancels a pending transfer, enter the replacement hash. Setup retains the original record until a mined replacement proves the same sender and nonce on the same testnet, with two confirmations. A speed-up confirms funding only when recipient and amount still match. A mined cancellation or other replacement is shown as **replaced**, without claiming the expected ETH was received.

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
    reduce_only=False,
)
```

Set the strategy constructor’s `quote_asset=Asset("USDC", asset_type="crypto")` so account valuation uses the venue settlement currency.

Standard accounts use perps equity and margin. Unified accounts use the actual HyperCore USDC balance in the spot ledger, with holds deducted from cash. That balance already includes unified PnL; per-DEX equity is not added again. Portfolio-margin and unknown account modes raise an explicit error because they require separate multi-asset valuation.

Canonical `Order(..., reduce_only=True)` is preserved, including closing orders created by `close_position()` and `sell_all()`. The older `custom_params={"reduce_only": True}` spelling also works, but cannot turn off canonical reduce-only intent. Restored orders retain this flag.

The quantity example is illustrative, not a testnet minimum or suggested trade. The venue and configured limits determine whether an order is accepted. Market orders persist a slippage-bounded execution price and submit an IOC limit against that price, rather than repricing the authority at signing time. Limit orders require explicit `gtc`, `ioc`, or `alo`; `day` is rejected rather than silently changed into a perpetual order. Only buy/sell, market/limit, and explicit reduce-only behavior are supported. No conditional, bracket, OCO, or automatic cancel/replace emulation.

Limit-order valuation, execution and recovery use the SDK's normalized venue price. If that normalization would raise a buy limit or lower a sell limit, the request is rejected before signing; choose a venue-valid price instead.

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

Use real contract metadata; placeholder contracts intentionally fail validation. `input_amount` is the amount spent, not an exact output quantity. `swap()` returns a durable operation rather than pretending an exact-input swap is an exact-output LumiBot buy order. Swaps require a same-chain provider route. Agent SDK 7.3.0's swap routing supports mainnet chains only: the Sepolia example demonstrates asset identity and is explicitly refused before pricing or signing. Testnet wallet balances and signatures remain supported. Do not switch to mainnet merely to make a test pass.

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
- Perpetual openings check available collateral before signing. Each internal SDK signature (including leverage setup) rechecks local and provider authority, and checks again after the signing wait before releasing the signature to the venue. Pending signature IDs survive interruption. A completed signature that was never released is not automatically replayed; the interrupted order becomes rejected when its signature job resolves. A prior leverage change may already have applied. Once a signature is released, an uncertain venue response still requires manual reconciliation.
- `submitted` means accepted/broadcast, not mined. Swaps require mined transfer evidence. Perpetual fills use paginated venue fill records, not quote estimates; partial fills emit only the new quantity.
- Allowance transactions and trade transactions are separate. When an interrupted allowance later confirms, setup offers **Resume swap with fresh quote**. It rechecks policy and valuation against the existing reservation. An uncertain trade is never automatically replayed.
- A timeout without a known job/hash/order ID stays unknown. Inspect the provider wallet and venue before any replacement. For an uncertain perpetual submission, owner setup offers **Link venue order**. Enter the exact ID from the same wallet's Hyperliquid history. The service verifies the wallet/network lookup, symbol, side, original quantity, bounded price, order type, reduce-only intent and submission time before linking and refreshing actual fills/status. An ID already linked elsewhere is refused. This read-only recovery never signs, replays an order or lets an owner declare an unknown trade failed without venue evidence. Bot tokens cannot call it.
- Revoking a binding stops future bot requests. It does not cancel already submitted orders, revoke ERC-20 allowances or browser grants, undo signed messages, or withdraw funds. Setup can cancel known perpetual orders after bot revocation; confirm venue status afterward. Review remaining approvals in MetaMask.
- Owner collateral moves also retain stable request IDs and pending signing jobs. Repeating an ID never signs again. Interrupted preparation is rejected before signing; interrupted signatures resolve without replay. Once a signature may have reached the venue, an unknown funding outcome blocks further wallet execution until independently reconciled. No automatic retry or balance-based guess declares success.
- Preserve the state directory across restarts. Deleting the journal also deletes idempotency and budget history; do not reset it to escape a pending operation.

Perpetual account equity already includes funding. Standard-account cash is the lesser of spendable collateral and equity minus used margin, floored at zero; this excludes extra spot buying power outside the equity snapshot. Unified-account cash deducts spot-ledger holds from pooled USDC, floored at zero. `positionsValue` is the equity residual, not leveraged position notional. `get_funding_payments(start_time, end_time, symbol=None)` exposes its ledger (Unix milliseconds) without posting the same cash flow twice. Fill fees remain available in the order's raw operation. Spot cost basis and unrealized PnL are left unavailable; they are not invented from current prices.

## Verification

```sh
# In integrations/metamask
npm test
npm run build
METAMASK_TEST_PUBLIC_TESTNET=1 npm test

# In repository root; dotenv disabled to avoid real account configuration
LUMIBOT_DISABLE_DOTENV=1 python -m pytest tests/test_metamask_broker.py tests/test_asset.py tests/test_public_docs_discovery.py tests/test_docs_broker_faq_callout.py

# Connected reads: private JSON array of setup responses for two testnet agents.
# This suite never submits a trade or signature and skips without configuration.
LUMIBOT_DISABLE_DOTENV=1 METAMASK_TEST_CONFIG_FILE="<private-testnet-config-file>" \
  python -m pytest tests/test_metamask_apitest.py -q
```

Local tests cover authorization separation, Host/Origin validation, idempotency, racing submissions, limits, expiry/revocation, restart uncertainty, approval persistence, exact chain identity, fill deduplication, partial fills, cancellation, transport loss, and broker lifecycle integration. The opt-in public test queries real Hyperliquid testnet market/candle APIs and submits no transactions.

The MetaMask companion CI workflow installs with scripts disabled and runs formatting, tests, and browser bundling on Node 22.18 and Node 24. It uses no wallet credentials or funded account. Public API and connected-wallet checks remain explicit opt-in qualification steps.

Before operational use, qualify a funded testnet wallet: login, create/list wallet, policy update/2FA, scoped-token issuance, balances, a small order and receipt, partial fills/cancel where available, explicit approval denial/expiry, service restart during pending work, and revocation. Test swaps only where MetaMask returns a supported route and reliable prices. Hyperliquid faucet eligibility is provider-specific; never fund mainnet just to make a test pass.

### Qualification status, 2026-10-30

| Path | Evidence | Remaining qualification |
| --- | --- | --- |
| Agent Wallet login, creation and Guard Mode | Two real, separate server wallets created and read back; provider approved adding HyperEVM testnet to one wallet, with Guard Mode and zero rolling outflow unchanged | Broader provider approval cases |
| Signing | Harmless message and EIP-712 verification signatures checked cryptographically against the real wallet; new signing boundary exercised | MFA denial and approval during an actual trade |
| Account and market reads | Connected Python broker checks pass for standard and unified wallets, including real pooled USDC, held margin, positions/PnL, fills/fees, empty EVM spot portfolios, marks and closed candles | Portfolio margin remains explicitly unsupported |
| Isolation and revocation | Wrong wallet/admin credentials denied; revoked and superseded tokens rejected; replacement token accepted | Deployment isolation for untrusted strategies |
| Policy recovery | Real expired, approved and denied requests observed through the broker; email approval changed only the requested testnet chain, and email rejection left the other wallet's policy unchanged | Approval/denial during an actual funded trade and restart during a pending approval |
| Perpetual execution | Real market open/reduce-only close with multiple fills and fees, full strategy iteration with fill callbacks, resting order/restart/cancel, and request-ID deduplication; restored cumulative fills are not replayed | Persistent partial-fill cancellation and approval interruption during funded execution |
| Spot execution | Exact-input guards and unsupported testnet route refusal | Mined receipts require a supported route; current provider offers no testnet swap route |
| Setup and funding | Connected owner UI, real same-wallet testnet spot-to-perps move, native testnet ETH funding to two wallets with independent exact-wei receipts, same-nonce recovery after a stale-fee failure, wallet rejection, narrow/wide layouts and service-failure recovery | Parent-chain finality and broader wallet-provider cases |

Hyperliquid perpetual orders use typed-data signatures. MetaMask [excludes signatures from its outflow-limit calculation](https://docs.metamask.io/agent-wallet/reference/outflow-policy/), so a zero outflow limit does not force MFA for these orders. Real testnet orders signed without an approval challenge. The signing-boundary tests cover pending, denied and expired jobs, but those simulated cases do not establish a real funded-trade MFA interruption.

Hyperliquid's official faucet requires prior mainnet deposits from the same address. Its Arbitrum Sepolia bridge uses a distinct USDC2 contract, not the token issued by the ordinary Circle testnet faucet. The verified USDC2 source has owner-only minting and no public claim function despite exposing `dripAmount` and `dripClaimed` getters. A transfer of existing Hyperliquid testnet collateral from another testnet participant avoids mainnet deposits. The funded tests above used mock collateral; a faucet or native-gas receipt alone does not prove trading collateral.

Alternate funding research found these constraints, checked on 2026-10-26:

| Route | Constraint |
| --- | --- |
| Chainstack HyperEVM faucet | API key plus recipient's existing mainnet ETH balance/history; not available to a fresh wallet |
| QuickNode HyperEVM faucet | Fresh test wallet was refused for insufficient mainnet balance |
| Alchemy HyperEVM faucet | Recipient must already hold at least 0.01 mainnet HYPE |
| Thirdweb HyperEVM faucet | Login and 0.01 test HYPE per day; useful for deposit gas, insufficient for a regular minimum-notional spot sale at the observed bid |
| Circle Arbitrum Sepolia USDC | Different token from the legacy Hyperliquid test bridge's collateral; cannot substitute on that bridge |
| Circle HyperEVM testnet USDC | Permissionless faucet delivered 20 USDC; onchain balance and successful transfer receipt verified. CoreDepositWallet's token getter matches Circle's contract, but Circle documents a mainnet-history requirement for testnet HyperCore recipients |

The native HyperEVM route is separate from the legacy Arbitrum USDC2 bridge:
Circle's HyperEVM test USDC uses CoreDepositWallet. However, Circle explicitly
documents that testnet HyperCore recipients must already exist on mainnet and
that transfers to missing recipients fail silently. Both fresh test wallets
returned `role: missing` from Hyperliquid's mainnet info API, so no deposit was
submitted. This restriction also applies to CCTP forwarding from another chain;
creating an Arc funding wallet does not remove it. Faucet funding and even a
successful EVM receipt do not prove destination HyperCore credit. The incoming
test-USDC receipt was verified through RPC; the current MetaMask Account API
returned HTTP 400 for that HyperEVM testnet transaction.

An eligible existing wallet could also claim test HYPE, transfer it to the test agent,
and potentially sell it on the HyperCore spot book for test USDC. This route
requires enough HYPE after gas, a supported EVM-to-Core transfer, venue minimum
order size and actual liquidity. It is not yet qualified. Do not assume a gas
faucet also supplies trading collateral or deposit real funds automatically.

## Runtime ownership and restart

Restored order snapshots seed fill and terminal-state checkpoints once. Later
account synchronization does not advance an already tracked order's callback
checkpoint. This prevents canceled/rejected history from being appended again
and keeps new partial-fill quantities available to the polling stream.

One companion owns each journal. A second process using the same state directory
fails with `STATE_IN_USE`, even on a different port. Engines sharing a Store
also share wallet execution locks, so another engine cannot mark an in-flight
signature rejected while its owner is waiting for approval. Session refresh
uses the same crash-safe lock mechanism instead of a stale directory lock.

Ownership uses a separate SQLite exclusive transaction on a local filesystem.
The OS releases it when the process dies; there is no expiring lease that could
admit a second signer while the first is paused. Keep the state directory on a
local filesystem, never delete or replace lock files while running, and stop all
older companion versions before upgrading. Do not copy an active journal or
run independently copied state for the same wallet. Multi-host active/active
operation is not supported. Tests cover duplicate processes, abrupt owner death,
restart, independent journals and concurrent engine reconciliation.

## Dependency qualification

The 2026-10-30 clean-install audit reports 66 affected production package
entries: 6 high, zero moderate, 60 low, and zero critical. A scoped override
updates the Solana vendor HTTP JSON-RPC client to Jayson 5.0.0, removing
its vulnerable `stream-json` dependency. Jayson 5 changes TCP/TLS framing;
the SDK consumer uses its browser HTTP entry point. Actual vendor HTTP
success, RPC errors, malformed JSON, notifications and request IDs pass
regressions on Node 22.18 and 24. Browser bundling, connected broker reads
and a real managed EIP-712 signature also pass after clean installation. Legacy UUID 8/9 vendor
paths use the patched 11.1.1 release, which retains the CommonJS entry points
used by the SDK. The lockfile also records the legacy WebSocket optional
UTF-8 validation peer required for clean installs with npm 10 and npm 11. Vendor JSON-RPC request-ID behavior, buffer-bound checks,
browser bundling, and real managed-wallet EIP-712 signing were verified after
the change. The [UUID advisory](https://github.com/advisories/GHSA-w5hq-g745-h8pq)
is no longer present. The audit still reports upstream issues in
`bigint-buffer`, nested Zod 3, `elliptic`, and `deepmerge`.

The 2026-10-26 production dependency audit reports 107 affected package entries
(6 high, 77 moderate, 24 low; zero critical), down from 119 entries and 10 high.
Scoped overrides update ws, bn.js, the Axios 0.x security branch and the affected
transitive esbuild version. A real local HTTP test checks the vendor's Axios
wrapper request and error contracts after the backport; the main broker's
authenticated testnet reads also pass after a clean install.

The earlier remaining root advisories concerned `bigint-buffer`, nested Zod 3, `elliptic`,
`stream-json` and old `uuid`. Counts include transitive paths and are not evidence
that every advisory is reachable through this broker. Current assessment:

- The `bigint-buffer` high advisory targets its native addon. A clean
  `npm ci --ignore-scripts` installation uses the package's JavaScript fallback.
  The companion defaults to skipping install scripts and checks the same native
  binding search paths before initializing any managed-wallet provider. A native
  addon or an indeterminate lookup failure stops startup with reinstall guidance.
  This enforces the supported installation, not an upstream package fix.
- Nested Zod 3 remains in Fox SDK 2.10.0. The broker uses patched Zod 4 at
  its boundary. Published-source inspection found five Fox modules importing
  Zod 3 and no array-schema constructors in those modules. The reported
  oversized-array validation path was not found in this pinned code. This is
  a limited static assessment, not an upstream fix or a guarantee for custom
  plugins or future SDK versions. Do not force Zod 4 into Fox: its tool-schema
  introspection uses Zod 3 internals.
- `stream-json` is absent after the scoped Jayson upgrade. `elliptic` and
  `deepmerge` retain legacy vendor paths. A complete call-path assessment
  is still required. No blanket
  "unreachable" exception is granted merely because Solana and prediction-market
  trading are not exposed by this broker.

Do not treat the integration as production-qualified until upstream fixes or a
reviewed reachability assessment resolves the relevant findings. Forced major
version overrides have not been used to conceal incompatible vendor pins.

Recheck with `npm audit --omit=dev`. See, for example, the
[bigint-buffer advisory](https://github.com/advisories/GHSA-3gc7-fjrx-p6mg) and
[Zod advisory](https://github.com/advisories/GHSA-mrhx-c7xc-m3w5).
Funded Hyperliquid testnet qualification has verified market opening,
reduce-only closing, multiple fill aggregation and fees, resting-order
cancellation, service restart recovery, stable request-ID deduplication, and
a full strategy-executor trading iteration with actual fill callbacks. Connected read-only checks also run a real strategy startup and verify that historical fills never replay. Snapshot registration belongs to the MetaMask adapter; the shared executor and its native qualification fingerprint remain unchanged. Startup restores cumulative fill history without adding total order size a second time.
Persistent partial-fill cancellation, approval interruption/denial during
funded perpetual execution and mined swap transfer units remain separate
qualification items. A separate funded EVM transaction reached real MFA,
remained queryable after its SDK client restarted, was denied through
the provider approval page, and a separate pending request expired after
its client process exited. Both left the Guard policy unchanged. This does not
qualify the perpetual-signature approval path. Testnet spot swap routing
is unavailable in the pinned provider SDK.

## Sources checked 2026-10-26

- [Agent Wallet quickstart](https://metamask.io/news/metamask-agent-wallet-quickstart)
- [Guard Mode and policy behavior](https://docs.metamask.io/agent-wallet/reference/trading-modes/)
- [Approval recovery](https://docs.metamask.io/agent-wallet/approve-requests/)
- [Supported chains](https://docs.metamask.io/agent-wallet/reference/supported-chains/)
- [MetaMask Connect](https://www.npmjs.com/package/@metamask/connect-evm)
- [Agent SDK](https://www.npmjs.com/package/@metamask/agent-sdk)
- [Hyperliquid API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [Hyperliquid testnet faucet requirements](https://hyperliquid.gitbook.io/hyperliquid-docs/onboarding/testnet-faucet)
- [Chainstack HyperEVM faucet requirements](https://faucet.chainstack.com/hyperliquid-testnet-faucet)
- [Alchemy faucet eligibility](https://www.alchemy.com/docs/faucets)
- [Thirdweb HyperEVM testnet faucet](https://thirdweb.com/HyperEVM-testnet)
- [Circle HyperCore testnet recipient restrictions](https://developers.circle.com/cctp/concepts/cctp-on-hypercore)
- [Circle native USDC addresses](https://developers.circle.com/stablecoins/usdc-contract-addresses)
- [Axios 0.34 security backport](https://github.com/axios/axios/releases/tag/v0.34.0)

Pinned packages: Agent SDK 7.3.0, Connect EVM 2.1.1, Smart Accounts Kit 2.0.0, viem 2.57.11, Zod 4.6.7. The standalone Agent Wallet CLI is not installed by this integration. Supported APIs were checked against the installed package declarations and official docs. Mobbin finance catalog was consulted; full authenticated flows were not accessible, so no claim of inspecting those flows is made.
