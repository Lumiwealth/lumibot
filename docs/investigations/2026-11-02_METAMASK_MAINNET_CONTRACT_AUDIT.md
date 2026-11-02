# MetaMask mainnet contract audit

Primary-source comparison of the native broker's testnet proof and mainnet requirements.

Last Updated: 2026-11-02

Status: Research complete; account-mode and finality repairs under validation. No funded mainnet qualification.

Audience: Broker contributors and operators.

## Overview

Reviewed native provider, venue, execution, journal, swap and fee code at `716d229dcd17629ee2208750571635669857680e`, then current first-party documentation and SDK source. Public unauthenticated metadata reads used no account, session, journal or signing credentials. Existing testnet strategy proof does not establish mainnet liquidity, account policy, execution or swap settlement.

## Contracts already handled

| Area | Mainnet/testnet contract | Native implementation assessment |
|---|---|---|
| API routing | Identical request interfaces use separate `api.hyperliquid.xyz` and `api.hyperliquid-testnet.xyz` origins. [API reference](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api) | Explicit network required; no network fallback or redirected write. |
| Signing | L1 trading actions use EIP-712 domain chain `1337` on both networks; phantom-agent source is `a` on mainnet and `b` on testnet. These differ from HyperEVM chain IDs `999`/`998`. [Official signing source](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py), [HyperEVM](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm) | Official signing primitives, source/domain validation and signature recovery already enforce the selected managed wallet. Do not substitute EVM chain IDs into perpetual signatures. |
| Identity and custody | Hyperliquid API-wallet signers differ from the account whose positions are queried. Vault/subaccount execution needs its own target address. [Nonce/account reference](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets) | This adapter uses its dedicated managed wallet as signer/account, verifies project/custody, and sends no vault target. Delegated API wallets and vaults are outside this contract. |
| Nonces and expiry | Nonces are unique per signer, bounded by chain time, and shared across subaccounts signed by that signer. Expired actions are rejected. [Nonce reference](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets), [Exchange reference](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint) | Durable monotonic nonces, one journal owner, frozen action expiry and no replay after uncertain handoff already address this. Separate network authority and journals; changing endpoints is not permission migration. |
| Assets and orders | Asset IDs come from selected-network metadata. Perpetual prices allow five significant figures and at most `6-szDecimals` decimal places; integer prices are exempt. Sizes use `szDecimals`. [Asset IDs](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids), [Precision rules](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size) | Dynamic metadata, exact quantities, supported TIF, frozen IOC slippage bounds, reduce-only intent and delisting checks already exist. Market orders can partially execute or leave canceled remainder; tighter limits do not guarantee liquidity. |
| Fees | Actual fees depend on volume, staking and market type; HIP-3 can add deployer fees. [Fee rules](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees) | Uses current user maker/taker rates, rechecks the envelope before release and records receipt fees. HIP-3 writes are held instead of assuming ordinary fees or USDC collateral. |
| Managed authority | Guard Mode enforces allowlists and can require approval; signatures are excluded from provider outflow accounting. [Trading modes](https://docs.metamask.io/agent-wallet/reference/trading-modes/), [Outflow policy](https://docs.metamask.io/agent-wallet/reference/outflow-policy/) | Guard/policy identity, explicit local grants, expiration and post-approval checks exist. Local Python grants are cooperative controls, not provider-enforced derivative loss or notional caps. Dedicated funding remains necessary. |

## Verified gaps and bounded follow-up

1. **Repaired: account-mode execution bypass.** At the baseline, `venue.account()` rejects portfolio/unknown modes, but direct submission bypassed it. Execution now validates account support before reserving a new intent and again before non-cancel handoff after approval. Cancellation and original-order reconciliation remain available. Negative controls cover both networks and account-mode changes during signing. Official modes distinguish standard, unified and portfolio accounts; unified balances/holds belong to spot state. [Account modes](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes)

2. **Repaired: inclusion-only swap settlement.** The baseline accepted one extra block. Receipts now require a finalized header, complete block hashes and matching transaction/receipt/canonical-header heights. Unsupported or pending finality cannot become a fill. Older filled records without provenance enter a durable unknown/read-only lane before validation; RPC receipt/output/fee agreement upgrades their provenance without resubmission. Malformed history preserves execution, fees and reservations and holds new spending. Finality is the selected RPC/chain contract, not a cross-chain withdrawal guarantee. [Ethereum RPC](https://ethereum.org/developers/docs/apis/json-rpc/#the-block-parameter)

3. **Medium: cross-DEX unified exposure remains outside account inventory.** `venue.account()` reads default-DEX positions, while unified same-collateral cross positions share margin across DEXs. A portfolio-mode guard alone does not fix this. The supported scope needs a dedicated default-DEX wallet without external positions/orders; otherwise inspect all relevant DEX exposure before making wallet-wide flatness or available-risk claims. REST documents `perpDexs` plus per-DEX `clearinghouseState`; the aggregate `allDexsClearinghouseState` is documented as a WebSocket subscription, not a REST helper in the current Python SDK. [Margin modes](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining), [Perpetual REST](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals), [SDK reads](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/info.py), [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)

4. **Medium: rate handling needs bounded load.** REST shares 1,200 weight/minute per IP; most info reads cost 20 and account/order-status reads cost 2. Current `_post()` reports read errors but has no local weighted scheduler or read backoff. Keep safe polling/load bounds and distinguish rate-limit diagnostics; any retry must remain read-only. Never retransmit a mutation whose receipt is uncertain. Address action quotas and unified-account daily action limits are separate. [Rate limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)

5. **Lower: preflight small opening notional.** The venue documents a $10 perpetual minimum and returns explicit margin, reduce-only, post-only and liquidity errors. Local order construction currently checks precision but not minimum notional. This is an avoidable signed rejection, not silent execution. Do not add an indiscriminate minimum rule to residual reduce-only cleanup without verifying its actual venue contract. [Errors](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)

## Public metadata observations

Point-in-time `meta` reads on 2026-11-02 returned 235 mainnet and 213 testnet markets. BTC/ETH indices were `0/1` versus `3/4`; size decimals were `5/4` on both, and default collateral token was `0`. No returned market violated the current parser. These observations support network-specific discovery, not fixed IDs. [Metadata contract](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)

`perpDexs` returned 10 named mainnet DEXs versus 276 testnet DEXs. Inventory plus one account-state read per named DEX would cost at least 40 versus 572 weight before other reads. Exhaustive per-DEX scans on every cash poll are inappropriate; these counts can change. [DEX inventory](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals), [Weights](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)

## Economic and swap limits

Mainnet liquidation behavior differs concretely: partial liquidation starts above 100,000 USDC, versus 10,000 on testnet; the first tranche is 20%. Liquidation depends on mark prices and margin. Hourly funding changes balances independently of fills. These are venue events, not local grant spending, and must not be synthesized or treated as prevented by low configured leverage. [Liquidations](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations), [Funding](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding)

MetaMask's chain list distinguishes network identity, per-chain product availability and threat-scanning coverage. Its CLI can bridge or use gasless EIP-7702 routes; native LumiBot intentionally rejects those routes and requires a same-chain exact-input transaction with independently verified token identity, gas budget and received output. Preserve those boundaries. Mainnet gas funding, contract approvals, chain-specific fee models, quote expiry and finality require independent qualification. [Supported chains](https://docs.metamask.io/agent-wallet/reference/supported-chains/), [Swap routes](https://docs.metamask.io/agent-wallet/guides/swap-and-bridge/)

No mainnet account reads, signatures, transactions, funding, merge or release were performed. Protocol research and deterministic tests can close code defects; they cannot prove funded mainnet execution.
