# Polymarket International CLOB Live Proof

One-line description: Verified credential, data, WebSocket, and live-submit status for LumiBot's international Polymarket CLOB adapter.

Last Updated: 2026-07-01

Status: Read-only account/data/WebSocket proof complete; live order submission blocked by Polymarket account/API-key binding.

Audience: LumiBot maintainers, broker-adapter implementers, BotSpot/Bot Manager follow-on engineers

## Overview

This document records the current verified state of the Polymarket international `polymarket.com` CLOB integration. It
supersedes earlier planning notes that said credentials had not been created or tested.

Scope is international CLOB only. Polymarket US remains a later, separate adapter because account access, credential
shape, and compliance/product flow are materially different.

## Source Documents Checked

- Polymarket CLOB authentication docs: `https://docs.polymarket.com/api-reference/authentication`
- Polymarket deposit-wallet docs: `https://docs.polymarket.com/trading/deposit-wallets`
- Polymarket market WebSocket docs: `https://docs.polymarket.com/market-data/websocket/market-channel`
- Polymarket user WebSocket docs: `https://docs.polymarket.com/market-data/websocket/user-channel`
- Public SDK issue for deposit-wallet/API-key signer mismatch:
  `https://github.com/Polymarket/py-clob-client-v2/issues`
- More specific public issue describing the L1 API-key binding problem:
  `https://github.com/Polymarket/clob-client-v2/issues/65`

Key source conclusions:

- CLOB auth has L1 private-key signing plus L2 API credentials. L2 is still not enough to create orders; order payloads
  are signed locally.
- Signature types are provider-specific: `0` EOA, `1` existing Polymarket proxy/Magic wallet, `2` Gnosis Safe, `3`
  deposit wallet / `POLY_1271`.
- Deposit-wallet trading requires a funder/deposit wallet, CLOB L2 credentials, and order options such as `tick_size`
  and `neg_risk`.
- Market WebSocket endpoint is `wss://ws-subscriptions-clob.polymarket.com/ws/market`.
- User WebSocket endpoint is `wss://ws-subscriptions-clob.polymarket.com/ws/user` and authenticates with CLOB API key,
  secret, and passphrase in the subscription payload.

## Local Credentials And Storage

Rob authorized local credential handling for this prototype. Credentials were created/derived from the logged-in
`polymarket.com` account and stored only in:

- `/Users/robertgrzesik/Development/lumibot/.env.local`

Security state:

- `.env.local` is gitignored.
- File mode is `0600`.
- Raw private key, CLOB API key, CLOB secret, and passphrase are not documented here and must not be committed, logged, or
  pasted into chat.

Credential model currently present:

- `TRADING_BROKER=polymarket`
- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_OWNER_ADDRESS`
- `POLYMARKET_WALLET_ADDRESS`
- `POLYMARKET_SIGNATURE_TYPE`
- `POLYMARKET_CLOB_API_KEY`
- `POLYMARKET_CLOB_API_SECRET`
- `POLYMARKET_CLOB_API_PASSPHRASE`
- `POLYMARKET_TEST_TOKEN_ID`
- `POLYMARKET_LIVE_TRADING_ENABLED=true`
- `POLYMARKET_TEST_MAX_NOTIONAL=5`

Important distinction:

- `POLYMARKET_OWNER_ADDRESS` is the signer/owner identity.
- `POLYMARKET_WALLET_ADDRESS` is the funder/proxy/deposit wallet passed to CLOB.
- For the current Magic/proxy account, read paths work with signature type `1`. Order submission is still blocked by the
  platform-side deposit-wallet/API-key mismatch described below.

## Direct API Proof

Proof script:

- `/Users/robertgrzesik/Development/lumibot/scripts/polymarket_smoke.py`

Read-only smoke command:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py
```

Redacted artifact:

- `/Users/robertgrzesik/Development/lumibot/logs/polymarket_smoke_20260701_190525.json`

Verified read-only results:

- CLOB credentials were loaded from `.env.local`; no in-process derivation was needed.
- CLOB collateral balance read succeeded.
- Raw balance was returned in 6-decimal units and scaled to about `$29.185517`.
- Open orders count: `0`.
- Recent trades count: `0`.
- Data API positions count: `0` for both owner and funder/proxy addresses.
- Data API position value: `0` for both owner and funder/proxy addresses.
- Public order book read succeeded for the selected test token.
- Quote read succeeded with bid/ask/mid.
- Last trade price read succeeded.
- Supported price history read succeeded.
- Public market WebSocket connected and returned events.
- Authenticated user WebSocket connected with CLOB L2 credentials; it returned no events because there were no account
  order/trade events during the proof window.

## Live Submit Proof

Market-order smoke command:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py --live-order
```

Redacted artifact:

- `/Users/robertgrzesik/Development/lumibot/logs/polymarket_smoke_20260701_190551.json`

Result:

- The tiny FAK market BUY did not submit.
- Polymarket returned `maker address not allowed, please use the deposit wallet flow`.
- Balance, open orders, and trades remained unchanged.

Limit/cancel smoke command:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py --limit-cancel
```

Redacted artifact:

- `/Users/robertgrzesik/Development/lumibot/logs/polymarket_smoke_20260701_190611.json`

Result:

- The tiny far-from-market limit order did not submit.
- Polymarket returned the same deposit-wallet-flow blocker before an order id existed, so cancel could not be exercised.

## Interpretation Of The Blocker

This is not currently a LumiBot mapping bug. The same rejection occurs through direct `py-clob-client-v2` calls before
LumiBot broker normalization is involved.

The public GitHub issue tracker has multiple recent open reports around the same credential/address family:

- `POLY_1271 deposit wallet order rejected: "the order signer address has to be the address of the API KEY"`.
- `create_or_derive_api_key()` binding API credentials to one address while order signing uses another address.
- Web/proxy accounts and deposit-wallet accounts seeing read APIs work while order submission fails due signer/API-key
  address mismatch.

For this account, the observed local matrix was:

- Signature type `1` with the current funder/proxy reads balances/orders/trades but order submit returns
  `maker address not allowed, please use the deposit wallet flow`.
- Signature type `3` with the current funder/proxy returns the order-signer/API-key mismatch.
- Signature types `0` and `2` did not produce a usable order path.

Next practical fix is not another LumiBot order mapping change. We need one of:

- A supported Polymarket deposit-wallet setup that yields CLOB credentials bound to the order signer/funder address.
- Polymarket-side SDK/API fix for Magic/proxy or deposit-wallet API-key binding.
- A documented and supported workaround from Polymarket for registering API credentials under the exact address CLOB
  validates during `POST /order`.

## LumiBot Changes Made From This Proof

Implementation updates:

- `PolymarketData` now exposes the real market/user WebSocket URLs and handles list payloads from the public market
  stream.
- `PolymarketCLOBStream` now has real WebSocket workers for public market and private user streams, plus HTTP polling
  reconciliation.
- `Polymarket` scales collateral balance/allowance raw units from 6-decimal USDC-like integer units into dollars.
- `Polymarket` stores optional `OWNER_ADDRESS` separately from `WALLET_ADDRESS`.
- `Polymarket` chooses a safer default signature type: proxy when owner and funder differ, deposit-wallet otherwise.
- Market and limit submits now pass SDK `PartialCreateOrderOptions(tick_size=..., neg_risk=...)` from the live book.
- Known platform-side signer/funder/deposit-wallet rejections are wrapped in a clear `LumibotBrokerAPIError`.
- `requirements.txt` now includes `websockets>=15.0.1`.

Tests added or expanded:

- Balance raw-unit scaling.
- SDK order args and `PartialCreateOrderOptions` mapping.
- Known deposit-wallet/order-signer blocker wrapping.
- WebSocket subscription payload shape.
- Data source handling for market-stream list payloads.
- Public WebSocket API smoke.
- Live tiny order API smoke xfails only for the exact current platform-side blocker.

## Test Commands And Results

Mocked/unit:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 120s python3 -m pytest -q tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py
```

Result:

- `19 passed`

Live/API with `.env.local` loaded:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 120s python3 -m dotenv -f .env.local run -- python3 -m pytest -q tests/test_polymarket_apitest.py
```

Result:

- `3 passed, 1 xfailed`
- The xfail is the tiny live market order, and only for the current deposit-wallet/API-key binding blocker.

Direct smoke:

- Read-only direct smoke: passed.
- Live market-order direct smoke: blocked by Polymarket deposit-wallet flow.
- Limit/cancel direct smoke: blocked by Polymarket deposit-wallet flow before cancelable order creation.

## What Works Now

- Credential loading from `.env.local`.
- CLOB L2 credential use for authenticated read paths.
- Account cash read and scaling.
- Position and account-value reads through Data API fallback.
- Open order reads.
- Recent trade reads.
- Market discovery/token data through `PolymarketData`.
- Order book, quote, last price, and supported price history.
- Public market WebSocket connection and event ingestion.
- Private user WebSocket authenticated subscription.
- LumiBot broker/data-source selection with `TRADING_BROKER=polymarket`.
- Unit and API tests around the implemented mapping.

## What Is Still Blocked

- Live order submit on Rob's current account.
- Limit-order cancel proof, because limit order creation is blocked before an order id exists.
- Real fill/private-event reconciliation, because no live order can currently be placed.
- BotSpot/Bot Manager credential UI/runtime support. That should wait until the LumiBot adapter has a working live
  submit path or a clear product decision around private-key/deposit-wallet custody.

## Next Steps

1. Decide whether to pursue a true deposit-wallet setup for this account or wait for Polymarket's SDK/API fix.
2. If pursuing deposit wallet, identify the exact builder/relayer credential shape required by the current UI/API. The
   visible one-value `RELAYER_API_KEY` is not the same as CLOB L2 credentials, and the Python relayer client examples use
   builder key/secret/passphrase.
3. Once a supported order path exists, rerun:
   - direct `scripts/polymarket_smoke.py --live-order`
   - direct `scripts/polymarket_smoke.py --limit-cancel`
   - `pytest -q tests/test_polymarket_apitest.py` with `.env.local` loaded
4. After live submit/cancel proof works, add one end-to-end LumiBot strategy smoke that places a tiny order, reconciles
   private user WebSocket events, refreshes positions/orders, and exits.
5. Only then start BotSpot/Bot Manager credential schema and UI work.
