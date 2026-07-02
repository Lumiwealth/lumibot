# Polymarket International CLOB Live Proof

One-line description: Verified credential, data, WebSocket, and live-submit status for LumiBot's international Polymarket CLOB adapter.

Last Updated: 2026-07-01

Status: Deposit-wallet flow complete; direct SDK and LumiBot live market order plus limit/cancel proofs succeeded.

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
- `POLYMARKET_PROXY_WALLET_ADDRESS` preserves the old proxy wallet used to fund the deposit wallet.
- `POLYMARKET_DEPOSIT_WALLET_ADDRESS` is the deterministic deposit wallet.
- The current active trading configuration uses `POLYMARKET_WALLET_ADDRESS=<deposit wallet>` and
  `POLYMARKET_SIGNATURE_TYPE=3`.

## Deposit Wallet Setup Proof

Helper script:

- `/Users/robertgrzesik/Development/lumibot/scripts/polymarket_deposit_wallet_setup.py`

Verified setup actions on 2026-07-01:

- Created builder HMAC credentials through the CLOB SDK and stored them only in `.env.local`.
- Derived deterministic deposit wallet for the current signer.
- Deployed the deposit wallet through the Polymarket relayer.
- Funded the deposit wallet with `5` pUSD from the existing proxy wallet via a gasless relayer proxy transfer.
- Approved all three CLOB-reported pUSD spender contracts from the deposit wallet via a relayer `WALLET` batch.
- Activated the deposit wallet in `.env.local` with `POLYMARKET_SIGNATURE_TYPE=3`.

Read-only verification command:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_deposit_wallet_setup.py
```

Latest result:

- Deposit wallet balance was present.
- Allowance count: `3`.
- Zero allowance count: `0`.

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

## Earlier Proxy-Mode Live Submit Proof

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

## Deposit-Wallet Live Submit Proof

Direct SDK read-only proof after activation:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py
```

Latest redacted artifact:

- `/Users/robertgrzesik/Development/lumibot/logs/polymarket_smoke_20260701_200400.json`

Verified:

- Deposit-wallet CLOB cash read succeeded.
- Positions count by deposit wallet: `1`.
- Recent trades count: `4`.
- Open orders count: `0`.
- Public market WebSocket returned events.
- Private user WebSocket authenticated.
- Order book, quote, last price, and history worked.

Direct SDK live market order proof:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py --live-order --amount 1.00
```

Result:

- `status=submitted`.
- Polymarket response had `success=true`, `status=matched`, a real `orderID`, and a transaction hash.

Direct SDK limit/cancel proof:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_smoke.py --limit-cancel --limit-price 0.01 --limit-size 5
```

Latest redacted artifact:

- `/Users/robertgrzesik/Development/lumibot/logs/polymarket_smoke_20260701_200430.json`

Result:

- Limit order submitted as `status=live`.
- Cancel response returned the order id under `canceled`.

LumiBot-level smoke helper:

- `/Users/robertgrzesik/Development/lumibot/scripts/polymarket_lumibot_smoke.py`

Read-only:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_lumibot_smoke.py
```

Verified:

- `strategy.get_cash()` returned the real deposit-wallet CLOB cash.
- `strategy.get_portfolio_value()` returned cash plus position value.
- Positions and open orders loaded through the broker.

Live market order through LumiBot:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_lumibot_smoke.py --market-order --amount 1.00
```

Equivalent inline proof also verified:

- `Strategy.create_order(...)` built a Polymarket prediction-contract market BUY.
- `Strategy.submit_order(order)` submitted through the LumiBot broker and returned `status=fill`.
- The returned normalized order included the real CLOB order id, filled quantity, and average fill price.
- Fresh reconciliation showed recent trades increased and open orders stayed `0`.

Live limit/cancel through LumiBot:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 90s python3 scripts/polymarket_lumibot_smoke.py --limit-cancel
```

Equivalent inline proof verified:

- `Strategy.create_order(...)` built a tiny limit BUY.
- `Strategy.submit_order(order)` returned an open CLOB order id.
- `broker.cancel_order(submitted)` canceled it.
- Open orders were `0` afterward.

## Interpretation Of The Original Blocker

The original proxy-mode blocker was not a LumiBot mapping bug. The same rejection occurred through direct
`py-clob-client-v2` calls before LumiBot broker normalization was involved.

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

The working fix was Polymarket's documented deposit-wallet flow: deploy deterministic deposit wallet, fund it with pUSD,
approve CLOB spenders from the deposit wallet, switch `POLYMARKET_WALLET_ADDRESS` to the deposit wallet, and use
`POLYMARKET_SIGNATURE_TYPE=3`.

## LumiBot Changes Made From This Proof

Implementation updates:

- `PolymarketData` now exposes the real market/user WebSocket URLs and handles list payloads from the public market
  stream.
- `PolymarketCLOBStream` now has real WebSocket workers for public market and private user streams, plus HTTP polling
  reconciliation.
- `Polymarket` scales collateral balance/allowance raw units from 6-decimal USDC-like integer units into dollars.
- `Polymarket` stores optional `OWNER_ADDRESS` separately from `WALLET_ADDRESS`.
- `Polymarket` honors explicit `POLYMARKET_SIGNATURE_TYPE` from env/config before falling back to inference.
- Market and limit submits now pass SDK `PartialCreateOrderOptions(tick_size=..., neg_risk=...)` from the live book.
- Market submit responses now parse `orderID`, `makingAmount`, `takingAmount`, and `status=matched`.
- Limit submit responses with empty amount fields parse cleanly and preserve the original LumiBot limit price.
- Known platform-side signer/funder/deposit-wallet rejections are wrapped in a clear `LumibotBrokerAPIError`.
- `requirements.txt` now includes `websockets>=15.0.1`.

Tests added or expanded:

- Balance raw-unit scaling.
- SDK order args and `PartialCreateOrderOptions` mapping.
- Known deposit-wallet/order-signer blocker wrapping.
- WebSocket subscription payload shape.
- Data source handling for market-stream list payloads.
- Public WebSocket API smoke.
- Explicit deposit-wallet signature type selection.
- Live CLOB response parsing for matched market orders and live limit orders.

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

Earlier result before deposit-wallet activation:

- `3 passed, 1 xfailed`.
- The xfail was the tiny live market order under proxy-mode configuration.

Current result for focused Polymarket broker unit tests:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 120s python3 -m pytest tests/test_polymarket_broker.py -q
```

- `16 passed`

Direct and LumiBot live smoke:

- Direct read-only smoke: passed.
- Direct live market order: passed.
- Direct live limit/cancel: passed.
- LumiBot read-only smoke: passed.
- LumiBot live market order: passed.
- LumiBot live limit/cancel: passed.

Residual note:

- Immediate post-submit cash reads can lag inside the same SDK client. A fresh CLOB/account read reconciles the correct
  balance.

## What Works Now

- Credential loading from `.env.local`.
- CLOB L2 credential use for authenticated read paths.
- Builder HMAC credential creation through the CLOB SDK.
- Deposit-wallet derivation, relayer deployment, proxy funding transfer, and deposit-wallet pUSD approvals.
- Account cash read and scaling.
- Position and account-value reads through Data API fallback.
- Open order reads.
- Recent trade reads.
- Market discovery/token data through `PolymarketData`.
- Order book, quote, last price, and supported price history.
- Public market WebSocket connection and event ingestion.
- Private user WebSocket authenticated subscription.
- LumiBot broker/data-source selection with `TRADING_BROKER=polymarket`.
- Direct SDK market order.
- Direct SDK limit order and cancel.
- LumiBot `Strategy.create_order(...)` / `Strategy.submit_order(...)` market order.
- LumiBot limit order and cancel.
- Unit and API tests around the implemented mapping.

## What Is Still Open

- The deposit wallet now has less than `$1` idle pUSD after the live proofs. Fund it again before additional market-order
  smoke tests.
- Immediate post-submit cash reads can be stale inside the same SDK client; use a fresh read/reconciliation for final
  account values.
- Private user WebSocket authenticated successfully, but a fill-event callback was not captured live during a running
  LumiBot strategy loop. HTTP reconciliation worked.
- BotSpot/Bot Manager credential UI/runtime support remains deferred until product decisions are made around private-key,
  builder-key, and deposit-wallet custody.

## Next Steps

1. Add a safer balance-reconciliation hook after successful submit so same-client cash reads are less stale.
2. Add an integration test path for `scripts/polymarket_lumibot_smoke.py` with live trading disabled by default.
3. Fund the deposit wallet again before more live market-order testing.
4. Run a real strategy loop with WebSockets enabled and capture a private user fill event during the loop.
5. Start BotSpot/Bot Manager credential schema and UI work only after deciding how hosted runtime should custody or
   receive the private key, builder key, CLOB L2 creds, and deposit-wallet address.

## July 2 Follow-Up: Full LumiBot Matrix, WebSockets, Backtesting

Scope: same international `polymarket.com` CLOB account and deposit wallet. Raw secret values remain only in
`.env.local`; console output and docs use redacted identifiers.

Additional implementation updates:

- Added safe generic prediction-market data methods on base `DataSource` so non-Polymarket brokers return empty or
  unsupported values instead of failing when strategy code calls `search_markets`, `get_event`, `get_market_metadata`,
  `get_market_rules`, `get_resolution_status`, `get_spread`, `get_midpoint`, `get_recent_trades`, `get_open_interest`,
  or `get_holders`.
- Expanded `PolymarketData` for market/event metadata, rules, resolution status, spread, midpoint, recent trades,
  open interest, holders, close time, resolution source, minimum order size, settlement price, and token/outcome
  resolution.
- Added GTD expiration, post-only, cancel-all, cancel-multiple, cancel-by-market, batch limit order support, recent
  trade retrieval, and conditional-token sell allowance sync to the `Polymarket` broker.
- Added conditional-token `setApprovalForAll` support to `scripts/polymarket_deposit_wallet_setup.py`.
- Added `PolymarketBacktesting`, exported it from `lumibot.backtesting`, and wired prediction-contract cash accounting
  in `BacktestingBroker`.
- Added `lumibot/example_strategies/polymarket_prediction_contract.py`.
- Updated README, public RST docs, and env docs for Polymarket live and backtesting support.

Additional live proof through normal LumiBot code:

- Read-only LumiBot state after refilling deposit wallet: cash about `$5.88`, portfolio about `$9.95`, one position, no
  open orders.
- FAK BUY filled: about `$1` notional, quantity about `34.482757`, average fill about `0.029`.
- FOK BUY filled: about `$1` notional, quantity about `34.482757`, average fill about `0.029`.
- Conditional-token sell approval was required before live SELL. After `--approve-conditional`, FAK SELL and FOK SELL
  filled and parsed correctly as quantity `1.0` at average fill `0.028`.
- GTC BUY, GTD BUY, post-only BUY, GTC SELL, GTD SELL, and post-only SELL all submitted as resting open orders and were
  canceled with `cancel_all_orders`.
- Marketable post-only BUY rejected with the expected CLOB error: `invalid post-only order: order crosses book`, now
  wrapped as a readable `LumibotBrokerAPIError`.
- Single cancel, multiple cancel, cancel-all, and cancel-by-market helpers executed through LumiBot. Resting orders were
  cleaned up; final read-only state showed zero open orders.
- Final WebSocket smoke with `scripts/polymarket_lumibot_smoke.py --websocket --order-kind fak-sell --limit-size 1`
  received public market events, one private user trade event, one broker trade-event row, quote-cache updates, and
  HTTP recent-trade reconciliation without subscriber errors.

Additional tests:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 120s python3 -m pytest -q tests/test_prediction_market_data_source_defaults.py tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py tests/test_polymarket_backtesting.py
```

Result:

- `41 passed, 1 warning`

Current open items after the July 2 live-follow-up:

- The deposit wallet has less idle pUSD after repeated live smoke trades. Re-fund before running more BUY market-order
  tests.
- BotSpot/Bot Manager support is still a separate product/runtime-secret project.

## July 2 Backtesting Hardening Follow-Up

Backtesting fixes added after the live matrix:

- `PolymarketBacktesting.get_quote()` and `get_last_price()` now read from loaded Polymarket/Pandas bars only for
  `prediction_contract` assets.
- Prediction-contract backtests no longer fall through to IBKR, Yahoo, or other default stock/crypto data sources when
  `DATA_SOURCE=polymarket` is set in the live environment.
- The example strategy runs end-to-end as a real backtest with `.env.local` loaded.
- Backtesting validates 0-to-1 prices, Polymarket tick-size rules, minimum order sizes, market close, and settlement to
  `$1` or `$0` when resolved metadata is available.
- Public README/RST wording now describes Polymarket trading and backtesting directly. Detailed docs still explain
  test-safety limits such as real-money live smoke tests being off by default.

Additional focused test result:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s python3 -m pytest -q tests/test_polymarket_backtesting.py tests/test_backtesting_data_source_env.py tests/test_prediction_market_data_source_defaults.py tests/test_polymarket_asset.py tests/test_polymarket_data.py tests/test_polymarket_broker.py
```

- `55 passed, 1 warning`

Example backtest proof:

```bash
/Users/robertgrzesik/Development/bin/safe-timeout 180s python3 -m dotenv -f .env.local run -- env IS_BACKTESTING=true BACKTESTING_START=2026-06-30 BACKTESTING_END=2026-07-01 BACKTESTING_SHOW_PROGRESS_BAR=false python3 -m lumibot.example_strategies.polymarket_prediction_contract
```

- Completed successfully with `PolymarketBacktesting`.
