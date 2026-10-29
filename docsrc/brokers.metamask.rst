MetaMask Agent Wallet
=====================

.. warning::

   This optional broker is a local beta. Offline tests and public testnet market
   reads do not establish authenticated wallet execution. Qualify your funded
   testnet wallet before using unattended trading.

``MetaMask`` connects LumiBot to dedicated MetaMask Agent Wallet server wallets.
Each agent receives a separate funded wallet and a scoped runtime token.
Normal browser connection uses MetaMask Connect, the successor to the older
MetaMask SDK. Browser-wallet delegation redemption is not implemented.

Setup
-----

Use the ``integrations/metamask`` directory from the same LumiBot revision.
Node 24 is recommended; Node 22.18 or later is required.

.. code-block:: console

   cd integrations/metamask
   npm ci --ignore-scripts
   npm run build
   npm start

Open the localhost URL printed by the service. Unlock with its local admin token,
sign in through the official MetaMask Agent Wallet login link, and import the CLI
token into the local page. Never give a strategy your CLI session, admin token,
seed phrase, or private key.

Create a dedicated wallet, review its Guard Mode policy, and set its allowed
markets/chains/tokens and limits. Setup defaults to testnet, BTC/ETH markets, a $25
trade-notional ceiling per order, $100 per rolling 24 hours, 1x leverage, 50 bps slippage, and a
24-hour token lifetime. These editable defaults exclude gas, funding, and losses.
Save the scoped broker configuration in your secret manager. The browser wallet
can request user-approved testnet ETH funding; Hyperliquid collateral is separate.

Use the original Google, email, or QR sign-in method when reconnecting. Each
method has separate wallets, even for the same email. Setup reports session
refresh failures and warns when existing agent wallets are missing.

After creating a binding, use **Hyperliquid testnet collateral** to read actual
spot USDC and perps equity. **Move USDC to perps** moves mock collateral within
that same bound wallet. Only owner credentials can request this testnet action;
bot tokens cannot transfer collateral or select a recipient. Unified accounts
already pool collateral and do not need this move. Verify balances after
submission. Funding IDs and pending signing jobs survive restart; uncertain
submissions block further wallet execution and are never automatically replayed.

Python configuration
--------------------

``Order(..., reduce_only=True)`` remains reduce-only through submission and
restoration, including orders created by ``close_position()`` and ``sell_all()``.
The older ``custom_params={"reduce_only": True}`` spelling remains supported;
custom parameters cannot weaken canonical closing intent.

.. code-block:: python

   from lumibot.brokers import MetaMask

   broker = MetaMask({
       "BROKER_URL": "http://127.0.0.1:8766",
       "BROKER_TOKEN": "<agent-scoped-token>",
       "WALLET_ADDRESS": "<agent-wallet-address>",
       "NETWORK": "testnet",
       "MODE": "perp",
   })

Environment selection supports ``TRADING_BROKER=metamask`` and
``DATA_SOURCE=metamask`` with ``METAMASK_BROKER_URL``,
``METAMASK_BROKER_TOKEN``, ``METAMASK_WALLET_ADDRESS``, ``METAMASK_NETWORK``, and
``METAMASK_MODE``. No wallet is created by the Python constructor.

Perpetuals use ``Asset("BTC", asset_type="crypto_future")`` and USDC settlement.
Market and limit orders support explicit leverage/reduce-only settings. Limit
orders require ``gtc``, ``ioc``, or ``alo``; unsupported order types fail explicitly.
Perpetual marks, closed candles, balances, positions, fill fees, and funding
ledger reads use the same configured testnet/mainnet environment.

For spot swaps, set ``MODE="spot"`` and call
``broker.swap(input_asset, output_asset, input_amount, identifier="stable-id")``.
Each crypto asset must provide ``chain_id``, ``token_address`` (or ``"native"``),
and ``decimals``. The amount is exact input, not an exact output buy quantity.
Poll with ``broker.get_swap(identifier)``. Both tokens must be owner-allowlisted,
on the same chain, and supported by the provider. Unpriced test tokens and
missing routes do not trigger a mainnet fallback.
Agent SDK 7.3.0's swap router supports mainnet chains only; testnet swaps are
explicitly refused before pricing or signing. Testnet balances and signatures
remain supported.

Recovery and scope
------------------

The service stores durable order IDs and reservations. Broadcasts and approvals
are not fills. Unknown submissions block new orders and are never automatically
retried. Interrupted allowance approval can be resumed from setup after its
receipt confirms, under the original budget and current policy checks.
Restored terminal orders are not replayed as new cancellation/error events.
Account snapshots retain unprocessed fill deltas for normal strategy callbacks.
Policy requests persist separately and expose approved, denied and expired
states after restart. Perpetual openings check authority before each SDK
signature and again before releasing it to the venue. Interrupted pending
signatures retain their recovery IDs and never automatically replay an order.

Revoking a runtime token stops new bot access. It does not cancel pending venue
orders, revoke onchain allowances, or undo signatures. Setup supports cancellation
of known perpetual orders after revocation. Keep the service state directory.

Run the service under a separate OS identity/container when strategies are
untrusted. Its MetaMask owner session can control multiple wallets: application
scopes are not cryptographic provider-issued subwallet credentials. Separate
funding and provider Guard policies remain necessary.

Unsupported features include arbitrary signing/transfers, cross-chain bridges,
gasless delegation execution, browser-wallet delegation redemption, conditional
orders, spot candle history, and historical account equity. Missing spot cost
basis/PnL remains unavailable rather than estimated silently.

Each journal permits one companion process. A second process fails with
``STATE_IN_USE`` even if it uses a different port. Crash-safe SQLite ownership
and session locks release when the process exits; keep state on a local
filesystem and never delete lock files while running. Stop older companion
versions before upgrading. Multi-host active/active operation is unsupported.

For examples, failure states, qualification steps and primary sources, see
`the full MetaMask integration guide <https://github.com/Lumiwealth/lumibot/blob/dev/docs/METAMASK.md>`__.

Qualification limits
--------------------

Companion CI checks formatting, tests, and browser bundling on Node 22.18 and
Node 24 without wallet credentials. Connected wallet execution and public venue
API checks remain separate opt-in qualification steps.

Connected identity, balances, market data, harmless signing, policy expiry and
token revocation have been verified. A dedicated Hyperliquid testnet wallet
has also passed market open/close, multiple-fill accounting, resting-order
cancellation, restart recovery and a full strategy-executor iteration with fill
callbacks. Restored cumulative fills are not replayed into order transactions.
Qualify your own funded wallet and its approval/recovery paths. The
pinned MetaMask dependency tree also has unresolved upstream security advisories;
this beta is not production-qualified. See the engineering guide for the audit,
trust boundary, and test checklist. Configure strategies with
``quote_asset=Asset("USDC", asset_type="crypto")`` for perpetual account valuation.
