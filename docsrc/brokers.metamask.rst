MetaMask Agent Wallet
=====================

.. meta::
   :description: Connect LumiBot to MetaMask Agent Wallet with scoped bot tokens, Hyperliquid testnet perpetuals, owner collateral setup, and durable order recovery.

.. include:: _snippets/botspot_mcp_callout.rst

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
Node 24 is recommended. Node 22 releases from 22.18 onward are also supported.
The checked-in lockfile supports their bundled npm 10 and npm 11 installers.

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
Funded testnet qualification verified explicit cancellation after a real partial
fill. Venue history and the broker journal agreed on fills, the canceled
remainder and fees; normal restart restored that terminal snapshot without
replaying its historical fill.
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
Connected read-only checks also verify strategy startup and polling against
real saved history without placing orders.
Qualify your own funded wallet and its approval/recovery paths. The
pinned MetaMask dependency tree also has unresolved upstream security advisories;
this beta is not production-qualified. See the engineering guide for the audit,
trust boundary, and test checklist. Configure strategies with
``quote_asset=Asset("USDC", asset_type="crypto")`` for perpetual account valuation.

Frequently asked questions
--------------------------

Which MetaMask wallet does a strategy use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each agent uses a dedicated, separately funded MetaMask Agent Wallet server wallet. MetaMask manages its signing keys. The strategy receives a scoped local broker token, while the owner protects the CLI session and setup credentials.

Why does the same email show different wallets?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Google, email-code, and QR sign-in methods have separate wallet identities. Reconnect using the method that created the wallets. Setup warns when existing agent wallets are absent from the current session.

Does testnet ETH fund Hyperliquid perpetuals?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. ETH pays EVM gas; Hyperliquid positions need mock USDC collateral in HyperCore. Refresh collateral balances in setup and move spot USDC to perps within the agent wallet when needed. Unified accounts pool collateral. Faucet eligibility is separate from wallet creation.

Optional browser gas funding has a separate receipt history. It confirms the
testnet, sender, recipient and exact ETH amount, then waits for a successful
mined receipt and two block confirmations. Refreshing after a reload resumes
read-only checks. A missing hash can be linked from MetaMask Activity.
Alternatively, **Review pending funding** keeps the original nonce, sender,
recipient and amount while refreshing the fee estimate. A consumed nonce
requires receipt reconciliation. A mined cancellation is shown as
**replaced**, without claiming that funding arrived. Unknown funding is
retained and never automatically resent.

Can a strategy transfer funds to another wallet?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bot tokens cannot request arbitrary transfers, withdrawals, or raw signatures. The owner-only setup collateral action moves testnet USDC within the same bound wallet. Protect the provider session separately because it can control multiple managed wallets.

What should I do after an interrupted order?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Keep the journal and reuse the original request ID. Check the saved order and provider approval status. Unknown submissions require independent reconciliation; the service does not automatically replay a signature or guess that a timeout means failure.

For an uncertain perpetual submission, owner setup offers **Link venue order**.
Enter the exact ID from the same wallet's Hyperliquid history. The service checks
the wallet, network, request details and submission time, then reads actual venue
status and fills. A mismatched or already-linked order is refused. Recovery never
signs another order and is unavailable to bot tokens.

Can I test spot swaps on testnet?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pinned provider SDK has no testnet spot swap route. The broker refuses that route before signing. Funded perpetual tests do not qualify spot swap receipts, and the setup flow does not switch to mainnet automatically.

Is this integration production-qualified?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is a local beta with funded perpetual and recovery checks. Standard and
USDC unified accounts have been exercised against real testnet collateral and
positions. Unified equity uses the spot collateral ledger without adding PnL
twice; portfolio margin is explicitly unsupported. Upstream dependency advisories
and broader funded approval and partial-fill cancellation qualification remain
open. MetaMask's outflow limits exclude typed-data signatures, so they do not
force an approval challenge for perpetual orders. Review the engineering guide
before operational use.
