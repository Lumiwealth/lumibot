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
markets/chains/tokens and limits. Setup defaults to testnet, a $10 trade-notional
ceiling per order, $25 per rolling 24 hours, 1x leverage, 50 bps slippage, and a
24-hour token lifetime. These editable defaults exclude gas, funding, and losses.
Save the scoped broker configuration in your secret manager. The browser wallet
can request user-approved testnet ETH funding; Hyperliquid collateral is separate.

Python configuration
--------------------

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

Recovery and scope
------------------

The service stores durable order IDs and reservations. Broadcasts and approvals
are not fills. Unknown submissions block new orders and are never automatically
retried. Interrupted allowance approval can be resumed from setup after its
receipt confirms, under the original budget and current policy checks.

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

For examples, failure states, qualification steps and primary sources, see
`the full MetaMask integration guide <https://github.com/Lumiwealth/lumibot/blob/dev/docs/METAMASK.md>`__.

Qualification limits
--------------------

Authenticated wallet execution requires connected testnet qualification. The
pinned MetaMask dependency tree also has unresolved upstream security advisories;
this beta is not production-qualified. See the engineering guide for the audit,
trust boundary, and test checklist. Configure strategies with
``quote_asset=Asset("USDC", asset_type="crypto")`` for perpetual account valuation.
