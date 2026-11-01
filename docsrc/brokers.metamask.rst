MetaMask
========

.. meta::
   :description: Set up MetaMask managed wallets in LumiBot with native Python, Guard Mode, durable order recovery, and explicit testnet qualification limits.

The ``MetaMask`` broker runs inside LumiBot's Python process and uses MetaMask
Agent Wallet managed signing. It calls MetaMask and Hyperliquid directly. No
Node, Bun, npm, MetaMask CLI or localhost service is required.

.. include:: _snippets/botspot_mcp_callout.rst

Install and connect
-------------------

The native candidate is unreleased. For source testing, use the shared version
branch in a clean environment:

.. code-block:: bash

   pip install 'lumibot[metamask] @ git+https://github.com/Lumiwealth/lumibot.git@version/4.6.40'

Authorize through the official `MetaMask Agent Wallet sign-in page
<https://developer.metamask.io/agentic/login>`_. Use the original Google/email
method to retain the same wallet identity. The optional Python login function
opens that page, accepts the copied browser token through a hidden prompt,
verifies its identity, and writes an owner-only session file:

.. code-block:: python

   from lumibot.tools.metamask.owner import login

   login("<private-directory>/session.json")

Login never creates a wallet, changes policy or funds an account. Select an
existing dedicated managed EVM wallet and review its Guard Mode policy in
MetaMask. Before trading, deliberately authorize its local registry, limits and
expiry using ``MetaMaskClient.authorize()``. Broker startup does not renew or
broaden authority. See the complete binding example in the repository's
`MetaMask setup guide
<https://github.com/Lumiwealth/lumibot/blob/version/4.6.40/docs/METAMASK.md>`_.

.. code-block:: python

   from lumibot.brokers import MetaMask
   from lumibot.entities import Asset

   config = {
       "SESSION_FILE": "<private-directory>/session.json",
       "STATE_FILE": "<private-directory>/broker.sqlite",
       "AGENT_ID": "strategy-wallet",
       "WALLET_ADDRESS": "<dedicated-managed-wallet-address>",
       "NETWORK": "testnet",
       "MODE": "perp",
   }
   broker = MetaMask(config)
   quote_asset = Asset("USDC", asset_type="crypto")

Pass ``broker`` and ``quote_asset`` to your normal LumiBot strategy. Use
``self.create_order()`` and ``self.submit_order()`` for perpetual orders. The
asset type is ``crypto_future``, the quantity is base units and multiplier is 1.
Leverage defaults to 1 and remains bounded by the reviewed binding.

Headless authentication can use explicit ``PROJECT_ID`` and ``ACCESS_TOKEN``.
Automatic refresh requires a durable ``SESSION_FILE``. Rotation is locked and
saved before reuse; an uncertain refresh requires reconnect instead of retrying
a potentially consumed token. Keep credentials outside strategy source.

Perpetual execution
-------------------

The native candidate supports Hyperliquid's default USDC perpetual DEX with
market and limit buy/sell orders. ``reduce_only=True`` survives shared closing
methods and restored snapshots. Legacy ``custom_params={"reduce_only": True}``
cannot turn off canonical reduce-only intent. Limit orders require ``gtc``,
``ioc`` or ``alo``. Market orders become frozen slippage-bounded IOC limits.
Conditional, bracket, OCO and automatic cancel/replace orders are unsupported.

Duplicate IDs reuse the same journaled operation. A different request under an
existing ID is rejected. Uncertain mutations keep their original request IDs
and block new execution until reconciliation. Pending signatures are never
replayed after restart. Cancellation has a separate lifecycle and cannot erase
fills or reject the original live order merely because cancellation failed.

Standard account cash uses actual withdrawable funds and margin. Unified
accounts use pooled HyperCore spot USDC once, without adding perpetual PnL
again. Portfolio margin is unsupported. HIP3 writes are held until native
collateral-aware valuation and effective DEX fee inputs are qualified.

``MetaMaskData`` provides actual perpetual marks and closed minute/hour/day
candles from the selected network. Public perpetual data needs only an explicit
``NETWORK``. The venue retains at most 5,000 bars per interval. Spot candles
and historical account equity are not provided.

Same-chain swaps
----------------

``MODE="spot"`` exposes exact-input ``broker.swap()`` and
``broker.get_swap(identifier)``. It does not turn an input amount into a claimed
exact-output buy. Assets require explicit ``chain_id``, ``token_address`` and
``decimals``. Register both assets and the native gas asset in the reviewed
binding. Configure ``CHAIN_RPC_URLS`` and explicit ``GAS_LIMITS`` when quote gas
ceilings are absent.

The native implementation verifies quote identity, fee-net source amount,
slippage, bounded allowances, sequential simulation and durable managed
transaction requests. MetaMask signs and broadcasts; Python never rebroadcasts
the signed transaction. Completion requires matching mined transaction identity,
nonce, bounded gas, canonical receipt and actual net ERC20 output transfers.
Quote estimates are not fill receipts.

Testnet swap routes, native output without qualified trace proof, gasless or
delegated routes, bridges and non-source-denominated fees are unsupported and
refused before execution. Spot account valuation covers the approved registry
using real provider USD prices. Missing prices fail explicitly; no test-token
peg or synthetic cash balance is invented. Native mainnet swaps remain
unqualified.

Migration and recovery
----------------------

Preserve the old complete ``broker.sqlite`` and use it as ``STATE_FILE`` with
the exact original agent, wallet and network. Stop the retired service through
its runtime owner before migration. Never copy an active database or run
independent journal copies for one wallet.

An explicitly selected ``LEGACY_SESSION_FILE`` can import the old session into
a distinct new ``SESSION_FILE``. Its old owner fence remains held during native
use. The original file is retained; subsequent OAuth rotations belong to the
native session. Do not restart the old service with stale credentials.

``BROKER_URL`` and ``BROKER_TOKEN`` are retired and raise migration guidance.
The former agent-scoped token is not a MetaMask access token. Known legacy
venue orders retain order/fill recovery. Old unused signatures are never
released. Expired uncertain native leverage is resolved by verified venue reads without
replaying the unsubmitted order. Legacy swaps without sufficient native
transaction evidence remain explicit reconciliation holds.

``client.owner_operations()`` exposes retained policy, gas-funding and
collateral-transfer receipts. These operations are never automatically replayed.
Funding and provider-policy changes belong in MetaMask's owner surfaces; the
old local funding UI is historical, not a Python runtime prerequisite.

Trust boundary and qualification
--------------------------------

Keys stay with MetaMask. Local Python checks are cooperative process controls;
strategy code can access its process credentials. Dedicated funding and
provider-enforced Guard policy are the actual boundaries. MetaMask's
`outflow policy
<https://docs.metamask.io/agent-wallet/reference/outflow-policy/>`_ does not make
a local notional limit a provider-enforced cap on typed-data signatures.

Native offline regression tests and connected Python wallet, policy, account,
market-data and ordinary strategy-startup checks have passed. Funded native
order/fill/cancel qualification still requires a valid owner grant. Old
JavaScript receipts do not qualify the new path. No production or mainnet
trading qualification is claimed.

Environment configuration uses ``TRADING_BROKER=metamask``, optional
``DATA_SOURCE=metamask``, ``METAMASK_SESSION_FILE``, ``METAMASK_STATE_FILE``,
``METAMASK_AGENT_ID``, ``METAMASK_PROJECT_ID``, ``METAMASK_ACCESS_TOKEN``,
``METAMASK_REFRESH_TOKEN``, ``METAMASK_WALLET_ADDRESS``, ``METAMASK_NETWORK`` and
``METAMASK_MODE``.

Execution and dependency review
-------------------------------

Managed Hyperliquid signing uses domain chain ID 1337 on both networks;
the signed source binds testnet or mainnet. Bridge chain IDs must not replace
the signing domain. Provider validation and Shield failures remain rejected
without automatic retry or venue submission. Guard Mode remains enabled.

Owner revocation is checked after remote preparation and serialized with final
handoff. An expired uncertain cancel can retry only after fresh reads confirm
the same order remains live. Expired uncertain leverage reconciles current
venue settings and rejects the unsubmitted intent; a fresh order ID is needed.
Accessor refresh and polling share cumulative fill, callback and terminal-event
handling. Live partial remainders use identity-checked open-order snapshots;
``orderStatus`` may retain the original size. Closed order refresh is read-only,
preserves lifecycle, and emits only new fill deltas. Inconsistent execution
evidence blocks new reservations until reconciliation succeeds.

Polling defaults to 10 seconds and shares fill/live-order reads within a wallet
sweep. Increase ``POLL_INTERVAL`` for large histories or multiple brokers sharing
an IP; Hyperliquid limits `aggregate weighted requests per IP
<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits>`_. Unified-account
cash is unheld USDC collateral, without promising order capacity or valuing
unrelated spot assets.

Atomic quantities remain exact under caller decimal contexts.

Managed swaps cannot be canceled locally. Receipt reconciliation includes gas
spent by reverted transactions and earlier approvals. OP Stack estimates and
actual fees include separate L1-data and operator charges. The execution-gas
ceiling is distinct from estimated rollup charges, which cannot be capped by
the transaction signer. Actual overruns preserve fills and block further spend.
After reviewing the exact recorded overrun, explicitly call
``client.acknowledge_fee_overrun(operation_id, expected_overrun)``. This retains
the adjusted daily charge. Legacy overruns without verified USD valuation stay
held.

Use a clean environment or remove retired ``polygon-api-client`` before
upgrading. Its certificate constraint prevents current CA-store fixes. The
maintained Massive SDK preserves existing ``POLYGON_API_KEY`` configuration and
Polygon endpoint behavior. The MetaMask extra requires patched ``urllib3`` and
``certifi``. Private state directories and owner-only regular session locks
remain mandatory. This is not a claim that the entire optional dependency tree
has no advisories.

New managed requests match MetaMask's high-level wallet-adapter provider origin,
while readable intent and durable client provenance identify LumiBot. Recovery
validates the originally recorded origin, without alternate-origin retries.
Guard Mode and exact policy checks stay enabled.

Actual native testnet ALO placement/cancellation and market opening/reduce-only
closing have passed on two dedicated wallets. A real Strategy/Trader ETH round
trip delivered unique callbacks and reconciled ending cash exactly, including
fees. The installed wheel passed native regression and connected-history tests
on Python 3.10 and 3.12 without JavaScript runtimes in PATH; connected checks
also denied child processes. The current wheel recovered an actual
canceled-partial receipt with matching venue fills and fees and no replayed
callbacks. Fresh live partial-flow attempts remain inconclusive. This is not
production or mainnet-swap qualification. Earlier JavaScript receipts cannot
qualify this implementation.

Frequently asked questions
--------------------------

Do I need Node or a local MetaMask service?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Install ``lumibot[metamask]`` and configure the native session, wallet and
journal. Managed signing and venue requests run through Python.

Who holds the wallet keys and enforces spending limits?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MetaMask retains wallet keys. Guard Mode enforces its reviewed provider policy;
local notional limits are cooperative process controls. Use a dedicated wallet
with bounded funding, and keep Guard Mode enabled.

Can I preserve the previous integration's journal?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Retain the exact state file, agent and wallet identity. Known venue orders
can reconcile; ambiguous legacy effects remain explicit holds. Old service
tokens are not MetaMask access tokens, and startup never renews authority.

What should I do after an approval timeout or unknown outcome?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspect the recorded operation with ``broker.client.get_order(operation_id)``.
Reuse its identity and reconcile the original request before creating another
trade. Do not bypass Guard Mode or increase permissions to clear a failure.

Does testnet qualification establish production swap readiness?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Native perps have funded testnet evidence, with the remaining qualification
limits recorded above. Mainnet trading and funded native swaps remain
unqualified; earlier JavaScript receipts do not establish Python readiness.
