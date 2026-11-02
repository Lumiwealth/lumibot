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

The native adapter is included in the published 4.6.44 package. The execution
ownership and submission-certainty hardening described here is an unreleased
source candidate. Package availability does not establish production
qualification. For candidate source testing, use the unreleased dev branch in
a clean environment:

.. code-block:: bash

   pip install 'lumibot[metamask] @ git+https://github.com/Lumiwealth/lumibot.git@dev'

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
<https://github.com/Lumiwealth/lumibot/blob/dev/docs/METAMASK.md>`_.

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

The runnable example is ``lumibot/example_strategies/metamask_perpetual.py``.
Select ``TRADING_BROKER=metamask``, ``METAMASK_NETWORK=testnet`` and
``METAMASK_MODE=perp`` with an existing authorized session and journal:

.. code-block:: bash

   python -m lumibot.example_strategies.metamask_perpetual

``MetaMaskPerpetualRoundTrip`` uses normal Strategy order, price, position and
cash methods with continuous Trader scheduling. It buys 0.005 ETH at 1x and
closes actual executed quantity through a reduce-only order on a later iteration.
Check current price, minimum notional and lot precision before running. A
rejected or zero-fill entry does not count as a successful round trip.

The same strategy name and parameters resume the journaled cycle without a new
entry. A different name intentionally starts another cycle. The example keeps
checking late fills and positions after reaching flat. Unknown submission
outcomes hold execution; a rejected close requires reconciliation.
Older native canceled records recover missing no-release provenance only from
intact persisted checkpoints. Missing or conflicting history remains unknown;
reconciliation and fee holds stay in force.

Use a dedicated managed wallet, one durable journal owner and a reviewed grant.
The launcher checks venue open orders because journal-backed ``get_orders()``
cannot discover foreign or manual orders. Grant expiry and remaining daily
capacity must cover entry and closing legs. ``get_cash()`` is broker-reported USDC cash,
not remaining policy budget. Startup never renews authority. Stop and join
Trader before calling ``broker.close()`` to release workers and the native client.

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

Testnet and mainnet
-------------------

Choose the network explicitly and preserve separate network bindings and
journals. Startup does not turn testnet authority into mainnet authority.
Market inventories and asset IDs differ, so metadata is resolved on the chosen
network. Perpetual signing uses EIP-712 chain ``1337`` on both networks, with
source ``a`` for mainnet and ``b`` for testnet. These are distinct from HyperEVM
chain IDs. See the `official signing source <https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/signing.py>`_.

The adapter validates supported account valuation before reserving a fresh
execution and after approval before releasing leverage or order actions.
Portfolio margin, unknown modes and unreadable account state block new execution.
Cancellation and original-order reconciliation remain available.

Perpetual position inventory covers the default USDC DEX. Unified accounts can
share collateral across DEXs, so this does not establish wallet-wide flatness.
Use a dedicated managed wallet without manual positions or orders on other
DEXs. Cross-DEX inventory and collateral require separate qualification. See
`account modes <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes>`_
and `margining <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining>`_.

Fees, funding, liquidity and liquidation depend on actual venue state. A grant
or leverage setting is not a maximum-loss guarantee. Venue minimum-notional
errors remain explicit rejections; the adapter does not enlarge an order or
apply an unverified minimum to reduce-only cleanup. See
`venue errors <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses>`_.

REST limits are shared across the public IP. Respect the documented
`request weights <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits>`_
across all strategies. There is no global weighted rate scheduler. Read errors
are explicit; uncertain writes retain their original identity and are never
automatically retransmitted. Research and public metadata reads do not establish
funded mainnet execution.

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
nonce, bounded gas, RPC-finalized canonical receipt and actual net ERC20 output transfers.
Quote estimates are not fill receipts.

The configured RPC must support ``eth_getBlockByNumber("finalized", false)``.
One later block does not establish finality. Pending finality retains submitted
state; unsupported finality or conflicting block identity holds reconciliation.
Old filled records without finality provenance are rechecked read-only, retaining
recorded quantities, fees and spending history. Missing evidence cannot release
a new trade or replay the old swap. This is the selected chain/RPC's finality
contract, not a bridge withdrawal guarantee. See
`Ethereum block tags <https://ethereum.org/developers/docs/apis/json-rpc/#the-block-parameter>`_.

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
market-data and ordinary strategy-startup checks have passed. Native testnet
opening, reduce-only closing and cancellation have funded receipts. Fresh live
partial-flow remains unqualified. Actual short-lived testnet grants have expired
on both dedicated wallets: new intents returned ``REVOKED`` without signing or
changing orders, execution effects or nonces. Active grants still skip the
ordinary connected expiry check. Managed swaps require separate funded proof.
Old JavaScript receipts do not qualify the new path. No production or mainnet
trading qualification is claimed.

The explicit ``scripts/qualify_metamask_partial.py`` testnet harness validates
live remaining size against ``frontendOpenOrders`` and the persisted native
client order identity. Paired fills, fees, callbacks and exact cleanup remain
required. ``--daily-caps MAKER TAKER`` validates separately reviewed existing
grants without changing authority or spending history. Defaults remain 300/150;
each wallet needs $50 reservation room before signing to cover entry and
residual cleanup within the $25 order limit.

Order-status evidence must match the persisted ``cloid``. Live
``frontendOpenOrders`` records match by ``oid`` and may omit ``cloid``; when
supplied, it must agree. Remaining-size and fill checks remain mandatory.

Environment configuration uses ``TRADING_BROKER=metamask``, optional
``DATA_SOURCE=metamask``, ``METAMASK_SESSION_FILE``, ``METAMASK_STATE_FILE``,
``METAMASK_AGENT_ID``, ``METAMASK_PROJECT_ID``, ``METAMASK_ACCESS_TOKEN``,
``METAMASK_REFRESH_TOKEN``, ``METAMASK_WALLET_ADDRESS``, ``METAMASK_NETWORK`` and
``METAMASK_MODE``.

Execution and dependency review
-------------------------------

An IOC order can report venue status ``filled`` after executing only part of its
requested quantity. When the original intent, persisted wire action, venue time
in force and exact fills prove this case, LumiBot records the actual execution
and cancels the unfilled remainder. The raw venue status remains available for
audit. A status label or remaining size alone cannot establish a fill;
incomplete or contradictory evidence stays held for reconciliation.

A real testnet IOC partial execution has been recovered through the corrected
broker with exact fills and fees, followed by an exact reduce-only close and
flat-account verification. This proves that recovery path. Controlled maker
partial-cancellation qualification remains inconclusive, and no production
readiness is claimed.

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

Local cancellation and release evidence
---------------------------------------

Local cancellation needs positive persisted native evidence that the order was
not released. Such orders expose ``confirmed_not_submitted`` to generic tools,
with an incomplete, non-retryable submission outcome. Earlier signing and
leverage effects remain in the audit history. Missing legacy evidence remains
unknown and cannot be cleared by local cancellation. A historical canceled label
alone is not acceptance proof; operator reconciliation is required, and this
projection does not recreate a journal hold already lost in the past. Known
venue identities and actual fills remain authoritative.

Cancellation signing and pending-signature release require a matching live
venue observation without a reconciliation error. An unresolved or
contradictory observation holds the cancellation as well as the entry. If an
explicit local cancellation retry later obtains valid no-release proof and
settled leverage evidence, it clears the resolved reconciliation hold while
retaining prior effects and fee-review flags.

Frequently asked questions
--------------------------

Do I need Node or a local MetaMask service?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Install ``lumibot[metamask]`` and configure the native session, wallet and
journal. Managed signing and venue requests run through Python.

Who holds the wallet keys and enforces spending limits?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MetaMask retains wallet keys. Guard Mode enforces its reviewed provider policy;
local notional limits are cooperative process controls. Use a dedicated wallet
with bounded funding, and keep Guard Mode enabled.

Can I preserve the previous integration's journal?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Retain the exact state file, agent and wallet identity. Known venue orders
can reconcile; ambiguous legacy effects remain explicit holds. Old service
tokens are not MetaMask access tokens, and startup never renews authority.

What should I do after an approval timeout or unknown outcome?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspect ``order.submission_outcome``: ``broker_state_certainty`` distinguishes
``confirmed_submitted``, ``confirmed_rejected``, ``confirmed_not_submitted`` for
proven pre-release cancellation, and ``unknown``. ``reconciliation_required``
marks unresolved or conflicting evidence. Agent
submission tools preserve this state and return incomplete, non-retryable results
while uncertainty remains. An acknowledgement is not a fill.

Inspect the recorded operation with ``broker.client.get_order(operation_id)``.
Reuse that operation's identity and reconcile the original request before
creating another trade. Do not bypass Guard Mode or increase permissions to clear
a failure.

Does testnet qualification establish production swap readiness?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Native perps have funded testnet evidence, with the remaining qualification
limits recorded above. Mainnet trading and funded native swaps remain
unqualified; earlier JavaScript receipts do not establish Python readiness.
