# IBKR REST Order Semantics

Contributor and user reference for advanced orders submitted through the IBKR Client Portal REST adapter.

Last Updated: 2026-08-31

Status: Current behavior

Audience: LumiBot users and contributors

## Overview

LumiBot's `Order` entity is provider-generic. `InteractiveBrokersREST` owns the
translation from that order tree to Interactive Brokers Client Portal tickets;
IBKR-specific fields and lifecycle rules do not belong in the generic order
API. This document describes the supported REST behavior and its deliberate
boundaries.

## Advanced order packages

The adapter sends each advanced package in one `execute_order()` request:

- **BRACKET** sends the executable parent followed by one or two attached
  children. The parent and each child receive distinct Client Portal `cOID`
  values; each child also receives `parentId` equal to the parent's `cOID`.
- **OTO** sends the executable parent followed by its one attached child, with
  the same distinct-child-`cOID` and parent-`parentId` relationship.
- **OCO** does not send the conceptual LumiBot parent. It sends exactly the two
  executable children as an IBKR single-group/OCA package (`isSingleGroup`).
  Each child receives a unique `cOID` so acknowledgements can be correlated if
  IBKR returns them out of request order. The conceptual parent remains a local
  container.

Each executable native leg receives and tracks its own broker order ID. For
BRACKET and OTO, the tracked parent uses the broker ID as the children's
`parent_identifier`. For OCO, children retain the local container's identifier;
the local parent has no broker order ID.

Client Portal does not guarantee one immediate acknowledgement row per submitted
advanced-order ticket. IBKR's published bracket example submits three tickets
but shows two immediate response rows, while its OCA example returns tickets in
a different order. LumiBot therefore uses client-order correlation values and a
bounded account-order poll to reconcile an incomplete BRACKET or OTO response;
OCO responses use `local_order_id` correlation. The adapter tracks a package
only after every expected executable ticket resolves to a distinct positive
broker ID. If the complete mapping remains missing or ambiguous, submission
fails closed and every broker ID discovered in either the response or the
bounded reconciliation receives a cancellation attempt.

This behavior follows IBKR's
[published bracket response example](https://ibkrcampus.com/campus/ibkr-quant-news/how-to-code-a-bracket-order-in-the-web-api/),
[current complex-order lesson](https://ibkrcampus.com/campus/trading-lessons/complex-orders/),
and the documented `order_ref` field returned by
[live-order monitoring](https://ibkrcampus.com/campus/ibkr-api-page/webapi-doc/#monitoring-live-orders).

Strategies should use the current generic parameter names when constructing
packages, for example:

```python
entry = strategy.create_order(
    asset,
    quantity=10,
    side="buy",
    order_type="limit",
    order_class="bracket",
    limit_price=100,
    secondary_limit_price=110,
    secondary_stop_price=95,
)
```

The REST adapter preserves explicit child quantity, side, order type, prices,
time-in-force, and exchange. Automatically generated children may inherit the
parent exchange for REST contract resolution without mutating the generic
`Order` objects.

For Client Portal REST price fields, a `stop` (`STP`) ticket serializes its
trigger as `price`; it does not send `auxPrice`. A `stop_limit` (`STP LMT`)
ticket serializes its limit as `price` and its trigger as `auxPrice`. This
matches IBKR's published Web API bracket field definitions.

## Cancellation and polling

Canceling a BRACKET or OTO parent attempts cancellation for the broker-backed
parent and every known broker-backed child. Canceling an OCO parent attempts
both executable children and never sends the local container ID to IBKR. An
explicit child cancellation remains scoped to that child. Requests are
deduplicated and remaining members are still attempted if one cancellation
fails; local terminal status does not suppress an explicit broker request.

Polling normalizes IBKR identifiers across integer and string representations
and updates already-tracked native legs in place. Flat broker responses do not
erase known child relationships, and the OCO local container remains connected
to its tracked children. Reconstruction of an advanced package submitted
before process startup is not claimed: it would require a tested, reliable
relationship field in the broker response.

The bounded acknowledgement poll uses a single-attempt account-order read and
never enters the adapter's normal retry-until-available polling loop. A deletion
error response is logged as a cancellation failure, not as a successful
cancellation. An "order does not exist" result can mean a rejected or already
inactive ticket, but it is not reported as proof that IBKR accepted a cancel.

## GTD limitation

The Client Portal REST order schema does not document a verified exact-date
expiration field equivalent to the Legacy socket API's `goodTillDate`. For
that reason, REST submission explicitly rejects `time_in_force="gtd"` and any
`good_till_date` supplied with another time-in-force value before contract
lookup or order execution. This prevents an expiration from being silently
discarded. Responses for orders created through another interface may still
contain and be parsed from `goodTillDate`.

Exact-date GTD remains separately supported by the Legacy IBKR socket adapter;
this REST limitation does not change generic `Order`, backtesting, Polymarket,
or other broker behavior. See IBKR's [Web API order documentation](https://ibkrcampus.com/campus/ibkr-api-page/webapi-doc/#orders),
[new-order endpoint reference](https://ibkrcampus.com/docs/web-api/api-reference/trading/trading-orders/submit-new-order),
and [TWS API documentation](https://www.interactivebrokers.com/docs/tws-api/doc/introduction)
for the separate provider contracts.

## Authorized paper-validation runbook

For the complete credential-free operator procedure, see
[IBKR REST Paper-Test Runbook](IBKR_REST_PAPER_TEST_RUNBOOK.md).

The repository's IBKR REST paper suites are real broker API tests. A paper
account limits the financial impact; it does not make the tests unit tests or
safe for an unattended production session. They are marked ``apitest`` and
``ibkr`` and are excluded from ordinary CI (the normal CI policy excludes
``apitest`` tests).

An explicitly authorized maintainer must use a dedicated IBKR paper username
and an already authenticated paper Client Portal gateway. Configure the
existing LumiBot IBKR settings without recording their values anywhere in the
repository. ``IB_USE_PAPER_ACCOUNT`` must be explicitly set to ``true``;
never use a production account for these tests. The fixture then verifies that
the authenticated selected account has the
paper ``DU`` identity convention and, when supplied, matches
``IB_ACCOUNT_ID``. An external ``IB_API_URL`` gateway must already be
authenticated to the paper account: the local paper flag does not convert an
external live session. Missing configuration or an unavailable gateway skips
the suite; a configured live-style account fails before an order request.
Within one pytest invocation, the paper fixture reuses a single authenticated
data source so its tests do not start competing local IBeam containers.

Run only the focused modules from an environment with LumiBot and its test
dependencies installed, after confirming that the gateway is the intended
paper session:

```bash
python -m pytest -q -m "apitest and ibkr" tests/test_ibkr_rest_advanced_orders_paper_apitest.py
python -m pytest -q -m "apitest and ibkr" tests/test_ibkr_rest_gtd_paper_apitest.py
```

The advanced-order suite obtains a current reference price, submits deliberately
non-marketable BRACKET/OTO/OCO tickets, and always attempts cancellation in
cleanup. The GTD module is a separate, read-only capability probe: it uses
only `/orders/whatif` and does not enable production GTD serialization. The
probe's result is evidence for a separate reviewed implementation decision;
passing it does not prove production OAuth readiness or authorize production
order testing.

Only masked account suffixes may appear in output. Never copy usernames,
passwords, account identifiers, cookies, tokens, gateway session material, or
raw broker responses into logs or issue reports. Process termination can still
interrupt a `finally` cleanup; after any interrupted run, the maintainer must
inspect the paper account and confirm that no test orders remain working.
