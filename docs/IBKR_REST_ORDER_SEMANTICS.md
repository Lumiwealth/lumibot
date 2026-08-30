# IBKR REST Order Semantics

Contributor and user reference for advanced orders submitted through the IBKR Client Portal REST adapter.

Last Updated: 2026-08-29

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
  children. The parent receives a Client Portal `cOID`; each child receives
  `parentId` equal to that cOID.
- **OTO** sends the executable parent followed by its one attached child, with
  the same cOID/`parentId` relationship.
- **OCO** does not send the conceptual LumiBot parent. It sends exactly the two
  executable children as an IBKR single-group/OCA package (`isSingleGroup`).
  Each child receives a unique `cOID` so acknowledgements can be correlated if
  IBKR returns them out of request order. The conceptual parent remains a local
  container.

Each executable native leg receives and tracks its own broker order ID. For
BRACKET and OTO, the tracked parent uses the broker ID as the children's
`parent_identifier`. For OCO, children retain the local container's identifier;
the local parent has no broker order ID.

IBKR's documented OCA example returns tickets in a different order from the
request. LumiBot therefore uses response `local_order_id` values to correlate
OCO acknowledgements. If the complete mapping remains ambiguous, submission
fails closed and every acknowledged broker ID receives a cancellation attempt.

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

Run only the focused modules, from the `lumibot-dev` environment, after
confirming that the gateway is the intended paper session:

```bash
conda run -n lumibot-dev python -m pytest -q -m "apitest and ibkr" tests/test_ibkr_rest_advanced_orders_paper_apitest.py
conda run -n lumibot-dev python -m pytest -q -m "apitest and ibkr" tests/test_ibkr_rest_gtd_paper_apitest.py
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
