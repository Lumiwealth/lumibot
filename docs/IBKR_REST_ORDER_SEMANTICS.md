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
