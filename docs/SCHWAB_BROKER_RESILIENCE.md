# Schwab Broker Resilience

**Date:** 2026-05-28
**Status:** Implementation guide
**Audience:** LumiBot maintainers, BotSpot runtime engineers, AI-agent prompt maintainers

Schwab can return account, order, and position records that are broader than normal strategy orders. Examples include mutual funds, sweep/cash-equivalent rows, bonds, option exercise records, and future order/status/asset strings that LumiBot does not know yet.

The important rule is: one unknown row must not invalidate the rest of a live broker refresh.

## Entity Policy

LumiBot has explicit unknown values for broker-returned data:

- `Asset.AssetType.UNKNOWN`
- `Order.OrderType.UNKNOWN`
- `Order.OrderSide.UNKNOWN`
- `Order.OrderStatus.UNKNOWN`

These values are for preserving broker state that LumiBot cannot classify yet. Strategy code should not use raw strings for order status filters. Continue to use enum filters such as:

```python
open_orders = self.get_orders(statuses=Order.ACTIVE_STATUSES)
```

`Order.ACTIVE_STATUSES` does not include `Order.OrderStatus.UNKNOWN`. If Schwab returns an unknown order type with a known active status such as `WORKING`, the order is preserved and remains active because its status maps to `Order.OrderStatus.NEW`. If Schwab returns an unknown status, the order is preserved but excluded from active-status filters.

## Raw Broker Metadata

When possible, unknown or degraded Schwab entities keep raw broker context:

- `raw_broker_payload`
- `raw_asset_type`
- `raw_order_type`
- `raw_order_status`
- `raw_order_side`
- `broker_parse_warning`
- `broker_parse_degraded`

This lets BotSpot and support tooling show that an unfamiliar row exists without pretending it is a normal stock/option/future record.

## Position Sync Safety

Schwab position parsing should include unknown positions by default when there is enough information to represent them. A row with symbol plus quantity should return a `Position`, even if the asset type is unfamiliar.

Rows are skipped only when they are truly unrepresentable, for example:

- no usable symbol, CUSIP, or instrument identifier
- quantity cannot be parsed as a number

If any row is skipped, the refresh is degraded. In a degraded Schwab position refresh, LumiBot updates parsed positions but does not remove existing tracked positions just because they were missing from the partial parse.

## Balance Safety

`get_cash()` and `get_portfolio_value()` force a live broker refresh. If Schwab or another broker cannot return fresh balance values:

- return `None` from the explicit getter
- leave cached internal cash and portfolio values unchanged
- never write `0` as a failure fallback

Zero is a valid balance, so it cannot mean "unknown."

## State-Changing Calls Stay Strict

This resilience policy is for read/parsing paths. It does not make submit/cancel/modify permissive. Failed state-changing requests must still fail loudly.

For Schwab successful order placement, remember that successful responses may provide the order id in headers rather than JSON. A successful response with an unusual body should not crash parsing. If an order id cannot be found, do not invent one.

## Test Expectations

Unit tests should include deliberately weird broker payloads:

- fake asset type
- fake order type
- fake order side/instruction
- fake order status
- mutual fund/bond/sweep-style rows
- option exercise rows
- malformed quantity mixed with valid sibling rows
- missing balance fields

The expected behavior is warnings plus best-effort fresh state, not whole-refresh failure.

## 2026-05-28 Local Schwab QA

Rob-authorized local QA used a temporary Schwab token and account `****4364`. The token file was deleted after the test.

Read-only baseline:

- balances returned fresh non-`None` cash, position value, and portfolio value fields
- positions returned 16 parsed positions, with no degraded parse flag
- recent order history returned two market/fill rows and parsed without errors

Live order flow:

- submitted one TSLL 1-share DAY limit buy while the market was closed
- `get_order(order_id)` immediately returned the fresh Schwab order in about 212ms with raw status `PENDING_ACTIVATION`
- the broad `get_orders_for_account` list did not include the new order on the first immediate call, but did include it on the next 250ms poll
- `cancel_order(order_id)` was accepted in about 261ms
- immediate post-cancel `get_order(order_id)` and broad order-list reads both returned `CANCELED`

Follow-up fix from this QA: Schwab single-order lookup must use `client.get_order(order_id, account_hash)`, not the non-existent `get_order_by_id` method. Unit coverage now locks this down.
