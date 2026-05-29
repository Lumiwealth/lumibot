# Live Order And Position Refresh

Lumibot strategy accessors keep backtests deterministic and live strategies current.

## Order Status Filters

Use `get_orders()` for backward-compatible access to all tracked strategy orders. When a strategy needs currently active orders, filter with `Order.OrderStatus` enum values instead of raw strings:

```python
from lumibot.entities import Order

open_orders = self.get_orders(statuses=Order.ACTIVE_STATUSES)
```

`Order.ACTIVE_STATUSES` includes:

- `Order.OrderStatus.UNPROCESSED`
- `Order.OrderStatus.SUBMITTED`
- `Order.OrderStatus.OPEN`
- `Order.OrderStatus.NEW`
- `Order.OrderStatus.PARTIALLY_FILLED`

You can also pass a single enum or an explicit collection:

```python
submitted = self.get_orders(statuses=Order.OrderStatus.SUBMITTED)

working = self.get_orders(statuses=[
    Order.OrderStatus.OPEN,
    Order.OrderStatus.NEW,
    Order.OrderStatus.PARTIALLY_FILLED,
])
```

Raw string status filters are rejected. This avoids silent bugs from typos such as `"partal_fill"` or broker-specific wording.

## Live Broker Refresh

In live trading, these accessors refresh broker state before returning:

```python
self.get_order(order_id)
self.get_orders(statuses=Order.ACTIVE_STATUSES)
self.get_position(asset)
self.get_positions()
self.get_cash()
self.get_portfolio_value()
```

Backtesting behavior is unchanged. Backtests use the simulated broker state for deterministic replay.

Live strategy accessor calls are fresh by default. If strategy code calls `get_orders()` or `get_positions()` twice in a row, Lumibot asks the connected live broker twice unless the caller explicitly passes a `broker_refresh_ttl_seconds` value. This avoids hiding broker state changes behind a local one-second cache. Internal framework loops may still pass an explicit TTL where repeated polling is intentional.

If a strategy submits an order and needs to check that same order immediately, prefer:

```python
submitted = self.submit_order(order)
latest = self.get_order(submitted.identifier)
```

Direct lookup by order id is the freshest path after a submit. Broad account order-list endpoints can have a short broker-side propagation delay, so use `get_order(submitted.identifier)` when the code is checking the exact submitted order. If strategy logic needs to query the broader order list immediately after submitting, a short `self.sleep(..., process_pending_orders=True)` before `get_orders(...)` can avoid broker propagation timing.

For duplicate-order guards, filter by active status and the exact asset or option contract. Filled, canceled, rejected, expired, and error orders should not block new entries.

### Balance Refresh Failure

`get_cash()` and `get_portfolio_value()` force a live broker balance refresh. If the broker refresh fails, these methods return `None` and leave the last known internal cash and portfolio values unchanged. They must not write `0` as a failure fallback because zero is a valid broker value.

Strategy code should treat `None` as "balance unavailable right now":

```python
cash = self.get_cash()
if cash is None:
    self.log_message("Broker cash unavailable; skipping sizing for this iteration.")
    return
```

## Broker Reconciliation Safety

Broad broker order-list endpoints are not treated as the only source of truth for active local orders. If an active tracked order is missing from the broad list, Lumibot performs a direct broker order lookup by identifier before updating the local status. If the direct lookup fails or cannot be parsed, Lumibot leaves the local order active and logs a warning instead of marking it canceled locally.

For Schwab, order history can include account-level mutual fund or bond activity such as sweep-fund entries, option exercise records, and other broker account-history records that are not normal strategy orders. Lumibot preserves unrecognized but representable Schwab order and position rows using `Asset.AssetType.UNKNOWN`, `Order.OrderType.UNKNOWN`, `Order.OrderSide.UNKNOWN`, or `Order.OrderStatus.UNKNOWN` instead of failing the whole refresh. A single unsupported broker-history row must not crash or poison the whole live refresh.

Unknown Schwab rows carry raw broker metadata on the returned entity where available:

- `raw_broker_payload`
- `raw_asset_type`
- `raw_order_type`
- `raw_order_status`
- `raw_order_side`
- `broker_parse_warning`
- `broker_parse_degraded`

Only truly unrepresentable rows, such as a position without any usable symbol/instrument identifier or a non-numeric quantity, are skipped. When that happens, Schwab position sync is treated as degraded: Lumibot updates parsed positions but does not remove tracked positions solely because they were missing from the partial parse.

## Migration Note

Old strategies using `get_orders()` still work. New and refined strategies should use `get_orders(statuses=Order.ACTIVE_STATUSES)` when they mean currently open or active orders.

Old code that expects `get_cash()` or `get_portfolio_value()` to always return a float should add a `None` guard before using those values for sizing or risk decisions.
