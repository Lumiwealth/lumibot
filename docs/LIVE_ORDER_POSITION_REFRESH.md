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
```

Backtesting behavior is unchanged. Backtests use the simulated broker state for deterministic replay.

Live refreshes are throttled briefly by default so repeated accessor calls in the same strategy iteration do not spam broker APIs. If a strategy submits an order and needs to check that same order immediately, prefer:

```python
submitted = self.submit_order(order)
self.sleep(1, process_pending_orders=True)
latest = self.get_order(submitted.identifier)
```

For duplicate-order guards, filter by active status and the exact asset or option contract. Filled, canceled, rejected, expired, and error orders should not block new entries.

## Broker Reconciliation Safety

Broad broker order-list endpoints are not treated as the only source of truth for active local orders. If an active tracked order is missing from the broad list, Lumibot performs a direct broker order lookup by identifier before updating the local status. If the direct lookup fails or cannot be parsed, Lumibot leaves the local order active and logs a warning instead of marking it canceled locally.

For Schwab, order history can include account-level mutual fund or bond activity such as sweep-fund entries. Lumibot skips those unsupported Schwab order legs while continuing to parse supported stock, option, future, forex, and index order legs.

## Migration Note

Old strategies using `get_orders()` still work. New and refined strategies should use `get_orders(statuses=Order.ACTIVE_STATUSES)` when they mean currently open or active orders.
