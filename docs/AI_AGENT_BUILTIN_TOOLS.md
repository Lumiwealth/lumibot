# AI Agent Built-In Tools

Lumibot agents include built-ins for:

- account and portfolio state
- market prices and history tables
- DuckDB queries
- Lumibot docs search
- Alpaca news
- indicators
- SEC fundamentals and filings
- memory
- notifications
- orders

Trading permission:

```python
self.agents.create(name="researcher", allow_trading=False)
```

`allow_trading=False` removes mutating order tools only:

- `orders_submit_order`
- `orders_cancel_order`
- `orders_modify_order`

It keeps read-only tools, including `orders_open_orders`, positions, portfolio, market data, indicators, SEC filings, memory, and notifications.

Indicator tools:

- `list_indicators`
- `get_indicator`
- `get_indicators`

Memory tools:

- `remember`
- `search_memory`
- `remember_decision`
- `remember_lesson`
- `open_thesis`
- `update_thesis`
- `close_thesis`

Notification tool:

- `notify_user`
