"""Priority trade-event policy for live order management.

Fill and partial-fill events must not wait behind sync_broker's hold window or a
long on_trading_iteration. Hedges depend on prompt on_filled_order delivery.
"""

from __future__ import annotations

PRIORITY_FILL_EVENTS = frozenset({"fill", "partial_fill"})


def should_hold_trade_event_for_sync(
    *, hold_trade_events: bool, is_backtesting: bool, type_event: str
) -> bool:
    """Return True when sync_broker may defer a trade event.

    Fill and partial-fill events always take the priority path in live mode so a
    slow broker sync (or a long on_trading_iteration) cannot delay hedges.
    """
    if not hold_trade_events or is_backtesting:
        return False
    if type_event in PRIORITY_FILL_EVENTS:
        return False
    return True
