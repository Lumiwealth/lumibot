"""Opt-in wake hints for backtests.

A strategy that knows it has nothing to do until some time or price level may say so with
``Strategy.wake_when``. While the hint holds, the backtest engine skips calling
``on_trading_iteration``; everything else in the iteration runs exactly as before (order
processing, fills, portfolio valuation, stats rows, lifecycle methods), so a strategy whose
hint is honest gets the same trades, events and stats as without it, only faster.

The engine wakes the strategy (runs ``on_trading_iteration``) at the first iteration where
any of these holds:

* a time in the hint is reached (``at``, or ``every`` after the hint was set);
* a watched asset's last price reaches a level, or the value of the strategy's positions in
  the watched assets reaches a level;
* a watched price cannot be read (no price means the hint cannot be checked exactly);
* an order event for the strategy happened (new, partial fill, fill, cancel, error), any
  lifecycle method ran (session open and close included), or its cash or positions changed
  for any other reason (expiry, settlement, dividends, splits, financing, deposits).

A hint lasts until the next time ``on_trading_iteration`` runs; set it again there to keep
sleeping. Waking early is always safe. Live trading ignores hints and runs every iteration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class WakeCondition:
    """What the engine checks on each iteration before calling ``on_trading_iteration``."""

    at: Optional[datetime] = None
    assets: tuple = ()
    price_above: Any = None  # a number for every asset, or {asset: level}
    price_below: Any = None
    value_above: Optional[float] = None
    value_below: Optional[float] = None
    signature: Any = None
    skipped: int = field(default=0)

    def has_price_levels(self) -> bool:
        return self.price_above is not None or self.price_below is not None

    def has_value_levels(self) -> bool:
        return self.value_above is not None or self.value_below is not None


def _level(levels, asset):
    if levels is None:
        return None
    if isinstance(levels, dict):
        return levels.get(asset)
    return levels


def account_signature(strategy) -> tuple:
    """Cash and held quantities: any change the strategy could observe without an order event."""
    broker = strategy.broker
    positions = broker.get_tracked_positions(strategy.name) or []
    held = tuple(sorted(((repr(p.asset), str(p.quantity)) for p in positions)))
    return strategy.cash, held


def store_wake_condition(strategy, condition: WakeCondition) -> None:
    """Keep the hint for the engine (backtests only; live trading ignores hints)."""
    if not getattr(strategy, "is_backtesting", False):
        if not getattr(strategy, "_wake_hint_live_noted", False):
            try:
                strategy._wake_hint_live_noted = True
            except Exception:
                pass
            logger.debug("wake_when is a backtest speed hint; live trading runs every iteration")
        strategy._wake_condition = None
        return
    condition.signature = account_signature(strategy)
    strategy._wake_condition = condition


def should_skip(strategy, executor) -> bool:
    """True when the hint holds and nothing the strategy could see has changed.

    Returns False (run the iteration) and clears the hint otherwise.
    """
    condition = getattr(strategy, "_wake_condition", None)
    if condition is None:
        return False
    try:
        skip = _holds(strategy, executor, condition)
    except Exception as exc:  # anything unexpected: run the iteration
        logger.debug("wake hint check failed; running the iteration: %s", exc)
        skip = False
    if skip:
        condition.skipped += 1
        strategy._wake_skipped_iterations = getattr(strategy, "_wake_skipped_iterations", 0) + 1
        return True
    strategy._wake_condition = None
    return False


def _holds(strategy, executor, condition: WakeCondition) -> bool:
    if getattr(executor, "_wake_event", False):
        return False
    now = strategy.get_datetime()
    if condition.at is not None and now >= condition.at:
        return False
    if account_signature(strategy) != condition.signature:
        return False
    if condition.has_price_levels():
        for asset in condition.assets:
            above, below = _level(condition.price_above, asset), _level(condition.price_below, asset)
            if above is None and below is None:
                continue
            price = strategy.get_last_price(asset)
            if price is None:
                return False
            price = float(price)
            if above is not None and price >= float(above):
                return False
            if below is not None and price <= float(below):
                return False
    if condition.has_value_levels():
        total = 0.0
        for asset in condition.assets:
            position = strategy.get_position(asset)
            quantity = float(position.quantity) if position is not None else 0.0
            if quantity == 0:
                continue
            price = strategy.get_last_price(asset)
            if price is None:
                return False
            multiplier = float(getattr(asset, "multiplier", 1) or 1)
            total += quantity * float(price) * multiplier
        if condition.value_above is not None and total >= float(condition.value_above):
            return False
        if condition.value_below is not None and total <= float(condition.value_below):
            return False
    return True


def build_condition(strategy, *, at=None, every=None, assets=None, price_above=None, price_below=None,
                    value_above=None, value_below=None) -> WakeCondition:
    """Validate ``Strategy.wake_when`` arguments and turn them into a ``WakeCondition``."""
    from lumibot.entities import Asset

    if assets is None:
        asset_list = ()
    elif isinstance(assets, (list, tuple, set)):
        asset_list = tuple(assets)
    else:
        asset_list = (assets,)
    asset_list = tuple(Asset(a) if isinstance(a, str) else a for a in asset_list)
    for levels in (price_above, price_below):
        if isinstance(levels, dict):
            levels_assets = tuple(Asset(a) if isinstance(a, str) else a for a in levels)
            asset_list = asset_list + tuple(a for a in levels_assets if a not in asset_list)
    if isinstance(price_above, dict):
        price_above = {(Asset(k) if isinstance(k, str) else k): v for k, v in price_above.items()}
    if isinstance(price_below, dict):
        price_below = {(Asset(k) if isinstance(k, str) else k): v for k, v in price_below.items()}
    has_levels = any(v is not None for v in (price_above, price_below, value_above, value_below))
    if has_levels and not asset_list:
        raise ValueError("wake_when: price and value levels need the assets they apply to (assets=...)")
    if asset_list and not has_levels:
        raise ValueError("wake_when: assets were given without a price or value level")

    wake_at = None
    if at is not None:
        wake_at = _localize(strategy, at)
    if every is not None:
        seconds = _seconds(every)
        if seconds <= 0:
            raise ValueError("wake_when: every must be a positive interval")
        due = strategy.get_datetime() + timedelta(seconds=seconds)
        wake_at = due if wake_at is None else min(wake_at, due)
    return WakeCondition(at=wake_at, assets=asset_list, price_above=price_above, price_below=price_below,
                         value_above=value_above, value_below=value_below)


def _seconds(every) -> float:
    if isinstance(every, timedelta):
        return every.total_seconds()
    from lumibot.strategies.strategy_executor import StrategyExecutor

    return float(StrategyExecutor._sleeptime_to_seconds(every))


def _localize(strategy, value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("wake_when: at must be a datetime")
    if value.tzinfo is not None:
        return value
    tz = getattr(strategy, "pytz", None) or getattr(strategy.get_datetime(), "tzinfo", None)
    if tz is None:
        return value
    localize = getattr(tz, "localize", None)
    return localize(value) if callable(localize) else value.replace(tzinfo=tz)
