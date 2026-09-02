"""Priority fill/hedge handling must not wait for a long on_trading_iteration.

Regression for Titus / CVNA live hedge delay (2026-09-02):
option fill arrived while a ~122s scan was running; hedge submission must not be
gated behind the full scan finishing.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from unittest.mock import MagicMock

from lumibot.entities import Asset
from lumibot.strategies.strategy_executor import (
    StrategyExecutor,
    should_hold_trade_event_for_sync,
)


@dataclass
class _DummyBroker:
    IS_BACKTESTING_BROKER: bool = False
    name: str = "dummy"
    _first_iteration: bool = False
    _hold_trade_events: bool = False
    _held_trades: list = field(default_factory=list)

    def is_market_open(self) -> bool:
        return True


class _Logger:
    def isEnabledFor(self, level: int) -> bool:
        return False

    def debug(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None


class _PriorityHedgeStrategy:
    """Mimics a live scan that blocks for a long time while a fill arrives mid-scan."""

    def __init__(self, scan_seconds: float = 0.6):
        self.broker = _DummyBroker()
        self.hide_trades = True
        self.portfolio_value = 100_000.0
        self.cash = 100_000.0
        self.sleeptime = "1S"
        self.is_backtesting = False
        self._first_iteration = False
        self._name = "PriorityHedgeStrategy"
        self.logger = _Logger()
        self.quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)

        self.scan_seconds = scan_seconds
        self.iteration_started = threading.Event()
        self.iteration_finished = threading.Event()
        self.recorded_fills = []
        self.recorded_fills_lock = threading.Lock()
        self._executor = None

    def log_message(self, message, color=None, broadcast=False):
        return None

    def load_variables_from_db(self):
        return None

    def backup_variables_to_db(self):
        return None

    def send_account_summary_to_discord(self):
        return None

    def send_discord_message(self, message, silent=False):
        return None

    def on_bot_crash(self, error):
        return None

    def _update_cash(self, *args, **kwargs):
        return None

    def _update_portfolio_value(self, *args, **kwargs):
        return self.portfolio_value

    def _copy_dict(self):
        return {}

    def _apply_daily_cash_financing_if_needed(self):
        return None

    def trace_stats(self, context, snapshot_before):
        return {}

    def get_datetime(self):
        from datetime import datetime

        return datetime.now()

    def get_positions(self):
        return []

    def get_orders(self, *args, **kwargs):
        return []

    def on_trading_iteration(self):
        self.iteration_started.set()
        # Pure-Python busy work to stress GIL the way a long option scan does.
        deadline = time.monotonic() + self.scan_seconds
        while time.monotonic() < deadline:
            _ = sum(i * i for i in range(200))
        self.iteration_finished.set()

    def on_filled_order(self, position, order, price, quantity, multiplier):
        with self.recorded_fills_lock:
            self.recorded_fills.append(
                {
                    "t": time.monotonic(),
                    "during_iteration": self.iteration_started.is_set()
                    and not self.iteration_finished.is_set(),
                    "order": order,
                }
            )

    def on_new_order(self, order):
        return None

    def on_canceled_order(self, order):
        return None

    def on_partially_filled_order(self, *args, **kwargs):
        return None

    def on_error_order(self, *args, **kwargs):
        return None


@dataclass
class _DummyPosition:
    asset: Asset


class _DummyOrder:
    def __init__(self, asset: Asset, side: str = "buy"):
        self.asset = asset
        self.side = side
        self.identifier = "cvna-option-1"
        self.order_class = None
        self.symbol = getattr(asset, "symbol", "CVNA")

    def is_buy_order(self) -> bool:
        return self.side.lower() == "buy"

    def is_parent(self) -> bool:
        return False


def _build_live_executor(strategy: _PriorityHedgeStrategy) -> StrategyExecutor:
    executor = StrategyExecutor(strategy)
    strategy._executor = executor
    # Keep cron path executing the body on every call.
    executor.cron_count = executor.cron_count_target
    executor.sync_broker = MagicMock()  # type: ignore[method-assign]
    executor._capture_locals = False
    executor._trace_stats = MagicMock()  # type: ignore[method-assign]
    executor.get_next_ap_scheduler_run_time = MagicMock(return_value=None)  # type: ignore[method-assign]
    return executor


def test_fill_callback_runs_during_long_live_iteration_not_after():
    """BEFORE metric: fill waited until scan end (~scan_seconds).
    AFTER metric: fill callback during scan, latency << scan duration.
    """
    scan_seconds = 0.7
    strategy = _PriorityHedgeStrategy(scan_seconds=scan_seconds)
    executor = _build_live_executor(strategy)

    asset = Asset("CVNA", asset_type=Asset.AssetType.OPTION)
    position = _DummyPosition(asset=asset)
    order = _DummyOrder(asset=asset)

    fill_enqueued_at = {}

    def _enqueue_fill_mid_scan():
        assert strategy.iteration_started.wait(timeout=2.0)
        time.sleep(0.05)
        fill_enqueued_at["t"] = time.monotonic()
        executor.add_event(
            executor.FILLED_ORDER,
            dict(
                position=position,
                order=order,
                price=1.25,
                quantity=Decimal("1"),
                multiplier=100,
            ),
        )

    feeder = threading.Thread(target=_enqueue_fill_mid_scan, daemon=True)
    feeder.start()

    t0 = time.monotonic()
    executor._on_trading_iteration()
    elapsed = time.monotonic() - t0
    feeder.join(timeout=2.0)

    with strategy.recorded_fills_lock:
        recorded = list(strategy.recorded_fills)

    assert recorded, "on_filled_order never ran — hedge path was blocked"
    fill = recorded[0]
    fill_latency = fill["t"] - fill_enqueued_at["t"]

    # AFTER: callback must occur while the scan is still running, and well before
    # the full scan duration (the historical failure mode waited for scan end).
    assert fill["during_iteration"] is True, (
        f"fill processed only after iteration finished (latency={fill_latency:.3f}s, "
        f"scan={scan_seconds}s) — hedge still gated behind on_trading_iteration"
    )
    assert fill_latency < scan_seconds * 0.5, (
        f"fill->callback latency {fill_latency:.3f}s too close to scan {scan_seconds}s"
    )
    assert elapsed >= scan_seconds * 0.8  # scan still took its time


def test_priority_events_wake_check_queue_without_half_second_sleep():
    strategy = _PriorityHedgeStrategy(scan_seconds=0.01)
    executor = _build_live_executor(strategy)

    asset = Asset("CVNA", asset_type=Asset.AssetType.STOCK)
    position = _DummyPosition(asset=asset)
    order = _DummyOrder(asset=asset)

    wake_to_fill = {}

    def _run_check_queue_once_via_wait():
        t_wait_start = time.monotonic()
        executor._queue_wakeup.wait(timeout=0.5)
        wake_to_fill["waited"] = time.monotonic() - t_wait_start
        executor.process_queue()

    waiter = threading.Thread(target=_run_check_queue_once_via_wait, daemon=True)
    waiter.start()
    time.sleep(0.05)
    executor.add_event(
        executor.FILLED_ORDER,
        dict(
            position=position,
            order=order,
            price=10.0,
            quantity=Decimal("1"),
            multiplier=1,
        ),
    )
    waiter.join(timeout=2.0)

    assert strategy.recorded_fills, "priority wake did not drain fill"
    assert wake_to_fill.get("waited", 1.0) < 0.25, (
        f"check_queue waited {wake_to_fill.get('waited')}s; expected immediate wake"
    )


def test_broker_does_not_hold_fill_events_during_sync():
    """Fills must take the priority path even when sync_broker holds trade events."""
    assert should_hold_trade_event_for_sync(
        hold_trade_events=True, is_backtesting=False, type_event="new"
    )
    assert not should_hold_trade_event_for_sync(
        hold_trade_events=True, is_backtesting=False, type_event="fill"
    )
    assert not should_hold_trade_event_for_sync(
        hold_trade_events=True, is_backtesting=False, type_event="partial_fill"
    )
    assert should_hold_trade_event_for_sync(
        hold_trade_events=True, is_backtesting=False, type_event="canceled"
    )
    assert not should_hold_trade_event_for_sync(
        hold_trade_events=False, is_backtesting=False, type_event="new"
    )
    assert not should_hold_trade_event_for_sync(
        hold_trade_events=True, is_backtesting=True, type_event="new"
    )
