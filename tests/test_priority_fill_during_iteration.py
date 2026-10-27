"""Priority fill/hedge handling must not wait for a long on_trading_iteration.

Regression for a live option-fill hedge delay (2026-09-02):
option fill arrived while a ~122s scan was running; hedge submission must not be
gated behind the full scan finishing.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.broker import Broker, LumibotBrokerAPIError
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


def test_held_broker_rejection_preserves_error_when_sync_finishes():
    broker = _DummyBroker(_hold_trade_events=True)
    broker.logger = _Logger()
    order = Order("test", Asset("LW"), 100, "sell_short")
    error = LumibotBrokerAPIError("Good until canceled short sale orders are not accepted.")
    Broker._process_trade_event(broker, order, Broker.ERROR_ORDER, error=error)

    received = []
    broker._process_trade_event = lambda *args, **kwargs: received.append((args, kwargs))
    Broker.process_held_trades(broker)

    assert len(received) == 1
    assert received[0][0] == (order, Broker.ERROR_ORDER)
    assert received[0][1]["error"] is error


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
        self.iteration_release = None
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
        if self.iteration_release is not None:
            self.iteration_release.wait()
            self.iteration_finished.set()
            return
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
        if self.iteration_release is not None:
            self.iteration_release.set()

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


def test_fill_callback_runs_during_long_live_iteration_not_after(record_property):
    """BEFORE metric: fill waited until scan end (~scan_seconds).
    AFTER metric: fill callback during scan, latency << scan duration.
    """
    scan_seconds = 0.7
    strategy = _PriorityHedgeStrategy(scan_seconds=scan_seconds)
    strategy.iteration_release = threading.Event()
    executor = _build_live_executor(strategy)

    asset = Asset("CVNA", asset_type=Asset.AssetType.OPTION)
    position = _DummyPosition(asset=asset)
    order = _DummyOrder(asset=asset)

    fill_enqueued_at = {}

    def _enqueue_fill_mid_scan():
        strategy.iteration_started.wait()
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
    feeder.join()

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
    assert strategy.iteration_finished.is_set()
    record_property("fill_callback_latency_seconds", fill_latency)
    record_property("iteration_duration_seconds", elapsed)


def test_priority_events_wake_check_queue_without_half_second_sleep(record_property):
    strategy = _PriorityHedgeStrategy(scan_seconds=0.01)
    executor = _build_live_executor(strategy)

    asset = Asset("CVNA", asset_type=Asset.AssetType.STOCK)
    position = _DummyPosition(asset=asset)
    order = _DummyOrder(asset=asset)

    wake_to_fill = {}

    def _run_check_queue_once_via_wait():
        t_wait_start = time.monotonic()
        executor._queue_wakeup.wait()
        wake_to_fill["waited"] = time.monotonic() - t_wait_start
        executor.process_queue()

    waiter = threading.Thread(target=_run_check_queue_once_via_wait, daemon=True)
    waiter.start()
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
    waiter.join()

    assert strategy.recorded_fills, "priority wake did not drain fill"
    assert executor.priority_queue.empty()
    record_property("queue_wakeup_wait_seconds", wake_to_fill["waited"])


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


def test_first_live_iteration_preserves_new_fill_across_competing_drains():
    strategy = _PriorityHedgeStrategy()
    strategy._first_iteration = True
    strategy.broker._first_iteration = True
    executor = _build_live_executor(strategy)
    asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)
    startup_order = _DummyOrder(asset)
    startup_order.identifier = "startup-order"
    current_order = _DummyOrder(asset)
    current_order.identifier = "current-order"

    def payload(order):
        return dict(position=_DummyPosition(asset), order=order, price=10,
                    quantity=Decimal("1"), multiplier=1)

    executor.add_event(executor.FILLED_ORDER, payload(startup_order))

    def first_iteration():
        strategy.iteration_started.set()
        executor.add_event(executor.FILLED_ORDER, payload(current_order))
        consumers = [threading.Thread(target=executor.process_queue) for _ in range(2)]
        for consumer in consumers:
            consumer.start()
        for consumer in consumers:
            consumer.join()
        strategy.iteration_finished.set()

    executor._on_trading_iteration_callable = first_iteration
    executor._on_trading_iteration()
    assert [row["order"].identifier for row in strategy.recorded_fills] == ["current-order"]
    assert strategy.recorded_fills[0]["during_iteration"] is True
    assert executor.priority_queue.empty()


def test_startup_events_remain_suppressed_before_first_live_iteration():
    strategy = _PriorityHedgeStrategy()
    strategy._first_iteration = True
    strategy.broker._first_iteration = True
    executor = _build_live_executor(strategy)
    asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)
    executor.add_event(executor.FILLED_ORDER, dict(
        position=_DummyPosition(asset), order=_DummyOrder(asset), price=10,
        quantity=Decimal("1"), multiplier=1))
    executor.process_queue()
    assert strategy.recorded_fills == []
    assert executor.priority_queue.empty()


def test_first_live_iteration_keeps_foreign_order_filtering():
    strategy = _PriorityHedgeStrategy()
    strategy._first_iteration = True
    strategy.broker._first_iteration = True
    executor = _build_live_executor(strategy)
    asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)
    foreign_order = _DummyOrder(asset)
    foreign_order.strategy = "another-strategy"

    def first_iteration():
        executor.add_event(executor.FILLED_ORDER, dict(
            position=_DummyPosition(asset), order=foreign_order, price=10,
            quantity=Decimal("1"), multiplier=1))
        executor.process_queue()

    executor._on_trading_iteration_callable = first_iteration
    executor._on_trading_iteration()
    assert strategy.recorded_fills == []


def test_process_queue_uses_nonblocking_retrieval():
    strategy = _PriorityHedgeStrategy(scan_seconds=0.01)
    executor = _build_live_executor(strategy)

    class NonBlockingOnlyQueue:
        def get(self):
            raise AssertionError("process_queue must not use blocking Queue.get")

        def get_nowait(self):
            from queue import Empty

            raise Empty

    executor.queue = NonBlockingOnlyQueue()
    executor.process_queue()


def test_process_queue_drains_priority_events_before_normal_backlog():
    strategy = _PriorityHedgeStrategy(scan_seconds=0.01)
    executor = _build_live_executor(strategy)
    processed = []
    executor.process_event = lambda event, payload: processed.append(event)

    executor.add_event(executor.NEW_ORDER, {"sequence": 1})
    executor.add_event(executor.NEW_ORDER, {"sequence": 2})
    executor.add_event(executor.FILLED_ORDER, {"sequence": 3})
    executor.process_queue()

    assert processed == [executor.FILLED_ORDER, executor.NEW_ORDER, executor.NEW_ORDER]


def test_priority_event_preempts_remaining_normal_backlog_when_it_arrives_mid_drain():
    strategy = _PriorityHedgeStrategy(scan_seconds=0.01)
    executor = _build_live_executor(strategy)
    processed = []

    def process(event, payload):
        processed.append(event)
        if payload["sequence"] == 1:
            executor.add_event(executor.FILLED_ORDER, {"sequence": 3})

    executor.process_event = process
    executor.add_event(executor.NEW_ORDER, {"sequence": 1})
    executor.add_event(executor.NEW_ORDER, {"sequence": 2})
    executor.process_queue()

    assert processed == [executor.NEW_ORDER, executor.FILLED_ORDER, executor.NEW_ORDER]
