import datetime
import json
import logging
from queue import Queue
from types import SimpleNamespace

import pytest

from lumibot.strategies import strategy as strategy_module
from lumibot.strategies import strategy_executor as strategy_executor_module
from lumibot.strategies._strategy import Vars, _Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor
from lumibot.traders.trader import Trader
from lumibot.entities import Asset, Order, SmartLimitConfig
from lumibot.strategies.scheduled_timing import ScheduledRunTiming


class _DummyBroker:
    IS_BACKTESTING_BROKER = False
    market = "NYSE"

    def __init__(self, market_open=True):
        self._first_iteration = True
        self._orders_queue = SimpleNamespace(queue=[])
        self.closed = False
        self.strategy_name = None
        self.trading_days = None
        self.market_open = market_open

    def is_backtesting_broker(self):
        return False

    def set_strategy_name(self, name):
        self.strategy_name = name

    def initialize_market_calendars(self, trading_days):
        self.trading_days = trading_days

    def is_market_open(self):
        return self.market_open

    def _close_connection(self):
        self.closed = True


class _DummyStrategy:
    def __init__(self):
        self.broker = _DummyBroker()
        self._name = "dummy"
        self.parameters = {}
        self.is_backtesting = False
        self.logger = logging.getLogger("test_scheduled_run_once")
        self.sleeptime = "1D"
        self._analysis = {}
        self._first_iteration = True
        self._last_on_trading_iteration_datetime = None
        self.portfolio_value = 100
        self.cash = 100
        self.vars = Vars()
        self.rows = []
        self.initialized = 0
        self.before_starting = 0
        self.iterations = 0
        self.ended = 0
        self.backups = 0
        self.cloud_updates = 0
        self.lifecycle_events = []
        self.orders = []
        self.published_orders = []

    @property
    def name(self):
        return self._name

    def log_message(self, *args, **kwargs):
        return None

    def initialize(self):
        self.initialized += 1

    def before_starting_trading(self):
        self.before_starting += 1

    def on_trading_iteration(self):
        self.iterations += 1
        self.vars.set("ran", self.iterations)

    def on_strategy_end(self):
        self.ended += 1
        self.lifecycle_events.append("strategy_end")

    def _dump_stats(self):
        self._analysis = {"iterations": self.iterations}

    def _update_portfolio_value(self):
        return None

    def _apply_daily_cash_financing_if_needed(self):
        return None

    def _copy_dict(self):
        return {}

    def trace_stats(self, context, snapshot_before):
        return {}

    def get_datetime(self):
        return datetime.datetime(2026, 5, 11, 9, 30)

    def get_positions(self):
        return []

    def get_orders(self):
        return self.orders

    def _append_row(self, row):
        self.rows.append(row)

    def send_account_summary_to_discord(self):
        return None

    def load_variables_from_db(self):
        return None

    def backup_variables_to_db(self):
        self.backups += 1

    def send_update_to_cloud(self):
        assert self.broker.closed is False
        self.cloud_updates += 1
        self.published_orders = list(self.get_orders())
        self.lifecycle_events.append("cloud_update")
        return True

    def on_bot_crash(self, error):
        return None


class _ScheduledStateDummyStrategy(_DummyStrategy, _Strategy):
    load_variables_from_db = _Strategy.load_variables_from_db
    backup_variables_to_db = _Strategy.backup_variables_to_db

    @property
    def cash(self):
        return self._cash

    @cash.setter
    def cash(self, value):
        self._cash = value


def _completion_executor(monkeypatch, *, post_seconds=0):
    """Real run_once and SmartLimit engine with deterministic broker/clock transport."""
    strategy = _DummyStrategy()
    strategy.broker.market = "24/7"
    strategy.broker._orders_queue = Queue()
    strategy.broker.get_active_tracked_orders = lambda strategy=None: [
        order for order in strategy_orders if order.is_active()
    ]
    strategy_orders = strategy.orders
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None
    executor._on_trading_iteration_callable = lambda: strategy.on_trading_iteration()
    # The drain owns progress in these deterministic tests, instead of a wall-clock thread.
    executor.check_queue = lambda: None
    clock = {"value": 0.0}
    base = datetime.datetime(2026, 10, 1, 14, 0, tzinfo=datetime.timezone.utc)
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TARGET_RUN_AT", base.isoformat())
    monkeypatch.setenv("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", str(post_seconds))
    monkeypatch.setattr(executor, "_scheduled_now_utc", lambda: base + datetime.timedelta(seconds=clock["value"]))
    monkeypatch.setattr(strategy_executor_module.time, "monotonic", lambda: clock["value"])
    monkeypatch.setattr(strategy_executor_module.time, "sleep", lambda seconds: clock.__setitem__("value", clock["value"] + seconds))
    return strategy, executor, clock


def _smart_order(strategy, *, hold=2, step=1):
    return Order(
        strategy.name, Asset("SPY"), 1, "buy", identifier="scheduled-smart",
        status="open", order_type="smart_limit",
        smart_limit=SmartLimitConfig(preset="fast", step_seconds=step, final_hold_seconds=hold),
        limit_price=100.0,
    )


@pytest.mark.parametrize("post_seconds", [0, 1])
def test_run_once_waits_for_smart_limit_and_broker_cancel_confirmation(monkeypatch, post_seconds):
    strategy, executor, clock = _completion_executor(monkeypatch, post_seconds=post_seconds)
    order = _smart_order(strategy)
    reprices = []
    cancel_requested = []
    strategy.get_quote = lambda asset, **kwargs: SimpleNamespace(bid=99.0, ask=101.0)
    strategy.broker.modify_order = lambda order, **kwargs: reprices.append((clock["value"], kwargs["limit_price"]))

    def cancel(order):
        cancel_requested.append(clock["value"])
        order.status = "cancelling"

    strategy.broker.cancel_order = cancel
    strategy.on_trading_iteration = lambda: strategy.orders.append(order)
    real_process = executor.process_queue

    def process():
        if clock["value"] >= 5 and cancel_requested:
            order.status = "canceled"
        real_process()

    executor.process_queue = process
    assert executor.run_once() is True
    assert len(reprices) == 2
    assert len(cancel_requested) == 1
    assert cancel_requested[0] == 4
    assert clock["value"] >= 5
    assert order.is_canceled()
    assert strategy.published_orders == [order]
    assert strategy.broker.closed


@pytest.mark.parametrize("terminal_status", ["fill", "error", "canceled"])
def test_run_once_releases_smart_limit_on_terminal_broker_status(monkeypatch, terminal_status):
    strategy, executor, clock = _completion_executor(monkeypatch)
    order = _smart_order(strategy, hold=999)
    strategy.on_trading_iteration = lambda: strategy.orders.append(order)
    strategy.get_quote = lambda asset, **kwargs: SimpleNamespace(bid=99.0, ask=101.0)
    strategy.broker.modify_order = lambda *args, **kwargs: None
    strategy.broker.cancel_order = lambda *args, **kwargs: pytest.fail("Must not cancel a terminal order")

    def process():
        if clock["value"] >= 1:
            order.status = terminal_status

    executor.process_queue = process
    assert executor.run_once() is True
    assert clock["value"] == 1
    assert strategy.published_orders[0].status == terminal_status


def test_run_once_waits_for_dequeued_submission_and_end_hook_work(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    submissions = strategy.broker._orders_queue
    release_times = []

    def begin_submission():
        submissions.put(object())
        submissions.get_nowait()  # Empty queue, but HTTP submission is still in flight.
        release_times.append(clock["value"] + 1)

    strategy.on_trading_iteration = begin_submission
    original_end = strategy.on_strategy_end

    def end():
        original_end()
        begin_submission()

    strategy.on_strategy_end = end

    def process():
        if release_times and clock["value"] >= release_times[0]:
            submissions.task_done()
            release_times.pop(0)

    executor.process_queue = process
    assert executor.run_once() is True
    assert clock["value"] == 2
    assert submissions.unfinished_tasks == 0
    assert strategy.broker.closed


def test_run_once_does_not_wait_for_passive_gtc_orders(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    strategy.on_trading_iteration = lambda: strategy.orders.append(Order(
        strategy.name, Asset("SPY"), 1, "buy", status="open", order_type="limit", limit_price=10,
        time_in_force="gtc",
    ))
    assert executor.run_once() is True
    assert clock["value"] == 0
    assert strategy.orders[0].is_active()


def test_completed_smart_entry_with_passive_bracket_child_does_not_block_exit(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    parent = _smart_order(strategy)
    parent.status = "fill"
    parent.child_orders.append(Order(
        strategy.name, Asset("SPY"), 1, "sell", status="open", order_type="stop", stop_price=90,
    ))
    strategy.orders.append(parent)
    assert parent.is_active()  # The protective child remains at the broker.
    assert executor._scheduled_pending_work() == {}
    assert executor.run_once() is True
    assert clock["value"] == 0


def test_scheduled_drain_reports_timeout_instead_of_success(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    strategy.broker._orders_queue.put(object())
    executor._scheduled_work_timeout_seconds = lambda: 1
    assert executor.run_once() is False
    assert isinstance(executor.exception, TimeoutError)
    assert clock["value"] == 1
    assert strategy.cloud_updates == 0
    assert executor._get_scheduled_timing()._timing["status"] == "drain_failed"
    assert strategy.broker.closed


def test_scheduled_drain_stop_does_not_claim_completion(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    strategy.broker._orders_queue.put(object())
    executor.process_queue = lambda: executor.stop_event.set() if clock["value"] >= 1 else None
    assert executor.run_once() is False
    assert isinstance(executor.exception, InterruptedError)
    assert executor._get_scheduled_timing()._timing["status"] == "drain_interrupted"
    assert strategy.cloud_updates == 0


def test_scheduled_drain_stop_racing_with_final_fill_is_not_success(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    order = _smart_order(strategy)
    strategy.orders.append(order)
    strategy.broker.modify_order = lambda *args, **kwargs: None

    def process():
        if clock["value"] >= 1:
            order.status = "fill"
            executor.stop_event.set()

    executor.process_queue = process
    assert executor.run_once() is False
    assert isinstance(executor.exception, InterruptedError)
    assert executor._get_scheduled_timing()._timing["status"] == "drain_interrupted"


def test_run_once_callback_can_start_another_smart_limit(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    first = _smart_order(strategy)
    second = _smart_order(strategy)
    second.identifier = "callback-smart"
    strategy.on_trading_iteration = lambda: strategy.orders.append(first)
    strategy.broker.modify_order = lambda *args, **kwargs: None
    strategy.broker.cancel_order = lambda *args, **kwargs: pytest.fail("Both orders fill")
    strategy.get_quote = lambda *args, **kwargs: SimpleNamespace(bid=99.0, ask=101.0)
    callbacks = []

    def on_fill(**kwargs):
        callbacks.append(clock["value"])
        strategy.orders.append(second)

    executor._on_filled_order = on_fill
    real_process = executor.process_queue

    def process():
        if clock["value"] >= 1 and first.is_active():
            first.status = "fill"
            executor.priority_queue.put((executor.FILLED_ORDER, {
                "position": None, "order": first, "price": 100.0, "quantity": 1, "multiplier": 1,
            }))
        if clock["value"] >= 2 and second in strategy.orders:
            second.status = "fill"
        real_process()

    # Cash accounting itself is covered by the normal trade-event tests.
    strategy._update_cash = lambda *args, **kwargs: None
    executor.process_queue = process
    assert executor.run_once() is True
    assert callbacks == [1]
    assert clock["value"] == 2
    assert [order.status for order in strategy.published_orders] == ["fill", "fill"]
    assert strategy.backups >= 2


def test_run_once_waits_for_non_smart_cancel_confirmation(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    order = Order(strategy.name, Asset("SPY"), 1, "buy", status="cancelling", order_type="limit", limit_price=10)
    strategy.on_trading_iteration = lambda: strategy.orders.append(order)
    executor.process_queue = lambda: setattr(order, "status", "canceled") if clock["value"] >= 1 else None
    assert executor.run_once() is True
    assert clock["value"] == 1
    assert order.is_canceled()


def test_scheduled_smart_limit_long_config_is_not_cut_off_by_default_timeout(monkeypatch):
    strategy, executor, clock = _completion_executor(monkeypatch)
    order = _smart_order(strategy, hold=999)
    strategy.on_trading_iteration = lambda: strategy.orders.append(order)
    strategy.broker.modify_order = lambda *args, **kwargs: None
    strategy.broker.cancel_order = lambda *args, **kwargs: pytest.fail("Fills within configured lifetime")
    strategy.get_quote = lambda *args, **kwargs: SimpleNamespace(bid=99.0, ask=101.0)
    executor.process_queue = lambda: setattr(order, "status", "fill") if clock["value"] >= 305 else None
    assert executor.run_once() is True
    assert clock["value"] == 305


def test_timing_drain_waits_for_pending_work_without_scheduled_environment(monkeypatch):
    monkeypatch.delenv("LUMIBOT_SCHEDULED_EXECUTION", raising=False)
    monkeypatch.delenv("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", raising=False)
    clock = {"value": 0.0}
    from threading import Event

    ScheduledRunTiming().drain_after_iteration(
        stop_event=Event(), process_queue=lambda: None,
        pending_work=lambda: clock["value"] < 2,
        monotonic=lambda: clock["value"],
        sleep=lambda seconds: clock.__setitem__("value", clock["value"] + seconds),
    )
    assert clock["value"] == 2


def test_strategy_executor_run_once_runs_one_live_iteration(monkeypatch):
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    strategy = _DummyStrategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True

    assert strategy.initialized == 1
    assert strategy.before_starting == 1
    assert strategy.iterations == 1
    assert strategy.ended == 1
    assert strategy.cloud_updates == 1
    assert strategy.lifecycle_events == ["strategy_end", "cloud_update"]
    assert strategy.backups >= 1
    assert strategy.broker.closed is True
    assert executor.result == {"iterations": 1}


def test_strategy_executor_run_once_skips_calendar_for_24_7_market(monkeypatch):
    def fail_get_trading_days(*args, **kwargs):
        raise AssertionError("24/7 run_once should not build exchange calendars")

    monkeypatch.setattr("lumibot.strategies.strategy_executor.get_trading_days", fail_get_trading_days)
    strategy = _DummyStrategy()
    strategy.broker.market = "24/7"
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True
    assert strategy.iterations == 1


def test_run_once_publishes_final_cloud_state_after_scheduled_drain(monkeypatch):
    strategy = _DummyStrategy()
    strategy.broker.market = "24/7"
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    def drain():
        strategy.lifecycle_events.append("broker_drain")
        strategy.orders.append({
            "identifier": "synthetic-filled-order-123",
            "status": "filled",
        })

    executor._scheduled_drain_after_iteration = drain

    assert executor.run_once() is True

    assert strategy.lifecycle_events == ["broker_drain", "strategy_end", "cloud_update"]
    assert strategy.cloud_updates == 1
    assert strategy.published_orders == [{
        "identifier": "synthetic-filled-order-123",
        "status": "filled",
    }]
    assert strategy.broker.closed is True


def test_run_once_cloud_publish_failure_does_not_change_success(monkeypatch):
    strategy = _DummyStrategy()
    strategy.broker.market = "24/7"
    strategy.send_update_to_cloud = lambda: (_ for _ in ()).throw(RuntimeError("listener unavailable"))
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True
    assert strategy.ended == 1
    assert strategy.broker.closed is True


def test_run_once_empty_parameters_skips_initialize_signature_inspection(monkeypatch):
    def fail_getfullargspec(*args, **kwargs):
        raise AssertionError("empty strategy parameters should not inspect initialize signature")

    strategy = _DummyStrategy()
    strategy.broker.market = "24/7"
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setattr(strategy_executor_module, "_getfullargspec", fail_getfullargspec)

    assert executor.run_once() is True
    assert strategy.initialized == 1
    assert strategy.iterations == 1


def test_run_once_no_arg_initialize_with_parameters_skips_signature_inspection(monkeypatch):
    def fail_getfullargspec(*args, **kwargs):
        raise AssertionError("plain no-arg initialize should use cheap code arg scan")

    strategy = _DummyStrategy()
    strategy.parameters = {"portfolio": [], "rebalance_period": 4}
    strategy.broker.market = "24/7"
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setattr(strategy_executor_module, "_getfullargspec", fail_getfullargspec)

    assert executor.run_once() is True
    assert strategy.initialized == 1
    assert strategy.iterations == 1


def test_run_once_closed_market_calendar_uses_wall_clock_not_strategy_datetime(monkeypatch):
    calls = {}

    def fake_get_trading_days(*args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return [{"date": datetime.date(2026, 5, 11)}]

    strategy = _DummyStrategy()
    strategy.broker.market_open = False
    strategy.get_datetime = lambda: (_ for _ in ()).throw(
        AssertionError("closed-market run_once should not touch strategy data-source time")
    )
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setattr("lumibot.strategies.strategy_executor.get_trading_days", fake_get_trading_days)
    monkeypatch.setattr(
        StrategyExecutor,
        "_scheduled_now_utc",
        lambda self: datetime.datetime(2026, 5, 11, 13, 30, tzinfo=datetime.timezone.utc),
    )

    assert executor.run_once() is True

    assert strategy.initialized == 1
    assert strategy.iterations == 0
    assert calls["kwargs"]["start_date"] == datetime.datetime(2026, 4, 27, 13, 30, tzinfo=datetime.timezone.utc)
    assert calls["kwargs"]["end_date"] == datetime.datetime(2026, 5, 26, 13, 30, tzinfo=datetime.timezone.utc)


def test_run_once_regular_equity_preopen_skips_calendar_and_market_check(monkeypatch):
    def fail_get_trading_days(*args, **kwargs):
        raise AssertionError("pre-open run_once should not build exchange calendars")

    def fail_market_open():
        raise AssertionError("pre-open run_once should use the scheduled wall-clock precheck")

    strategy = _DummyStrategy()
    strategy.broker.name = "alpaca"
    strategy.broker.market = "NASDAQ"
    strategy.broker.is_market_open = fail_market_open
    strategy.get_datetime = lambda: (_ for _ in ()).throw(
        AssertionError("pre-open run_once should not touch strategy data-source time")
    )
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setattr("lumibot.strategies.strategy_executor.get_trading_days", fail_get_trading_days)
    monkeypatch.setattr(
        StrategyExecutor,
        "_scheduled_now_utc",
        lambda self: datetime.datetime(2026, 5, 11, 12, 0, tzinfo=datetime.timezone.utc),
    )

    assert executor.run_once() is True

    assert strategy.initialized == 1
    assert strategy.iterations == 0
    assert strategy.broker.trading_days is None


def test_scheduled_exact_run_waits_after_initialization_and_writes_timing(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    fake_mono = {"value": 0.0}
    base = datetime.datetime(2026, 5, 11, 13, 30, tzinfo=datetime.timezone.utc)
    target = base + datetime.timedelta(seconds=1)
    timing_file = tmp_path / "scheduled_timing.json"
    strategy = _DummyStrategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TARGET_RUN_AT", target.isoformat().replace("+00:00", "Z"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", "1000")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", "0")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TIMING_FILE", str(timing_file))
    monkeypatch.setattr(StrategyExecutor, "_scheduled_now_utc", lambda self: base + datetime.timedelta(seconds=fake_mono["value"]))
    monkeypatch.setattr(strategy_executor_module.time, "monotonic", lambda: fake_mono["value"])
    monkeypatch.setattr(strategy_executor_module.time, "sleep", lambda seconds: fake_mono.__setitem__("value", fake_mono["value"] + seconds))

    assert executor.run_once() is True

    assert strategy.initialized == 1
    assert strategy.iterations == 1
    timing = json.loads(timing_file.read_text(encoding="utf-8"))
    assert timing["strategy_initialized_at"] == "2026-05-11T13:30:00Z"
    assert timing["wait_started_at"] == "2026-05-11T13:30:00Z"
    assert timing["iteration_started_at"] == "2026-05-11T13:30:01Z"
    assert timing["target_drift_ms"] == 0
    assert timing["status"] == "completed"
    assert timing["exact_timing_verified"] is True


def test_scheduled_exact_run_waits_before_preopen_market_closed_exit(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda *args, **kwargs: [{"date": datetime.date(2026, 5, 11)}],
    )
    fake_mono = {"value": 0.0}
    base = datetime.datetime(2026, 5, 11, 13, 29, 50, tzinfo=datetime.timezone.utc)
    target = base + datetime.timedelta(seconds=10)
    timing_file = tmp_path / "scheduled_timing.json"
    strategy = _DummyStrategy()
    strategy.broker.name = "alpaca"
    strategy.broker.market = "NASDAQ"
    strategy.broker.is_market_open = lambda: fake_mono["value"] >= 10.0
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TARGET_RUN_AT", target.isoformat().replace("+00:00", "Z"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", "1000")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", "0")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TIMING_FILE", str(timing_file))
    monkeypatch.setattr(
        StrategyExecutor,
        "_scheduled_now_utc",
        lambda self: base + datetime.timedelta(seconds=fake_mono["value"]),
    )
    monkeypatch.setattr(strategy_executor_module.time, "monotonic", lambda: fake_mono["value"])
    monkeypatch.setattr(
        strategy_executor_module.time,
        "sleep",
        lambda seconds: fake_mono.__setitem__("value", fake_mono["value"] + seconds),
    )

    assert executor.run_once() is True

    assert strategy.iterations == 1
    assert strategy.broker.trading_days is not None
    timing = json.loads(timing_file.read_text(encoding="utf-8"))
    assert timing["wait_started_at"] == "2026-05-11T13:29:50Z"
    assert timing["iteration_started_at"] == "2026-05-11T13:30:00Z"
    assert timing["status"] == "completed"


def test_scheduled_exact_run_skips_when_target_drift_exceeds_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    now = datetime.datetime(2026, 5, 11, 13, 30, 2, tzinfo=datetime.timezone.utc)
    target = datetime.datetime(2026, 5, 11, 13, 30, tzinfo=datetime.timezone.utc)
    timing_file = tmp_path / "scheduled_timing.json"
    strategy = _DummyStrategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TARGET_RUN_AT", target.isoformat().replace("+00:00", "Z"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", "1000")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TIMING_FILE", str(timing_file))
    monkeypatch.setattr(StrategyExecutor, "_scheduled_now_utc", lambda self: now)
    monkeypatch.setattr(strategy_executor_module.time, "monotonic", lambda: 10.0)

    assert executor.run_once() is False

    assert strategy.initialized == 1
    assert strategy.iterations == 0
    assert strategy.ended == 0
    assert strategy.backups >= 1
    assert strategy.broker.closed is True
    timing = json.loads(timing_file.read_text(encoding="utf-8"))
    assert timing["status"] == "missed_target"
    assert timing["target_drift_ms"] == 2000
    assert timing["exact_timing_verified"] is False


def test_scheduled_exact_run_drains_after_iteration(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    fake_mono = {"value": 0.0}
    queue_calls = {"count": 0}
    base = datetime.datetime(2026, 5, 11, 13, 30, tzinfo=datetime.timezone.utc)
    timing_file = tmp_path / "scheduled_timing.json"
    strategy = _DummyStrategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None
    executor.process_queue = lambda: queue_calls.__setitem__("count", queue_calls["count"] + 1)

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TARGET_RUN_AT", base.isoformat().replace("+00:00", "Z"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", "1000")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", "1")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_TIMING_FILE", str(timing_file))
    monkeypatch.setattr(StrategyExecutor, "_scheduled_now_utc", lambda self: base + datetime.timedelta(seconds=fake_mono["value"]))
    monkeypatch.setattr(strategy_executor_module.time, "monotonic", lambda: fake_mono["value"])
    monkeypatch.setattr(strategy_executor_module.time, "sleep", lambda seconds: fake_mono.__setitem__("value", fake_mono["value"] + seconds))

    assert executor.run_once() is True

    timing = json.loads(timing_file.read_text(encoding="utf-8"))
    assert strategy.iterations == 1
    assert queue_calls["count"] >= 4
    assert timing["drain_started_at"] == "2026-05-11T13:30:00Z"
    assert timing["drain_finished_at"] == "2026-05-11T13:30:01Z"
    assert timing["status"] == "completed"


def test_trader_run_all_run_once_uses_executor_run_once():
    class Executor:
        name = "dummy"
        result = {"ok": True}
        exception = None

        def __init__(self):
            self.called = False

        def run_once(self):
            self.called = True

    class Strategy:
        broker = _DummyBroker()

        def __init__(self):
            self._executor = Executor()

    strategy = Strategy()
    trader = Trader(strategies=[strategy])

    result = trader.run_all(run_once=True)

    assert strategy._executor.called is True
    assert result == {"dummy": {"ok": True}}


def test_run_live_enables_run_once_for_scheduled_execution(monkeypatch):
    captured = {}

    class DummyTrader:
        def add_strategy(self, strategy):
            captured["strategy"] = strategy

        def run_all(self, **kwargs):
            captured.update(kwargs)

    strategy = object.__new__(strategy_module.Strategy)
    monkeypatch.setattr(strategy_module, "Trader", DummyTrader)
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")

    strategy_module.Strategy.run_live(strategy)

    assert captured["strategy"] is strategy
    assert captured["run_once"] is True


def test_run_live_explicit_run_once_false_overrides_env(monkeypatch):
    captured = {}

    class DummyTrader:
        def add_strategy(self, strategy):
            captured["strategy"] = strategy

        def run_all(self, **kwargs):
            captured.update(kwargs)

    strategy = object.__new__(strategy_module.Strategy)
    monkeypatch.setattr(strategy_module, "Trader", DummyTrader)
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")

    strategy_module.Strategy.run_live(strategy, run_once=False)

    assert captured["strategy"] is strategy
    assert captured["run_once"] is False


def test_scheduled_state_file_loads_and_persists_self_vars(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    state_file.write_text(
        json.dumps(
            {
                "count": 2,
                "trade_date": "2026-05-11",
                "run_date": {"__lumibot_type__": "date", "value": "2026-05-10"},
                "run_at": {"__lumibot_type__": "datetime", "value": "2026-05-11T09:30:00"},
                "bands": {"__lumibot_type__": "tuple", "value": ["low", "high"]},
            }
        ),
        encoding="utf-8",
    )
    strategy = object.__new__(_Strategy)
    strategy.is_backtesting = False
    strategy.vars = Vars()
    strategy.logger = logging.getLogger("test_scheduled_state")
    strategy._last_backup_state = None

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    _Strategy.load_variables_from_db(strategy)

    assert strategy.vars.get("count") == 2
    assert strategy.vars.get("trade_date") == "2026-05-11"
    assert strategy.vars.get("run_date") == datetime.date(2026, 5, 10)
    assert strategy.vars.get("run_at") == datetime.datetime(2026, 5, 11, 9, 30)
    assert strategy.vars.get("bands") == ("low", "high")

    strategy.vars.set("count", 3)
    strategy.vars.set("next_date", datetime.date(2026, 5, 12))
    strategy.vars.set("limits", (1, datetime.date(2026, 5, 13)))
    _Strategy.backup_variables_to_db(strategy)

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["count"] == 3
    assert persisted["trade_date"] == "2026-05-11"
    assert persisted["next_date"] == {"__lumibot_type__": "date", "value": "2026-05-12"}
    assert persisted["limits"] == {
        "__lumibot_type__": "tuple",
        "value": [1, {"__lumibot_type__": "date", "value": "2026-05-13"}],
    }


def test_run_once_restores_state_before_closed_market_exit(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    state_file.write_text('{"count": 2}', encoding="utf-8")
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    strategy = _ScheduledStateDummyStrategy()
    strategy.broker = _DummyBroker(market_open=False)
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted == {"count": 2}
    assert strategy.iterations == 0
    assert strategy.ended == 0


def test_run_once_restores_state_before_lifecycle_hooks(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    state_file.write_text('{"count": 2}', encoding="utf-8")
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    class Strategy(_ScheduledStateDummyStrategy):
        def before_starting_trading(self):
            super().before_starting_trading()
            self.vars.set("count", self.vars.get("count") + 1)

    strategy = Strategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["count"] == 3
    assert strategy.iterations == 1
