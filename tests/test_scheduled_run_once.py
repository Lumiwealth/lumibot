import datetime
import json
import logging
from types import SimpleNamespace

from lumibot.strategies import strategy as strategy_module
from lumibot.strategies import strategy_executor as strategy_executor_module
from lumibot.strategies._strategy import Vars, _Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor
from lumibot.traders.trader import Trader


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

    def _append_row(self, row):
        self.rows.append(row)

    def send_account_summary_to_discord(self):
        return None

    def load_variables_from_db(self):
        return None

    def backup_variables_to_db(self):
        self.backups += 1

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
