import datetime
import json
import logging
from types import SimpleNamespace

from lumibot.strategies import strategy as strategy_module
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
