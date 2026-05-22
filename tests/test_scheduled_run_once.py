import datetime
import json
import logging
from decimal import Decimal
from types import SimpleNamespace

import pytest

from lumibot.entities import Asset
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
        self.hide_trades = True
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

    def _update_cash(self, order, quantity, price, multiplier):
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

    def send_discord_message(self, *args, **kwargs):
        return None

    def on_filled_order(self, position, order, price, quantity, multiplier):
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


def test_scheduled_state_load_errors_fail_closed(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    state_file.write_text("{bad json", encoding="utf-8")
    strategy = object.__new__(_Strategy)
    strategy.is_backtesting = False
    strategy.vars = Vars()
    strategy.logger = logging.getLogger("test_scheduled_state")

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    with pytest.raises(json.JSONDecodeError):
        _Strategy.load_variables_from_db(strategy)


def test_scheduled_state_backup_errors_fail_closed(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    strategy = object.__new__(_Strategy)
    strategy.is_backtesting = False
    strategy.vars = Vars()
    strategy.vars.set("not_json", object())
    strategy.logger = logging.getLogger("test_scheduled_state")

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    with pytest.raises(TypeError):
        _Strategy.backup_variables_to_db(strategy)
    assert not state_file.exists()


def test_scheduled_state_rejects_sensitive_keys_before_local_write(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    strategy = object.__new__(_Strategy)
    strategy.is_backtesting = False
    strategy.vars = Vars()
    strategy.vars.set("api_key", "do-not-persist")
    strategy.logger = logging.getLogger("test_scheduled_state")

    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    with pytest.raises(ValueError, match="sensitive-looking keys"):
        _Strategy.backup_variables_to_db(strategy)
    assert not state_file.exists()


def test_scheduled_state_escapes_reserved_type_marker_dicts():
    original = {"payload": {"__lumibot_type__": "date", "value": "2026-05-12"}}

    restored = _Strategy._deserialize_variables_from_backup(
        _Strategy._serialize_variables_for_backup(original)
    )

    assert restored == original


def test_scheduled_state_preserves_decimal_precision_and_type():
    original = {"price": Decimal("123.456789123456789")}

    restored = _Strategy._deserialize_variables_from_backup(
        _Strategy._serialize_variables_for_backup(original)
    )

    assert restored == original
    assert isinstance(restored["price"], Decimal)


def test_scheduled_state_preserves_sets_with_stable_encoding():
    original = {
        "symbols": {"MSFT", "AAPL"},
        "typed_values": {Decimal("1.25"), Decimal("3.50")},
    }

    first_json = _Strategy._serialize_variables_for_backup(original)
    second_json = _Strategy._serialize_variables_for_backup(original)
    restored = _Strategy._deserialize_variables_from_backup(first_json)

    assert first_json == second_json
    assert restored == original
    assert isinstance(restored["symbols"], set)
    assert isinstance(restored["typed_values"], set)


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


def test_run_once_persists_state_when_no_previous_state_file_exists(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}],
    )
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    strategy = _ScheduledStateDummyStrategy()
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None

    assert executor.run_once() is True

    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["ran"] == 1
    assert strategy.iterations == 1


def test_scheduled_state_persists_across_multiple_run_once_invocations(tmp_path, monkeypatch):
    state_file = tmp_path / "scheduled_state.json"
    state_file.write_text(
        json.dumps(
            {
                "count": 1,
                "saved_broker_connection": {"id": "broker-connection-1", "provider": "alpaca"},
                "saved_env_var_sets": ["env-set-1"],
                "_agent_runtime_state": {
                    "research": {
                        "memory_notes": [{"summary": "prior compact note"}],
                        "runs": [{"date": "2026-05-10", "count": 0}],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "lumibot.strategies.strategy_executor.get_trading_days",
        lambda market: [{"date": datetime.date(2026, 5, 11)}, {"date": datetime.date(2026, 5, 12)}],
    )
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))

    class FilledOrder:
        asset = SimpleNamespace(asset_type=Asset.AssetType.STOCK)
        side = "buy"

        def is_parent(self):
            return False

        def is_buy_order(self):
            return True

    class Strategy(_ScheduledStateDummyStrategy):
        def __init__(self, now):
            super().__init__()
            self.now = now

        def get_datetime(self):
            return self.now

        def on_trading_iteration(self):
            self.iterations += 1
            count = self.vars.get("count", 0)
            seen_counts = list(self.vars.get("seen_counts", []))
            seen_counts.append(count)
            self.vars.set("seen_counts", seen_counts)
            self.vars.set("count", count + 1)
            self.vars.set(
                "historical_bars_assumptions",
                {
                    "symbol": "AAPL",
                    "lookback_days": 3,
                    "as_of": self.get_datetime().date(),
                    "uses_prior_close": True,
                },
            )

            agent_state = self.vars.get("_agent_runtime_state", {})
            research_state = agent_state.setdefault("research", {"memory_notes": [], "runs": []})
            research_state.setdefault("memory_notes", []).append(
                {"summary": f"iteration saw count {count}", "created_at": self.get_datetime()}
            )
            research_state.setdefault("runs", []).append(
                {"date": self.get_datetime().date(), "count": count}
            )
            self.vars.set("_agent_runtime_state", agent_state)

            self._executor.add_event(
                StrategyExecutor.FILLED_ORDER,
                {
                    "position": SimpleNamespace(asset="AAPL"),
                    "order": FilledOrder(),
                    "price": 101.25,
                    "quantity": 2,
                    "multiplier": 1,
                },
            )

        def on_filled_order(self, position, order, price, quantity, multiplier):
            fills = list(self.vars.get("filled_callbacks", []))
            fills.append(
                {
                    "date": self.get_datetime().date(),
                    "symbol": str(position.asset),
                    "price": price,
                    "quantity": quantity,
                }
            )
            self.vars.set("filled_callbacks", fills)

    first_strategy = Strategy(datetime.datetime(2026, 5, 11, 9, 30))
    first_executor = StrategyExecutor(first_strategy)
    first_strategy._executor = first_executor
    first_executor.sync_broker = lambda: None

    assert first_executor.run_once() is True

    after_first = json.loads(state_file.read_text(encoding="utf-8"))
    assert after_first["count"] == 2
    assert after_first["seen_counts"] == [1]
    assert after_first["filled_callbacks"][0]["symbol"] == "AAPL"
    assert after_first["_agent_runtime_state"]["research"]["memory_notes"][-1]["summary"] == "iteration saw count 1"

    second_strategy = Strategy(datetime.datetime(2026, 5, 12, 9, 30))
    second_executor = StrategyExecutor(second_strategy)
    second_strategy._executor = second_executor
    second_executor.sync_broker = lambda: None

    assert second_executor.run_once() is True

    after_second = json.loads(state_file.read_text(encoding="utf-8"))
    assert after_second["count"] == 3
    assert after_second["seen_counts"] == [1, 2]
    assert after_second["saved_broker_connection"] == {"id": "broker-connection-1", "provider": "alpaca"}
    assert after_second["saved_env_var_sets"] == ["env-set-1"]
    assert after_second["filled_callbacks"] == [
        {"date": {"__lumibot_type__": "date", "value": "2026-05-11"}, "symbol": "AAPL", "price": 101.25, "quantity": 2},
        {"date": {"__lumibot_type__": "date", "value": "2026-05-12"}, "symbol": "AAPL", "price": 101.25, "quantity": 2},
    ]
    assert after_second["historical_bars_assumptions"] == {
        "symbol": "AAPL",
        "lookback_days": 3,
        "as_of": {"__lumibot_type__": "date", "value": "2026-05-12"},
        "uses_prior_close": True,
    }
    assert after_second["_agent_runtime_state"]["research"]["memory_notes"][-2:] == [
        {"summary": "iteration saw count 1", "created_at": {"__lumibot_type__": "datetime", "value": "2026-05-11T09:30:00"}},
        {"summary": "iteration saw count 2", "created_at": {"__lumibot_type__": "datetime", "value": "2026-05-12T09:30:00"}},
    ]
