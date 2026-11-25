"""Opt-in saved memory (``self.vars``) for live runs, kept in a local JSON file.

Turned on only by ``LUMIBOT_LIVE_VARS_FILE``. The file is read once when the
strategy starts and never again during the run; it is written atomically after
an iteration once the save interval has passed, right after order events, and
on shutdown. Backtests and scheduled one-shot runs never use it.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import signal
from types import SimpleNamespace

import pytest

from lumibot.entities import Asset, Order
from lumibot.strategies import live_vars_file as live_vars_module
from lumibot.strategies._strategy import Vars, _Strategy
from lumibot.strategies.live_vars_file import (
    DEFAULT_SAVE_INTERVAL_SECONDS,
    ENV_FILE,
    ENV_INTERVAL,
    LiveVarsFile,
)
from lumibot.strategies.strategy_executor import StrategyExecutor


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class _Broker:
    IS_BACKTESTING_BROKER = False
    market = "24/7"

    def __init__(self):
        self._first_iteration = True
        self._orders_queue = SimpleNamespace(queue=[])

    def is_market_open(self):
        return True

    def set_strategy_name(self, name):
        self.strategy_name = name

    def order_belongs_to_local_strategy(self, order, strategy=None):
        return True

    def _record_foreign_callback(self):
        pass

    def _record_fill_callback_entry(self):
        pass

    def _get_order_health(self):
        return SimpleNamespace(iteration=lambda runtime: None)

    def _speed_summary(self):
        return SimpleNamespace(iteration=lambda runtime: None, maybe_emit=lambda logger: None,
                               fill_callback=lambda a, b: None)


class _Strategy_(_Strategy):
    """Small live strategy: the real ``_Strategy`` variable code, no broker."""

    # The DB backup path stays off: no DB_CONNECTION_STR in these tests.
    db_connection_str = None
    hide_trades = False
    should_backup_variables_to_database = False

    def __init__(self, is_backtesting=False):
        self.broker = _Broker()
        self._name = "memory_test"
        self.parameters = {}
        self.is_backtesting = is_backtesting
        self.logger = logging.getLogger("test_live_vars_file")
        self.sleeptime = "1M"
        self._analysis = {}
        self._first_iteration = True
        self._last_on_trading_iteration_datetime = None
        self._last_backup_state = None
        self.vars = Vars()
        self.iterations = 0
        self.initialize_saw = None

    @property
    def name(self):
        return self._name

    @property
    def portfolio_value(self):
        return 100.0

    @property
    def cash(self):
        return 100.0

    def log_message(self, *args, **kwargs):
        return None

    def initialize(self):
        self.initialize_saw = self.vars.all()
        # A typical strategy sets its defaults here.
        self.vars.set("legs", None)

    def on_trading_iteration(self):
        self.iterations += 1
        self.vars.set("iterations", self.iterations)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.vars.set("legs", [order.asset.symbol])

    def on_partially_filled_order(self, position, order, price, quantity, multiplier):
        self.vars.set("partial", True)

    def on_canceled_order(self, order):
        self.vars.set("canceled", order.identifier)

    def _update_portfolio_value(self):
        return None

    def _apply_daily_cash_financing_if_needed(self):
        return None

    def _copy_dict(self):
        return {}

    def trace_stats(self, context, snapshot_before):
        return {}

    def get_datetime(self):
        return datetime.datetime(2026, 11, 24, 10, 0)

    def get_positions(self):
        return []

    def get_orders(self):
        return []

    def _append_row(self, row):
        return None

    def send_account_summary_to_discord(self):
        return None

    def send_discord_message(self, *args, **kwargs):
        return None

    def _checkpoint_order_ownership(self):
        return None

    def _restore_order_ownership(self):
        return None


def _read(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _write(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _executor(strategy):
    executor = StrategyExecutor(strategy)
    executor.sync_broker = lambda: None
    executor.process_queue = lambda: None
    executor._on_trading_iteration_callable = strategy.on_trading_iteration
    executor._run_live_trading_iteration_with_priority_drains = lambda fn: fn()
    return executor


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (ENV_FILE, ENV_INTERVAL, "LUMIBOT_SCHEDULED_EXECUTION", "LUMIBOT_SCHEDULED_STATE_FILE",
                 "DB_CONNECTION_STR"):
        monkeypatch.delenv(name, raising=False)


# ---------------------------------------------------------------- off by default


def test_off_by_default_no_store_no_file(tmp_path):
    strategy = _Strategy_()
    assert LiveVarsFile.from_environment(strategy) is None

    executor = _executor(strategy)
    executor._activate_live_vars_file()
    strategy.vars.set("a", 1)
    executor._on_trading_iteration()
    executor._save_live_vars_now("shutdown")

    assert executor._live_vars_file is None
    assert list(tmp_path.iterdir()) == []


def test_default_interval_is_600_seconds(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    store = LiveVarsFile.from_environment(_Strategy_())
    assert DEFAULT_SAVE_INTERVAL_SECONDS == 600
    assert store.interval_seconds == 600


def test_bad_interval_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    monkeypatch.setenv(ENV_INTERVAL, "soon")
    assert LiveVarsFile.from_environment(_Strategy_()).interval_seconds == 600


def test_backtest_ignores_setting(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    _write(path, {"lumibot_live_vars": 1, "vars": {"a": 5}})
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_(is_backtesting=True)

    assert LiveVarsFile.from_environment(strategy) is None
    executor = _executor(strategy)
    executor._activate_live_vars_file()
    assert executor._live_vars_file is None
    assert strategy.vars.get("a") is None


def test_scheduled_run_once_ignores_setting(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    assert LiveVarsFile.from_environment(_Strategy_(), run_once=True) is None


# ---------------------------------------------------------------- load once


def test_loads_once_after_initialize_and_restored_values_win(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    _write(path, {"lumibot_live_vars": 1, "vars": {"legs": ["SPY 500C"], "count": 3}})
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    executor = _executor(strategy)

    executor._initialize()
    executor._activate_live_vars_file()

    # initialize() set legs=None; the saved memory comes back on top of it.
    assert strategy.vars.get("legs") == ["SPY 500C"]
    assert strategy.vars.get("count") == 3


def test_never_reloads_mid_run_even_if_file_changes(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    _write(path, {"lumibot_live_vars": 1, "vars": {"count": 1}})
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()

    loads = []
    original = LiveVarsFile.load_once
    monkeypatch.setattr(LiveVarsFile, "load_once", lambda self: loads.append(1) or original(self))
    _write(path, {"lumibot_live_vars": 1, "vars": {"count": 99}})

    executor._activate_live_vars_file()  # a second call is a no-op
    for _ in range(3):
        executor._on_trading_iteration()

    assert strategy.vars.get("count") == 1
    assert loads == []


def test_missing_file_starts_fresh(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "missing" / "vars.json"))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    assert strategy.vars.all() == {}


def test_corrupt_file_starts_fresh_with_warning(tmp_path, monkeypatch, caplog):
    path = tmp_path / "vars.json"
    path.write_text("{not json", encoding="utf-8")
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)

    with caplog.at_level(logging.WARNING, logger="test_live_vars_file"):
        store.load_once()

    assert strategy.vars.all() == {}
    assert any("could not be read" in record.getMessage() for record in caplog.records)
    # The unreadable file is kept aside for inspection, not silently overwritten.
    assert any(name.name.startswith("vars.json.corrupt-") for name in tmp_path.iterdir())


def test_file_that_is_not_an_object_starts_fresh(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    _write(path, [1, 2, 3])
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    LiveVarsFile.from_environment(strategy).load_once()
    assert strategy.vars.all() == {}


def test_assets_and_datetimes_round_trip(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    asset = Asset("SPY")
    when = datetime.datetime(2026, 11, 24, 15, 30, tzinfo=datetime.timezone.utc)
    strategy.vars.set("asset", asset)
    strategy.vars.set("when", when)
    assert store.save("test") is True

    restored = _Strategy_()
    LiveVarsFile.from_environment(restored).load_once()
    assert restored.vars.get("asset") == asset
    assert restored.vars.get("when") == when


# ---------------------------------------------------------------- saving


def test_save_after_interval_only(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    monkeypatch.setenv(ENV_INTERVAL, "600")
    clock = _Clock()
    monkeypatch.setattr(live_vars_module, "_monotonic", clock)
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()

    executor._on_trading_iteration()  # first iteration: nothing saved yet -> save
    assert _read(path)["vars"]["iterations"] == 1

    clock.now += 599
    executor._on_trading_iteration()
    assert _read(path)["vars"]["iterations"] == 1  # interval not passed

    clock.now += 1
    executor._on_trading_iteration()
    assert _read(path)["vars"]["iterations"] == 3


def test_save_only_when_content_changed(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("a", 1)
    assert store.save("test") is True
    mtime = os.stat(path).st_mtime_ns
    assert store.save("test") is False
    assert os.stat(path).st_mtime_ns == mtime
    strategy.vars.set("a", 2)
    assert store.save("test") is True


def test_save_immediately_after_fill_and_survives_next_iteration(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    clock = _Clock()
    monkeypatch.setattr(live_vars_module, "_monotonic", clock)
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._initialize()
    executor._activate_live_vars_file()
    executor._on_trading_iteration()

    order = Order(strategy.name, Asset("SPY"), 1, "buy", identifier="o-1")
    clock.now += 5  # well inside the save interval
    executor._on_filled_order(SimpleNamespace(asset=order.asset), order, 500.0, 1, 1)
    assert _read(path)["vars"]["legs"] == ["SPY"]

    executor._on_trading_iteration()
    assert strategy.vars.get("legs") == ["SPY"]
    assert _read(path)["vars"]["legs"] == ["SPY"]


def test_save_after_partial_fill_and_cancel(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()
    order = Order(strategy.name, Asset("SPY"), 1, "buy", identifier="o-2")

    executor._on_partially_filled_order(None, order, 500.0, 1, 1)
    assert _read(path)["vars"]["partial"] is True

    executor._on_canceled_order(order)
    assert _read(path)["vars"]["canceled"] == "o-2"


def test_save_on_graceful_exit(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    clock = _Clock()
    monkeypatch.setattr(live_vars_module, "_monotonic", clock)
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()
    executor._on_trading_iteration()
    strategy.vars.set("late", "value")

    executor.gracefully_exit()
    assert _read(path)["vars"]["late"] == "value"


def test_sigterm_handler_saves_then_hands_over(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("on_stop", 1)

    handed_over = []
    handler = live_vars_module._make_sigterm_handler(store, lambda sig, frame: handed_over.append(sig))
    handler(signal.SIGTERM, None)

    assert _read(path)["vars"]["on_stop"] == 1
    assert handed_over == [signal.SIGTERM]


def test_unserializable_key_skipped_with_warning(tmp_path, monkeypatch, caplog):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("ok", {"x": [1, 2.5, "y", None, True]})
    strategy.vars.set("handle", object())

    with caplog.at_level(logging.WARNING, logger="test_live_vars_file"):
        assert store.save("test") is True

    saved = _read(path)["vars"]
    assert saved == {"ok": {"x": [1, 2.5, "y", None, True]}}
    assert any("handle" in record.getMessage() for record in caplog.records)


def test_atomic_write_keeps_old_file_when_replace_crashes(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("a", 1)
    assert store.save("test") is True
    before = path.read_bytes()

    def crash(*args, **kwargs):
        raise OSError("simulated crash before rename")

    monkeypatch.setattr(live_vars_module.os, "replace", crash)
    strategy.vars.set("a", 2)
    assert store.save("test") is False  # logged, never raised

    assert path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["vars.json"]


def test_atomic_write_keeps_old_file_when_write_crashes(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("a", 1)
    store.save("test")
    before = path.read_bytes()

    monkeypatch.setattr(live_vars_module.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError("disk")))
    strategy.vars.set("a", 2)
    assert store.save("test") is False
    assert path.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["vars.json"]

    # A later save after the fault clears still writes the new value.
    monkeypatch.undo()
    monkeypatch.setenv(ENV_FILE, str(path))
    assert store.save("test") is True
    assert _read(path)["vars"]["a"] == 2


def test_does_not_touch_db_or_scheduled_state_paths(tmp_path, monkeypatch):
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    calls = []
    strategy.load_variables_from_db = lambda: calls.append("load_db")
    strategy.backup_variables_to_db = lambda: calls.append("backup_db")
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("a", 1)
    store.save("test")
    assert calls == []


# ---------------------------------------------------------------- review fixes (Nov 25)


def test_save_retries_when_vars_change_during_encoding(tmp_path, monkeypatch):
    """A nested dict changed by the iteration thread mid-encode raises RuntimeError;
    the post-fill save must still land instead of being dropped with an error."""
    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    strategy.vars.set("legs", {"short": "SPY 500P"})

    real = _Strategy._encode_variable_for_backup
    calls = {"n": 0}

    def flaky(value):
        if isinstance(value, dict) and calls["n"] == 0:
            calls["n"] += 1
            raise RuntimeError("dictionary changed size during iteration")
        return real(value)

    monkeypatch.setattr(_Strategy, "_encode_variable_for_backup", staticmethod(flaky))
    assert store.save("filled_order") is True
    assert _read(path)["vars"]["legs"] == {"short": "SPY 500P"}


def test_fill_wakes_the_iteration_before_the_disk_save(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()
    calls = []
    monkeypatch.setattr(executor, "_save_live_vars_now", lambda reason: calls.append(("save", reason)))
    monkeypatch.setattr(executor, "_wake_iteration_after_fill", lambda partial=False: calls.append(("wake", partial)))
    order = Order(strategy.name, Asset("SPY"), 1, "buy", identifier="o-3")

    executor._on_filled_order(SimpleNamespace(asset=order.asset), order, 500.0, 1, 1)
    executor._on_partially_filled_order(None, order, 500.0, 1, 1)

    assert calls == [("wake", False), ("save", "filled_order"), ("wake", True), ("save", "partially_filled_order")]


def test_sigterm_handler_is_untouched_when_feature_off():
    before = signal.getsignal(signal.SIGTERM)
    _executor(_Strategy_())
    _executor(_Strategy_())
    assert signal.getsignal(signal.SIGTERM) is before


def test_sigterm_handler_installed_once_and_does_not_keep_executors_alive(tmp_path, monkeypatch):
    import gc
    import weakref

    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    original = signal.getsignal(signal.SIGTERM)
    monkeypatch.setattr(live_vars_module, "_process_handler_installed", False)
    try:
        first = _executor(_Strategy_())
        installed = signal.getsignal(signal.SIGTERM)
        assert installed is not original
        second = _executor(_Strategy_())
        assert signal.getsignal(signal.SIGTERM) is installed  # not chained a second time
        ref = weakref.ref(second)
        del second
        gc.collect()
        assert ref() is None
        assert first is not None
    finally:
        signal.signal(signal.SIGTERM, original)


def test_sigterm_saves_every_active_store(tmp_path, monkeypatch):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    strategy = _Strategy_()
    executor = _executor(strategy)
    executor._activate_live_vars_file()
    strategy.vars.set("on_stop", 7)
    handed = []
    handler = live_vars_module._make_process_sigterm_handler(lambda sig, frame: handed.append(sig))
    handler(signal.SIGTERM, None)
    assert _read(tmp_path / "vars.json")["vars"]["on_stop"] == 7
    assert handed == [signal.SIGTERM]


def test_concurrent_saves_leave_one_valid_file(tmp_path, monkeypatch):
    import threading

    path = tmp_path / "vars.json"
    monkeypatch.setenv(ENV_FILE, str(path))
    strategy = _Strategy_()
    store = LiveVarsFile.from_environment(strategy)
    store.load_once()
    errors = []

    def worker(n):
        try:
            for i in range(40):
                strategy.vars.set(f"k{n}", {"i": i, "list": list(range(i % 5))})
                store.save(f"t{n}")
        except Exception as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    saved = _read(path)["vars"]
    assert set(saved) >= {"k0", "k1", "k2", "k3"}
    assert not [p for p in os.listdir(tmp_path) if p.endswith(".tmp")]


def test_warns_once_when_database_backup_is_also_on(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(ENV_FILE, str(tmp_path / "vars.json"))
    monkeypatch.setenv("DB_CONNECTION_STR", "sqlite:///:memory:")
    strategy = _Strategy_()
    with caplog.at_level(logging.WARNING):
        LiveVarsFile.from_environment(strategy).load_once()
    assert any("DB_CONNECTION_STR" in r.getMessage() for r in caplog.records)


def test_run_reads_saved_memory_right_after_initialize():
    """run() order: _initialize(), then the one-time load, before any other lifecycle call."""
    import inspect

    source = inspect.getsource(StrategyExecutor.run)
    init_at = source.index("self._initialize()")
    load_at = source.index("self._activate_live_vars_file()")
    assert init_at < load_at
    between = source[init_at + len("self._initialize()"):load_at]
    assert "before_market_opens" not in between and "_before_starting_trading" not in between
    later_hooks = [source.find(name, init_at) for name in ("before_market_opens", "_before_starting_trading")]
    assert all(pos == -1 or pos > load_at for pos in later_hooks)
