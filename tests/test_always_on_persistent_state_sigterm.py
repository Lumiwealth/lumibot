"""Regression tests for persistent always-on state file & SIGTERM graceful flush."""

from __future__ import annotations

import json
import signal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lumibot.strategies._strategy import _Strategy
from lumibot.traders.trader import Trader


class _MinimalLiveStrategy:
    """Minimal harness bound to _Strategy state-file methods."""

    is_backtesting = False

    def __init__(self):
        self.logger = MagicMock()
        self._last_backup_state = None
        self.vars = MagicMock()
        self._vars_data: dict = {}
        self.vars.all.side_effect = lambda: dict(self._vars_data)
        self.vars.set.side_effect = lambda k, v: self._vars_data.__setitem__(k, v)
        self.broker = None
        self._name = "AlwaysOnTest"
        self.should_backup_variables_to_database = False
        self.db_connection_str = None

    _scheduled_state_enabled = _Strategy._scheduled_state_enabled
    _encode_variable_for_backup = staticmethod(_Strategy._encode_variable_for_backup)
    _decode_variable_from_backup = staticmethod(_Strategy._decode_variable_from_backup)
    _serialize_variables_for_backup = _Strategy._serialize_variables_for_backup
    _deserialize_variables_from_backup = _Strategy._deserialize_variables_from_backup
    _backup_variables_to_scheduled_state_file = _Strategy._backup_variables_to_scheduled_state_file
    _load_variables_from_scheduled_state_file = _Strategy._load_variables_from_scheduled_state_file
    _checkpoint_order_ownership = lambda self: None
    _restore_order_ownership = lambda self: None
    _restore_legacy_assets = staticmethod(lambda decoded, _existing: decoded)
    backup_variables_to_db = _Strategy.backup_variables_to_db
    load_variables_from_db = _Strategy.load_variables_from_db


def test_persistent_state_enabled_for_always_on_live_bot(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.delenv("LUMIBOT_SCHEDULED_EXECUTION", raising=False)
    monkeypatch.setenv("LUMIBOT_PERSISTENT_STATE_ENABLED", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")

    strat = _MinimalLiveStrategy()
    assert strat._scheduled_state_enabled() is True

    strat._vars_data["open_spreads"] = [{"symbol": "SPY", "qty": 2}]
    strat.backup_variables_to_db()
    assert state_file.exists()
    saved = json.loads(state_file.read_text(encoding="utf-8"))
    assert saved["open_spreads"] == [{"symbol": "SPY", "qty": 2}]

    # Reload into a fresh strategy instance (simulates restart after AWS retirement)
    restarted = _MinimalLiveStrategy()
    restarted.load_variables_from_db()
    assert restarted._vars_data["open_spreads"] == [{"symbol": "SPY", "qty": 2}]


def test_persistent_state_disabled_when_backtesting_or_backend_none(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    monkeypatch.setenv("LUMIBOT_PERSISTENT_STATE_ENABLED", "true")
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "none")

    strat = _MinimalLiveStrategy()
    assert strat._scheduled_state_enabled() is False

    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")
    strat.is_backtesting = True
    assert strat._scheduled_state_enabled() is False


def test_trader_registers_sigterm_handler_to_stop_pool(monkeypatch):
    registered: dict[int, object] = {}
    orig_signal = signal.signal

    def fake_signal(sig, handler):
        registered[sig] = handler
        return orig_signal(sig, signal.SIG_DFL) if sig == signal.SIGINT else None

    monkeypatch.setattr(signal, "signal", fake_signal)

    trader = Trader(backtest=False)
    mock_strat = SimpleNamespace(
        _name="AlwaysOnSigterm",
        is_backtesting=False,
        broker=SimpleNamespace(
            IS_BACKTESTING_BROKER=False,
            is_backtesting_broker=lambda: False,
            _close_connection=lambda: None,
        ),
        backup_variables_to_db=MagicMock(),
    )
    trader._strategies = [mock_strat]
    monkeypatch.setattr(trader, "_set_logger", lambda: None)
    monkeypatch.setattr(trader, "_init_pool", lambda: None)
    monkeypatch.setattr(trader, "_start_pool", lambda: None)
    monkeypatch.setattr(trader, "_join_pool", lambda: None)
    monkeypatch.setattr(trader, "_collect_analysis", lambda: {})
    monkeypatch.setattr(trader, "_get_backtest_profiling_config", lambda **_kw: None)

    trader.run_all(async_=False)

    assert signal.SIGINT in registered
    assert registered[signal.SIGINT] == trader._stop_pool
    assert signal.SIGTERM in registered
    assert registered[signal.SIGTERM] == trader._stop_pool
