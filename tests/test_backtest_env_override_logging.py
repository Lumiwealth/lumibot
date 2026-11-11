"""The BACKTESTING_START/END override warning must not configure the root logger.

A bare ``logging.warning(...)`` on an unconfigured root logger makes Python call
``logging.basicConfig()``. That adds a second console handler to the root, and every
LumiBot record (which propagates to the root) is then printed twice: once as
``2026-.. | INFO | ...`` and once as ``INFO:lumibot....``. Cloud backtest logs showed
exactly that from the first override warning onwards (2026-11-10).
"""

import datetime
import logging

from lumibot.strategies import Strategy


class _Noop(Strategy):
    def on_trading_iteration(self):
        pass


def test_backtest_window_override_warns_through_lumibot_without_touching_root(monkeypatch):
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    for handler in saved_handlers:
        root.removeHandler(handler)

    seen = []

    class _Capture(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    capture = _Capture(level=logging.WARNING)
    lumibot_logger = logging.getLogger("lumibot")
    lumibot_logger.addHandler(capture)

    monkeypatch.setenv("BACKTESTING_START", "2025-01-02")
    monkeypatch.setenv("BACKTESTING_END", "2025-01-09")
    monkeypatch.setattr(_Noop, "run_backtest", classmethod(lambda cls, *a, **k: ({}, None)))
    try:
        _Noop.backtest(
            backtesting_start=datetime.datetime(2024, 1, 1),
            backtesting_end=datetime.datetime(2024, 2, 1),
        )
        added_to_root = root.handlers[:]
    finally:
        lumibot_logger.removeHandler(capture)
        for handler in root.handlers[:]:
            root.removeHandler(handler)
        for handler in saved_handlers:
            root.addHandler(handler)
        root.setLevel(saved_level)

    assert added_to_root == [], "the override warning configured the root logger (duplicate log lines)"
    assert any("BACKTESTING_START env var override" in m for m in seen)
    assert any("BACKTESTING_END env var override" in m for m in seen)


def test_custom_stream_errors_do_not_configure_the_root_logger():
    """Broker stream errors are logged through LumiBot, not the bare root logger."""
    from lumibot.trading_builtins.custom_stream import CustomStream

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    for handler in saved_handlers:
        root.removeHandler(handler)
    stream = CustomStream()

    @stream.add_action("boom")
    def _boom():
        raise RuntimeError("synthetic failure")

    try:
        stream.dispatch("boom", wait_until_complete=True)
        added_to_root = root.handlers[:]
    finally:
        for handler in root.handlers[:]:
            root.removeHandler(handler)
        for handler in saved_handlers:
            root.addHandler(handler)

    assert added_to_root == []
