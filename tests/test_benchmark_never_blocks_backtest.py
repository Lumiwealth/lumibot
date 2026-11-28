"""A benchmark that cannot be fetched, is slow, or errors never fails or stalls a backtest.

Why: the benchmark is only a comparison line drawn next to the strategy's results. A backtest
once sat near the end for about half an hour waiting on its default benchmark's history, and
a data source that raises while reading benchmark bars used to fail the whole run after the
simulation had already finished. The strategy's results are complete before the benchmark is
read, so the wait is bounded and any failure leaves the comparison line empty, says so in the
log and records it in the results (``benchmark_unavailable``).
"""

import time
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

import lumibot.strategies._strategy as strategy_module
from lumibot.strategies._strategy import _Strategy


class _Logger:
    def __init__(self):
        self.lines = []

    def info(self, msg, *a):
        self.lines.append(msg % a if a else msg)

    warning = error = debug = info


def _fake_strategy():
    return SimpleNamespace(
        is_backtesting=True,
        _benchmark_asset="SPY",
        _backtesting_start=datetime(2026, 10, 1),
        _backtesting_end=datetime(2026, 11, 1),
        broker=SimpleNamespace(data_source=SimpleNamespace(SOURCE="YAHOO")),
        logger=_Logger(),
        _quote_asset=None,
        _sleeptime="1D",
        _benchmark_returns_df="unset",
        _analysis={"total_return": 0.1},
    )


def test_slow_benchmark_is_abandoned_after_the_bounded_wait(monkeypatch):
    monkeypatch.setenv("LUMIBOT_BENCHMARK_WAIT_SECONDS", "0.5")

    def slow_returns(symbol, start, end):
        time.sleep(5)
        return pd.DataFrame({"return": [0.0]}, index=pd.date_range("2026-10-01", periods=1))

    monkeypatch.setattr(strategy_module, "get_symbol_returns", slow_returns)
    fake = _fake_strategy()

    started = time.monotonic()
    _Strategy._dump_benchmark_stats(fake)
    elapsed = time.monotonic() - started

    assert elapsed < 3, elapsed
    assert fake._benchmark_returns_df is None
    assert fake._analysis["total_return"] == 0.1
    assert fake._analysis["benchmark_unavailable"]["benchmark"] == "SPY"
    assert "did not arrive within" in fake._analysis["benchmark_unavailable"]["reason"]
    assert any("comparison line" in line for line in fake.logger.lines)


def test_benchmark_fetch_error_never_fails_the_backtest(monkeypatch):
    def broken_returns(symbol, start, end):
        raise ConnectionError("history service unreachable")

    monkeypatch.setattr(strategy_module, "get_symbol_returns", broken_returns)
    fake = _fake_strategy()

    _Strategy._dump_benchmark_stats(fake)

    assert fake._benchmark_returns_df is None
    assert "history service unreachable" in fake._analysis["benchmark_unavailable"]["reason"]


def test_empty_benchmark_is_recorded_as_missing(monkeypatch):
    monkeypatch.setattr(strategy_module, "get_symbol_returns", lambda *_a: pd.DataFrame())
    fake = _fake_strategy()

    _Strategy._dump_benchmark_stats(fake)

    assert fake._analysis["benchmark_unavailable"]["reason"] == "no benchmark data was returned"


def test_working_benchmark_is_unchanged(monkeypatch):
    frame = pd.DataFrame({"return": [0.0, 0.01]}, index=pd.date_range("2026-10-01", periods=2))
    monkeypatch.setattr(strategy_module, "get_symbol_returns", lambda *_a: frame)
    fake = _fake_strategy()

    _Strategy._dump_benchmark_stats(fake)

    assert fake._benchmark_returns_df is frame
    assert "benchmark_unavailable" not in fake._analysis


@pytest.mark.parametrize("value", ["abc", "-1"])
def test_bad_wait_setting_uses_the_default(monkeypatch, value):
    monkeypatch.setenv("LUMIBOT_BENCHMARK_WAIT_SECONDS", value)
    assert _Strategy._benchmark_wait_seconds() == 120.0
