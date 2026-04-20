"""Tests for BACKTESTING_START / BACKTESTING_END env var overrides on `Strategy.backtest()`.

Why: strategies commonly hardcode `backtesting_start` / `backtesting_end` in their
`if __name__ == "__main__":` block. When the same code runs in a managed container
(bot_manager, MCP `start_backtest`, etc.), the infrastructure sets these env vars
from the caller's requested date range. Without this override, the hardcoded dates
win and callers have no way to actually change the range without rewriting the
strategy source.

These tests mock `run_backtest` to assert the dates that ultimately get propagated,
so we don't need real data or a full backtest run.
"""

from datetime import datetime
from unittest.mock import patch

from lumibot.strategies import Strategy


class _StubStrategy(Strategy):
    """Minimal strategy — we never actually execute it; we intercept run_backtest."""

    def initialize(self, parameters=None):  # noqa: ARG002 — matches base signature
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        return


class TestBacktestEnvOverride:
    def _run_backtest_capturing_dates(self, monkeypatch, env):
        """Invoke `Strategy.backtest()` with whatever env overrides are set and
        return the (backtesting_start, backtesting_end) that ultimately got
        handed to `run_backtest`. Hardcoded defaults mimic what a real
        `__main__` block would pass."""
        for key, value in env.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)

        captured = {}

        def _fake_run_backtest(cls, *, backtesting_start, backtesting_end, **kwargs):
            captured["start"] = backtesting_start
            captured["end"] = backtesting_end
            return ({}, None)  # (results, strategy)

        with patch.object(Strategy, "run_backtest", classmethod(_fake_run_backtest)):
            _StubStrategy.backtest(
                datasource_class=None,
                backtesting_start=datetime(2020, 1, 1),
                backtesting_end=datetime(2020, 6, 30),
            )
        return captured["start"], captured["end"]

    def test_env_var_overrides_hardcoded_start_date(self, monkeypatch):
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": "2023-03-15", "BACKTESTING_END": None},
        )
        assert start == datetime(2023, 3, 15), "BACKTESTING_START must win over passed arg"
        assert end == datetime(2020, 6, 30), "end unchanged when only START env set"

    def test_env_var_overrides_hardcoded_end_date(self, monkeypatch):
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": None, "BACKTESTING_END": "2024-12-31"},
        )
        assert start == datetime(2020, 1, 1), "start unchanged when only END env set"
        assert end == datetime(2024, 12, 31)

    def test_env_vars_override_both(self, monkeypatch):
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": "2019-07-01", "BACKTESTING_END": "2022-09-30"},
        )
        assert start == datetime(2019, 7, 1)
        assert end == datetime(2022, 9, 30)

    def test_full_iso_datetime_accepted(self, monkeypatch):
        """Callers can pass either `YYYY-MM-DD` or a full ISO datetime."""
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {
                "BACKTESTING_START": "2021-02-03T09:30:00",
                "BACKTESTING_END": "2021-02-03T16:00:00",
            },
        )
        assert start == datetime(2021, 2, 3, 9, 30)
        assert end == datetime(2021, 2, 3, 16, 0)

    def test_unparseable_env_var_falls_back_to_passed_arg(self, monkeypatch, caplog):
        """Garbage in the env must NOT silently produce a weird date. We keep the
        passed-in value and log a warning — silent fallback to `now` or the epoch
        would produce misleading backtest results."""
        import logging

        caplog.set_level(logging.WARNING)
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": "not-a-date", "BACKTESTING_END": "also-not-a-date"},
        )
        assert start == datetime(2020, 1, 1), "keeps passed arg on parse failure"
        assert end == datetime(2020, 6, 30)
        assert any("BACKTESTING_START" in rec.message for rec in caplog.records)
        assert any("BACKTESTING_END" in rec.message for rec in caplog.records)

    def test_no_env_vars_preserves_passed_arg(self, monkeypatch):
        """Without env vars, the code path MUST stay byte-identical to before —
        local dev runs with hardcoded dates continue to behave the same."""
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": None, "BACKTESTING_END": None},
        )
        assert start == datetime(2020, 1, 1)
        assert end == datetime(2020, 6, 30)

    def test_empty_string_env_var_is_ignored(self, monkeypatch):
        """Empty strings are treated as unset (bot_manager sometimes sends ""
        for missing fields). Must not attempt to parse and fail the run."""
        start, end = self._run_backtest_capturing_dates(
            monkeypatch,
            {"BACKTESTING_START": "", "BACKTESTING_END": ""},
        )
        assert start == datetime(2020, 1, 1)
        assert end == datetime(2020, 6, 30)
