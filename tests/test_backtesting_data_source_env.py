"""Tests for BACKTESTING_DATA_SOURCE environment variable handling.

These tests validate the datasource selection logic inside `Strategy.run_backtest()`
without running real backtests (which would be slow and flaky in CI).
"""

from datetime import datetime

import pytest

from lumibot.strategies import Strategy


class SimpleTestStrategy(Strategy):
    """Minimal strategy for testing datasource auto-selection."""

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        return


class TestBacktestingDataSourceEnv:
    """Test BACKTESTING_DATA_SOURCE environment variable."""

    class _SelectedDataSource(Exception):
        """Raised by stub backtesting classes to prove datasource selection."""

    class _PolygonSelected(_SelectedDataSource):
        pass

    class _ThetaDataSelected(_SelectedDataSource):
        pass

    class _YahooSelected(_SelectedDataSource):
        pass

    def test_auto_select_polygon_case_insensitive(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class PolygonDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._PolygonSelected()

        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "PolygonDataBacktesting", PolygonDataBacktesting)
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "polygon")

        with pytest.raises(self._PolygonSelected):
            SimpleTestStrategy.run_backtest(
                None,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                polygon_api_key="test_key",
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data: polygon" in record.message
            for record in caplog.records
        )

    def test_auto_select_thetadata_case_insensitive(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class ThetaDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._ThetaDataSelected()

        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "ThetaDataBacktesting", ThetaDataBacktesting)
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "THETADATA")

        with pytest.raises(self._ThetaDataSelected):
            SimpleTestStrategy.run_backtest(
                None,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                thetadata_username="test_user",
                thetadata_password="test_pass",
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data: THETADATA" in record.message
            for record in caplog.records
        )

    def test_auto_select_yahoo_case_insensitive(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class YahooDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._YahooSelected()

        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "YahooDataBacktesting", YahooDataBacktesting)
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "Yahoo")

        with pytest.raises(self._YahooSelected):
            SimpleTestStrategy.run_backtest(
                None,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data: Yahoo" in record.message
            for record in caplog.records
        )

    def test_invalid_data_source_raises_error(self, monkeypatch):
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "InvalidSource")

        with pytest.raises(ValueError, match="Unknown BACKTESTING_DATA_SOURCE"):
            SimpleTestStrategy.run_backtest(
                None,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 31),
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

    def test_env_override_wins_over_explicit_datasource(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class PolygonDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._PolygonSelected()

        class YahooDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._YahooSelected()

        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "PolygonDataBacktesting", PolygonDataBacktesting)
        monkeypatch.setattr(strategy_module, "YahooDataBacktesting", YahooDataBacktesting)
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "polygon")

        with pytest.raises(self._PolygonSelected):
            SimpleTestStrategy.run_backtest(
                YahooDataBacktesting,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                polygon_api_key="test_key",
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data: polygon" in record.message
            for record in caplog.records
        )

    def test_explicit_datasource_used_when_env_none(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class YahooDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._YahooSelected()

        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "YahooDataBacktesting", YahooDataBacktesting)
        monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")

        with pytest.raises(self._YahooSelected):
            SimpleTestStrategy.run_backtest(
                YahooDataBacktesting,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert not any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data:" in record.message
            for record in caplog.records
        )

    def test_default_thetadata_when_no_env_set(self, monkeypatch, caplog):
        import logging

        caplog.set_level(logging.INFO, logger="lumibot.strategies._strategy")

        class ThetaDataBacktesting:
            def __init__(self, *args, **kwargs):
                raise TestBacktestingDataSourceEnv._ThetaDataSelected()

        import lumibot.credentials
        import lumibot.strategies._strategy as strategy_module

        monkeypatch.setattr(strategy_module, "ThetaDataBacktesting", ThetaDataBacktesting)
        monkeypatch.setattr(lumibot.credentials, "BACKTESTING_DATA_SOURCE", "ThetaData")
        monkeypatch.delenv("BACKTESTING_DATA_SOURCE", raising=False)

        with pytest.raises(self._ThetaDataSelected):
            SimpleTestStrategy.run_backtest(
                None,
                backtesting_start=datetime(2023, 1, 1),
                backtesting_end=datetime(2023, 1, 10),
                thetadata_username="test_user",
                thetadata_password="test_pass",
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_tearsheet=False,
                save_stats_file=False,
                save_logfile=False,
            )

        assert any(
            "Using BACKTESTING_DATA_SOURCE setting for backtest data: ThetaData" in record.message
            for record in caplog.records
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
