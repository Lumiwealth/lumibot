"""Installed entrypoint runs the real engine with explicitly synthetic data."""
import pytest


@pytest.mark.usefixtures("disable_datasource_override")
def test_first_backtest_records_one_actual_simulated_fill(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    from unittest.mock import Mock

    from lumibot.example_strategies.first_backtest import run_example

    # The offline example must not fetch Yahoo's risk-free rate while saving settings.
    rate_lookup = Mock(return_value=0.0)
    monkeypatch.setattr("lumibot.strategies.strategy.get_risk_free_rate", rate_lookup)
    _, strategy = run_example()
    rate_lookup.assert_not_called()
    position = strategy.get_position("DEMO")
    assert position is not None
    assert float(position.quantity) == 1
    trades = strategy.broker._trade_event_log_df
    fills = trades[trades["status"] == "fill"]
    assert len(fills) == 1
    assert float(fills.iloc[0]["filled_quantity"]) == 1


def test_offline_example_rejects_external_datasource_override_before_engine(monkeypatch):
    from lumibot.example_strategies.first_backtest import FirstBacktest, run_example
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "yahoo")

    def unexpected(*args, **kwargs):
        raise AssertionError("Offline check must not start an externally overridden engine")

    monkeypatch.setattr(FirstBacktest, "run_backtest", unexpected)
    with pytest.raises(ValueError, match="BACKTESTING_DATA_SOURCE=none"):
        run_example()
