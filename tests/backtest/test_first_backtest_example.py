"""Installed entrypoint runs the real engine with explicitly synthetic data."""
import pytest


@pytest.mark.usefixtures("disable_datasource_override")
def test_first_backtest_records_one_actual_simulated_fill(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    from lumibot.example_strategies.first_backtest import run_example
    _, strategy = run_example()
    position = strategy.get_position("DEMO")
    assert position is not None
    assert float(position.quantity) == 1
    trades = strategy.broker._trade_event_log_df
    fills = trades[trades["status"] == "fill"]
    assert len(fills) == 1
    assert float(fills.iloc[0]["filled_quantity"]) == 1
