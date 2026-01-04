import pandas as pd

from lumibot.tools.indicators import create_tearsheet


def test_create_tearsheet_does_not_crash_on_flat_returns(tmp_path):
    # Flat portfolio value + flat benchmark => degenerate returns series.
    idx = pd.to_datetime(["2025-01-02", "2025-01-03"])
    strategy_df = pd.DataFrame({"portfolio_value": [100000.0, 100000.0]}, index=idx)
    benchmark_df = pd.DataFrame({"symbol_cumprod": [1.0, 1.0]}, index=idx)

    out = tmp_path / "tearsheet.html"
    create_tearsheet(
        strategy_df=strategy_df,
        strat_name="FlatStrategy",
        tearsheet_file=str(out),
        benchmark_df=benchmark_df,
        benchmark_asset="SPY",
        show_tearsheet=False,
        save_tearsheet=True,
        risk_free_rate=0.0,
        strategy_parameters={},
        lumibot_version="test",
        backtesting_data_source="thetadata",
        backtesting_data_sources="thetadata",
        backtest_time_seconds=1.0,
    )

    assert out.exists()
    assert "Tearsheet not generated" in out.read_text(encoding="utf-8", errors="ignore")

