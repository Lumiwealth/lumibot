from datetime import datetime, timezone

import pandas as pd

from lumibot.tools.indicators import _prepare_tearsheet_returns, plot_indicators, plot_returns


def test_prepare_tearsheet_returns_handles_mixed_timezone_indexes():
    strategy_df = pd.DataFrame(
        {
            "return": [0.0, 0.01, -0.005],
            "portfolio_value": [100000.0, 101000.0, 100495.0],
        },
        index=pd.DatetimeIndex(
            [
                datetime(2026, 3, 8, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 9, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
            ]
        ),
    )
    benchmark_df = pd.DataFrame(
        {"symbol_cumprod": [1.0, 1.02, 1.01]},
        index=pd.DatetimeIndex(
            [
                datetime(2026, 3, 8),
                datetime(2026, 3, 9),
                datetime(2026, 3, 10),
            ]
        ),
    )

    result = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    assert result is not None
    assert result.index.tz is None
    assert set(result.columns) == {"strategy", "benchmark"}


def test_plot_returns_handles_mixed_timezone_trade_times(tmp_path):
    strategy_df = pd.DataFrame(
        {
            "return": [0.0, 0.01],
            "portfolio_value": [100000.0, 101000.0],
            "cash_adjusted_portfolio_value": [100000.0, 101000.0],
            "positions": [[], []],
            "cash": [100000.0, 99000.0],
        },
        index=pd.DatetimeIndex([datetime(2026, 3, 8, tzinfo=timezone.utc), datetime(2026, 3, 9, tzinfo=timezone.utc)]),
    )
    benchmark_df = pd.DataFrame(
        {"return": [0.0, 0.01], "symbol_cumprod": [1.0, 1.01]},
        index=pd.DatetimeIndex([datetime(2026, 3, 8), datetime(2026, 3, 9)]),
    )
    trades_df = pd.DataFrame(
        {
            "time": [datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc), datetime(2026, 3, 9, 12, 0)],
            "status": ["fill", "fill"],
            "side": ["buy", "sell"],
            "asset": ["BTC", "BTC"],
            "filled_quantity": [1, 1],
            "price": [50000.0, 50500.0],
        }
    )

    plot_returns(
        strategy_df,
        "Strategy",
        benchmark_df,
        "BTC",
        plot_file_html=str(tmp_path / "trades.html"),
        trades_file=str(tmp_path / "trades.csv"),
        trades_df=trades_df,
        show_plot=False,
        initial_budget=100000.0,
    )

    assert (tmp_path / "trades.csv").exists()


def test_plot_indicators_sorts_mixed_timezone_datetimes(tmp_path):
    markers = pd.DataFrame(
        {
            "datetime": [datetime(2026, 3, 8, 12, 0, tzinfo=timezone.utc), datetime(2026, 3, 8, 13, 0)],
            "name": ["entry", "exit"],
            "value": [1.0, 2.0],
        }
    )

    plot_indicators(
        plot_file_html=str(tmp_path / "indicators.html"),
        chart_markers_df=markers,
        chart_lines_df=pd.DataFrame(),
        chart_ohlc_df=pd.DataFrame(),
        strategy_name="Mixed TZ",
        show_indicators=False,
    )

    assert (tmp_path / "indicators.csv").exists()
