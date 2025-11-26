import pandas as pd

from lumibot.tools.indicators import _prepare_tearsheet_returns


def test_tearsheet_preserves_initial_equity():
    strategy_df = pd.DataFrame(
        {
            "portfolio_value": [100_000, 115_000, 120_000],
        },
        index=pd.to_datetime(
            [
                "2025-01-02 09:30:00-05:00",
                "2025-01-02 15:59:00-05:00",
                "2025-01-03 15:59:00-05:00",
            ]
        ),
    )

    benchmark_df = pd.DataFrame(
        {
            "symbol_cumprod": [1.0, 1.01, 1.02],
        },
        index=pd.to_datetime(
            [
                "2025-01-02 09:30:00-05:00",
                "2025-01-02 15:59:00-05:00",
                "2025-01-03 15:59:00-05:00",
            ]
        ),
    )

    df_final = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    assert df_final is not None
    assert "strategy" in df_final.columns

    strategy_returns = df_final["strategy"]
    cumulative_return = (strategy_returns + 1).cumprod().iloc[-1] - 1

    # Last portfolio value / first portfolio value - 1
    expected_return = (120_000 / 100_000) - 1
    assert abs(cumulative_return - expected_return) < 1e-9

    # Ensure at least one daily return reflects the real gains
    assert (strategy_returns.abs() > 0).any()
