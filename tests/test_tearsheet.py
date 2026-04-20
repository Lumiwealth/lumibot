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


def test_tearsheet_uses_forward_fill_for_weekends():
    """Weekend rows should carry Friday's value so Monday captures the weekend gap."""
    strategy_df = pd.DataFrame(
        {"portfolio_value": [100_000, 110_000]},
        index=pd.to_datetime(
            [
                "2025-01-03 15:59:00-05:00",  # Friday
                "2025-01-06 15:59:00-05:00",  # Monday
            ]
        ),
    )

    benchmark_df = pd.DataFrame(
        {"symbol_cumprod": [1.0, 1.05]},
        index=pd.to_datetime(
            [
                "2025-01-03 15:59:00-05:00",
                "2025-01-06 15:59:00-05:00",
            ]
        ),
    )

    df_final = _prepare_tearsheet_returns(strategy_df, benchmark_df)

    assert df_final is not None
    monday = pd.Timestamp("2025-01-06")
    assert monday in df_final.index
    # Monday should reflect the move from Friday -> Monday, not 0 (which would imply backfill).
    assert df_final.loc[monday, "strategy"] != 0


def test_tearsheet_prepends_anchor_when_first_day_has_non_zero_return():
    """v4.5.1 bug fix: QuantStats cumulative-returns plot previously started
    the Strategy line at the first-day return (e.g. -4%) while the Benchmark
    started at 0% (its first pct_change is NaN → 0). Visual artifact: blue
    Strategy line began below yellow Benchmark line, even though both should
    start at 0% (portfolio allocated, no move yet).

    Fix: if the first row of df_final has a non-zero strategy or benchmark
    return, prepend a day-0 anchor row with both returns = 0 so the
    cumulative curves share a common 0% starting point.
    """
    # Day 1 portfolio moves from 100k open → 96k close (-4%). Benchmark
    # day 1 cumprod starts at 1.0 → 1.0 (first pct_change is NaN → 0).
    strategy_df = pd.DataFrame(
        {"portfolio_value": [100_000, 96_000, 97_000]},
        index=pd.to_datetime(
            [
                "2022-07-05 09:30:00-04:00",
                "2022-07-05 16:00:00-04:00",
                "2022-07-06 16:00:00-04:00",
            ]
        ),
    )
    benchmark_df = pd.DataFrame(
        {"symbol_cumprod": [1.0, 1.0, 1.01]},
        index=pd.to_datetime(
            [
                "2022-07-05 09:30:00-04:00",
                "2022-07-05 16:00:00-04:00",
                "2022-07-06 16:00:00-04:00",
            ]
        ),
    )

    df_final = _prepare_tearsheet_returns(strategy_df, benchmark_df)
    assert df_final is not None

    # The first row must be at 0% for both strategy and benchmark so the
    # cumulative plot starts at 0%. Without the anchor, the Strategy line
    # starts at -4% and the Benchmark starts at 0% — exactly the Rob
    # reported artifact on the 2026-04-19 Alpha Picks tearsheet.
    first_row = df_final.iloc[0]
    assert abs(first_row["strategy"]) < 1e-9, (
        f"First day's strategy return is {first_row['strategy']} but should "
        f"be 0 (anchor). Without the anchor, the QuantStats cumulative plot "
        f"starts the Strategy line at first-day return instead of 0%."
    )
    assert abs(first_row["benchmark"]) < 1e-9, (
        f"First day's benchmark return is {first_row['benchmark']} but should "
        f"be 0 (anchor)."
    )
