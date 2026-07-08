from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import _strategy as strategy_module
from lumibot.strategies.strategy import Strategy


class _StatsOnlyStrategy(Strategy):
    __test__ = False

    def initialize(self):
        self.set_market("24/7")
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        raise AssertionError("This regression test should not run the trading loop.")


def _stub_stats_flow_math(monkeypatch) -> None:
    def cumulative_to_period_flows(values):
        return pd.to_numeric(values, errors="coerce").diff().fillna(0.0)

    def cash_flow_adjusted_returns(values, cumulative_external_flows=None):
        numeric_values = pd.to_numeric(values, errors="coerce")
        previous_values = numeric_values.shift(1)
        if cumulative_external_flows is None:
            external_period = pd.Series(0.0, index=numeric_values.index, dtype=float)
        else:
            external_period = cumulative_to_period_flows(cumulative_external_flows).reindex(numeric_values.index).fillna(0.0)
        returns = (numeric_values - previous_values - external_period) / previous_values
        return returns.where(previous_values.notna()).astype(float)

    monkeypatch.setattr(strategy_module, "cumulative_to_period_flows", cumulative_to_period_flows)
    monkeypatch.setattr(strategy_module, "cash_flow_adjusted_returns", cash_flow_adjusted_returns)


def test_dump_stats_end_to_end_regression_for_datetime_indexes():
    """
    Broad regression: end-of-backtest stats generation must not crash.

    This protects the full pipeline:
    - Strategy._append_row() -> _format_stats() -> day_deduplicate() -> stats_summary()
    """
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    backtesting_broker = BacktestingBroker(data_source=broker)
    strat = _StatsOnlyStrategy(broker=backtesting_broker)

    # Ensure benchmark code paths are skipped (no network / no Yahoo).
    strat._benchmark_asset = None
    strat._stats_file = None

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    portfolio_values = [100_000.0, 150_000.0, 75_000.0, 90_000.0]
    for i, value in enumerate(portfolio_values):
        strat._append_row(
            {
                "datetime": start + timedelta(days=i),
                "portfolio_value": value,
            }
        )

    strat._dump_stats()

    assert strat._analysis is not None
    assert set(strat._analysis) == {"cagr", "volatility", "sharpe", "max_drawdown", "romad", "total_return"}

    def _is_number(value: object) -> bool:
        return isinstance(value, (int, float))

    assert _is_number(strat._analysis["cagr"])
    assert _is_number(strat._analysis["volatility"])
    assert _is_number(strat._analysis["sharpe"])
    assert _is_number(strat._analysis["romad"])
    assert _is_number(strat._analysis["total_return"])
    assert isinstance(strat._analysis["max_drawdown"], dict)
    assert set(strat._analysis["max_drawdown"]) == {"drawdown", "date"}
    assert 0 <= float(strat._analysis["max_drawdown"]["drawdown"]) <= 1

    # Calling twice should remain safe (production crash paths often call stats in cleanup).
    strat._dump_stats()

    # Strategy returns index should remain datetime-like (required by tearsheet/stat functions).
    assert strat._strategy_returns_df is not None
    assert not strat._strategy_returns_df.empty
    assert isinstance(pd.Timestamp(strat._strategy_returns_df.index[0]), pd.Timestamp)


def test_dump_stats_emits_parquet_file_when_stats_file_is_set(tmp_path) -> None:
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    backtesting_broker = BacktestingBroker(data_source=broker)
    strat = _StatsOnlyStrategy(broker=backtesting_broker)

    strat._benchmark_asset = None
    strat._stats_file = str(tmp_path / "stats.csv")

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i, value in enumerate([100_000.0, 101_000.0, 99_000.0]):
        strat._append_row(
            {
                "datetime": start + timedelta(days=i),
                "portfolio_value": value,
            }
        )

    strat._dump_stats()

    stats_csv = tmp_path / "stats.csv"
    stats_parquet = tmp_path / "stats.parquet"
    assert stats_csv.exists()
    assert stats_parquet.exists()

    parquet_df = pd.read_parquet(stats_parquet)
    assert not parquet_df.empty
    assert "portfolio_value" in parquet_df.columns


def test_dump_stats_parquet_is_resilient_to_object_positions(tmp_path) -> None:
    """Regression: stats.parquet export must not crash when positions contain objects.

    Production failure was triggered by stats rows containing nested Python objects
    (e.g., Asset instances). We now coerce object-ish columns to JSON strings before
    parquet export so backtests fail loudly only in required/contract mode.
    """
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 5, tzinfo=timezone.utc),
    )
    backtesting_broker = BacktestingBroker(data_source=broker)
    strat = _StatsOnlyStrategy(broker=backtesting_broker)

    strat._benchmark_asset = None
    strat._stats_file = str(tmp_path / "stats.csv")

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    strat._append_row(
        {
            "datetime": start,
            "portfolio_value": 100_000.0,
            # Old behavior included raw Asset objects; keep a variant here to ensure
            # sanitizer continues to protect parquet export.
            "positions": [{"asset": Asset("SPY"), "quantity": 1}],
        }
    )

    strat._dump_stats()

    stats_parquet = tmp_path / "stats.parquet"
    assert stats_parquet.exists()
    parquet_df = pd.read_parquet(stats_parquet)
    assert "positions" in parquet_df.columns
    assert isinstance(parquet_df["positions"].iloc[0], str)


def test_format_stats_tolerates_missing_initial_portfolio_and_external_flows(monkeypatch) -> None:
    _stub_stats_flow_math(monkeypatch)
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    strat = _StatsOnlyStrategy(broker=BacktestingBroker(data_source=broker))
    strat._benchmark_asset = None
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    strat._append_row({
        "datetime": start,
        "portfolio_value": None,
        "cash_adjustments_net_total": None,
    })
    strat._append_row({
        "datetime": start + timedelta(days=1),
        "portfolio_value": 100_000.0,
        "cash_adjustments_net_total": None,
    })
    strat._append_row({
        "datetime": start + timedelta(days=2),
        "portfolio_value": 101_000.0,
        "cash_adjustments_net_total": 0.0,
    })

    stats = strat._format_stats()

    assert "return" in stats.columns
    assert "cash_adjusted_portfolio_value" in stats.columns
    assert pd.isna(stats["return"].iloc[0])
    assert float(stats["cash_adjusted_portfolio_value"].iloc[0]) == 100_000.0
    assert float(stats["cash_adjusted_portfolio_value"].iloc[-1]) == 101_000.0


def test_format_stats_preserves_valid_initial_portfolio_base(monkeypatch) -> None:
    _stub_stats_flow_math(monkeypatch)
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    strat = _StatsOnlyStrategy(broker=BacktestingBroker(data_source=broker))
    strat._benchmark_asset = None
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    strat._append_row({
        "datetime": start,
        "portfolio_value": 100_000.0,
        "cash_adjustments_net_total": 0.0,
    })
    strat._append_row({
        "datetime": start + timedelta(days=1),
        "portfolio_value": None,
        "cash_adjustments_net_total": None,
    })
    strat._append_row({
        "datetime": start + timedelta(days=2),
        "portfolio_value": 101_000.0,
        "cash_adjustments_net_total": 0.0,
    })

    stats = strat._format_stats()

    assert float(stats["cash_adjusted_portfolio_value"].iloc[0]) == 100_000.0
    assert float(stats["cash_adjusted_portfolio_value"].iloc[-1]) == 100_000.0


def test_format_stats_tolerates_no_valid_portfolio_values(monkeypatch) -> None:
    _stub_stats_flow_math(monkeypatch)
    broker = PandasDataBacktesting(
        datetime_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    strat = _StatsOnlyStrategy(broker=BacktestingBroker(data_source=broker))
    strat._benchmark_asset = None
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    strat._append_row({"datetime": start, "portfolio_value": None})
    strat._append_row({"datetime": start + timedelta(days=1), "portfolio_value": None})

    stats = strat._format_stats()

    assert "return" in stats.columns
    assert "cash_adjusted_portfolio_value" in stats.columns
    assert stats["return"].isna().all()
    assert stats["cash_adjusted_portfolio_value"].isna().all()
