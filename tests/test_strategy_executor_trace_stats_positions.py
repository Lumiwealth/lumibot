from __future__ import annotations

from datetime import UTC, datetime

from lumibot.entities import Asset, Position
from lumibot.strategies.strategy_executor import StrategyExecutor


class _TraceStatsStubStrategy:
    __test__ = False

    def __init__(self):
        self._rows: list[dict] = []
        self.portfolio_value = 123.45
        self.cash = 67.89

    def trace_stats(self, context, snapshot_before):  # noqa: ANN001
        return {}

    def get_datetime(self):
        return datetime(2026, 1, 1, tzinfo=UTC)

    def get_positions(self):
        return [
            Position(strategy="test", asset=Asset("SPY"), quantity=1),
        ]

    def _append_row(self, row: dict) -> None:
        self._rows.append(row)


def test_strategy_executor_trace_stats_does_not_embed_asset_objects() -> None:
    """Regression: trace_stats must not embed raw Asset objects into stats rows.

    These objects are not reliably serializable (parquet/pyarrow) and bloat logs.
    """
    strategy = _TraceStatsStubStrategy()
    dummy_executor = type("_DummyExecutor", (), {"strategy": strategy})()

    StrategyExecutor._trace_stats(dummy_executor, context=None, snapshot_before={})

    assert strategy._rows
    row = strategy._rows[-1]
    assert "positions" in row
    assert isinstance(row["positions"], list)
    assert row["positions"], "expected at least one position"
    assert isinstance(row["positions"][0]["asset"], (dict, str))
