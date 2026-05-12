from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from lumibot.components.agents.duckdb_tools import DuckDBQueryLayer


class _Strategy:
    def get_datetime(self) -> datetime:
        return datetime(2024, 1, 3)


def test_duckdb_query_layer_registers_and_queries_frames() -> None:
    layer = DuckDBQueryLayer(_Strategy())
    frame = pd.DataFrame(
        {
            "datetime": pd.date_range("2024-01-01", periods=3, freq="D"),
            "close": [100.0, 101.5, 102.25],
        }
    )

    info = layer._register_frame("prices", frame, {"kind": "test"})
    result = layer.query(sql="select datetime, close from prices order by datetime", limit=2)

    assert info["row_count"] == 3
    assert result["row_count"] == 3
    assert result["truncated"] is True
    assert [row["close"] for row in result["rows"]] == [100.0, 101.5]


def test_duckdb_query_layer_rejects_write_sql() -> None:
    layer = DuckDBQueryLayer(_Strategy())

    with pytest.raises(ValueError, match="read-only"):
        layer.query(sql="delete from prices")
