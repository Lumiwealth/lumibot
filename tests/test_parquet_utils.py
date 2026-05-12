from __future__ import annotations

import logging

import pandas as pd
import pytest

from lumibot.tools.parquet_utils import coerce_object_columns_to_json_strings, write_parquet_with_logging


def test_coerce_object_columns_to_json_strings_only_coerces_mixed_objects() -> None:
    df = pd.DataFrame(
        {
            "payload": [{"a": 1}, ["x", "y"], None],
            "label": ["one", "two", "three"],
            "price": [1.0, 2.0, 3.0],
        }
    )

    sanitized, coerced = coerce_object_columns_to_json_strings(df)

    assert coerced == ["payload"]
    payload_values = sanitized["payload"].tolist()
    assert payload_values[:2] == ['{"a":1}', '["x","y"]']
    assert pd.isna(payload_values[2])
    assert sanitized["label"].tolist() == ["one", "two", "three"]
    assert sanitized["price"].tolist() == [1.0, 2.0, 3.0]


def test_write_parquet_with_logging_best_effort_returns_zero_stats_on_failure(monkeypatch, tmp_path) -> None:
    def fail_to_parquet(self, *args, **kwargs):
        raise RuntimeError("forced parquet failure")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_to_parquet)
    df = pd.DataFrame({"value": [1, 2, 3]})

    stats = write_parquet_with_logging(
        df=df,
        path=str(tmp_path / "missing.parquet"),
        artifact="unit-test",
        logger=logging.getLogger("tests.parquet_utils"),
        index=False,
        required=False,
    )

    assert stats.artifact == "unit-test"
    assert stats.rows == 3
    assert stats.cols == 1
    assert stats.bytes == 0


def test_write_parquet_with_logging_required_raises_on_failure(monkeypatch, tmp_path) -> None:
    def fail_to_parquet(self, *args, **kwargs):
        raise RuntimeError("forced parquet failure")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_to_parquet)

    with pytest.raises(RuntimeError, match="PARQUET_EXPORT_FAILED"):
        write_parquet_with_logging(
            df=pd.DataFrame({"value": [1]}),
            path=str(tmp_path / "missing.parquet"),
            artifact="unit-test",
            logger=logging.getLogger("tests.parquet_utils"),
            index=False,
            required=True,
        )
