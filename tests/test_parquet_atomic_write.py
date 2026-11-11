"""Parquet artifacts must never be left half written.

A process killed while writing a parquet file used to leave a truncated file
in place ("No magic bytes found at end of file"). That file was then uploaded
as the run's artifact and every later query on it failed. Writes now go to a
temporary file in the same folder and replace the target in one step.
"""

import logging

import pandas as pd
import pytest

from lumibot.tools.parquet_utils import write_parquet_with_logging


def _write(frame, path):
    return write_parquet_with_logging(
        df=frame,
        path=str(path),
        artifact="test",
        logger=logging.getLogger("test"),
        index=False,
        required=True,
    )


def test_failed_write_keeps_previous_complete_file(tmp_path, monkeypatch):
    target = tmp_path / "artifact.parquet"
    _write(pd.DataFrame({"value": [1, 2, 3]}), target)

    original_to_parquet = pd.DataFrame.to_parquet

    def killed_mid_write(self, path, *args, **kwargs):
        with open(path, "wb") as handle:
            handle.write(b"PAR1 partial")
        raise OSError("process stopped while writing")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", killed_mid_write)
    with pytest.raises(RuntimeError):
        _write(pd.DataFrame({"value": [4, 5, 6, 7]}), target)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", original_to_parquet)

    assert pd.read_parquet(target)["value"].tolist() == [1, 2, 3]
    assert [path.name for path in tmp_path.iterdir()] == ["artifact.parquet"]


def test_successful_write_replaces_file(tmp_path):
    target = tmp_path / "artifact.parquet"
    _write(pd.DataFrame({"value": [1]}), target)
    stats = _write(pd.DataFrame({"value": [2, 3]}), target)

    assert pd.read_parquet(target)["value"].tolist() == [2, 3]
    assert stats.rows == 2
    assert stats.bytes == target.stat().st_size
    assert [path.name for path in tmp_path.iterdir()] == ["artifact.parquet"]
