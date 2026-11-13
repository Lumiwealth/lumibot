"""Parquet artifacts must never be left half written.

A process killed while writing a parquet file used to leave a truncated file
in place ("No magic bytes found at end of file"). That file was then uploaded
as the run's artifact and every later query on it failed. Writes now go to a
temporary file in the same folder and replace the target in one step.
"""

import logging
import os
import signal
import subprocess
import sys
import time

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


def test_killed_native_spool_write_preserves_complete_file_and_pending_parts(tmp_path):
    if not hasattr(signal, "SIGSTOP"):
        pytest.skip("Native process interruption requires POSIX signals")
    target = tmp_path / "detail.parquet"
    ready = tmp_path / "native-write-started"
    program = r'''
import logging, os, signal, sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
from lumibot.components.agents.manager import _AgentDetailSpool
target, ready = map(Path, sys.argv[1:])
spool = _AgentDetailSpool(target)
for value in (1, 2, 3):
    spool.append(pd.DataFrame({"call": [value]}), artifact="detail", logger=logging.getLogger("test"))
original = pq.ParquetWriter
class InterruptedWriter:
    def __init__(self, *args, **kwargs): self.writer = original(*args, **kwargs)
    def __enter__(self): return self
    def __exit__(self, *args): self.writer.close()
    def write_table(self, table):
        self.writer.write_table(table)
        ready.write_text("native row group written; footer not committed")
        os.kill(os.getpid(), signal.SIGSTOP)
pq.ParquetWriter = InterruptedWriter
spool.finalize()
'''
    child = subprocess.Popen([sys.executable, "-c", program, str(target), str(ready)],
                             env={**os.environ, "LUMIBOT_DISABLE_DOTENV": "true"})
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert child.poll() is None, "Native writer exited before reaching interruption"
            assert time.monotonic() < deadline, "Native writer never reached the interruption point"
            time.sleep(0.01)
        child.kill()
        assert child.wait() == -signal.SIGKILL
        assert pd.read_parquet(target)["call"].tolist() == [1, 2]
        parts = list(target.with_name("detail.parts").glob("*.parquet"))
        assert len(parts) == 1
        assert pd.read_parquet(parts[0])["call"].tolist() == [3]
        temporary = target.with_name(f"detail.parquet.tmp-{child.pid}")
        assert temporary.exists()
        with pytest.raises(Exception, match="[Pp]arquet|[Ff]ooter|magic bytes"):
            pd.read_parquet(temporary)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
