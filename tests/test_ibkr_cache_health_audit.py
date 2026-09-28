from __future__ import annotations

import pandas as pd
import pytest

from lumibot.tools.ibkr_history_health import audit_ibkr_cache_frame


def _load_audit_script():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_ibkr_cache_health.py"
    spec = importlib.util.spec_from_file_location("audit_ibkr_cache_health", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_audit_script_disables_dotenv_discovery(monkeypatch) -> None:
    import os

    monkeypatch.delenv("LUMIBOT_DISABLE_DOTENV", raising=False)
    _load_audit_script()
    assert os.environ["LUMIBOT_DISABLE_DOTENV"] == "1"


def test_audit_limit_rejects_negative_values() -> None:
    module = _load_audit_script()

    with pytest.raises(module.argparse.ArgumentTypeError, match="--limit must be nonnegative"):
        module._nonnegative_int("-1")
    assert module._nonnegative_int("0") == 0


def test_cache_audit_distinguishes_real_rows_from_placeholders() -> None:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-04-28 16:00", tz="America/New_York"),
            pd.Timestamp("2025-04-29 16:00", tz="America/New_York"),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, pd.NA],
            "high": [101.0, pd.NA],
            "low": [99.0, pd.NA],
            "close": [100.5, pd.NA],
            "volume": [1000, pd.NA],
            "missing": [False, True],
        },
        index=index,
    )

    result = audit_ibkr_cache_frame(frame)

    assert result["rows"] == 2
    assert result["real_rows"] == 1
    assert result["placeholder_rows"] == 1
    assert result["all_placeholder"] is False
    assert result["duplicate_timestamps"] == 0
    assert result["monotonic"] is True
    assert result["real_ohlc_null_rows"] == 0


def test_cache_audit_flags_duplicate_non_monotonic_and_null_real_ohlc() -> None:
    index = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-04-30 16:00", tz="America/New_York"),
            pd.Timestamp("2025-04-29 16:00", tz="America/New_York"),
            pd.Timestamp("2025-04-29 16:00", tz="America/New_York"),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, pd.NA, 99.0],
            "high": [101.0, 100.0, 100.0],
            "low": [99.0, 98.0, 98.0],
            "close": [100.5, 99.0, 99.0],
            "missing": [False, False, False],
        },
        index=index,
    )

    result = audit_ibkr_cache_frame(frame)

    assert result["duplicate_timestamps"] == 1
    assert result["monotonic"] is False
    assert result["real_ohlc_null_rows"] == 1


def test_local_audit_reports_corrupt_objects_without_stopping(monkeypatch, tmp_path) -> None:
    script = _load_audit_script()
    cache_root = tmp_path / "cache"
    ibkr_root = cache_root / "ibkr"
    ibkr_root.mkdir(parents=True)
    healthy = pd.DataFrame(
        {"open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0], "missing": [False]},
        index=pd.DatetimeIndex([pd.Timestamp("2025-04-29", tz="UTC")]),
    )
    healthy.to_parquet(ibkr_root / "healthy.parquet")
    (ibkr_root / "corrupt.parquet").write_bytes(b"not parquet")
    monkeypatch.setattr(script, "LUMIBOT_CACHE_FOLDER", cache_root.as_posix())

    result = script.run_audit(remote=False)

    assert result["objects"] == 1
    assert len(result["read_errors"]) == 1
    assert result["read_errors"][0]["object"].endswith("corrupt.parquet")
