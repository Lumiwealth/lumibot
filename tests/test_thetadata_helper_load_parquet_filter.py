import pandas as pd

from lumibot.tools.thetadata_helper import load_cache


def test_load_cache_filters_large_parquet_without_full_read(tmp_path, monkeypatch) -> None:
    """Regression: avoid loading multi-year intraday caches into memory when only a slice is needed.

    For production backtests, some cache namespaces contain multi-year minute OHLC parquet files.
    Reading them in full can trigger ECS OOMKills (BotManager ERROR_CODE_CRASH with no traceback).

    When preserve_full_history=False and bounds are provided, load_cache() should use PyArrow dataset
    filtering (predicate pushdown) instead of pd.read_parquet() full reads.
    """

    dt_index = pd.date_range("2025-01-01", periods=1000, freq="min", tz="UTC")
    df = pd.DataFrame(
        {
            "datetime": dt_index,
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
            "missing": False,
        }
    )
    path = tmp_path / "stock_NVDA_minute_ohlc.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)

    full = load_cache(path)
    assert len(full) == len(df)

    start = dt_index[100].to_pydatetime()
    end = dt_index[199].to_pydatetime()

    def _boom(*args, **kwargs):
        raise AssertionError("pd.read_parquet should not be called for filtered cache loads")

    monkeypatch.setattr(pd, "read_parquet", _boom)

    sliced = load_cache(path, start=start, end=end, preserve_full_history=False)
    assert len(sliced) == 100
    assert sliced.index.min() >= start
    assert sliced.index.max() <= end
