"""Actual helper publication must preserve concurrent coverage and warm reuse."""
import io

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import backtest_cache as bc, ibkr_helper as h
from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot, decorate_adjusted_frame


class Conflict(RuntimeError):
    response = {"Error": {"Code": "PreconditionFailed"}}


class MemoryS3:
    def __init__(self, body):
        self.body, self.etag = body, '"initial"'
        self.reads, self.writes = 0, 0

    def get_object(self, **kwargs):
        self.reads += 1
        return {"Body": io.BytesIO(self.body), "ETag": self.etag}

    def put_object(self, *, Body, **kwargs):
        if kwargs.get("IfMatch") != self.etag:
            raise Conflict()
        self.writes += 1
        self.body, self.etag = Body, f'"write-{self.writes}"'
        return {"ETag": self.etag}


def parquet(frame):
    out = io.BytesIO()
    frame.to_parquet(out)
    return out.getvalue()


@pytest.fixture
def basis(monkeypatch):
    actions = pd.DataFrame({"Dividends": [1.25], "Stock Splits": [0.]},
                           index=pd.DatetimeIndex(["2024-06-07"], tz="America/New_York"))
    value = EquityBasisSnapshot.from_verified_actions(
        actions, as_of="2024-06-10T22:00Z", coverage_through="2024-06-10")
    monkeypatch.setattr(h, "_get_current_equity_basis", lambda _: value)
    monkeypatch.setattr(h, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {})
    monkeypatch.setattr(h, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    return value


def prices(basis, timestamps, closes):
    frame = pd.DataFrame({name: closes for name in ("open", "high", "low", "close")},
                         index=pd.DatetimeIndex(timestamps, tz="America/New_York"))
    frame["volume"], frame["missing"] = 100., False
    return decorate_adjusted_frame(frame, basis, provider_prices_verified=True)


def setup_cache(monkeypatch, tmp_path, basis, initial, timestep):
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(bc, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    client = MemoryS3(parquet(initial))
    manager = bc.BacktestCacheManager(
        bc.BacktestCacheSettings(backend="s3", mode=bc.CacheMode.S3_READWRITE,
                                bucket="test-bucket", prefix="test", version="v44"),
        client_factory=lambda _: client)
    monkeypatch.setattr(h, "get_backtest_cache", lambda: manager)
    args = dict(asset=Asset("SPY"), quote=Asset("USD", asset_type="forex"),
                timestep=timestep, exchange="SMART", include_after_hours=False, source="Trades")
    path = h._cache_file_for(**args, equity_basis=basis.split_fingerprint)
    assert manager.ensure_local_file(path, payload={"provider": "ibkr"})
    return args, path, manager, client


@pytest.mark.parametrize("corrected_close", [100., 110.])
def test_public_fetch_retains_remote_only_rows_after_two_checkpoints(monkeypatch, tmp_path, basis, corrected_close):
    a = prices(basis, ["2024-06-07 09:30"], [100.])
    b = prices(basis, ["2024-06-07 09:40"], [101.])
    c = prices(basis, ["2024-06-07 09:50"], [102.])
    d = prices(basis, ["2024-06-07 10:00"], [103.])
    args, path, manager, client = setup_cache(monkeypatch, tmp_path, basis, a, "minute")
    monkeypatch.setattr(h, "_repair_us_stock_index_minute_gaps", lambda frame, **_: frame)
    calls = []

    def fetch(**kwargs):
        calls.append(1)
        # Another writer adds B after this reader hydrated A.
        client.body, client.etag = parquet(pd.concat([a, b])), '"concurrent"'
        first_page = pd.concat([prices(basis, ["2024-06-07 09:30"], [corrected_close]), c])
        kwargs["_page_checkpoint"](first_page)
        kwargs["_page_checkpoint"](d)
        return pd.concat([first_page, d])

    monkeypatch.setattr(h, "_fetch_history_between_dates", fetch)
    value = h.get_price_data(**args, start_dt=a.index[0].to_pydatetime(), end_dt=d.index[0].to_pydatetime())
    expected = pd.concat([a, b, c, d]).index
    assert value.index.equals(expected)
    assert value.close.tolist() == [corrected_close, 101., 102., 103.]
    assert calls == [1]
    assert pd.read_parquet(io.BytesIO(client.body)).index.equals(expected)
    # One hydration plus one conflict read; later writes need no remote read.
    assert client.reads == 2
    assert manager.ensure_local_file(path, payload={"provider": "ibkr"}, force_download=True)
    reloaded = h._verified_equity_cache(h._read_cache_frame(path), basis)
    assert reloaded.index.equals(expected)
    assert reloaded._ibkr_split_basis.eq(basis.split_fingerprint).all()


def test_public_daily_warm_action_cache_does_not_rewrite_or_download(monkeypatch, tmp_path, basis):
    initial = prices(basis, ["2024-06-06 16:00", "2024-06-07 16:00"], [100., 101.])
    initial["dividend"], initial["stock_splits"] = [0., 1.25], [0., 0.]
    args, path, manager, client = setup_cache(monkeypatch, tmp_path, basis, initial, "day")

    def forbidden(**kwargs):
        pytest.fail("Fully cached history must not call the provider")

    monkeypatch.setattr(h, "_fetch_history_between_dates", forbidden)
    before = path.stat().st_mtime_ns
    first = h.get_price_data(**args, start_dt=initial.index[0].to_pydatetime(), end_dt=initial.index[-1].to_pydatetime())
    second = h.get_price_data(**args, start_dt=initial.index[0].to_pydatetime(), end_dt=initial.index[-1].to_pydatetime())
    pd.testing.assert_frame_equal(first, second)
    assert first.dividend.tolist() == [0., 1.25]
    assert path.stat().st_mtime_ns == before
    assert client.reads == 1 and client.writes == 0


def test_normal_write_does_not_reparse_local_parquet(monkeypatch, tmp_path, basis):
    frame = prices(basis, ["2024-06-07 09:30"], [100.])
    _, path, _, _ = setup_cache(monkeypatch, tmp_path, basis, frame, "minute")
    monkeypatch.setattr(h, "_read_cache_frame", lambda _: pytest.fail("No conflict means no extra parquet parse"))
    result = h._write_cache_frame(path, frame)
    pd.testing.assert_frame_equal(result, frame)


def test_publication_detects_atomic_replacement_and_retains_basis(monkeypatch, tmp_path, basis):
    frame = prices(basis, ["2024-06-07 09:30"], [100.])
    remote_only = prices(basis, ["2024-06-07 09:40"], [101.])
    _, path, manager, _ = setup_cache(monkeypatch, tmp_path, basis, frame, "minute")

    def publish(local_path, **kwargs):
        replacement = local_path.with_suffix(".replacement")
        pd.concat([frame, remote_only]).to_parquet(replacement)
        replacement.replace(local_path)

    monkeypatch.setattr(manager, "on_local_update", publish)
    result = h._write_cache_frame(path, frame)
    assert result.close.tolist() == [100., 101.]
    assert result.attrs["ibkr_equity_basis"] == basis.to_dict()
    assert result._ibkr_split_basis.eq(basis.split_fingerprint).all()
