"""Production history decoding must preserve volume units across cache reuse."""
from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper as h
from lumibot.tools.ibkr_volume import IbkrVolumeMetadataError

@pytest.fixture(autouse=True)
def _frozen_equity_actions(monkeypatch):
    from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot
    from lumibot.tools import ibkr_helper
    ledger = pd.DataFrame(columns=["Dividends", "Stock Splits"])
    basis = EquityBasisSnapshot.from_verified_actions(ledger, as_of=pd.Timestamp("2026-10-10T17:00Z"), coverage_through="2026-10-09")
    monkeypatch.setattr(ibkr_helper, "_get_current_equity_basis", lambda _: basis)



def _fetch(monkeypatch, payload, timestep="minute", source="Trades", asset_type="stock"):
    start = pd.Timestamp("2024-06-07 13:30", tz="UTC")
    if timestep == "day":
        start = pd.Timestamp("2024-06-07 20:00", tz="UTC")
    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 4815747)
    monkeypatch.setattr(h, "_ibkr_history_request", lambda **_: deepcopy(payload))
    return h._fetch_history_between_dates(
        asset=Asset("NVDA", asset_type=asset_type), quote=Asset("USD", asset_type="forex"),
        timestep=timestep, start_dt=start.to_pydatetime(),
        end_dt=(start + pd.Timedelta(minutes=2)).to_pydatetime(),
        exchange="SMART", include_after_hours=False, source=source, source_was_explicit=True,
    )


def _payload(backend="rest", factor=100, timestep="minute"):
    stamp = pd.Timestamp("2024-06-07 13:30", tz="UTC")
    if backend == "tws" and timestep == "day":
        stamp = stamp.normalize()
    return {"volumeFactor": factor,
            "_botspot_meta": {"provider": "ibkr", "backend": backend},
            "data": [{"t": int(stamp.timestamp() * 1000), "o": 119.79,
                      "h": 121.69, "l": 118.02, "c": 120.89, "v": 2398561.2}]}


@pytest.mark.parametrize("timestep", ["day", "minute", "15minute", "hour"])
@pytest.mark.parametrize("backend,factor", [("rest", 100), ("tws", 1)])
def test_production_pager_normalizes_volume_only(monkeypatch, backend, factor, timestep):
    payload = _payload(backend, factor, timestep)
    before = deepcopy(payload)
    frame = _fetch(monkeypatch, payload, timestep)
    assert frame.iloc[0].volume == pytest.approx(2398561.2 * factor)
    assert frame.iloc[0][["open", "high", "low", "close"]].tolist() == [119.79, 121.69, 118.02, 120.89]
    assert payload == before


@pytest.mark.parametrize("volume", [-1, None, 0])
def test_unavailable_volume_and_zero_are_not_fabricated(monkeypatch, volume):
    payload = _payload()
    payload["data"][0]["v"] = volume
    frame = _fetch(monkeypatch, payload)
    assert pd.isna(frame.iloc[0].volume) if volume is None else frame.iloc[0].volume == volume


@pytest.mark.parametrize("backend", ["rest", "tws"])
def test_unknown_stock_volume_units_fail_before_caching(monkeypatch, backend):
    payload = _payload(backend)
    del payload["volumeFactor"]
    with pytest.raises(IbkrVolumeMetadataError, match="missing"):
        _fetch(monkeypatch, payload)


@pytest.mark.parametrize("source", ["Midpoint", "Bid_Ask"])
def test_quote_history_needs_no_volume_factor(monkeypatch, source):
    payload = _payload()
    del payload["volumeFactor"]
    payload["data"][0]["v"] = 0
    assert _fetch(monkeypatch, payload, source=source).iloc[0].volume == 0


def test_empty_stock_history_does_not_fail_volume_validation(monkeypatch):
    assert _fetch(monkeypatch, {"data": []}).empty


def test_warm_replay_uses_normalized_parquet_without_second_scaling(monkeypatch, tmp_path):
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(h, "get_backtest_cache", lambda: SimpleNamespace(
        ensure_local_file=lambda *a, **k: None, on_local_update=lambda *a, **k: None))
    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 4815747)
    payload = _payload()
    second = deepcopy(payload["data"][0]); second["t"] += 60000
    payload["data"].append(second)
    calls = []
    def request(**_):
        calls.append(1)
        return deepcopy(payload)
    monkeypatch.setattr(h, "_ibkr_history_request", request)
    kwargs = dict(asset=Asset("NVDA"), quote=Asset("USD", asset_type="forex"),
                  timestep="minute", start_dt=pd.Timestamp("2024-06-07 13:30Z").to_pydatetime(),
                  end_dt=pd.Timestamp("2024-06-07 13:31Z").to_pydatetime(), exchange="SMART",
                  include_after_hours=False, source="Trades")
    first = h._get_cached_bars_for_source(**kwargs)
    assert len(calls) == 1
    second = h._get_cached_bars_for_source(**kwargs)
    assert len(calls) == 1
    pd.testing.assert_frame_equal(first, second)
    assert first.volume.tolist() == pytest.approx([239856120, 239856120])


@pytest.mark.parametrize("asset_type,source,migrate", [
    ("stock", "Trades", True), ("future", "Trades", True),
    ("crypto", "Trades", True), ("crypto", "Trades_DERIVED_DAILY", True),
    ("index", "Trades", False), ("stock", "Midpoint", False), ("future", "Bid_Ask", False),
])
@pytest.mark.parametrize("timestep", ["day", "minute"])
def test_volume_semantics_only_rekeys_traded_cache(monkeypatch, tmp_path, asset_type, source, migrate, timestep):
    monkeypatch.setattr(h, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    path = h._cache_file_for(asset=Asset("TEST", asset_type=asset_type), quote=None,
                           timestep=timestep, exchange="SMART", source=source, include_after_hours=False)
    assert ("_VOL_V1" in path.name) is migrate
    legacy = path.with_name(path.name.replace("_VOL_V1", ""))
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_bytes(b"preserved original cache")
    assert legacy.read_bytes() == b"preserved original cache"
    if migrate:
        assert not path.exists()
