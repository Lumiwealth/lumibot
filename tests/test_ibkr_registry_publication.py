import io
import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import futures_roll, ibkr_helper
from lumibot.tools.backtest_cache import CacheMode
from lumibot.tools.ibkr_history_health import ibkr_history_health_snapshot, reset_ibkr_history_health


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch, tmp_path):
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setattr(ibkr_helper, "_DISABLE_CONIDS_REMOTE_UPLOAD", False)
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_CONID_CACHE", {})
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    reset_ibkr_history_health()
    yield
    reset_ibkr_history_health()


class PreconditionFailed(RuntimeError):
    response = {"Error": {"Code": "PreconditionFailed"}}


class Registry:
    def __init__(self, initial, concurrent=None):
        self.mapping = dict(initial)
        self.version = 1
        self.concurrent = concurrent
        self.puts = []

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(json.dumps(self.mapping).encode()), "ETag": str(self.version)}

    def upload_file(self, path, bucket, key):
        self.mapping = json.loads(path.read_text()) if hasattr(path, "read_text") else json.loads(open(path).read())

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        if self.concurrent is not None:
            self.mapping.update(self.concurrent)
            self.concurrent = None
            self.version += 1
        if kwargs.get("IfMatch") != str(self.version):
            raise PreconditionFailed("concurrent registry update")
        self.mapping = json.loads(kwargs["Body"])
        self.version += 1
        return {"ETag": str(self.version)}


def publish(tmp_path, s3, mapping, required_keys):
    path = tmp_path / "conids.json"
    path.write_text(json.dumps(mapping))
    manager = SimpleNamespace(enabled=True, mode=CacheMode.S3_READWRITE,
                              _settings=SimpleNamespace(backend="s3", bucket="test-bucket"),
                              remote_key_for=lambda *a, **k: "ibkr/conids.json",
                              _get_client=lambda: s3)
    ibkr_helper._merge_upload_conids_json(manager, path, mapping=mapping, required_keys=required_keys)
    return json.loads(path.read_text())


def test_new_contract_preserves_unrelated_corrected_remote_id(tmp_path):
    s3 = Registry({"stock|XYZ|USD||": 222})
    local = {"stock|XYZ|USD||": 111, "future|GC|USD|COMEX|20260626": 333}
    result = publish(tmp_path, s3, local, {"future|GC|USD|COMEX|20260626"})
    assert s3.mapping == {"stock|XYZ|USD||": 222, "future|GC|USD|COMEX|20260626": 333}
    assert result == s3.mapping
    assert s3.puts[0]["IfMatch"] == "1"


def test_seed_only_fills_absent_remote_keys(tmp_path):
    s3 = Registry({"stock|XYZ|USD||": 222})
    result = publish(tmp_path, s3, {"stock|XYZ|USD||": 111, "future|GC|USD|COMEX|20260626": 333}, set())
    assert result == s3.mapping == {"stock|XYZ|USD||": 222, "future|GC|USD|COMEX|20260626": 333}


def test_conflicting_write_reloads_remote_without_losing_new_keys(tmp_path, monkeypatch):
    monkeypatch.setattr(ibkr_helper.time, "sleep", lambda _: None)
    s3 = Registry({"stock|XYZ|USD||": 222}, {"stock|XYZ|USD||": 444, "stock|NEW|USD||": 555})
    publish(tmp_path, s3, {"stock|XYZ|USD||": 111, "future|GC|USD|COMEX|20260626": 333},
            {"future|GC|USD|COMEX|20260626"})
    assert s3.mapping == {"stock|XYZ|USD||": 444, "stock|NEW|USD||": 555,
                          "future|GC|USD|COMEX|20260626": 333}
    assert len(s3.puts) == 2


def test_missing_roll_segment_is_reported_even_when_later_segment_resolves(monkeypatch):
    reset_ibkr_history_health()
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    boundary = datetime(2026, 8, 1, tzinfo=timezone.utc)
    end = datetime(2026, 9, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(futures_roll, "build_roll_schedule", lambda *a, **k:
                        [("MGCQ26", start, boundary), ("MGCV26", boundary, end)])
    def resolve(*, asset, **kwargs):
        if asset.expiration.month == 8:
            raise RuntimeError("Unable to resolve IBKR conid")
        return 123
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", resolve)
    segments = ibkr_helper._resolve_cont_future_segments(
        asset=Asset("MGC", asset_type="cont_future"), start_dt=start, end_dt=end, exchange="COMEX")
    assert len(segments) == 1
    health = ibkr_history_health_snapshot()
    assert health["complete"] is False
    assert health["series"][0]["symbol"] == "MGC"
    assert health["series"][0]["requested_start"] == start.isoformat()
    assert health["series"][0]["requested_end"] == boundary.isoformat()
    assert health["series"][0]["outcome"] == "partial"
    reset_ibkr_history_health()


@pytest.mark.parametrize("start,end", [
    ("2026-10-09 18:01", "2026-10-10 12:00"),
    ("2026-10-09 17:00", "2026-10-11 18:00"),
    ("2026-10-31 12:00", "2026-11-01 18:00"),
])
def test_futures_weekend_stays_closed_until_sunday_wall_clock_open(start, end):
    assert ibkr_helper._us_futures_closed_interval(
        pd.Timestamp(start, tz="America/New_York"), pd.Timestamp(end, tz="America/New_York"))


def test_futures_sunday_open_is_not_closed():
    assert not ibkr_helper._us_futures_closed_interval(
        pd.Timestamp("2026-11-01 18:00", tz="America/New_York"),
        pd.Timestamp("2026-11-01 18:01", tz="America/New_York"))


def test_expired_future_missing_from_rest_is_discovered_through_shared_tws(monkeypatch):
    calls = []
    def request(*, url, querystring, **kwargs):
        calls.append((url, querystring))
        if url.endswith("/trsrv/futures"):
            return {"MGC": [{"conid": 999, "expirationDate": "20261229"}]}
        assert url.endswith("/tws/secdef/contracts")
        assert querystring == {"symbol": "MGC", "exchange": "COMEX", "currency": "USD", "expiry": "202604"}
        return {"contracts": [{"conid": 123, "symbol": "MGC", "secType": "FUT", "exchange": "COMEX",
                                "currency": "USD", "expiry": "20260428"}]}
    monkeypatch.setattr(ibkr_helper, "queue_request", request)
    monkeypatch.setattr(ibkr_helper, "_load_negative_conid_cache", lambda: None)
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE", {})
    mapping, added = {}, set()
    asset = Asset("MGC", asset_type="future", expiration=date(2026, 4, 28))
    assert ibkr_helper._lookup_conid_future(asset=asset, exchange="COMEX", mapping=mapping, keys_added=added) == 123
    for quote in ("", "USD"):
        key = ibkr_helper.IbkrConidKey("future", "MGC", quote, "COMEX", "20260428").to_key()
        assert mapping[key] == 123
        assert key in added
    assert len(calls) == 2


@pytest.mark.parametrize("field,value", [("symbol", "GC"), ("exchange", "CME"), ("currency", "EUR"),
                                        ("secType", "STK"), ("expiry", "20260626")])
def test_tws_discovery_rejects_wrong_contract_identity(monkeypatch, field, value):
    contract = {"conid": 123, "symbol": "MGC", "secType": "FUT", "exchange": "COMEX",
                "currency": "USD", "expiry": "20260428"}
    contract[field] = value
    monkeypatch.setattr(ibkr_helper, "queue_request", lambda **k: {"contracts": [contract]})
    with pytest.raises(RuntimeError, match="identity"):
        ibkr_helper._lookup_conid_future_tws(asset=Asset("MGC", asset_type="future", expiration=date(2026, 4, 28)),
                                            exchange="COMEX", mapping={}, keys_added=set())


def test_tws_discovery_rejects_ambiguous_same_month(monkeypatch):
    contracts = [{"conid": conid, "symbol": "MGC", "secType": "FUT", "exchange": "COMEX",
                  "currency": "USD", "expiry": expiry}
                 for conid, expiry in [(123, "20260427"), (456, "20260428")]]
    monkeypatch.setattr(ibkr_helper, "queue_request", lambda **k: {"contracts": contracts})
    with pytest.raises(RuntimeError, match="ambiguous"):
        ibkr_helper._lookup_conid_future_tws(asset=Asset("MGC", asset_type="future", expiration=date(2026, 4, 28)),
                                            exchange="COMEX", mapping={}, keys_added=set())


@pytest.mark.parametrize("symbol,expiration", [("NG", date(2026, 9, 28)), ("CL", date(2026, 9, 22)),
                                              ("MCL", date(2026, 9, 21))])
def test_energy_tws_lookup_uses_exact_expiry_not_delivery_month(monkeypatch, symbol, expiration):
    def request(*, querystring, **kwargs):
        assert querystring["expiry"] == expiration.strftime("%Y%m%d")
        return {"contracts": [{"conid": 123, "symbol": symbol, "secType": "FUT", "exchange": "NYMEX",
                               "currency": "USD", "expiry": expiration.strftime("%Y%m%d")}]}
    monkeypatch.setattr(ibkr_helper, "queue_request", request)
    assert ibkr_helper._lookup_conid_future_tws(
        asset=Asset(symbol, asset_type="future", expiration=expiration), exchange="NYMEX",
        mapping={}, keys_added=set()) == 123
