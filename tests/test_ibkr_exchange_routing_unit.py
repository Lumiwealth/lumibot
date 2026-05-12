from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import lumibot.tools.ibkr_helper as ibkr_helper
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.tools.backtest_cache import CacheMode
from lumibot.tools.ibkr_secdef import (
    IbkrFuturesExchangeAmbiguousError,
    select_futures_exchange_from_secdef_search_payload,
)


def test_ibkr_secdef_select_futures_exchange_prefers_usd_and_us_venues():
    payload = [
        {"currency": "EUR", "sections": [{"secType": "FUT", "exchange": "EUREX"}]},
        {"currency": "USD", "sections": [{"secType": "FUT", "exchange": "COMEX"}]},
    ]
    assert select_futures_exchange_from_secdef_search_payload("GC", payload) == "COMEX"


def test_ibkr_secdef_select_futures_exchange_ambiguous_raises():
    payload = [
        {"currency": "USD", "sections": [{"secType": "FUT", "exchange": "CME"}, {"secType": "FUT", "exchange": "CBOT"}]}
    ]
    with pytest.raises(IbkrFuturesExchangeAmbiguousError):
        select_futures_exchange_from_secdef_search_payload("ES", payload)


def _make_minute_df() -> pd.DataFrame:
    idx = pd.date_range("2025-12-08 09:31", periods=2, freq="1min", tz="America/New_York")
    return pd.DataFrame(
        {
            "open": [100.0, 100.25],
            "high": [100.0, 100.25],
            "low": [99.75, 100.0],
            "close": [99.875, 100.125],
            "bid": [99.75, 100.0],
            "ask": [100.0, 100.25],
            "volume": [1000, 1000],
        },
        index=idx,
    )


def test_ibkr_rest_backtesting_exchange_override_is_respected_in_quote_and_last_price(monkeypatch):
    df = _make_minute_df()
    captured: list[str | None] = []

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        captured.append(exchange)
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=df.index[0].to_pydatetime(),
        datetime_end=(df.index[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()
    data_source._update_datetime(df.index[1].to_pydatetime())

    quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
    fut = Asset("GC", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 2, 26), multiplier=100)

    q = data_source.get_quote(fut, quote=quote_asset, exchange="COMEX")
    assert q is not None
    assert "COMEX" in captured

    price = data_source.get_last_price(fut, quote=quote_asset, exchange="NYMEX")
    assert "NYMEX" in captured
    assert price is not None

    comex_key, _comex_legacy = data_source._build_dataset_keys(fut, quote_asset, "minute", "COMEX")
    nymex_key, _nymex_legacy = data_source._build_dataset_keys(fut, quote_asset, "minute", "NYMEX")
    assert comex_key in data_source._data_store
    assert nymex_key in data_source._data_store
    assert comex_key != nymex_key


def test_ibkr_lookup_conid_future_bulk_populates_mapping(monkeypatch):
    payload = {
        "GC": [
            {"conid": 111, "expirationDate": "20260226"},
            {"conid": 222, "expirationDate": "20260428"},
        ]
    }

    def fake_queue_request(*, url, querystring, headers=None, timeout=None):
        return payload

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    mapping: dict[str, int] = {}
    keys_added: set[str] = set()
    fut = Asset("GC", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 2, 26))
    conid = ibkr_helper._lookup_conid_future(asset=fut, exchange="COMEX", mapping=mapping, keys_added=keys_added)
    assert conid == 111

    for exp, expected in (("20260226", 111), ("20260428", 222)):
        for quote_symbol in ("", "USD"):
            key = ibkr_helper.IbkrConidKey("future", "GC", quote_symbol, "COMEX", exp).to_key()
            assert mapping[key] == expected
            assert key in keys_added


def test_ibkr_conids_merge_before_upload_unions_remote_keys(tmp_path):
    local_path = tmp_path / "conids.json"
    mapping = {"future:GC:USD:COMEX:20260226": 111}
    local_path.write_text(json.dumps(mapping), encoding="utf-8")

    remote_initial = {"future:ES:USD:CME:20260320": 222}
    remote_key = "ibkr/conids.json"
    bucket = "dummy-bucket"

    class FakeS3:
        def __init__(self):
            self._objects = {remote_key: json.dumps(remote_initial).encode("utf-8")}

        def get_object(self, Bucket, Key):
            assert Bucket == bucket
            return {"Body": io.BytesIO(self._objects.get(Key, b"{}"))}

        def upload_file(self, filename, Bucket, Key):
            assert Bucket == bucket
            self._objects[Key] = Path(filename).read_bytes()

    fake_s3 = FakeS3()

    class FakeCacheManager:
        enabled = True
        mode = CacheMode.S3_READWRITE

        def __init__(self):
            self._settings = SimpleNamespace(backend="s3", bucket=bucket)

        def remote_key_for(self, _local_path, payload=None):
            return remote_key

        def _get_client(self):
            return fake_s3

        def on_local_update(self, _path, payload=None):
            raise AssertionError("Unexpected on_local_update in S3_READWRITE merge path")

    cache_manager = FakeCacheManager()
    ibkr_helper._merge_upload_conids_json(
        cache_manager,
        local_path,
        mapping=mapping,
        required_keys=set(mapping.keys()),
        max_attempts=1,
    )

    uploaded = json.loads(fake_s3._objects[remote_key].decode("utf-8"))
    assert uploaded == {**remote_initial, **mapping}


def test_futures_roll_rules_cover_mgc_and_cl():
    from datetime import datetime

    from lumibot.tools import futures_roll

    ref = datetime(2026, 1, 12, 12, 0, 0)

    # MGC should follow GC-style delivery months (not the default quarterly schedule).
    _y, mgc_month = futures_roll.determine_contract_year_month("MGC", reference_date=ref)
    assert mgc_month in (2, 4, 6, 8, 10, 12)

    # CL should be monthly (all months valid).
    _y, cl_month = futures_roll.determine_contract_year_month("CL", reference_date=ref)
    assert 1 <= cl_month <= 12


def test_futures_roll_monthly_rules_advance_past_expired_months():
    from datetime import datetime

    from lumibot.tools import futures_roll

    # With monthly rules, a late-month reference can be past multiple roll points.
    # This regression test ensures we don't select an already-rolled contract month
    # (which can cause non-increasing roll schedules / hangs).
    _y, cl_month = futures_roll.determine_contract_year_month("CL", reference_date=datetime(2026, 1, 23, 12, 0, 0))
    assert cl_month == 3


def test_futures_roll_build_roll_schedule_does_not_hang_for_monthly_contracts():
    from datetime import datetime

    from lumibot.entities import Asset
    from lumibot.tools import futures_roll

    asset = Asset("CL", asset_type=Asset.AssetType.CONT_FUTURE)
    schedule = futures_roll.build_roll_schedule(
        asset,
        start=datetime(2026, 1, 23, 12, 0, 0),
        end=datetime(2026, 1, 23, 18, 0, 0),
        year_digits=2,
    )
    assert schedule
    for _symbol, seg_start, seg_end in schedule:
        assert seg_end > seg_start


def test_ibkr_contract_expiration_date_handles_cl_last_trade_rule():
    # Values verified against the IBKR /trsrv/futures "expirationDate" for CL via the downloader.
    assert ibkr_helper._contract_expiration_date("CL", year=2026, month=5).isoformat() == "2026-04-21"
    assert ibkr_helper._contract_expiration_date("CL", year=2026, month=3).isoformat() == "2026-02-20"
    # Micro crude uses the same month codes but IBKR's expirationDate is typically 1 trading day earlier.
    assert ibkr_helper._contract_expiration_date("MCL", year=2026, month=5).isoformat() == "2026-04-20"
    assert ibkr_helper._contract_expiration_date("MCL", year=2026, month=3).isoformat() == "2026-02-19"
