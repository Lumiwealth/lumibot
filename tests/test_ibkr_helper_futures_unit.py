from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from lumibot.entities import Asset


def _mnq_june_2026_chain():
    return {
        "MNQ": [
            {"symbol": "MNQ", "conid": 770561201, "expirationDate": 20260618, "ltd": 20260618},
            {"symbol": "MNQ", "conid": 793356225, "expirationDate": 20260918, "ltd": 20260918},
            {"symbol": "MNQ", "conid": 815824267, "expirationDate": 20261218, "ltd": 20261218},
        ]
    }


def _stub_cache():
    class _StubCache:
        enabled = False

        def ensure_local_file(self, path, payload=None):
            return

        def on_local_update(self, path, payload=None):
            return

    return _StubCache()


def test_ibkr_helper_future_requires_expiration(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        raise AssertionError(f"Should not attempt remote calls for invalid futures asset: {url}")

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset(symbol="MES", asset_type="future")
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 1, 0, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="futures require an explicit expiration"):
        ibkr_helper.get_price_data(asset=asset, quote=None, timestep="minute", start_dt=start, end_dt=end)


def test_ibkr_helper_future_applies_contract_multiplier_and_min_tick(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    class _StubCache:
        def ensure_local_file(self, path, payload=None):
            return

        def on_local_update(self, path, payload=None):
            return

    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: _StubCache())
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda *, asset, quote, exchange: 999)
    monkeypatch.setattr(ibkr_helper, "_fetch_contract_info", lambda conid: {"multiplier": "5", "minTick": "0.25"})

    asset = Asset(symbol="MES", asset_type=Asset.AssetType.FUTURE, expiration=datetime(2025, 12, 19).date())
    assert asset.multiplier == 1
    assert getattr(asset, "min_tick", None) is None

    ibkr_helper._maybe_apply_future_contract_metadata(asset=asset, exchange="CME")

    assert asset.multiplier == 5
    assert getattr(asset, "min_tick", None) == pytest.approx(0.25, rel=1e-12)

    cache_file = tmp_path / "ibkr" / "future" / "contracts" / "CONID_999.json"
    assert cache_file.exists()


def test_ibkr_helper_resolve_conid_accepts_usd_key_for_futures(monkeypatch, tmp_path):
    import json
    from datetime import date

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    class _StubCache:
        def ensure_local_file(self, path, payload=None):
            return

        def on_local_update(self, path, payload=None):
            return

    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: _StubCache())

    def _fail_lookup(*, asset, quote, exchange):  # noqa: ANN001
        raise AssertionError("Should not call remote conid lookup when a cached USD-key exists")

    monkeypatch.setattr(ibkr_helper, "_lookup_conid_remote", _fail_lookup)

    conids_path = tmp_path / "ibkr" / "conids.json"
    conids_path.parent.mkdir(parents=True, exist_ok=True)
    conids_path.write_text(json.dumps({"future|MES|USD|CME|20251219": 730283085}), encoding="utf-8")

    asset = Asset(symbol="MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19))
    assert ibkr_helper._resolve_conid(asset=asset, quote=None, exchange="CME") == 730283085


@pytest.mark.parametrize(
    "root",
    [
        "BRR",        # CME CF Bitcoin Reference Rate (IBKR root; 'regular' BTC futures)
        "MBT",        # Micro Bitcoin
        "ETHUSDRR",   # CME CF Ether-Dollar Reference Rate (IBKR root; 'regular' ETH futures)
        "MET",        # Micro Ether
        # EUR variants
        "BTCEURRR",
        "ETHEURRR",
        "EBMEUR",
        "EEM",
    ],
)
def test_ibkr_helper_crypto_futures_contract_expirations_use_last_friday(root):
    """Crypto futures expire on the last Friday trading day of the month (holiday-adjusted)."""
    from datetime import date

    import lumibot.tools.ibkr_helper as ibkr_helper

    assert ibkr_helper._contract_expiration_date(root, year=2024, month=12) == date(2024, 12, 27)
    # Good Friday 2024-03-29 -> expiry shifts to Thursday 2024-03-28.
    assert ibkr_helper._contract_expiration_date(root, year=2024, month=3) == date(2024, 3, 28)


def test_ibkr_helper_cont_future_segments_resolve_crypto_futures_expirations(monkeypatch, tmp_path):
    """Regression: IBKR cont_future must not compute third-Friday expirations for crypto futures."""
    import json
    from datetime import date, datetime, timezone

    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.tools import futures_roll

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    class _StubCache:
        enabled = False

        def ensure_local_file(self, path, payload=None):
            return

        def on_local_update(self, path, payload=None):
            return

    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: _StubCache())

    def _fail_lookup(*, asset, quote, exchange):  # noqa: ANN001
        raise AssertionError("Should not call remote conid lookup when a cached USD-key exists")

    monkeypatch.setattr(ibkr_helper, "_lookup_conid_remote", _fail_lookup)

    # Only the correct expiry exists in the registry.
    conids_path = tmp_path / "ibkr" / "conids.json"
    conids_path.parent.mkdir(parents=True, exist_ok=True)
    conids_path.write_text(json.dumps({"future|MBT|USD|CME|20241227": 605997194}), encoding="utf-8")

    # Keep the schedule deterministic and focused on the failing month.
    def _fake_schedule(asset, start, end, year_digits=2):  # noqa: ANN001
        return [("MBTZ24", start, end)]

    monkeypatch.setattr(futures_roll, "build_roll_schedule", _fake_schedule)

    segments = ibkr_helper._resolve_cont_future_segments(
        asset=Asset("MBT", asset_type=Asset.AssetType.CONT_FUTURE),
        start_dt=datetime(2024, 12, 1, tzinfo=timezone.utc),
        end_dt=datetime(2024, 12, 2, tzinfo=timezone.utc),
        exchange="CME",
    )

    assert segments, "Expected at least one cont_future segment"
    contract_asset = segments[0][0]
    assert contract_asset.asset_type == "future"
    assert contract_asset.expiration == date(2024, 12, 27)


def test_ibkr_helper_future_conid_uses_same_month_ibkr_listing_for_holiday_mismatch(monkeypatch, tmp_path):
    """Regression: MNQM26 is listed by IBKR as 20260618 while the old synthetic anchor computed 20260619."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", _stub_cache)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_CONID_CACHE", {})
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE_LOADED", True)
    monkeypatch.setattr(ibkr_helper, "_is_future_or_current_expiration", lambda value: True)
    negative_key = ibkr_helper.IbkrConidKey("future", "MNQ", "", "CME", "20260619").to_key()
    monkeypatch.setattr(
        ibkr_helper,
        "_NEGATIVE_CONID_CACHE",
        {
            negative_key: {
                "ts": 1777509385.2244134,
                "reason": "no_conid",
                "message": "stale exact-date miss",
            }
        },
    )

    calls = []

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        calls.append(dict(querystring))
        assert querystring["symbols"] == "MNQ"
        assert querystring["exchange"] == "CME"
        return _mnq_june_2026_chain()

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    asset = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 6, 19))
    mapping = {}
    keys_added = set()

    conid = ibkr_helper._lookup_conid_future(asset=asset, exchange="CME", mapping=mapping, keys_added=keys_added)

    assert conid == 770561201
    assert calls, "A future-dated stale negative cache entry must not block a fresh IBKR lookup"
    assert getattr(asset, "_ibkr_resolved_expiration") == date(2026, 6, 18)
    assert mapping["future|MNQ||CME|20260618"] == 770561201
    assert mapping["future|MNQ|USD|CME|20260618"] == 770561201
    assert mapping["future|MNQ||CME|20260619"] == 770561201
    assert mapping["future|MNQ|USD|CME|20260619"] == 770561201
    assert negative_key not in ibkr_helper._NEGATIVE_CONID_CACHE


def test_ibkr_helper_cont_future_segments_use_ibkr_listed_mnq_expiration(monkeypatch, tmp_path):
    """The cont_future segment should fetch history using the IBKR-listed contract date, not the stale anchor."""
    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.tools import futures_roll

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", _stub_cache)
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_CONID_CACHE", {})
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE_LOADED", True)
    monkeypatch.setattr(ibkr_helper, "_NEGATIVE_CONID_CACHE", {})

    def fake_queue_request(url: str, querystring, headers=None, timeout=None):
        assert querystring["symbols"] == "MNQ"
        assert querystring["exchange"] == "CME"
        return _mnq_june_2026_chain()

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    def fake_schedule(asset, start, end, year_digits=2):  # noqa: ANN001
        return [("MNQM26", start, end)]

    monkeypatch.setattr(futures_roll, "build_roll_schedule", fake_schedule)

    segments = ibkr_helper._resolve_cont_future_segments(
        asset=Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE),
        start_dt=datetime(2026, 4, 22, tzinfo=timezone.utc),
        end_dt=datetime(2026, 4, 29, tzinfo=timezone.utc),
        exchange="CME",
    )

    assert segments, "Expected MNQ cont_future to resolve to an explicit contract segment"
    contract_asset, seg_start, seg_end = segments[0]
    assert contract_asset.asset_type == "future"
    assert contract_asset.expiration == date(2026, 6, 18)
    assert seg_start == datetime(2026, 4, 22, tzinfo=timezone.utc)
    assert seg_end == datetime(2026, 4, 29, tzinfo=timezone.utc)
