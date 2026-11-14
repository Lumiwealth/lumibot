"""IBKR only accepts its own legal bar sizes; "60 minutes" must ask for "1h".

Regression (Oct 2026, hourly futures strategies on COMEX and NYMEX): the valid
LumiBot timestep "60 minutes" was sent to IBKR as bar size "60min". IBKR
rejected it permanently, LumiBot treated the rejection as a transient gap, and
the strategy silently ran with zero hourly bars and zero fills.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lumibot.entities import Asset

# Recorded provider answer (TWS backend through the downloader), shortened only
# where the provider itself elided the list.
RECORDED_321 = (
    "TWS permanent error 321: Historical data bar size setting is invalid. "
    "Legal ones are: 1 secs, 5 secs, 10 secs, 15 secs, 30 secs, 1 min, 2 mins, 3 mins, "
    "5 mins, 10 mins, 15 mins, 20 mins, 30 mins, 1 hour, 2 hours, 3 hours, 4 hours, "
    "8 hours, 1 day, 1W, 1M"
)
LEGAL = {"1min", "2min", "3min", "5min", "10min", "15min", "20min", "30min",
         "1h", "2h", "3h", "4h", "8h", "1d"}


@pytest.fixture(autouse=True)
def _reset_health():
    from lumibot.tools.ibkr_history_health import reset_ibkr_history_health_for_testing

    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


@pytest.mark.parametrize(
    "timestep,expected",
    [
        ("60 minutes", ("1h", 3600, "hour")),
        ("60minute", ("1h", 3600, "hour")),
        ("1 hour", ("1h", 3600, "hour")),
        ("120minute", ("2h", 7200, "2hour")),
        ("240 minutes", ("4h", 14400, "4hour")),
        ("480minute", ("8h", 28800, "8hour")),
        ("30minute", ("30min", 1800, "30minute")),
    ],
)
def test_minute_multiples_use_the_legal_hour_bar(timestep, expected):
    from lumibot.tools.ibkr_helper import _timestep_to_ibkr_bar

    assert _timestep_to_ibkr_bar(timestep) == expected


def test_hourly_bars_share_one_cache_file_whatever_the_spelling():
    from lumibot.tools.ibkr_helper import _cache_file_for

    asset = Asset("MGC", asset_type="future", expiration=datetime(2026, 12, 29).date())
    paths = {
        _cache_file_for(asset=asset, quote=None, timestep=spelling, exchange="COMEX",
                        source="Trades", include_after_hours=True)
        for spelling in ("60 minutes", "60minute", "1hour", "hour")
    }
    assert len(paths) == 1


def _crypto_history_stub(monkeypatch, tmp_path, *, reject_all=False):
    import lumibot.tools.ibkr_helper as helper

    monkeypatch.setattr(helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    helper._RUNTIME_CONID_CACHE.clear()
    bars_sent = []

    def fake_queue_request(url, querystring, headers=None, timeout=None, **_kwargs):
        if url.endswith("/ibkr/iserver/secdef/search"):
            return [{"conid": 123, "sections": [{"secType": "CRYPTO", "exchange": "PAXOS"}]}]
        if url.endswith("/ibkr/iserver/marketdata/history"):
            bars_sent.append(querystring["bar"])
            if reject_all or querystring["bar"] not in LEGAL:
                return {"error": RECORDED_321}
            return {"data": [
                {"t": 1700000000000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10},
                {"t": 1700003600000, "o": 2, "h": 3, "l": 2, "c": 3, "v": 11},
            ]}
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(helper, "queue_request", fake_queue_request)
    return helper, bars_sent


def test_sixty_minute_request_asks_ibkr_for_one_hour_bars(monkeypatch, tmp_path):
    helper, bars_sent = _crypto_history_stub(monkeypatch, tmp_path)

    frame = helper.get_price_data(
        asset=Asset("BTC", asset_type="crypto"), quote=Asset("USD", asset_type="forex"),
        timestep="60 minutes",
        start_dt=datetime.fromtimestamp(1700000000, tz=timezone.utc),
        end_dt=datetime.fromtimestamp(1700003600, tz=timezone.utc),
        source="Trades",
    )

    assert not frame.empty
    assert bars_sent and set(bars_sent) == {"1h"}


def test_permanent_bar_size_rejection_is_a_visible_error(monkeypatch, tmp_path):
    helper, _ = _crypto_history_stub(monkeypatch, tmp_path, reject_all=True)

    with pytest.raises(Exception, match="bar size setting is invalid"):
        helper.get_price_data(
            asset=Asset("BTC", asset_type="crypto"), quote=Asset("USD", asset_type="forex"),
            timestep="1 hour",
            start_dt=datetime.fromtimestamp(1700000000, tz=timezone.utc),
            end_dt=datetime.fromtimestamp(1700003600, tz=timezone.utc),
            source="Trades",
        )


def test_size_with_no_legal_ibkr_bar_fails_before_asking(monkeypatch, tmp_path):
    helper, bars_sent = _crypto_history_stub(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match="45minute"):
        helper.get_price_data(
            asset=Asset("BTC", asset_type="crypto"), quote=Asset("USD", asset_type="forex"),
            timestep="45minute",
            start_dt=datetime.fromtimestamp(1700000000, tz=timezone.utc),
            end_dt=datetime.fromtimestamp(1700003600, tz=timezone.utc),
            source="Trades",
        )
    assert bars_sent == []
