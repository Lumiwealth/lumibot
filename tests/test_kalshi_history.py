"""Recorded public current/archive candle contracts; no account or database fakes."""

import json
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.data_sources import KalshiData
from lumibot.entities import Asset, Bars
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient


def recorded():
    return json.loads((Path(__file__).parent / "fixtures/kalshi/history_responses.json").read_text())


def contract(outcome="yes"):
    return Asset("SYNTHETIC-EVENT", "prediction_contract", outcome=outcome)


@pytest.fixture
def source():
    rows = recorded()
    client = Mock(spec=KalshiClient)

    def request(method, path, **kwargs):
        if path == "/markets/candlesticks":
            return rows["live"]
        if path.endswith("/candlesticks"):
            return rows["archive"]
        raise AssertionError(path)

    client.request.side_effect = request
    result = KalshiData(client=client)
    yield result, rows
    result.shutdown()


def bounds(rows):
    values = [r["end_period_ts"] for r in rows["live"]["markets"][0]["candlesticks"]]
    return pd.Timestamp(min(values), unit="s", tz="UTC"), pd.Timestamp(max(values), unit="s", tz="UTC")


def test_current_yes_no_bars_preserve_quote_only_and_fractional_volume(source):
    data, rows = source
    start, end = bounds(rows)
    yes = data.get_historical_prices(contract(), None, "hour", start=start, end=end)
    no = data.get_historical_prices(contract("no"), None, "hour", start=start, end=end)
    assert isinstance(yes, Bars) and yes.asset == contract()
    # Decimal complements convert to floats once; compare numeric precision, not binary bit patterns.
    y, n = yes.df, no.df
    assert len(y) == len(rows["live"]["markets"][0]["candlesticks"])
    assert pd.isna(y.iloc[0]["close"]) and y.iloc[0]["volume"] == 0
    pd.testing.assert_series_equal(n["open"], 1 - y["open"], check_names=False, check_exact=False)
    pd.testing.assert_series_equal(n["high"], 1 - y["low"], check_names=False, check_exact=False)
    pd.testing.assert_series_equal(n["low"], 1 - y["high"], check_names=False, check_exact=False)
    pd.testing.assert_series_equal(n["close"], 1 - y["close"], check_names=False, check_exact=False)
    pd.testing.assert_series_equal(n["bid"], 1 - y["ask"], check_names=False, check_exact=False)
    pd.testing.assert_series_equal(n["ask"], 1 - y["bid"], check_names=False, check_exact=False)
    assert n["volume"].equals(y["volume"])


def test_history_clamps_bar_end_and_timeshift(source, monkeypatch):
    data, rows = source
    _, end = bounds(rows)
    monkeypatch.setattr(data, "get_datetime", lambda **kwargs: end)
    result = data.get_historical_prices(contract(), 2, "hour", timeshift=pd.Timedelta(hours=1))
    assert len(result.df) == 2
    assert result.df.index.max() == end - pd.Timedelta(hours=1)


def test_archived_decimal_strings_and_metadata_fallback(source):
    data, rows = source
    original = data._client.request.side_effect

    def request(method, path, **kwargs):
        if path == "/markets/candlesticks":
            return {"markets": []}
        if path == "/markets/SYNTHETIC-EVENT":
            raise KalshiAPIError("not found", status_code=404)
        if path == "/historical/markets/SYNTHETIC-EVENT":
            return {"market": rows["archived_market"]}
        return original(method, path, **kwargs)

    data._client.request.side_effect = request
    stamps = [r["end_period_ts"] for r in rows["archive"]["candlesticks"]]
    bars = data.get_historical_prices(
        contract(),
        None,
        "hour",
        start=pd.Timestamp(min(stamps), unit="s", tz="UTC"),
        end=pd.Timestamp(max(stamps), unit="s", tz="UTC"),
    )
    assert bars.df.iloc[0]["open"] == 0.02
    assert bars.df.iloc[0]["volume"] == 984.8
    assert data.get_market_metadata(contract())["ticker"] == "SYNTHETIC-EVENT"


def test_chunked_windows_are_complete_and_no_duplicate_endpoints(source, monkeypatch):
    data, rows = source
    start, end = bounds(rows)
    monkeypatch.setattr(data, "_HISTORY_WINDOW", 2)
    bars = data.get_historical_prices(contract(), None, "hour", start=start, end=end)
    assert bars.df.index.is_unique
    windows = [c.kwargs["params"] for c in data._client.request.call_args_list]
    assert len(windows) > 1
    assert all(b["start_ts"] == a["end_ts"] + 3600 for a, b in zip(windows, windows[1:]))


def test_custom_timestep_resamples_with_completed_end_labels(source):
    data, rows = source
    start, end = bounds(rows)
    bars = data.get_historical_prices(contract(), None, "2h", start=start, end=end)
    assert not bars.df.empty
    assert all(t.hour % 2 == 0 for t in bars.df.index)
    assert bars.df.index.max() <= end


def test_plural_history_reuses_existing_dispatch_and_keeps_each_outcome(source, monkeypatch):
    data, rows = source
    _, end = bounds(rows)
    monkeypatch.setattr(data, "get_datetime", lambda **kwargs: end)
    yes, no = contract(), contract("no")
    result = data.get_bars([yes, no], 2, "hour", sleep_time=0)
    assert isinstance(result[yes], Bars) and isinstance(result[no], Bars)
    assert len(result) == 2
