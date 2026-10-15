import pandas as pd
import pytest

from lumibot.tools import yahoo_helper
from lumibot.tools.yahoo_helper import YahooHelper


class _FakeTicker:
    def __init__(self, frame):
        self._frame = frame

    def history(self, **kwargs):
        return self._frame.copy()


class _FakeYFinance:
    def __init__(self, frame):
        self._frame = frame

    def Ticker(self, symbol):
        return _FakeTicker(self._frame)


def _intraday_frame(tz="America/New_York"):
    """Three 1-minute bars as yfinance returns them (tz-aware)."""
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-01-02 09:30:00", tz=tz),
            pd.Timestamp("2025-01-02 09:31:00", tz=tz),
            pd.Timestamp("2025-01-02 09:32:00", tz=tz),
        ]
    )
    return pd.DataFrame(
        {
            "Open": [200.0, 200.1, 200.2],
            "High": [200.5, 200.4, 200.6],
            "Low": [199.8, 199.9, 200.0],
            "Close": [200.3, 200.2, 200.5],
            "Volume": [100, 200, 150],
            "Dividends": [0.0, 0.0, 0.0],
            "Stock Splits": [0.0, 0.0, 0.0],
        },
        index=idx,
    )


def _daily_frame():
    idx = pd.DatetimeIndex(["2025-01-02", "2025-01-03"])
    return pd.DataFrame(
        {
            "Open": [200.0, 201.0],
            "High": [202.0, 203.0],
            "Low": [199.0, 200.0],
            "Close": [201.0, 202.0],
            "Volume": [1000, 1100],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 0.0],
        },
        index=idx,
    )


def _patch_yahoo(monkeypatch, frame, info):
    monkeypatch.setattr(yahoo_helper, "yf", _FakeYFinance(frame))
    monkeypatch.setattr(YahooHelper, "sleep_and_get_proxy", staticmethod(lambda: None))
    monkeypatch.setattr(YahooHelper, "get_symbol_info", staticmethod(lambda symbol: info))


US_INFO = {"info": {"market": "us_market", "exchangeTimezoneName": "America/New_York"}}
CCC_INFO = {"info": {"market": "ccc_market", "exchangeTimezoneName": "UTC"}}


@pytest.mark.parametrize("interval", ["1m", "15m"])
def test_intraday_us_timestamps_are_preserved(monkeypatch, interval):
    """Intraday bars must keep their real bar timestamps instead of all being
    stamped to 16:00 (which collapses every bar onto a single index value)."""
    _patch_yahoo(monkeypatch, _intraday_frame(), US_INFO)
    df = YahooHelper.download_symbol_data("AAPL", interval=interval)

    assert not df.empty
    times = df.index
    # Distinct bar times are preserved: no duplicate timestamps.
    assert len(times.unique()) == len(times)
    assert times.hour[0] == 9 and times.minute[0] == 30


def test_daily_us_bars_are_still_stamped_at_session_close(monkeypatch):
    """Daily US bars keep the existing behaviour: stamped at 16:00 ET."""
    _patch_yahoo(monkeypatch, _daily_frame(), US_INFO)
    df = YahooHelper.download_symbol_data("AAPL", interval="1d")

    assert not df.empty
    assert set(df.index.hour) == {16}
    assert set(df.index.minute) == {0}


def test_intraday_crypto_timestamps_are_preserved(monkeypatch):
    """Crypto intraday bars keep their timestamps instead of all being stamped
    to 23:59."""
    frame = _intraday_frame(tz="UTC")
    _patch_yahoo(monkeypatch, frame, CCC_INFO)
    df = YahooHelper.download_symbol_data("BTC-USD", interval="1m")

    assert not df.empty
    times = df.index
    assert len(times.unique()) == len(times)
    assert times.minute[0] == 30


def test_intraday_timestamps_survive_when_info_is_missing(monkeypatch):
    """When symbol info is unavailable the intraday timestamps are untouched."""
    _patch_yahoo(monkeypatch, _intraday_frame(), None)
    df = YahooHelper.download_symbol_data("AAPL", interval="1m")

    assert not df.empty
    times = df.index
    assert len(times.unique()) == len(times)
    assert times.hour[0] == 9 and times.minute[0] == 30
