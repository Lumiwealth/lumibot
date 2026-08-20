from datetime import datetime

import pandas as pd
import pytz

from lumibot.backtesting import YahooDataBacktesting
from lumibot.tools import yahoo_helper
from lumibot.tools.yahoo_helper import YahooHelper


class _FakeTicker:
    def __init__(self, symbol, frames, history_calls):
        self.symbol = symbol
        self._frames = frames
        self._history_calls = history_calls

    @property
    def info(self):
        return {}

    def history(self, **kwargs):
        auto_adjust = kwargs["auto_adjust"]
        self._history_calls.append(auto_adjust)
        return self._frames[auto_adjust].copy()


class _FakeYFinance:
    def __init__(self, frames, history_calls):
        self._frames = frames
        self._history_calls = history_calls

    def Ticker(self, symbol):
        return _FakeTicker(symbol, self._frames, self._history_calls)


def test_yahoo_auto_adjust_keeps_ohlcv_and_separates_cache_modes(monkeypatch, tmp_path):
    """Modern yfinance frames must retain OHLCV for both adjustment modes."""
    index = pd.DatetimeIndex(["2025-01-01 16:00:00-05:00", "2025-01-02 16:00:00-05:00"])
    frames = {
        # yfinance's adjusted response has no Adj Open/High/Low columns.
        True: pd.DataFrame(
            {
                "Open": [101.0, 102.0],
                "High": [103.0, 104.0],
                "Low": [99.0, 100.0],
                "Close": [102.0, 103.0],
                "Volume": [1000, 1100],
                "Dividends": [0.0, 0.0],
                "Stock Splits": [0.0, 0.0],
                "Capital Gains": [0.0, 0.0],
            },
            index=index,
        ),
        False: pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [98.0, 99.0],
                "Close": [101.0, 102.0],
                "Adj Close": [99.0, 100.0],
                "Volume": [1000, 1100],
                "Dividends": [0.0, 0.0],
                "Stock Splits": [0.0, 0.0],
                "Capital Gains": [0.0, 0.0],
            },
            index=index,
        ),
    }
    history_calls = []
    monkeypatch.setattr(yahoo_helper, "yf", _FakeYFinance(frames, history_calls))
    monkeypatch.setattr(YahooHelper, "sleep_and_get_proxy", staticmethod(lambda: None))
    monkeypatch.setattr(YahooHelper, "get_symbol_info", staticmethod(lambda symbol: {}))
    monkeypatch.setattr(YahooHelper, "LUMIBOT_YAHOO_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(YahooHelper, "CACHING_ENABLED", True)

    eastern = pytz.timezone("America/New_York")
    last_needed = eastern.localize(datetime(2025, 1, 2))
    adjusted = YahooHelper.get_symbol_data("SPY", auto_adjust=True, last_needed_datetime=last_needed)
    unadjusted = YahooHelper.get_symbol_data("SPY", auto_adjust=False, last_needed_datetime=last_needed)

    ohlcv = ["Open", "High", "Low", "Close", "Volume"]
    assert adjusted[ohlcv].to_dict("list") == {
        "Open": [101.0, 102.0],
        "High": [103.0, 104.0],
        "Low": [99.0, 100.0],
        "Close": [102.0, 103.0],
        "Volume": [1000, 1100],
    }
    assert unadjusted[ohlcv].to_dict("list") == {
        "Open": [100.0, 101.0],
        "High": [102.0, 103.0],
        "Low": [98.0, 99.0],
        "Close": [101.0, 102.0],
        "Volume": [1000, 1100],
    }

    cache_files = {path.name for path in tmp_path.glob("*.pickle")}
    assert cache_files == {"SPY_1d_adjusted.pickle", "SPY_1d_unadjusted.pickle"}

    # Repeating either request must hit its own mode-specific cache.
    YahooHelper.get_symbol_data("SPY", auto_adjust=True, last_needed_datetime=last_needed)
    YahooHelper.get_symbol_data("SPY", auto_adjust=False, last_needed_datetime=last_needed)
    assert history_calls == [True, False]

    # The data-source path now has a real opening price instead of None.
    for auto_adjust, expected_open in ((True, 102.0), (False, 101.0)):
        data_source = YahooDataBacktesting(
            datetime_start=eastern.localize(datetime(2025, 1, 1)),
            datetime_end=eastern.localize(datetime(2025, 1, 3)),
            auto_adjust=auto_adjust,
            show_progress_bar=False,
        )
        data_source._datetime = eastern.localize(datetime(2025, 1, 3))
        assert data_source.get_last_price("SPY", timestep="day") == expected_open


def test_yahoo_batch_fetch_passes_adjustment_mode_without_recursing(monkeypatch):
    """The multi-asset data-source path must use the same adjustment mode."""
    frame = pd.DataFrame(
        {
            "Open": [101.0],
            "High": [103.0],
            "Low": [99.0],
            "Close": [102.0],
            "Volume": [1000],
        },
        index=pd.DatetimeIndex(["2025-01-01 16:00:00-05:00"]),
    )
    calls = []

    def fetch_symbols_data(symbols, interval, caching=True, auto_adjust=False):
        calls.append((symbols, interval, caching, auto_adjust))
        return {symbol: frame.copy() for symbol in symbols}

    monkeypatch.setattr(YahooHelper, "fetch_symbols_data", staticmethod(fetch_symbols_data))

    result = YahooHelper.get_symbols_data(["SPY", "QQQ"], auto_adjust=True, caching=False)

    assert calls == [(["SPY", "QQQ"], "1d", False, True)]
    assert set(result) == {"SPY", "QQQ"}
    assert list(result["SPY"].columns) == ["Open", "High", "Low", "Close", "Volume"]
