"""YahooHelper.get_risk_free_rate: nearest ^IRX close, without re-converting the index per call.

get_greeks reads the risk free rate on every call. Re-running pd.to_datetime over an index
that already holds datetimes took about 9 ms per call and was 87% of a greeks call in a
minute-level option backtest (yappi, 2026-11-10).
"""
from datetime import datetime

import pandas as pd
import pytz

import lumibot.tools.yahoo_helper as yahoo_helper
from lumibot.tools.yahoo_helper import YahooHelper

NY = pytz.timezone("America/New_York")


def _irx(index):
    return pd.DataFrame({"Close": [4.10, 4.20, 4.30, 4.40]}, index=index)


def _dates():
    return pd.DatetimeIndex(
        [NY.localize(datetime(2026, 8, day)) for day in (3, 4, 5, 6)]
    )


def test_risk_free_rate_is_the_nearest_close(monkeypatch):
    frame = _irx(_dates())
    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(lambda *a, **k: frame))
    assert YahooHelper.get_risk_free_rate(dt=NY.localize(datetime(2026, 8, 4, 10, 30))) == 4.20 / 100
    assert YahooHelper.get_risk_free_rate(dt=NY.localize(datetime(2026, 8, 5, 15, 0))) == 4.40 / 100
    assert YahooHelper.get_risk_free_rate(dt=NY.localize(datetime(2026, 8, 1, 9, 30))) == 4.10 / 100


def test_risk_free_rate_does_not_reconvert_a_datetime_index(monkeypatch):
    frame = _irx(_dates())
    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(lambda *a, **k: frame))
    conversions = []
    real = pd.to_datetime

    def counting(*args, **kwargs):
        conversions.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(yahoo_helper.pd, "to_datetime", counting)
    for minute in range(30):
        assert YahooHelper.get_risk_free_rate(dt=NY.localize(datetime(2026, 8, 4, 10, minute))) == 4.20 / 100
    assert conversions == []


def test_risk_free_rate_still_converts_and_sorts_other_indexes(monkeypatch):
    unsorted = ["2026-08-05", "2026-08-03", "2026-08-06", "2026-08-04"]
    frame = pd.DataFrame({"Close": [4.30, 4.10, 4.40, 4.20]}, index=unsorted)
    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(lambda *a, **k: frame))
    assert YahooHelper.get_risk_free_rate(dt=datetime(2026, 8, 4, 1, 0)) == 4.20 / 100
