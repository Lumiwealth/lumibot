from datetime import datetime

import pytz

from lumibot.backtesting import YahooDataBacktesting


def test_yahoo_daily_last_price_cache_reuses_same_trading_date(monkeypatch):
    data_source = YahooDataBacktesting(
        datetime_start=datetime(2025, 1, 1),
        datetime_end=datetime(2025, 1, 5),
        show_progress_bar=False,
    )
    eastern = pytz.timezone("America/New_York")
    calls = {"count": 0}

    def fake_last_daily_open_price(asset, timestep, quote=None, exchange=None):
        calls["count"] += 1
        return 123.45

    monkeypatch.setattr(data_source, "_get_last_daily_open_price", fake_last_daily_open_price)

    data_source._datetime = eastern.localize(datetime(2025, 1, 2, 8, 30))
    first = data_source.get_last_price("SPY", timestep="day")

    data_source._datetime = eastern.localize(datetime(2025, 1, 2, 16, 0))
    second = data_source.get_last_price("SPY", timestep="day")

    data_source._datetime = eastern.localize(datetime(2025, 1, 3, 8, 30))
    third = data_source.get_last_price("SPY", timestep="day")

    assert first == second == third == 123.45
    assert calls["count"] == 2
