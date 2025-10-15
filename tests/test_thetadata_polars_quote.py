import datetime

import polars as pl
import pytest
import pytz

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_polars_quote_fallback_prefers_mid(monkeypatch):
    """The polars backtester should match pandas by using bid/ask mid when Theta quote fallback triggers."""

    # Avoid killing user processes or starting ThetaTerminal during test setup
    from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars

    monkeypatch.setattr(ThetaDataBacktestingPolars, "kill_processes_by_name", lambda self, keyword: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda: None)

    tz = pytz.timezone("America/New_York")
    start_dt = tz.localize(datetime.datetime(2025, 3, 30, 9, 30))
    end_dt = tz.localize(datetime.datetime(2025, 3, 31, 10, 0))

    datasource = ThetaDataBacktestingPolars(datetime_start=start_dt, datetime_end=end_dt)

    asset = Asset("PLTR", asset_type=Asset.AssetType.STOCK)
    quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
    current_dt = tz.localize(datetime.datetime(2025, 3, 31, 9, 30))
    monkeypatch.setattr(datasource, "get_datetime", lambda: current_dt)

    bid_price = 2.53
    ask_price = 2.81

    sample_rows = [
        {
            "datetime": tz.localize(datetime.datetime(2025, 3, 31, 9, 31)),
            "open": 1.89,
            "high": 1.89,
            "low": 1.89,
            "close": 1.89,
            "volume": 0.0,
            "count": 0.0,
            "bid_size": 10.0,
            "bid_exchange": 1.0,
            "bid": bid_price,
            "bid_condition": 50.0,
            "ask_size": 11.0,
            "ask_exchange": 2.0,
            "ask": ask_price,
            "ask_condition": 50.0,
            "price_change": 0.0,
            "dividend_yield": 0.0,
            "return": 0.0,
        }
    ]
    sample_df = pl.DataFrame(sample_rows)

    def fake_pull(
        _self,
        _asset,
        length,
        timestep="minute",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        return sample_df

    monkeypatch.setattr(datasource, "_pull_source_symbol_bars", fake_pull.__get__(datasource))

    quote = datasource.get_quote(asset, quote=quote_asset)

    expected_mid = (bid_price + ask_price) / 2

    assert quote.mid_price == pytest.approx(expected_mid)
    assert quote.price == pytest.approx(expected_mid)
    assert getattr(quote, "source", None) == "polars"
