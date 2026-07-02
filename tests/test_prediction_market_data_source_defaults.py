import pandas as pd

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Bars, Quote


class DefaultPredictionMarketDataSource(DataSource):
    SOURCE = "TEST"

    def get_chains(self, asset, quote=None):
        return {}

    def get_historical_prices(self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, **kwargs):
        return Bars(
            pd.DataFrame(columns=["open", "high", "low", "close", "volume"]),
            source=self.SOURCE,
            asset=asset,
        )

    def get_last_price(self, asset, quote=None, exchange=None):
        return 1.0

    def get_quote(self, asset, quote=None, exchange=None):
        return Quote(asset=asset, bid=0.4, ask=0.6)


def test_prediction_market_default_methods_do_not_break_regular_data_sources():
    data = DefaultPredictionMarketDataSource()
    asset = Asset("SPY")

    assert data.search_markets("election") == []
    assert data.get_event(slug="event") == {}
    assert data.get_market_metadata(asset) == {}
    assert data.get_market_rules(asset) == {}
    assert data.get_resolution_status(asset)["status"] == "unsupported"
    assert data.get_recent_trades(asset) == []
    assert data.get_open_interest(asset) is None
    assert data.get_holders(asset) == []
    assert data.get_spread(asset) == 0.19999999999999996
    assert data.get_midpoint(asset) == 0.5
