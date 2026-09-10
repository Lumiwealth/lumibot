"""Read-only real Demo market-data contracts; no API keys or orders."""

from itertools import islice

import pytest

from lumibot.data_sources import KalshiData
from lumibot.entities import Asset, Bars, Quote

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi]


def test_public_demo_prices_quotes_and_history(record_property):
    source = KalshiData(config={"IS_DEMO": True, "API_KEY_ID": None, "PRIVATE_KEY": None, "PRIVATE_KEY_PATH": None})
    try:
        markets = source._client.pages(
            "/markets", "markets", params={"status": "open", "limit": 1000}, authenticated=False
        )
        market = next(
            (
                row
                for row in islice(markets, 5000)
                if row.get("market_type") == "binary"
                and not row.get("mve_collection_ticker")
                and row.get("last_price_dollars") is not None
            ),
            None,
        )
        if market is None:
            pytest.skip("No open binary Demo contract available for read-only market-data qualification")
        asset = Asset(market["ticker"], asset_type="prediction_contract")
        record_property("market_ticker", asset.symbol)
        assert 0 <= source.get_last_price(asset) <= 1
        assert 0 <= source.get_last_prices([asset])[asset] <= 1
        quote = source.get_quote(asset)
        assert isinstance(quote, Quote)
        for price in (quote.bid, quote.ask):
            assert price is None or 0 <= price <= 1
        bars = source.get_historical_prices(asset, 24, timestep="hour")
        assert isinstance(bars, Bars)
        assert bars.df.index.tz is not None
        assert bars.df.index.is_monotonic_increasing
        assert bars.df[["open", "high", "low", "close"]].map(lambda price: 0 <= price <= 1).all().all()
        record_property("traded_hourly_bars", len(bars.df))
        result = source.get_bars([asset], 24, timestep="hour", sleep_time=0)
        assert isinstance(result[asset], Bars)
        assert not result.errors
    finally:
        source.shutdown()
