"""Kalshi DataSource foundation, sharing the broker's provider client."""

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient


class KalshiData(DataSource):
    """Provider metadata access; quote/history normalization is a later increment."""

    SOURCE = "KALSHI"
    MIN_TIMESTEP = "minute"

    def __init__(self, config=None, *, client=None, **kwargs):
        super().__init__(**kwargs)
        config = config or {}
        self.name = "kalshi"
        configured = config.get("CLIENT", config.get("KALSHI_CLIENT"))
        self._client = client if client is not None else configured
        self._owns_client = self._client is None
        if self._owns_client:
            self._client = KalshiClient(config)

    def get_market_metadata(self, market=None, **kwargs):
        ticker = market.symbol if isinstance(market, Asset) else market
        if not isinstance(ticker, str) or not ticker:
            raise ValueError("Kalshi metadata requires a market ticker")
        payload = self._client.request("GET", f"/markets/{KalshiClient.path_id(ticker)}", authenticated=False)
        row = payload.get("market")
        if not isinstance(row, dict) or row.get("ticker") != ticker:
            raise KalshiAPIError("Kalshi returned an invalid market response")
        return row

    def get_chains(self, asset, quote=None):
        return {}

    def get_last_price(self, asset, quote=None, exchange=None):
        raise NotImplementedError("Kalshi outcome-aware pricing is not implemented yet")

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs
    ):
        raise NotImplementedError("Kalshi historical prices are not implemented yet")

    def shutdown(self):
        super().shutdown()
        if self._owns_client:
            self._client.close()
