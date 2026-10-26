"""Kalshi selected-contract prices through the existing DataSource interface."""

from decimal import Decimal

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Quote
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value


class KalshiData(DataSource):
    """USD prediction contracts, selected by market ticker and YES/NO outcome."""

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

    @staticmethod
    def _validate_asset(asset, quote=None, exchange=None):
        if not isinstance(asset, Asset) or asset.asset_type != "prediction_contract":
            raise ValueError("Kalshi requires a prediction_contract Asset")
        if asset.outcome not in ("yes", "no"):
            raise ValueError("Kalshi requires an explicit Asset outcome='yes' or outcome='no'")
        if quote is not None and (quote.symbol != "USD" or quote.asset_type != "forex"):
            raise ValueError("Kalshi prediction contracts are quoted in USD")

    def _market(self, asset, quote=None, exchange=None):
        self._validate_asset(asset, quote, exchange)
        return self.get_market_metadata(asset)

    @staticmethod
    def _selected_price(market, outcome):
        value = market.get("last_price_dollars")
        if value is None:
            return None
        price = decimal_value(value, "last price")
        if not 0 <= price <= 1:
            raise KalshiAPIError("Kalshi returned an invalid prediction price")
        return float(price if outcome == "yes" else Decimal(1) - price)

    def get_last_price(self, asset, quote=None, exchange=None):
        return self._selected_price(self._market(asset, quote, exchange), asset.outcome)

    def get_quote(self, asset, quote=None, exchange=None):
        market = self._market(asset, quote, exchange)
        payload = self._client.request(
            "GET", f"/markets/{KalshiClient.path_id(asset.symbol)}/orderbook", authenticated=False
        )
        book = payload.get("orderbook_fp")
        if not isinstance(book, dict):
            raise KalshiAPIError("Kalshi returned an invalid fixed-point order book")

        def best(side):
            levels = book.get(side + "_dollars")
            if not isinstance(levels, list):
                raise KalshiAPIError("Kalshi order book is missing a side")
            parsed = []
            for level in levels:
                if not isinstance(level, (list, tuple)) or len(level) != 2:
                    raise KalshiAPIError("Kalshi returned an invalid book level")
                price, size = (decimal_value(value, "book level") for value in level)
                if not 0 <= price <= 1 or size < 0:
                    raise KalshiAPIError("Kalshi returned an invalid book level")
                if size:
                    parsed.append((price, size))
            return max(parsed, key=lambda value: value[0]) if parsed else None

        own = best(asset.outcome)
        other = best("no" if asset.outcome == "yes" else "yes")
        return Quote(
            asset=asset,
            price=self._selected_price(market, asset.outcome),
            bid=float(own[0]) if own else None,
            ask=float(Decimal(1) - other[0]) if other else None,
            bid_size=float(own[1]) if own else None,
            ask_size=float(other[1]) if other else None,
            raw_data=payload,
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs
    ):
        raise NotImplementedError("Kalshi historical prices are not implemented yet")

    def shutdown(self):
        super().shutdown()
        if self._owns_client:
            self._client.close()
