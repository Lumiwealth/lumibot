"""Market data from the same wallet service and network used for execution."""

from lumibot.data_sources.data_source import DataSource
from lumibot.tools.metamask_client import MetaMaskClient, decimal_value, token_payload


class MetaMaskData(DataSource):
    SOURCE = "METAMASK"
    MIN_TIMESTEP = "minute"

    def __init__(self, config=None, *, client=None, **kwargs):
        super().__init__(**kwargs)
        self.client = client or MetaMaskClient(config or {})
        self.name = "metamask"

    def get_last_price(self, asset, quote=None, exchange=None):
        if asset.asset_type == "crypto_future":
            if quote is not None and quote.symbol != "USDC":
                raise ValueError("Hyperliquid prices are denominated in USDC")
            response = self.client.call("GET", "/v1/price", params={"symbol": asset.symbol})
        elif asset.asset_type == "crypto":
            response = self.client.call(
                "POST",
                "/v1/token-price",
                payload={
                    "asset": token_payload(asset),
                    **({"quote": token_payload(quote)} if quote is not None else {}),
                },
            )
        else:
            raise ValueError("MetaMask supports onchain crypto and Hyperliquid crypto_future assets")
        return decimal_value(response["price"])

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="minute",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        **kwargs,
    ):
        if asset.asset_type != "crypto_future":
            raise NotImplementedError(
                "MetaMask spot candles are not available; configure a separate historical data source"
            )
        from datetime import datetime, timedelta, timezone

        import pandas as pd

        from lumibot.entities import Bars

        intervals = {"minute": ("1m", 60), "hour": ("1h", 3600), "day": ("1d", 86400)}
        if timestep not in intervals or not 1 <= length <= 5000:
            raise ValueError("Use minute/hour/day and 1–5000 bars")
        interval, seconds = intervals[timestep]
        end = datetime.now(timezone.utc) - (timeshift or timedelta())
        response = self.client.call(
            "GET",
            "/v1/candles",
            params={
                "symbol": asset.symbol,
                "interval": interval,
                "start": int((end - timedelta(seconds=seconds * (length + 1))).timestamp() * 1000),
                "end": int(end.timestamp() * 1000),
            },
        )
        frame = pd.DataFrame(response, columns=["timestamp", "open", "high", "low", "close", "volume"])
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
        frame = frame.set_index("timestamp").tail(length)
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = pd.to_numeric(frame[column])
        return Bars(frame, self.SOURCE, asset, quote=quote, raw=response)

    def get_chains(self, asset, quote=None):
        raise NotImplementedError("MetaMask does not offer option chains")
