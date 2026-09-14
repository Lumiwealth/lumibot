"""Kalshi price data through the existing LumiBot DataSource interface."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, AssetsMapping, Bars, Quote
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value


class KalshiData(DataSource):
    SOURCE = "KALSHI"
    MIN_TIMESTEP = "minute"
    _INTERVALS = {
        "minute": 1,
        "1minute": 1,
        "1m": 1,
        "hour": 60,
        "1hour": 60,
        "60minute": 60,
        "1h": 60,
        "day": 1440,
        "1day": 1440,
        "1440minute": 1440,
        "1d": 1440,
    }

    def __init__(self, config=None, *, client=None, **kwargs):
        super().__init__(**kwargs)
        self.name = "kalshi"
        self._client = client if client is not None else KalshiClient(config)
        self._owns_client = client is None

    @staticmethod
    def _validate_asset(asset, quote=None, exchange=None):
        if not isinstance(asset, Asset) or asset.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
            raise ValueError("Kalshi requires an Asset with asset_type='prediction_contract' and a market ticker")
        if not asset.symbol or asset.multiplier != 1:
            raise ValueError("Kalshi prediction contracts require a ticker and multiplier=1")
        if quote is not None and (quote.symbol != "USD" or quote.asset_type != Asset.AssetType.FOREX):
            raise ValueError("Kalshi only supports USD quotes")
        if exchange is not None and str(exchange).upper() != "KALSHI":
            raise ValueError("Kalshi does not support routing to another exchange")
        return asset.symbol

    @staticmethod
    def _number(row, field, *, legacy=None, scale=1):
        value = row.get(field)
        if value is not None:
            return float(decimal_value(value, field))
        if legacy and row.get(legacy) is not None:
            return float(decimal_value(row[legacy], legacy) / scale)
        return None

    def _market(self, asset):
        ticker = self._validate_asset(asset)
        payload = self._client.request("GET", f"/markets/{self._client.path_id(ticker)}", authenticated=False)
        market = payload.get("market")
        if not isinstance(market, dict) or market.get("ticker") != ticker:
            raise KalshiAPIError("Kalshi returned an invalid market response")
        return market

    def get_chains(self, asset, quote=None):
        return {}

    def get_last_price(self, asset, quote=None, exchange=None):
        self._validate_asset(asset, quote, exchange)
        return self._number(self._market(asset), "last_price_dollars", legacy="last_price", scale=100)

    def get_last_prices(self, assets, quote=None, exchange=None):
        # Preserve the existing AssetsMapping contract while keeping one failed
        # ticker from discarding successful reads for the other assets.
        result = {}
        for asset in assets:
            try:
                result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)
            except (KalshiAPIError, ValueError):
                result[asset] = None
                logging.getLogger(__name__).warning("Kalshi last-price read unavailable for a requested asset")
        return AssetsMapping(result)

    def get_quote(self, asset, quote=None, exchange=None):
        self._validate_asset(asset, quote, exchange)
        market = self._market(asset)
        return Quote(
            asset=asset,
            price=self._number(market, "last_price_dollars", legacy="last_price", scale=100),
            bid=self._number(market, "yes_bid_dollars", legacy="yes_bid", scale=100),
            ask=self._number(market, "yes_ask_dollars", legacy="yes_ask", scale=100),
            bid_size=self._number(market, "yes_bid_size_fp"),
            ask_size=self._number(market, "yes_ask_size_fp"),
            volume=self._number(market, "volume_fp", legacy="volume"),
            timestamp=datetime.now(timezone.utc),
            raw_data=market,
        )

    def _candlesticks(self, ticker, start, end, interval):
        params = {"start_ts": start, "end_ts": end, "period_interval": interval}
        try:
            payload = self._client.request(
                "GET",
                "/markets/candlesticks",
                params={**params, "market_tickers": ticker},
                authenticated=False,
            )
        except KalshiAPIError as exc:
            if exc.status_code != 404:
                raise
            payload = {"markets": []}
        markets = payload.get("markets")
        if not isinstance(markets, list):
            raise KalshiAPIError("Kalshi candlestick response is missing markets")
        for market in markets:
            if market.get("market_ticker", market.get("ticker")) == ticker:
                return market["candlesticks"]
        # Archived markets disappear from the live endpoint. A candle's date
        # alone cannot identify archival: Kalshi archives by settlement time.
        historical = self._client.request(
            "GET",
            f"/historical/markets/{self._client.path_id(ticker)}/candlesticks",
            params=params,
            authenticated=False,
        )
        return historical["candlesticks"]

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        **kwargs,
    ):
        ticker = self._validate_asset(asset, quote, exchange)
        if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
            raise ValueError("Kalshi history length must be a positive integer")
        interval = self._INTERVALS.get((timestep or "minute").lower())
        if interval is None:
            raise ValueError("Kalshi historical prices support minute, hour, and day timesteps")
        if timeshift is not None and not isinstance(timeshift, timedelta):
            raise ValueError("Kalshi history timeshift must be a timedelta")
        end_dt = self.get_datetime(adjust_for_delay=True) - (timeshift or timedelta())
        end = int(end_dt.timestamp())
        step = interval * 60
        end = (end // step) * step
        start = end - (length - 1) * step
        rows = []
        raw = []
        # Bound each response below the API's 10,000-candle aggregate limit.
        for window_start in range(start, end + 1, 4999 * step):
            candles = self._candlesticks(ticker, window_start, min(end, window_start + 4998 * step), interval)
            if not isinstance(candles, list):
                raise KalshiAPIError("Kalshi returned invalid candlesticks")
            raw.extend(candles)
            for candle in candles:
                ts = int(candle["end_period_ts"])
                if not start <= ts <= end:
                    continue
                prices = candle.get("price") or {}
                values = {
                    name: self._number(prices, f"{name}_dollars", legacy=name, scale=100)
                    for name in ("open", "high", "low", "close")
                }
                # No trades means null OHLC. Never invent executions from bid/ask
                # candles or carry a prior close forward as a traded candle.
                if any(value is None for value in values.values()):
                    continue
                rows.append(
                    {
                        "datetime": datetime.fromtimestamp(ts, timezone.utc),
                        **values,
                        "volume": self._number(candle, "volume_fp", legacy="volume") or 0,
                    }
                )
        if rows:
            frame = pd.DataFrame(rows).set_index("datetime").sort_index()
            frame = frame[~frame.index.duplicated(keep="last")].tail(length)
        else:
            frame = pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"],
                index=pd.DatetimeIndex([], tz="UTC", name="datetime"),
            )
        return Bars(frame, self.SOURCE, asset, quote=quote, raw=raw)

    # The base get_bars implements plural Strategy history, including per-asset
    # failures. Reuse that dispatcher instead of adding another public method.

    def shutdown(self):
        super().shutdown()
        if self._owns_client:
            self._client.close()
