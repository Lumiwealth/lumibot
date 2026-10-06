from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from lumibot.data_sources import PandasData
from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset, Data, Order, Quote


class PolymarketBacktesting(PandasData):
    """Backtesting data source for Polymarket prediction contracts.

    Uses Polymarket's real CLOB price-history endpoint as close-price bars and
    keeps prediction-contract prices inside Polymarket's 0..1 price range.
    """

    SOURCE = "POLYMARKET_BACKTESTING"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "hour", "representations": ["1H", "hour"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        *,
        assets: list[Asset] | None = None,
        markets: list[dict] | None = None,
        pandas_data=None,
        polymarket_data: PolymarketData | None = None,
        timestep: str = "minute",
        quote: Asset | None = None,
        **kwargs,
    ):
        self.polymarket_data = polymarket_data or PolymarketData(config=kwargs.get("config"))
        self.quote_asset = quote or Asset("USD", Asset.AssetType.FOREX)
        self._prediction_timestep = timestep
        auto_adjust = kwargs.pop("auto_adjust", False)

        source_data = []
        if pandas_data is not None:
            source_data = pandas_data
        else:
            source_data = self._load_assets(
                assets=assets or [],
                markets=markets or [],
                start=datetime_start,
                end=datetime_end,
                timestep=timestep,
                quote=self.quote_asset,
            )

        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=source_data,
            auto_adjust=auto_adjust,
            **kwargs,
        )
        self.name = "polymarket_backtesting"

    def get_market_metadata(self, market=None, **kwargs) -> dict:
        return self.polymarket_data.get_market_metadata(market, **kwargs)

    def get_market_rules(self, market=None, **kwargs) -> dict:
        return self.polymarket_data.get_market_rules(market, **kwargs)

    def get_resolution_status(self, market=None, **kwargs) -> dict:
        return self.polymarket_data.get_resolution_status(market, **kwargs)

    def get_settlement_price(self, asset: Asset, market=None, **kwargs) -> float | None:
        return self.polymarket_data.get_settlement_price(asset, market=market, **kwargs)

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
        quote = quote or self.quote_asset
        return super().get_historical_prices(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
            **kwargs,
        )

    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        asset = self._coerce_prediction_asset(asset)
        data = self._get_loaded_prediction_data(asset, quote=quote or self.quote_asset)
        if data is None:
            return Quote(asset=asset)

        row, bar_timestamp = self._current_prediction_row(data)
        if row is None:
            return Quote(asset=asset)

        price = self._row_price(row, "close")
        bid = self._row_price(row, "bid")
        ask = self._row_price(row, "ask")
        if bid is None:
            bid = price
        if ask is None:
            ask = price

        return Quote(
            asset=asset,
            price=price,
            bid=bid,
            ask=ask,
            volume=self._row_price(row, "volume"),
            timestamp=self.get_datetime(),
            raw_data={
                "open": self._row_price(row, "open"),
                "high": self._row_price(row, "high"),
                "low": self._row_price(row, "low"),
                "close": price,
                "bid": bid,
                "ask": ask,
                "volume": self._row_price(row, "volume"),
                "bar_timestamp": bar_timestamp,
                "bar_timestep": getattr(data, "timestep", None),
                "source": "polymarket_backtesting",
            },
        )

    def get_last_price(self, asset, quote=None, exchange=None):
        asset = self._coerce_prediction_asset(asset)
        data = self._get_loaded_prediction_data(asset, quote=quote or self.quote_asset)
        if data is None:
            return None

        row, _ = self._current_prediction_row(data)
        if row is None:
            return None
        return self._row_price(row, "close")

    def validate_order(self, order: Order) -> None:
        asset = getattr(order, "asset", None)
        if getattr(asset, "asset_type", None) != Asset.AssetType.PREDICTION_CONTRACT:
            return

        try:
            rules = self.get_market_rules(getattr(asset, "polymarket_market", None) or asset)
        except Exception:
            rules = {}
        min_order_size = rules.get("min_order_size")
        if min_order_size is not None:
            min_size = Decimal(str(min_order_size))
            quantity = Decimal(str(getattr(order, "quantity", "0") or "0"))
            if quantity < min_size:
                raise ValueError(
                    f"Polymarket order size {quantity} is below the market minimum order size {min_size}"
                )

        limit_price = getattr(order, "limit_price", None)
        if limit_price is None:
            return

        price = Decimal(str(limit_price))
        if price < Decimal("0") or price > Decimal("1"):
            raise ValueError(f"Polymarket limit price must be between 0 and 1, got {price}")

        tick_size = rules.get("tick_size")
        if tick_size is None:
            return
        tick = Decimal(str(tick_size))
        if tick <= 0:
            return
        units = price / tick
        if units != units.to_integral_value():
            raise ValueError(f"Polymarket limit price {price} does not conform to market tick size {tick}")

    def _load_assets(
        self,
        *,
        assets: list[Asset],
        markets: list[dict],
        start: datetime,
        end: datetime,
        timestep: str,
        quote: Asset,
    ) -> list[Data]:
        resolved_assets = list(assets)
        market_by_symbol = {}
        for market_spec in markets:
            market = self.polymarket_data.resolve_market(
                url=market_spec.get("url"),
                slug=market_spec.get("slug"),
                market_id=market_spec.get("market_id") or market_spec.get("id"),
                condition_id=market_spec.get("condition_id") or market_spec.get("conditionId"),
            )
            resolved = self.polymarket_data.resolve_contract(
                market,
                outcome=market_spec.get("outcome"),
                token_id=market_spec.get("token_id"),
            )
            resolved_assets.append(resolved)
            market_by_symbol[str(resolved.symbol)] = market

        data_objects = []
        for asset in resolved_assets:
            if asset.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
                raise ValueError("PolymarketBacktesting assets must use asset_type='prediction_contract'")
            bars = self.polymarket_data.get_historical_prices(
                asset,
                length=None,
                timestep=timestep,
                quote=quote,
                start=start,
                end=end,
            )
            df = bars.df.copy()
            market = market_by_symbol.get(str(asset.symbol)) or getattr(asset, "polymarket_market", None)
            df = self._apply_market_close_and_settlement(df, asset, market=market, start=start, end=end)
            self._validate_price_frame(df, asset)
            data_objects.append(Data(asset, df, timestep=self._normalize_timestep(timestep), quote=quote))
        return data_objects

    def _get_loaded_prediction_data(self, asset: Asset, *, quote: Asset):
        for candidate_quote in (quote, self.quote_asset, None):
            tuple_to_find = self.find_asset_in_data_store(asset, candidate_quote)
            if tuple_to_find in self._data_store:
                return self._data_store[tuple_to_find]
        return None

    def _current_prediction_row(self, data):
        try:
            iter_count = data.get_iter_count(self.get_datetime())
        except Exception:
            return None, None
        if iter_count is None or iter_count < 0:
            return None, None
        try:
            row = data.df.iloc[iter_count]
        except Exception:
            return None, None
        try:
            timestamp = data._timestamp_for_iter_count(iter_count)
            timestamp = timestamp.to_pydatetime() if timestamp is not None else None
        except Exception:
            timestamp = None
        return row, timestamp

    @staticmethod
    def _row_price(row, key: str):
        if row is None or key not in row:
            return None
        value = row.get(key)
        if pd.isna(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return value

    @staticmethod
    def _coerce_prediction_asset(asset) -> Asset:
        if isinstance(asset, Asset):
            if asset.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
                raise ValueError("PolymarketBacktesting requires prediction_contract assets")
            return asset
        return Asset(str(asset), asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    def _apply_market_close_and_settlement(
        self,
        df: pd.DataFrame,
        asset: Asset,
        *,
        market: dict | None,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        result = df.copy()
        close_dt = None
        if market is not None:
            try:
                close_dt = self.polymarket_data.get_market_close(market)
            except Exception:
                close_dt = None
        if close_dt is not None:
            close_ts = self._normalize_timestamp(close_dt, result.index)
            result = result[result.index <= close_ts]

        try:
            settlement_price = self.get_settlement_price(asset, market=market)
        except Exception:
            settlement_price = None
        if settlement_price is None:
            return result

        settlement_ts = close_dt or end
        settlement_ts = self._normalize_timestamp(settlement_ts, result.index)
        start_ts = self._normalize_timestamp(start, result.index)
        end_ts = self._normalize_timestamp(end, result.index)
        if settlement_ts < start_ts or settlement_ts > end_ts:
            return result

        settlement_row = {
            "open": settlement_price,
            "high": settlement_price,
            "low": settlement_price,
            "close": settlement_price,
            "volume": 0,
            "bid": settlement_price,
            "ask": settlement_price,
        }
        result.loc[settlement_ts, list(settlement_row.keys())] = list(settlement_row.values())
        return result.sort_index()

    @staticmethod
    def _normalize_timestamp(value, index) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        tz = getattr(index, "tz", None)
        if tz is not None:
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize(tz)
            else:
                timestamp = timestamp.tz_convert(tz)
        elif timestamp.tzinfo is not None:
            timestamp = timestamp.tz_localize(None)
        return timestamp

    @staticmethod
    def _validate_price_frame(df: pd.DataFrame, asset: Asset) -> None:
        if df.empty:
            return
        for column in ("open", "high", "low", "close"):
            if column not in df.columns:
                raise ValueError(f"Polymarket history for {asset} is missing {column} prices")
            values = pd.to_numeric(df[column], errors="coerce")
            if values.isna().any():
                raise ValueError(f"Polymarket history for {asset} contains non-numeric {column} prices")
            if ((values < 0) | (values > 1)).any():
                raise ValueError(f"Polymarket prediction-contract prices must be between 0 and 1 for {asset}")

    @staticmethod
    def _normalize_timestep(timestep: str) -> str:
        normalized = str(timestep or "minute").lower()
        if normalized in {"1m", "minute"}:
            return "minute"
        if normalized in {"1h", "hour"}:
            return "hour"
        if normalized in {"1d", "day"}:
            return "day"
        raise ValueError(f"Unsupported Polymarket backtesting timestep: {timestep!r}")
