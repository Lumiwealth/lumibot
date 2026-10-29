"""Kalshi completed-candle replay using PandasData and BacktestingBroker.

Like PolymarketBacktesting, this loads ordinary Data/Bars. Kalshi candles are
end-labelled and include real quote OHLC, so their availability and fills are
handled without borrowing Polymarket's close-time resolution shortcut.
"""

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

import pandas as pd

from lumibot.data_sources import PandasData
from lumibot.data_sources.kalshi_data import KalshiData
from lumibot.entities import Asset, Bars, Data, Order, Quote


class KalshiBacktesting(PandasData):
    """Selected YES/NO candle replay; depth and queue fills remain estimates."""

    SOURCE = "KALSHI_BACKTESTING"

    def __init__(
        self,
        datetime_start,
        datetime_end,
        *,
        assets=None,
        pandas_data=None,
        kalshi_data=None,
        market_metadata=None,
        timestep="minute",
        quote=None,
        fee_rate="0.07",
        maker_fee_rate="0",
        balance_precision="0.0001",
        config=None,
        **kwargs,
    ):
        self.kalshi_data = kalshi_data if kalshi_data is not None else KalshiData(config=config)
        self._owns_kalshi_data = kalshi_data is None
        self.quote_asset = quote or Asset("USD", "forex")
        self._prediction_period, _ = KalshiData._history_period(timestep)
        self.fee_rate = Decimal(str(fee_rate))
        self.maker_fee_rate = Decimal(str(maker_fee_rate))
        self.balance_precision = Decimal(str(balance_precision))
        if self.fee_rate < 0 or self.balance_precision <= 0:
            raise ValueError("Backtest fee rate must be nonnegative and balance precision positive")
        self._market_metadata = dict(market_metadata or {})
        self._frames = {}
        loaded = list(pandas_data.values()) if isinstance(pandas_data, dict) else list(pandas_data or [])
        for asset in assets or []:
            self.kalshi_data._validate_asset(asset, self.quote_asset)
            bars = self.kalshi_data.get_historical_prices(asset, None, timestep, start=datetime_start, end=datetime_end)
            self._frames[asset] = bars.df.copy()
            if asset.symbol not in self._market_metadata:
                self._market_metadata[asset.symbol] = self.kalshi_data.get_market_metadata(asset)
            if bars.df.empty:
                continue
            native = (
                "day"
                if self._prediction_period % 1440 == 0
                else "hour"
                if self._prediction_period % 60 == 0
                else "minute"
            )
            loaded.append(Data(asset, bars.df, timestep=native, quote=self.quote_asset))
        for item in loaded:
            asset = item.asset[0] if isinstance(item.asset, tuple) else item.asset
            self.kalshi_data._validate_asset(asset, self.quote_asset)
            self._frames.setdefault(asset, item.df.copy().sort_index())
            if asset.symbol not in self._market_metadata:
                self._market_metadata[asset.symbol] = self.kalshi_data.get_market_metadata(asset)
        kwargs.setdefault("market", "24/7")
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=loaded,
            auto_adjust=kwargs.pop("auto_adjust", False),
            **kwargs,
        )
        self.name = "kalshi_backtesting"
        self.load_data()

    def load_data(self):
        # Reuse PandasData's store/calendar; do not forward-fill absent candles
        # or turn their empty trade prices into fabricated executions.
        self._data_store = self.pandas_data
        index = pd.DatetimeIndex([self.datetime_start, self.datetime_end])
        for frame in self._frames.values():
            index = index.union(frame.index)
        for row in self._market_metadata.values():
            if row.get("settlement_ts"):
                index = index.union(pd.DatetimeIndex([KalshiData._utc(row["settlement_ts"])]))
        index = index.tz_convert(self.datetime_start.tzinfo).sort_values().unique()
        self._date_index = index[(index >= self.datetime_start) & (index <= self.datetime_end)]
        self._timestep = (
            "day" if self._prediction_period % 1440 == 0 else "hour" if self._prediction_period % 60 == 0 else "minute"
        )
        self._find_asset_in_data_store_cache.clear()
        return self.get_trading_days_pandas()

    def _available(self, asset, end=None):
        KalshiData._validate_asset(asset)
        frame = self._frames.get(asset)
        if frame is None:
            return pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"], index=pd.DatetimeIndex([], tz="UTC")
            )
        end = KalshiData._utc(end if end is not None else self.get_datetime())
        return frame[frame.index <= end]

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, **kwargs
    ):
        period, _ = KalshiData._history_period(timestep or f"{self._prediction_period}min")
        now = end = KalshiData._utc(self.get_datetime())
        if timeshift is not None:
            end -= (
                pd.Timedelta(minutes=period * timeshift)
                if isinstance(timeshift, (int, float))
                else pd.Timedelta(timeshift)
            )
        # A negative offset cannot make a preloaded future candle observable.
        end = min(end, now)
        frame = self._available(asset, end).copy()
        if period > self._prediction_period and not frame.empty:
            aggregation = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": lambda values: values.sum(min_count=1),
            }
            aggregation.update({k: "last" for k in ("bid", "ask", "open_interest") if k in frame})
            frame = frame.resample(f"{period}min", closed="right", label="right").agg(aggregation)
            frame = frame[frame.index <= end].dropna(how="all")
        if length is not None:
            frame = frame.tail(int(length))
        return Bars(frame, self.SOURCE, asset, quote=quote or self.quote_asset)

    @staticmethod
    def _number(row, key):
        value = row.get(key)
        return None if value is None or pd.isna(value) else float(value)

    def get_last_price(self, asset, quote=None, exchange=None):
        frame = self._available(asset)
        prices = frame["close"].dropna()
        return float(prices.iloc[-1]) if len(prices) else None

    def get_quote(self, asset, quote=None, exchange=None):
        frame = self._available(asset)
        if frame.empty:
            return Quote(asset)
        row, stamp = frame.iloc[-1], frame.index[-1]
        return Quote(
            asset,
            price=self.get_last_price(asset),
            bid=self._number(row, "bid"),
            ask=self._number(row, "ask"),
            volume=self._number(row, "volume"),
            timestamp=stamp,
            quote_time=stamp,
            bid_time=stamp,
            ask_time=stamp,
            raw_data={
                "bar_timestamp": stamp,
                "bar_timestep": self._timestep,
                "source": self.SOURCE,
                "liquidity_model": "historical candle close; depth unavailable",
            },
        )

    def get_market_metadata(self, market=None, **kwargs):
        ticker = market.symbol if isinstance(market, Asset) else market
        row = dict(self._market_metadata.get(ticker) or {})
        now = KalshiData._utc(self.get_datetime())
        settled = row.get("settlement_ts") and KalshiData._utc(row["settlement_ts"]) <= now
        for key in list(row):
            if (
                "price" in key
                or "bid" in key
                or "ask" in key
                or key.startswith("volume")
                or key.startswith("open_interest")
            ):
                row.pop(key)
        if not settled:
            for key in ("result", "settlement_value_dollars", "settlement_ts", "expiration_value"):
                row.pop(key, None)
        row["status"] = (
            "settled"
            if settled
            else "closed"
            if row.get("close_time") and now >= KalshiData._utc(row["close_time"])
            else "active"
        )
        return row

    def get_resolution_status(self, market=None, **kwargs):
        row = self.get_market_metadata(market)
        return {
            "resolved": row.get("status") == "settled",
            "status": row.get("status", "unknown"),
            "winner": row.get("result"),
            "settlement_price": row.get("settlement_value_dollars"),
        }

    def get_prediction_contract_opposite(self, asset):
        if asset.asset_type != "prediction_contract":
            return None
        return Asset.from_dict({**asset.to_dict(), "outcome": "no" if asset.outcome == "yes" else "yes"})

    def validate_order(self, order):
        KalshiData._validate_asset(order.asset, order.quote, order.exchange)
        quantity = Decimal(str(order.quantity))
        if quantity <= 0 or quantity % Decimal(".01"):
            raise ValueError("Kalshi contract quantity must be positive in increments of .01")
        if order.order_type not in (Order.OrderType.MARKET, Order.OrderType.LIMIT):
            raise ValueError("Kalshi has no native stop/trailing/compound order at this endpoint")
        if order.limit_price is not None and not 0 < order.limit_price < 1:
            raise ValueError("Kalshi order limit price must be between zero and one")
        if str(order.time_in_force).lower() == "gtd" and order.good_till_date is None:
            raise ValueError("Kalshi GTD requires good_till_date")
        options = order.custom_params or {}
        if (getattr(order, "reduce_only", False) or options.get("reduce_only")) and str(order.time_in_force).lower() != "ioc":
            raise ValueError("Kalshi reduce_only requires time_in_force='ioc'")
        if options.get("post_only"):
            if order.order_type != Order.OrderType.LIMIT:
                raise ValueError("Kalshi post_only requires a limit order")
            quote = self.get_quote(order.asset)
            crossed = (order.is_buy_order() and quote.ask is not None and order.limit_price >= quote.ask) or (
                order.is_sell_order() and quote.bid is not None and order.limit_price <= quote.bid
            )
            if crossed:
                raise ValueError("Kalshi post_only order would cross the historical quote")

    def get_backtest_fill_price(self, order):
        metadata = self._market_metadata.get(order.asset.symbol, {})
        if metadata.get("close_time") and KalshiData._utc(self.get_datetime()) >= KalshiData._utc(
            metadata["close_time"]
        ):
            return None
        frame = self._available(order.asset)
        if frame.empty or KalshiData._utc(frame.index[-1]) < KalshiData._utc(self.get_datetime()).floor(
            f"{self._prediction_period}min"
        ):
            return None
        price = self._number(frame.iloc[-1], "ask" if order.is_buy_order() else "bid")
        if price is None or not 0 < price < 1:
            return None
        if order.order_type == Order.OrderType.LIMIT:
            if (order.is_buy_order() and order.limit_price < price) or (
                order.is_sell_order() and order.limit_price > price
            ):
                return None
        return order.limit_price if (order.custom_params or {}).get("post_only") else price

    def get_backtest_fill_quantity(self, order, broker):
        if not (getattr(order, "reduce_only", False) or (order.custom_params or {}).get("reduce_only")):
            return order.quantity
        closing_asset = order.asset if order.is_sell_order() else self.get_prediction_contract_opposite(order.asset)
        position = broker.get_tracked_position(order.strategy, closing_asset)
        return min(order.quantity, max(0, position.quantity) if position is not None else 0)

    def calculate_backtest_trade_cost(self, order, price, quantity=None):
        quantity, price = Decimal(str(order.quantity if quantity is None else quantity)), Decimal(str(price))
        rate = self.maker_fee_rate if (order.custom_params or {}).get("post_only") else self.fee_rate
        model_fee = rate * quantity * price * (1 - price)
        trade_fee = model_fee.quantize(Decimal(".000001"), rounding=ROUND_CEILING)
        revenue = quantity * price * (-1 if order.is_buy_order() else 1)
        aligned = ((revenue - trade_fee) / self.balance_precision).to_integral_value(
            rounding=ROUND_FLOOR
        ) * self.balance_precision
        return revenue - aligned

    def process_backtest_lifecycle(self, broker, strategy):
        now = KalshiData._utc(self.get_datetime())
        for order in list(broker.get_active_tracked_orders(strategy.name)):
            row = self._market_metadata.get(order.asset.symbol, {})
            if row.get("close_time") and now >= KalshiData._utc(row["close_time"]):
                broker.cancel_order(order)
        for position in list(broker.get_tracked_positions(strategy.name)):
            if position.asset.asset_type != "prediction_contract":
                continue
            row = self._market_metadata.get(position.asset.symbol, {})
            if not row.get("settlement_ts") or now < KalshiData._utc(row["settlement_ts"]):
                continue
            value = row.get("settlement_value_dollars")
            if value is None and row.get("result") in ("yes", "no"):
                value = 1 if row["result"] == "yes" else 0
            if value is None:
                continue
            value = Decimal(str(value))
            value = value if position.asset.outcome == "yes" else Decimal(1) - value
            if not 0 <= value <= 1:
                raise ValueError("Kalshi settlement payout must be between zero and one")
            payout = float(Decimal(str(position.quantity)) * value)
            strategy._set_cash_position(strategy.cash + payout)
            strategy._record_backtest_cash_event(
                event_type="other_cash",
                amount=payout,
                raw_type="settlement",
                raw_subtype="prediction_settlement",
                description=f"Settlement of {position.asset}",
                is_external_cash_flow=False,
            )
            order = Order(strategy.name, position.asset, position.quantity, "sell", quote=self.quote_asset)
            broker._process_trade_event(order, broker.CASH_SETTLED, price=value, filled_quantity=position.quantity)

    def shutdown(self):
        super().shutdown()
        if self._owns_kalshi_data:
            self.kalshi_data.shutdown()
