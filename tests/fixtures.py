from decimal import Decimal
from typing import List, Any
from datetime import datetime, timedelta, time
import pytz

import pytest
import logging
import datetime
from pathlib import Path

import pandas as pd

from lumibot import LUMIBOT_SOURCE_PATH
from lumibot.data_sources import DataSource
from lumibot.entities import Data, Asset, Bars
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.strategies import Strategy
from lumibot.tools.helpers import (
    get_trading_days,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def polygon_data_backtesting():
    datetime_start = datetime.datetime(2023, 1, 1)
    datetime_end = datetime.datetime(2023, 2, 1)
    api_key = "fake_api_key"
    pandas_data = []
    
    polygon_data_instance = PolygonDataBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        pandas_data=pandas_data,
        api_key=api_key,
    )
    
    return polygon_data_instance


@pytest.fixture(scope="function")
def pandas_data_fixture():
    """
    Get a dictionary of Lumibot Data objects from the test data in tests/data folder
    """
    pandas_data = []
    symbols = ["SPY", "TLT", "GLD"]
    quote = Asset(symbol='USD', asset_type="forex")

    lumibot_git_dir = Path(LUMIBOT_SOURCE_PATH).parent
    data_dir = lumibot_git_dir / "data"

    for symbol in symbols:
        csv_path = data_dir / f"{symbol}.csv"
        asset = Asset(
            symbol=symbol,
            asset_type="stock",
        )

        df = pd.read_csv(
            csv_path,
            parse_dates=True,
            index_col=0,
            header=0,
        )

        df = df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividend",
            }
        )
        df = df[["open", "high", "low", "close", "volume", "dividend"]]
        df.index.name = "datetime"

        data = Data(
            asset,
            df,
            date_start=datetime.datetime(2019, 1, 2),
            date_end=datetime.datetime(2019, 12, 31),
            timestep="day",
            quote=quote,
        )
        pandas_data.append(data)
    return pandas_data



class BacktestingTestStrategy(Strategy):

    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.asset = self.parameters.get("asset", Asset('AMZN'))
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.lookback_timestep = self.parameters.get("lookback_timestep", "day")
        self.lookback_length = self.parameters.get("lookback_length", 0)

        self.last_prices: dict[str, Decimal] = {}
        self.historical_prices: dict[str, pd.DataFrame] = {}
        self.order_tracker: dict = {}

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"AlpacaBacktestTestStrategy: Filled Order: {order}")
        self.order_tracker["filled_at"] = self.get_datetime()
        self.order_tracker["avg_fill_price"] = order.avg_fill_price

    def on_new_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: New Order: {order}")
        self.order_tracker["submitted_at"] = self.get_datetime()

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self):
        now = self.get_datetime()
        current_asset_price = self.get_last_price(self.asset)
        self.last_prices[now.isoformat()] = current_asset_price

        if self.lookback_length > 0:
            bars = self.get_historical_prices(
                asset=self.asset,
                length=self.lookback_length,
                timestep=self.lookback_timestep,
                quote=self.quote_asset
            )
            self.historical_prices[now.isoformat()] = bars.df

        if len(self.order_tracker) == 0:

            if not current_asset_price:
                return

            # Buy 1 shares of the asset for the test
            qty = 1
            self.log_message(f"Buying {qty} shares of {self.asset} at {current_asset_price} @ {now}")
            order = self.create_order(self.asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order)
            self.order_tracker = {
                "symbol": self.asset.symbol,
                "iteration_at": now,
                "last_price": current_asset_price,
                "order_id": submitted_order.identifier,
            }

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


class BaseDataSourceTester:

    def _create_data_source(self) -> DataSource:
        raise NotImplementedError()

    def check_get_last_price(self, data_source, asset, quote=None, exchange=None) -> None:
        price = data_source.get_last_price(asset=asset, quote=quote, exchange=exchange)
        assert price is not None
        assert isinstance(price, float)

    # noinspection PyMethodMayBeStatic
    def check_length(self, bars: Bars, length: int) -> None:
        assert len(bars.df) == abs(length)

    # noinspection PyMethodMayBeStatic
    def check_index(self, bars: Bars, data_source_tz: pytz.tzinfo = None) -> None:
        assert isinstance(bars.df.index[-1], pd.Timestamp)
        assert bars.df.index.name in ['timestamp', 'date', 'datetime', 'Date']

        if data_source_tz:
            assert bars.df.index[-1].tzinfo.zone == data_source_tz.zone

    # noinspection PyMethodMayBeStatic
    def check_columns(self, bars: Bars) -> None:
        expected_columns = ['open', 'high', 'low', 'close', 'volume']
        for column in expected_columns:
            assert column in bars.df.columns

        assert bars.df["return"].iloc[-1] is not None

    # noinspection PyMethodMayBeStatic
    def check_daily_bars(
            self,
            *,
            bars: Bars,
            now: datetime,
            data_source_tz: pytz.tzinfo = None,
            time_check: time | None = None,
            market: str = 'NYSE',
            remove_incomplete_current_bar: bool = False,
    ):
        assert bars.df.index[-1] <= now
        timestamp = bars.df.index[-1]
        assert timestamp.hour == time_check.hour
        assert timestamp.minute == time_check.minute

        today = now.date()
        trading_days = get_trading_days(
            market=market,
            start_date=today - timedelta(days=7),
            end_date=today + timedelta(days=1),
            tzinfo=data_source_tz
        )

        if today in list(trading_days.index.date):
            market_open = trading_days.loc[str(today), 'market_open']
            market_close = trading_days.loc[str(today), 'market_close']

            if market_open <= now <= market_close:
                # Only check during market hours since that when strategies run.
                if remove_incomplete_current_bar:
                    # Check that the last bar is the latest complete bar, which
                    # is the bar from the previous trading date.
                    assert bars.df.index[-1].date() == trading_days.index[-2].date()
                else:
                    # Check that the last bar is the bar from today.
                    assert bars.df.index[-1].date() == trading_days.index[-1].date()

    # noinspection PyMethodMayBeStatic
    def check_minute_bars(
            self,
            *,
            bars: Bars,
            now: datetime,
            data_source_tz: pytz.tzinfo = None,
            market: str = 'NYSE',
            remove_incomplete_current_bar: bool = False,
    ):
        assert bars.df.index[-1] <= now

        today = now.date()
        trading_days = get_trading_days(
            market=market,
            start_date=today - timedelta(days=7),
            end_date=today + timedelta(days=1),
            tzinfo=data_source_tz
        )

        if today in list(trading_days.index.date):
            market_open = trading_days.loc[str(today), 'market_open']
            market_close = trading_days.loc[str(today), 'market_close']

            if market_open <= now <= market_close:
                # Only check during market hours since that when strategies run.
                if remove_incomplete_current_bar:
                    # Check that the last bar is the latest complete bar, which
                    # is some bar before the current minute's bar. We're not guaranteed
                    # to have bars every minute because trades don't always happen every minute.
                    assert bars.df.index[-1] < now.replace(second=0, microsecond=0)
                else:
                    # Check that the last bar is perhaps the current minutes bar.
                    # We're not guaranteed to have bars every minute because trades don't always happen every minute.
                    assert bars.df.index[-1] <= now.replace(second=0, microsecond=0)
