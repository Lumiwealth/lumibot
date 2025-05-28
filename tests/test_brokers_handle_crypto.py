import pytest
from datetime import datetime, timedelta

import pytz
from lumibot.entities import Asset, Order, Bars
from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting, YahooDataBacktesting, CcxtBacktesting
from lumibot.brokers.alpaca import Alpaca
from lumibot.credentials import ALPACA_TEST_CONFIG, POLYGON_CONFIG


class TestBrokerHandlesCrypto:

    length = 5
    base = Asset(symbol='BTC', asset_type='crypto')
    quote = Asset(symbol='USD', asset_type='forex')
    timestep = "day"
    start = datetime(2019, 3, 1)
    end = datetime(2019, 3, 3)

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_yahoo_backtesting_with_symbol(self):
        data_source = YahooDataBacktesting(datetime_start=self.start, datetime_end=self.end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # test_get_last_price
        asset = Asset(symbol='BTC-USD')
        last_price = broker.get_last_price(asset)
        assert isinstance(last_price, float)
        assert last_price > 0.0

        # test_get_historical_prices
        bars = broker.data_source.get_historical_prices(
            asset=asset,
            length=self.length,
            timestep=self.timestep
        )

        assert isinstance(bars, Bars)
        assert len(bars.df) == self.length
        # get the date of the last bar, which should be the day before the start date
        last_date = bars.df.index[-1]
        assert last_date.date() == (self.start - timedelta(days=1)).date()
        last_price = bars.df['close'].iloc[-1]
        assert last_price > 0.0

        # test_submit_limit_order
        limit_price = 1.0  # Make sure we never hit this price
        order = Order(
            strategy="test",
            asset=asset,
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=limit_price,
        )
        assert order.status == "unprocessed"
        broker.submit_order(order)
        assert order.status == "new"
        broker.cancel_order(order)

    @pytest.mark.skipif(
        not POLYGON_CONFIG["API_KEY"],
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        not POLYGON_CONFIG["IS_PAID_SUBSCRIPTION"],
        reason="This test requires a paid Polygon.io API key"
    )
    def test_polygon_backtesting_with_base_and_quote(self):
        # Expensive polygon subscriptions required if we go back to 2019. Just use recent dates.
        start = datetime.now() - timedelta(days=4)
        end = datetime.now() - timedelta(days=2)

        data_source = PolygonDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            api_key=POLYGON_CONFIG["API_KEY"]
        )
        broker = BacktestingBroker(data_source=data_source)

        # test_get_last_price
        last_price = broker.data_source.get_last_price(asset=self.base, quote=self.quote)
        assert isinstance(last_price, float)
        assert last_price > 0.0

        # test_get_historical_prices
        bars = broker.data_source.get_historical_prices(
            asset=self.base,
            length=self.length,
            timestep=self.timestep,
            quote=self.quote
        )

        assert isinstance(bars, Bars)
        assert len(bars.df) == self.length
        # get the date of the last bar, which should be the day before the start date
        last_date = bars.df.index[-1]
        assert last_date.date() == (start - timedelta(days=1)).date()
        last_price = bars.df['close'].iloc[-1]
        assert last_price > 0.0

        # test_submit_limit_order
        limit_price = 1.0  # Make sure we never hit this price
        order = Order(
            strategy="test",
            asset=self.base,
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=limit_price,
            quote=self.quote
        )
        assert order.status == "unprocessed"
        broker.submit_order(order)
        assert order.status == "new"
        broker.cancel_order(order)

    @pytest.mark.xfail(reason="need to handle github timezone")
    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_broker_with_base_and_quote(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)

        # test_get_last_price
        last_price = broker.data_source.get_last_price(asset=self.base, quote=self.quote)
        assert isinstance(last_price, float)
        assert last_price > 0.0

        # test_get_historical_prices
        bars = broker.data_source.get_historical_prices(
            asset=self.base,
            length=self.length,
            timestep=self.timestep,
            quote=self.quote
        )

        assert isinstance(bars, Bars)
        assert len(bars.df) == self.length
        # get the date of the last bar, which should be the day before the start date
        last_date = bars.df.index[-1]
        assert last_date.date() == datetime.now(pytz.timezone("America/New_York")).date()
        last_price = bars.df['close'].iloc[-1]
        assert last_price > 0.0

        # test_submit_limit_order
        limit_price = 100.0  # Make sure we never hit this price
        order = Order(
            strategy="test",
            asset=self.base,
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=limit_price,
            quote=self.quote
        )
        assert order.status == "unprocessed"
        broker.submit_order(order)
        assert order.status == "new"
        broker.cancel_order(order)

    def test_ccxt_backtesting_with_base_and_quote(self):
        start = (datetime.now() - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        kwargs = {
            # "max_data_download_limit":10000, # optional
            "exchange_id": "kraken"  #"kucoin" #"bybit" #"okx" #"bitmex" # "binance"
        }
        data_source = CcxtBacktesting(
            datetime_start=start,
            datetime_end=end,
            **kwargs
        )
        broker = BacktestingBroker(data_source=data_source)

        # test_get_last_price
        last_price = broker.data_source.get_last_price(asset=self.base, quote=self.quote)
        assert isinstance(last_price, float)
        assert last_price > 0.0

        # test_get_historical_prices
        bars = broker.data_source.get_historical_prices(
            asset=self.base,
            length=self.length,
            timestep=self.timestep,
            quote=self.quote
        )

        assert isinstance(bars, Bars)
        assert len(bars.df) == self.length
        # get the date of the last bar, which should be the day before the start date
        last_date = bars.df.index[-1]
        assert last_date.date() == (start - timedelta(days=1)).date()
        last_price = bars.df['close'].iloc[-1]
        assert last_price > 0.0

        # test_submit_limit_order
        limit_price = 1.0  # Make sure we never hit this price
        order = Order(
            strategy="test",
            asset=self.base,
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=limit_price,
            quote=self.quote
        )
        assert order.status == "unprocessed"
        broker.submit_order(order)
        assert order.status == "new"
        broker.cancel_order(order)
