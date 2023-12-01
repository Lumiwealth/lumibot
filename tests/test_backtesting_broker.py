import datetime

from lumibot.backtesting import BacktestingBroker
from lumibot.data_sources import PandasData


class TestBacktestingBroker:
    def test_limit_fills(self):
        start = datetime.datetime(2023, 8, 1)
        end = datetime.datetime(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Limit triggered by candle body
        limit_price = 105
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit triggered by candle wick
        limit_price = 109
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit Sell Triggered by a gap up candle
        limit_price = 85
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == 100

        # Limit Buy Triggered by a gap down candle
        limit_price = 115
        assert broker.limit_order(limit_price, 'buy', open_=100, high=110, low=90) == 100

        # Limit not triggered
        limit_price = 120
        assert not broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90)

    def test_stop_fills(self):
        start = datetime.datetime(2023, 8, 1)
        end = datetime.datetime(2023, 8, 2)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Stop triggered by candle body
        stop_price = 95
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop triggered by candle wick
        stop_price = 91
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop Sell Triggered by a gap down candle
        stop_price = 115
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == 100

        # Stop Buy Triggered by a gap up candle
        stop_price = 85
        assert broker.stop_order(stop_price, 'buy', open_=100, high=110, low=90) == 100

        # Stop not triggered
        stop_price = 80
        assert not broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90)
