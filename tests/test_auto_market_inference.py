import types

class DummyDataSource:
    def __init__(self, cls_name):
        self.__class__.__name__ = cls_name  # dynamic spoof of class name
    def get_datetime(self):
        import datetime
        return datetime.datetime.utcnow()

from lumibot.brokers.broker import Broker

class DummyBroker(Broker):
    IS_BACKTESTING_BROKER = True  # avoid starting threads
    def _get_stream_object(self): return None
    def _register_stream_events(self): return None
    def _run_stream(self): return None
    def cancel_order(self, order): pass
    def _modify_order(self, order, limit_price=None, stop_price=None): pass
    def _submit_order(self, order): pass
    def _get_balances_at_broker(self, quote_asset, strategy): return (0,0,0)
    def get_historical_account_value(self): return {}
    def _pull_positions(self, strategy): return []
    def _pull_position(self, strategy, asset): return None
    def _parse_broker_order(self, response, strategy_name, strategy_object=None): return None
    def _pull_broker_order(self, identifier): return None
    def _pull_broker_all_orders(self): return []


def test_projectx_sets_us_futures():
    ds = DummyDataSource('ProjectXData')
    b = DummyBroker(data_source=ds)
    assert b.market == 'us_futures'


def test_tradovate_sets_us_futures():
    ds = DummyDataSource('TradovateData')
    b = DummyBroker(data_source=ds)
    assert b.market == 'us_futures'


def test_ccxt_sets_24_7():
    ds = DummyDataSource('CcxtData')
    b = DummyBroker(data_source=ds)
    assert b.market == '24/7'


def test_no_change_if_configured():
    ds = DummyDataSource('ProjectXData')
    b = DummyBroker(data_source=ds, config={'MARKET':'CUSTOM'})
    assert b.market == 'CUSTOM'
