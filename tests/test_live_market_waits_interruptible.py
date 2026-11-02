"""A strategy stop releases live market waits without waiting for the next bell."""
import logging
import threading
from types import SimpleNamespace

import pytest

import lumibot.brokers.alpaca as alpaca_module
from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.broker import Broker


WAITS = [Broker._await_market_to_open, Broker._await_market_to_close, Alpaca._await_market_to_close]
INTERVAL = 23_400


def broker_for_wait(monkeypatch, method, sleep):
    broker = object.__new__(Alpaca)
    broker.logger = logging.getLogger('market-wait-test')
    broker.is_market_open = lambda: method is not Broker._await_market_to_open
    broker.get_time_to_open = lambda: INTERVAL
    broker.get_time_to_close = lambda: INTERVAL
    broker.process_pending_orders = lambda **kwargs: None
    broker.sleep = sleep
    monkeypatch.setattr(alpaca_module, 'time', SimpleNamespace(sleep=sleep), raising=False)
    return broker


def forbidden_sleep(seconds):
    raise AssertionError('A strategy stop must release the market wait through its stop event')


@pytest.mark.parametrize('method', WAITS, ids=['open', 'close', 'alpaca-close'])
def test_already_stopped_strategy_does_not_sleep_until_the_bell(monkeypatch, method):
    broker = broker_for_wait(monkeypatch, method, forbidden_sleep)
    stopped = threading.Event()
    stopped.set()
    method(broker, strategy=SimpleNamespace(_executor=SimpleNamespace(stop_event=stopped)))


@pytest.mark.parametrize('method', WAITS, ids=['open', 'close', 'alpaca-close'])
def test_stop_during_a_market_wait_releases_its_waiter(monkeypatch, method):
    broker = broker_for_wait(monkeypatch, method, forbidden_sleep)
    entered = threading.Event()
    errors = []
    observations = []

    class ObservedStop(threading.Event):
        def wait(self, timeout=None):
            observations.append(timeout)
            entered.set()
            return super().wait(timeout)

    stopped = ObservedStop()
    strategy = SimpleNamespace(_executor=SimpleNamespace(stop_event=stopped))

    def wait_for_market():
        try:
            method(broker, strategy=strategy)
        except BaseException as exc:
            errors.append(exc)
        finally:
            entered.set()

    worker = threading.Thread(target=wait_for_market)
    worker.start()
    entered.wait()
    stopped.set()
    worker.join()
    assert not errors
    assert observations == [INTERVAL]


@pytest.mark.parametrize('method', WAITS, ids=['open', 'close', 'alpaca-close'])
def test_direct_callers_keep_their_normal_market_interval(monkeypatch, method):
    intervals = []
    broker = broker_for_wait(monkeypatch, method, intervals.append)
    method(broker, strategy=None)
    assert intervals == [INTERVAL]
