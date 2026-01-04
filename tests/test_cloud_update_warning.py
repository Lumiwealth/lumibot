import logging
from types import SimpleNamespace

import requests

from lumibot.strategies._strategy import _Strategy


def test_send_update_to_cloud_connection_error_logs_warning(monkeypatch, caplog):
    dummy = SimpleNamespace(
        is_backtesting=False,
        lumiwealth_api_key="test_key_123",
        _logged_missing_lumiwealth_api_key=False,
        _name="DummyStrategy",
        broker=SimpleNamespace(name="DummyBroker"),
        logger=logging.getLogger("tests.cloud_update"),
    )

    dummy.get_portfolio_value = lambda: 100.0
    dummy.get_cash = lambda: 100.0
    dummy.get_positions = lambda: []
    dummy.get_orders = lambda: []

    def raise_connection_error(*_args, **_kwargs):
        raise requests.exceptions.ConnectionError("Connection reset by peer")

    monkeypatch.setattr(requests, "post", raise_connection_error)

    caplog.set_level(logging.DEBUG)
    result = _Strategy.send_update_to_cloud(dummy)

    assert result is False
    assert any(
        record.levelno == logging.WARNING
        and "Connection error when sending to cloud" in record.getMessage()
        for record in caplog.records
    )
    assert all(record.levelno < logging.ERROR for record in caplog.records)


def test_send_update_to_cloud_timeout_logs_warning(monkeypatch, caplog):
    dummy = SimpleNamespace(
        is_backtesting=False,
        lumiwealth_api_key="test_key_123",
        _logged_missing_lumiwealth_api_key=False,
        _name="DummyStrategy",
        broker=SimpleNamespace(name="DummyBroker"),
        logger=logging.getLogger("tests.cloud_update"),
    )

    dummy.get_portfolio_value = lambda: 100.0
    dummy.get_cash = lambda: 100.0
    dummy.get_positions = lambda: []
    dummy.get_orders = lambda: []

    def raise_timeout(*_args, **_kwargs):
        raise requests.exceptions.Timeout("timeout")

    monkeypatch.setattr(requests, "post", raise_timeout)

    caplog.set_level(logging.DEBUG)
    result = _Strategy.send_update_to_cloud(dummy)

    assert result is False
    assert any(
        record.levelno == logging.WARNING
        and "Timeout error when sending to cloud" in record.getMessage()
        for record in caplog.records
    )
    assert all(record.levelno < logging.ERROR for record in caplog.records)

