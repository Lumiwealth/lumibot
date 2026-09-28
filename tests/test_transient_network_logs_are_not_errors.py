import logging
from types import SimpleNamespace

import pytest

from lumibot.brokers.tradier import Tradier, TradierTransientBrokerReadError
from lumibot.strategies._strategy import _Strategy


def _tradier_read_stub(**kwargs):
    values = {
        "_is_transient_broker_read_error": Tradier._is_transient_broker_read_error,
        "_current_lumibot_positions_snapshot": lambda: [],
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def test_update_broker_balances_exception_logs_info(monkeypatch, caplog):
    def raise_balance_error(_quote_asset, _strategy):
        raise ConnectionError("Remote end closed connection without response")

    dummy = SimpleNamespace(
        is_backtesting=False,
        last_broker_balances_update=None,
        _quote_asset=None,
        broker=SimpleNamespace(_get_balances_at_broker=raise_balance_error),
        logger=logging.getLogger("tests.broker_balances"),
    )

    caplog.set_level(logging.DEBUG)
    result = _Strategy.update_broker_balances(dummy, force_update=True)

    assert result is False
    assert any(
        record.levelno == logging.INFO and "Error getting broker balances" in record.getMessage()
        for record in caplog.records
    )
    assert any(
        record.levelno == logging.INFO
        and "Error getting broker balances" in record.getMessage()
        and record.exc_info
        for record in caplog.records
    )
    assert all(record.levelno < logging.ERROR for record in caplog.records)


def test_tradier_pull_orders_exception_logs_info(monkeypatch):
    def raise_orders_error():
        raise ConnectionError("Max retries exceeded")

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(
            orders=SimpleNamespace(get_orders=raise_orders_error),
        )
    )

    # Avoid relying on global logging config (which is frequently mutated across tests and across environments).
    # Instead, patch the module-level logger and assert that the code path logs at INFO with `exc_info=True`.
    import lumibot.brokers.tradier as tradier_module

    info_calls = []
    error_calls = []

    def fake_info(msg, *args, **kwargs):
        info_calls.append((msg, kwargs))

    def fake_error(msg, *args, **kwargs):
        error_calls.append((msg, kwargs))

    monkeypatch.setattr(tradier_module, "logger", SimpleNamespace(info=fake_info, error=fake_error))

    result = Tradier._pull_broker_all_orders(dummy)

    assert result == []
    assert error_calls == []
    assert any("Error pulling orders from Tradier" in msg for msg, _kwargs in info_calls)
    assert any(
        "Error pulling orders from Tradier" in msg and kwargs.get("exc_info") for msg, kwargs in info_calls
    )


def test_tradier_transient_positions_failure_preserves_local_positions(monkeypatch):
    local_position = object()

    def raise_positions_error():
        raise ConnectionError("HTTPSConnectionPool: too many 500 error responses")

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(
            account=SimpleNamespace(get_positions=raise_positions_error),
        ),
        _current_lumibot_positions_snapshot=lambda: [local_position],
    )

    result = Tradier._pull_positions(dummy, "unit-test")

    assert result == [local_position]


def test_tradier_transient_orders_failure_skips_reconciliation(monkeypatch):
    def raise_orders_error():
        raise ConnectionError("HTTPSConnectionPool: too many 500 error responses")

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(
            orders=SimpleNamespace(get_orders=raise_orders_error),
        )
    )

    with pytest.raises(TradierTransientBrokerReadError):
        Tradier._pull_broker_all_orders(dummy)
