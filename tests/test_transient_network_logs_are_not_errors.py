import logging
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.brokers.tradier import Tradier, TradierTransientBrokerReadError
from lumibot.strategies._strategy import _Strategy


class _TradierReadStub(SimpleNamespace):
    _DEFAULT_READ_MIN_INTERVAL_SECONDS = Tradier._DEFAULT_READ_MIN_INTERVAL_SECONDS
    _DEFAULT_TRANSIENT_READ_BACKOFF_SECONDS = Tradier._DEFAULT_TRANSIENT_READ_BACKOFF_SECONDS
    _ensure_tradier_read_control_state = Tradier._ensure_tradier_read_control_state
    _get_cached_tradier_read = Tradier._get_cached_tradier_read
    _tradier_read_is_in_backoff = Tradier._tradier_read_is_in_backoff
    _cache_tradier_read = Tradier._cache_tradier_read
    _start_tradier_read_backoff = Tradier._start_tradier_read_backoff
    _copy_tradier_read_value = staticmethod(Tradier._copy_tradier_read_value)
    _clean_order_records = staticmethod(Tradier._clean_order_records)

    def _is_transient_broker_read_error(self, error: Exception) -> bool:
        return Tradier._is_transient_broker_read_error(error)

    def _current_lumibot_positions_snapshot(self):
        return []

    def _normalize_symbol_for_internal(self, symbol, asset_type=None):
        return str(symbol)


def _tradier_read_stub(**kwargs):
    values = {
        "_tradier_read_cache": {},
        "_tradier_read_backoff_until": {},
        "_tradier_read_min_interval_seconds": 0.0,
        "_tradier_transient_read_backoff_seconds": 30.0,
    }
    values.update(kwargs)
    return _TradierReadStub(**values)


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


def test_tradier_transient_balances_failure_reuses_cache_and_backs_off(monkeypatch):
    calls = {"count": 0}

    def get_balance():
        calls["count"] += 1
        if calls["count"] == 1:
            return pd.DataFrame([{"total_equity": 1200.0, "total_cash": 1000.0}])
        raise ConnectionError("HTTPSConnectionPool: too many 500 error responses")

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(account=SimpleNamespace(get_account_balance=get_balance)),
        _tradier_read_min_interval_seconds=0.0,
        _tradier_transient_read_backoff_seconds=60.0,
    )

    first_result = Tradier._get_balances_at_broker(dummy, None, None)
    second_result = Tradier._get_balances_at_broker(dummy, None, None)
    third_result = Tradier._get_balances_at_broker(dummy, None, None)

    assert first_result == (1000.0, 200.0, 1200.0)
    assert second_result == first_result
    assert third_result == first_result
    assert calls["count"] == 2


def test_tradier_cached_position_read_avoids_provider_call_during_min_interval(monkeypatch):
    calls = {"count": 0}

    def get_positions():
        calls["count"] += 1
        return pd.DataFrame([{"symbol": "SPY", "quantity": 3}])

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(account=SimpleNamespace(get_positions=get_positions)),
        _tradier_read_min_interval_seconds=60.0,
    )

    first_result = Tradier._pull_positions(dummy, "unit-test")
    second_result = Tradier._pull_positions(dummy, "unit-test")

    assert calls["count"] == 1
    assert len(first_result) == 1
    assert len(second_result) == 1
    assert first_result[0].asset.symbol == second_result[0].asset.symbol == "SPY"


def test_tradier_orders_backoff_uses_cache_without_provider_call(monkeypatch):
    calls = {"count": 0}

    def get_orders():
        calls["count"] += 1
        if calls["count"] == 1:
            return pd.DataFrame([{"id": "abc", "status": "open"}])
        raise ConnectionError("HTTPSConnectionPool: too many 500 error responses")

    dummy = _tradier_read_stub(
        tradier=SimpleNamespace(orders=SimpleNamespace(get_orders=get_orders)),
        _tradier_read_min_interval_seconds=0.0,
        _tradier_transient_read_backoff_seconds=60.0,
    )

    first_result = Tradier._pull_broker_all_orders(dummy)
    second_result = Tradier._pull_broker_all_orders(dummy)
    third_result = Tradier._pull_broker_all_orders(dummy)

    assert first_result == [{"id": "abc", "status": "open"}]
    assert second_result == first_result
    assert third_result == first_result
    assert calls["count"] == 2
