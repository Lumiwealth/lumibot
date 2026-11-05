"""The opt-in API harness must not conceal lifecycle or account-selection bugs."""

from types import SimpleNamespace
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from lumibot.entities import Asset, Order
from tests import test_smart_limit_live_alpaca as harness
from tests import test_smart_limit_live_matrix_alpaca as matrix


def test_alpaca_harness_binds_quote_data_to_the_dedicated_account(monkeypatch):
    config = {"API_KEY": "isolated-test-key", "API_SECRET": "synthetic"}
    monkeypatch.setattr(harness, "ALPACA_TEST_CONFIG", config)
    source = object()
    built = {}

    def data_source(selected):
        assert selected == {**config, "PAPER": True}
        return source

    def broker(selected, **kwargs):
        built.update(kwargs)
        api = MagicMock()
        api.get_account.return_value.trading_blocked = False
        api.get_all_positions.return_value = []
        api.get_orders.return_value = []
        return SimpleNamespace(api=api)

    monkeypatch.setattr(harness, "AlpacaData", data_source, raising=False)
    monkeypatch.setattr(harness, "Alpaca", broker)
    harness._alpaca()
    assert built["data_source"] is source


def test_matrix_uses_the_same_safe_account_and_normal_reconciliation_path():
    # No fallback account and no copied provider status may return in a second
    # API-test helper while the smoke helper exercises the real lifecycle.
    assert matrix._alpaca is harness._alpaca
    assert matrix._wait_fill is harness._wait_fill


def test_invalid_dedicated_credentials_never_fall_back_to_default_account(monkeypatch):
    attempts = []

    def broker(config, **kwargs):
        attempts.append(config["API_KEY"])
        api = MagicMock()
        if config["API_KEY"] == "isolated-test-key":
            api.get_account.side_effect = RuntimeError("401 unauthorized")
            api.get_all_positions.side_effect = RuntimeError("401 unauthorized")
        else:
            api.get_all_positions.return_value = []
        return SimpleNamespace(api=api)

    monkeypatch.setattr(harness, "ALPACA_TEST_CONFIG", {"API_KEY": "isolated-test-key", "API_SECRET": "synthetic"})
    monkeypatch.setattr(harness, "ALPACA_CONFIG", {"API_KEY": "default-account-key", "API_SECRET": "synthetic"}, raising=False)
    monkeypatch.setattr(harness, "Alpaca", broker)
    with pytest.raises(RuntimeError, match="401"):
        harness._alpaca()
    assert attempts == ["isolated-test-key"]


def test_wait_fill_uses_normal_refresh_without_copying_status_into_local_order():
    original = Order("neutral", Asset("SYN"), 1, "buy", status="new", identifier="owned")
    observed = Order("neutral", Asset("SYN"), 1, "buy", status="fill", identifier="owned", limit_price=100)
    api = MagicMock()
    api.get_order_by_id.return_value = SimpleNamespace(status="filled", limit_price=100)
    strategy = SimpleNamespace(broker=SimpleNamespace(api=api), get_order=MagicMock(return_value=observed))

    filled, _, _ = harness._wait_fill(strategy, original, timeout=1, drive_smart_limit=False)

    assert filled
    strategy.get_order.assert_called_once_with("owned", broker_refresh=True, broker_refresh_ttl_seconds=1)
    assert original.status == "new"


@pytest.mark.parametrize("status", ["partially_filled", "pending_cancel", "pending_replace"])
def test_close_recovery_requires_broker_terminal_confirmation(status):
    api = MagicMock()
    api.get_order_by_id.return_value = SimpleNamespace(id="owned", status=status, qty="0.001", filled_qty="0.0008")
    broker = SimpleNamespace(api=api)
    with pytest.raises(AssertionError, match="confirmed cancellation"):
        harness._confirmed_crypto_remainder(broker, SimpleNamespace(identifier="owned"), Decimal("0.001"))
    api.get_all_positions.assert_not_called()


def test_close_recovery_bounds_the_remaining_quantity_and_rejects_active_orders():
    # Shapes and decimal strings mirror a real canceled, partially filled API
    # response and its remaining position; ids are synthetic.
    api = MagicMock()
    api.get_order_by_id.return_value = SimpleNamespace(id="owned", status="canceled", qty="0.0009985", filled_qty="0.000890007")
    api.get_all_positions.return_value = [SimpleNamespace(symbol="BTCUSD", qty="0.000108493", qty_available="0.000108493")]
    api.get_orders.return_value = []
    broker = SimpleNamespace(api=api)
    assert harness._confirmed_crypto_remainder(broker, SimpleNamespace(identifier="owned"), Decimal("0.0009985")) == Decimal("0.000108493")
    api.get_orders.return_value = [SimpleNamespace(id="still-working")]
    with pytest.raises(AssertionError, match="active orders"):
        harness._confirmed_crypto_remainder(broker, SimpleNamespace(identifier="owned"), Decimal("0.0009985"))


def test_close_recovery_does_not_sell_more_than_the_confirmed_unfilled_remainder():
    api = MagicMock()
    api.get_order_by_id.return_value = SimpleNamespace(id="owned", status="canceled", qty="1", filled_qty="0.8")
    api.get_all_positions.return_value = [SimpleNamespace(symbol="BTCUSD", qty="0.5", qty_available="0.5")]
    api.get_orders.return_value = []
    assert harness._confirmed_crypto_remainder(SimpleNamespace(api=api), SimpleNamespace(identifier="owned"), Decimal("1")) == Decimal("0.2")
