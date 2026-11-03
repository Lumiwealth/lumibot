"""The opt-in API harness must not conceal lifecycle or account-selection bugs."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lumibot.entities import Asset, Order
from tests import test_smart_limit_live_alpaca as harness


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
