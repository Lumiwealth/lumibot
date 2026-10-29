"""Account foundation tests; account arithmetic is pure logic, not a demo pass."""

from decimal import Decimal
from unittest.mock import Mock

import pytest

from lumibot.brokers import Broker, Kalshi
from lumibot.brokers.kalshi import _parse_balances
from lumibot.data_sources import DataSource, KalshiData
from lumibot.entities import Asset
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient


@pytest.mark.parametrize(
    "row,expected",
    [
        ({"balance": 10000, "balance_dollars": "100.0000", "portfolio_value": 5000}, (100, 50, 150)),
        ({"balance": 10000, "balance_dollars": "100.0000", "portfolio_value": 0}, (100, 0, 100)),
        ({"balance": 0, "balance_dollars": "0.0000", "portfolio_value": 0}, (0, 0, 0)),
        ({"balance": 10000, "portfolio_value": 2500}, (100, 25, 125)),
        (
            {"balance": 10000, "balance_dollars": "100.1234", "portfolio_value": 2500},
            (Decimal("100.1234"), Decimal("25"), Decimal("125.1234")),
        ),
    ],
)
def test_balance_arithmetic_preserves_cash_and_adds_holdings(row, expected):
    # Current authenticated response semantics supersede the older changelog description.
    assert _parse_balances(row) == tuple(Decimal(str(v)) for v in expected)


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"balance_dollars": "1"},
        {"balance_dollars": "NaN", "portfolio_value": 100},
        {"balance_dollars": "0", "portfolio_value": "Infinity"},
    ],
)
def test_bad_snapshot_never_becomes_zero_account(row):
    with pytest.raises(KalshiAPIError):
        _parse_balances(row)


def test_skeleton_reuses_existing_broker_and_data_source():
    client = Mock(spec=KalshiClient)
    broker = Kalshi(client=client)
    assert isinstance(broker, Broker)
    assert isinstance(broker.data_source, (KalshiData, DataSource))
    assert broker.data_source._client is client
    assert broker._orders_thread is None
    client.request.assert_not_called()
    broker.close()
    client.close.assert_not_called()


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, {}),
        ({"SUBACCOUNT": 0}, {"subaccount": 0}),
        ({"SUBACCOUNT": 7, "EXCHANGE_INDEX": 2}, {"subaccount": 7, "exchange_index": 2}),
    ],
)
def test_account_scope_is_explicit_or_left_to_provider_binding(config, expected):
    client = Mock(spec=KalshiClient)
    client.request.side_effect = KalshiAPIError("test request boundary")
    broker = Kalshi(config, client=client)
    try:
        with pytest.raises(KalshiAPIError, match="test request boundary"):
            broker._get_balances_at_broker(Asset("USD", asset_type="forex"), None)
        client.request.assert_called_once_with("GET", "/portfolio/balance", params=expected)
    finally:
        broker.close()


def test_explicit_client_wins_and_custom_data_source_is_preserved():
    explicit, configured, source = Mock(spec=KalshiClient), Mock(spec=KalshiClient), Mock(spec=DataSource)
    broker = Kalshi({"CLIENT": configured}, client=explicit, data_source=source)
    assert broker._client is explicit
    assert broker.data_source is source
    broker.close()
    explicit.close.assert_not_called()
    configured.close.assert_not_called()


def test_config_client_is_reused_without_direct_credentials():
    client = Mock(spec=KalshiClient)
    broker = Kalshi({"CLIENT": client})
    assert broker._client is client
    broker.close()


def test_trading_rejects_an_ambiguous_prediction_contract_without_sending():
    # Trading is now implemented; the account-only placeholder is replaced by
    # the actual instrument boundary, using the existing Order/Asset entities.
    from lumibot.entities import Order

    client = Mock(spec=KalshiClient)
    broker = Kalshi(client=client)
    try:
        with pytest.raises(ValueError, match="outcome"):
            broker.submit_order(Order("neutral", Asset("EVENT", "prediction_contract"), 1, "buy", limit_price=.2))
        client.request.assert_not_called()
    finally:
        broker.close()


@pytest.mark.parametrize("source_name", [None, "kalshi"])
def test_existing_credentials_factory_constructs_kalshi_and_shares_client(monkeypatch, source_name):
    import lumibot.credentials as credentials

    client = Mock(spec=KalshiClient)
    monkeypatch.setattr(credentials, "trading_broker_name", "kalshi")
    monkeypatch.setattr(credentials, "data_source_name", source_name)
    monkeypatch.setattr(credentials, "KALSHI_CONFIG", {"CLIENT": client})
    broker, source = credentials._build_default_live_credentials()
    try:
        assert isinstance(broker, Kalshi)
        assert broker.data_source is source
        assert source._client is client
        client.request.assert_not_called()
    finally:
        broker.close()


def test_owned_client_closes_once(monkeypatch):
    client = Mock(spec=KalshiClient)
    monkeypatch.setattr("lumibot.brokers.kalshi.KalshiClient", lambda *a, **k: client)
    broker = Kalshi()
    broker.close()
    broker.close()
    client.close.assert_called_once_with()


def test_standard_strategy_cash_and_portfolio_methods_work_without_order_threads(monkeypatch):
    from lumibot.strategies import Strategy

    class AccountCheck(Strategy):
        def on_trading_iteration(self):
            pass

    broker = Kalshi(client=Mock(spec=KalshiClient))
    # This isolates LumiBot lifecycle wiring; provider wire parsing has separate
    # pure arithmetic tests and the authenticated API check requires real keys.
    monkeypatch.setattr(broker, "_get_balances_at_broker", lambda *a, **k: (100.0, 50.0, 150.0))
    try:
        strategy = AccountCheck(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
        assert strategy.update_broker_balances()
        assert strategy.get_cash() == 100
        assert strategy.get_portfolio_value() == 150
        assert broker._orders_thread is None
    finally:
        broker.close()


def test_kalshi_api_marker_does_not_require_other_provider_keys(monkeypatch):
    from types import SimpleNamespace

    from tests.conftest import pytest_runtest_setup

    for name in ("POLYGON_API_KEY", "THETADATA_USERNAME", "THETADATA_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    item = SimpleNamespace(get_closest_marker=lambda name: object() if name in {"apitest", "kalshi"} else None)
    pytest_runtest_setup(item)


@pytest.mark.parametrize("lazy", ["true", "false"])
def test_eager_and_lazy_environment_credentials_select_kalshi(lazy):
    import os
    import subprocess
    import sys

    env = {key: value for key, value in os.environ.items() if not key.startswith("KALSHI_")}
    env.update(
        LUMIBOT_DISABLE_DOTENV="1",
        BACKTESTING_DATA_SOURCE="none",
        TRADING_BROKER="kalshi",
        LUMIBOT_LAZY_CREDENTIALS=lazy,
        DATA_SOURCE="kalshi",
        IS_BACKTESTING="false",
    )
    code = """
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
key = ed25519.Ed25519PrivateKey.generate()
os.environ["KALSHI_API_KEY_ID"] = "synthetic-offline"
os.environ["KALSHI_PRIVATE_KEY"] = key.private_bytes(serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
import lumibot.credentials as credentials
broker = credentials.BROKER
assert broker.name == "Kalshi"
assert credentials.DATA_SOURCE._client is broker._client
assert broker._orders_thread is None
broker.close()
"""
    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_recorded_demo_flat_balance_matches_independent_website_total():
    import json
    from pathlib import Path

    # Authenticated demo response recorded 2026-10-26; timestamp neutralized.
    # The independent website showed cash=100, holdings=0, portfolio=100.
    # The older changelog's total-equity description does not match this API.
    row = json.loads((Path(__file__).parent / "fixtures/kalshi/balance_flat.json").read_text())
    assert _parse_balances(row) == (Decimal("100"), Decimal("0"), Decimal("100"))
