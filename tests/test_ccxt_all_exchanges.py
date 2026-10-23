"""Every CCXT exchange is reachable through the same generic path.

Offline only: exchange classes are constructed, never connected. Orders go to a
stubbed ``create_order``. Before this change LumiBot's environment-based broker
selection knew only Coinbase, Kraken and WEEX, so ``TRADING_BROKER=okx`` (or any
of the other ~100 CCXT exchanges) logged "Unknown trading broker name".
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import ccxt
import pytest

from lumibot import credentials
from lumibot.brokers.ccxt import Ccxt
from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order
from lumibot.tools import ccxt_exchange_catalog as catalog

ALL_EXCHANGES = list(ccxt.exchanges)
SPOT_EXCHANGES = [eid for eid in ALL_EXCHANGES if getattr(ccxt, eid)().has.get("spot")]


@pytest.fixture(autouse=True)
def _no_telemetry(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")


def _env_with_required(exchange_id):
    return {
        catalog.credential_env_key(exchange_id, field): f"test-{field}"
        for field in catalog.required_credential_fields(exchange_id)
    }


@pytest.mark.parametrize("exchange_id", ALL_EXCHANGES)
def test_every_ccxt_exchange_builds_from_environment_and_constructs(exchange_id):
    env = _env_with_required(exchange_id)
    config = catalog.build_ccxt_config_from_env(exchange_id, env)

    catalog.validate_ccxt_config(exchange_id, config)
    source = CcxtData(config)

    assert source.api.id == exchange_id
    for field in catalog.required_credential_fields(exchange_id):
        assert getattr(source.api, field) == f"test-{field}"
    assert catalog.is_generic_ccxt_broker_name(exchange_id) is (exchange_id not in catalog.NATIVE_BROKER_NAMES)


@pytest.mark.parametrize("exchange_id", SPOT_EXCHANGES)
def test_every_spot_exchange_submits_a_unified_limit_order(exchange_id):
    broker = Ccxt({"exchange_id": exchange_id, "sandbox": False},
                  connect_stream=False, start_orders_thread=False)
    # Precision is expressed in the exchange's own CCXT precision mode.
    amount, price = {ccxt.TICK_SIZE: (0.0001, 0.01), ccxt.DECIMAL_PLACES: (4, 2),
                     ccxt.SIGNIFICANT_DIGITS: (8, 5)}[broker.api.precisionMode]
    broker.api.set_markets([{
        "id": "BTCUSDT", "symbol": "BTC/USDT", "base": "BTC", "quote": "USDT",
        "baseId": "BTC", "quoteId": "USDT", "settle": None, "settleId": None,
        "type": "spot", "spot": True, "swap": False, "future": False,
        "option": False, "contract": False, "linear": False, "inverse": False,
        "active": True, "precision": {"amount": amount, "price": price},
        "limits": {"amount": {"min": None, "max": None},
                   "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None}}, "info": {},
    }])
    broker.data_source._markets_loaded = True
    broker.api.create_order = Mock(return_value={"id": "fake-order", "status": "open"})
    submitted = Order("test-strategy", Asset("BTC", asset_type="crypto"), Decimal("1"), "buy",
                      limit_price=Decimal("20000"), quote=Asset("USDT", asset_type="crypto"))

    broker._submit_order(submitted)

    assert submitted.identifier == "fake-order", submitted.error_message
    args, _ = broker.api.create_order.call_args
    assert args[:3] == ("BTC/USDT", "limit", "buy")


@pytest.mark.parametrize("exchange_id,expected", [
    ("okx", ["OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"]),
    ("kucoin", ["KUCOIN_API_KEY", "KUCOIN_API_SECRET", "KUCOIN_API_PASSPHRASE"]),
    ("hyperliquid", ["HYPERLIQUID_PRIVATE_KEY", "HYPERLIQUID_WALLET_ADDRESS"]),
    ("binance", ["BINANCE_API_KEY", "BINANCE_API_SECRET"]),
    ("kraken", ["KRAKEN_API_KEY", "KRAKEN_API_SECRET"]),
    ("weex", ["WEEX_API_KEY", "WEEX_API_SECRET", "WEEX_API_PASSPHRASE"]),
])
def test_required_fields_come_from_ccxt_and_match_existing_names(exchange_id, expected):
    keys = [catalog.credential_env_key(exchange_id, f) for f in catalog.required_credential_fields(exchange_id)]
    assert keys == expected


def test_coinbase_keeps_its_historical_variable_names():
    config = catalog.build_ccxt_config_from_env(
        "coinbase", {"COINBASE_API_KEY_NAME": "name", "COINBASE_PRIVATE_KEY": "pem"})
    assert config["apiKey"] == "name" and config["secret"] == "pem"


def test_missing_credentials_name_the_exact_variables():
    config = catalog.build_ccxt_config_from_env("okx", {"OKX_API_KEY": "k"})
    with pytest.raises(ValueError, match="OKX_API_SECRET, OKX_API_PASSPHRASE"):
        catalog.validate_ccxt_config("okx", config)


def test_sandbox_uses_testnet_when_ccxt_has_one_and_says_so_when_not():
    env = {**_env_with_required("okx"), "OKX_SANDBOX": "true"}
    config = catalog.build_ccxt_config_from_env("okx", env)
    catalog.validate_ccxt_config("okx", config)
    assert CcxtData(config).api.urls["api"] == ccxt.okx().urls["test"]

    kraken = catalog.build_ccxt_config_from_env(
        "kraken", {**_env_with_required("kraken"), "KRAKEN_SANDBOX": "true"})
    with pytest.raises(ValueError, match="no testnet"):
        catalog.validate_ccxt_config("kraken", kraken)


@pytest.mark.parametrize("name,expected", [
    ("OKX", "okx"), ("ccxt:alpaca", "alpaca"), ("ccxt_binanceusdm", "binanceusdm"),
    ("not-an-exchange", None), ("", None),
])
def test_exchange_name_resolution(name, expected):
    assert catalog.resolve_ccxt_exchange_id(name) == expected


@pytest.mark.parametrize("name", ["alpaca", "tradier", "schwab", "bitunix", "tradovate", "ib"])
def test_native_broker_names_keep_priority(name):
    assert catalog.is_generic_ccxt_broker_name(name) is False


def test_trading_broker_env_routes_any_exchange_to_ccxt(monkeypatch):
    for key, value in _env_with_required("okx").items():
        monkeypatch.setenv(key, value)
    built = []
    monkeypatch.setattr(credentials, "trading_broker_name", "okx")
    monkeypatch.setattr(credentials, "data_source_name", "binance")
    monkeypatch.setattr(credentials, "_broker_class",
                        lambda name: (lambda config, **kw: built.append((name, config)) or SimpleNamespace(name="ccxt")))

    broker, data_source = credentials._build_default_live_credentials()

    assert built[0][0] == "Ccxt"
    assert built[0][1]["exchange_id"] == "okx"
    assert built[0][1]["password"] == "test-password"
    assert isinstance(data_source, CcxtData) and data_source.api.id == "binance"


def test_trading_broker_env_reports_missing_credentials(monkeypatch):
    monkeypatch.delenv("BYBIT_API_KEY", raising=False)
    monkeypatch.delenv("BYBIT_API_SECRET", raising=False)
    monkeypatch.setattr(credentials, "trading_broker_name", "bybit")
    monkeypatch.setattr(credentials, "data_source_name", None)
    with pytest.raises(ValueError, match="BYBIT_API_KEY, BYBIT_API_SECRET"):
        credentials._build_default_live_credentials()


def test_fake_exchange_added_by_a_future_ccxt_release_works_unchanged(monkeypatch):
    class fakeexchange(ccxt.Exchange):  # noqa: N801 - CCXT ids are lowercase
        id = "fakeexchange"
        name = "Fake Exchange"

        def describe(self):
            return self.deep_extend(super().describe(), {
                "id": "fakeexchange", "name": "Fake Exchange",
                "requiredCredentials": {"apiKey": True, "secret": True, "uid": True},
                "has": {"spot": True, "createOrder": True, "fetchBalance": True, "fetchOHLCV": False},
                "urls": {"api": {"public": "https://fake.invalid"}},
            })

    monkeypatch.setattr(ccxt, "fakeexchange", fakeexchange, raising=False)
    monkeypatch.setattr(ccxt, "exchanges", [*ccxt.exchanges, "fakeexchange"])

    env = {"FAKEEXCHANGE_API_KEY": "k", "FAKEEXCHANGE_API_SECRET": "s", "FAKEEXCHANGE_UID": "u"}
    config = catalog.build_ccxt_config_from_env("fakeexchange", env)
    catalog.validate_ccxt_config("fakeexchange", config)
    entry = catalog.exchange_entry("fakeexchange")

    assert [f["key"] for f in entry["credentialFields"]] == list(env)
    assert entry["supportLevel"] == "trading_no_history"
    source = CcxtData(config)
    source._markets_loaded = True
    source.api.markets = {"BTC/USDT": {"symbol": "BTC/USDT"}}
    with pytest.raises(NotImplementedError, match="does not provide historical candles"):
        source.get_barset_from_api(source.api, "BTC/USDT", "1m")


def test_catalog_lists_every_installed_exchange_with_capabilities():
    data = catalog.build_catalog()
    assert [e["id"] for e in data["exchanges"]] == ALL_EXCHANGES
    for entry in data["exchanges"]:
        assert entry["supportLevel"] in {"trading", "trading_no_history", "data_only"}
        assert all(f["key"].startswith(entry["envPrefix"] + "_") for f in entry["credentialFields"])
        assert "countries" not in entry
