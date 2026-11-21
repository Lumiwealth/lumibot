"""Smallest order and quantity step for an instrument, from the exchange's market info.

A chart ticket that defaulted to 1 asked for 1 whole BTC on Phemex BTC/USDT:USDT
(about 68,000 dollars). Readers use these rules to start at the smallest valid
order instead. Offline: markets are set, never loaded from the network.
"""

from decimal import Decimal

import ccxt
import pytest

from lumibot.brokers.ccxt import Ccxt
from lumibot.entities import Asset


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")


def _market(**overrides):
    market = {
        "id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT",
        "baseId": "BTC", "quoteId": "USDT", "settle": "USDT", "settleId": "USDT",
        "type": "swap", "spot": False, "swap": True, "future": False, "option": False,
        "contract": True, "linear": True, "inverse": False, "contractSize": 1.0,
        "active": True, "precision": {"amount": 0.001, "price": 0.1},
        "limits": {"amount": {"min": None, "max": None}, "price": {"min": None, "max": None},
                   "cost": {"min": None, "max": None}, "leverage": {"min": None, "max": None}},
        "info": {},
    }
    market.update(overrides)
    return market


def _broker(exchange_id, market):
    broker = Ccxt({"exchange_id": exchange_id, "apiKey": "k", "secret": "s", "sandbox": False},
                  connect_stream=False, start_orders_thread=False)
    broker.api.set_markets([market])
    broker.data_source._markets_loaded = True
    return broker


def test_phemex_perpetual_step_is_the_minimum_when_no_minimum_is_published():
    # Phemex publishes BTC/USDT:USDT with amount precision 0.001 and no amount minimum.
    broker = _broker("phemex", _market())
    assert broker.api.precisionMode == ccxt.TICK_SIZE

    rules = broker.get_order_size_rules(Asset("BTC/USDT:USDT", asset_type="crypto_future"))

    assert rules == {"min_quantity": Decimal("0.001"), "quantity_step": Decimal("0.001"),
                     "contract_size": Decimal("1"), "quantity_unit": "contracts"}


def test_published_minimum_wins_and_a_split_pair_resolves_the_same_market():
    market = _market(limits={"amount": {"min": 0.01, "max": None}, "price": {"min": None, "max": None},
                             "cost": {"min": None, "max": None}, "leverage": {"min": None, "max": None}})
    broker = _broker("phemex", market)

    rules = broker.get_order_size_rules(Asset("BTC", asset_type="crypto"), Asset("USDT:USDT", asset_type="crypto"))

    assert rules["min_quantity"] == Decimal("0.01")
    assert rules["quantity_step"] == Decimal("0.001")


def test_decimal_places_precision_becomes_a_step():
    market = _market(id="BTCUSDT", symbol="BTC/USDT", type="spot", spot=True, swap=False, contract=False,
                     linear=None, inverse=None, settle=None, settleId=None, contractSize=None,
                     precision={"amount": 4, "price": 2})
    broker = _broker("kraken", market)
    # Few exchanges still publish decimal places (ccxt 4.5: foxbit); the mode is what matters.
    broker.api.precisionMode = ccxt.DECIMAL_PLACES

    rules = broker.get_order_size_rules(Asset("BTC", asset_type="crypto"), Asset("USDT", asset_type="crypto"))

    assert rules == {"min_quantity": Decimal("0.0001"), "quantity_step": Decimal("0.0001"),
                     "contract_size": None, "quantity_unit": "base"}


def test_base_broker_has_no_rules():
    from lumibot.brokers.broker import Broker

    assert Broker.get_order_size_rules(object(), Asset("SPY")) is None
