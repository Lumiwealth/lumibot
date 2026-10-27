"""Every CCXT market type in one model that matches LumiBot's Bitunix futures broker.

Bitunix (LumiBot's first crypto-futures broker) represents a perpetual as
``Asset(<symbol>, asset_type="crypto_future", leverage=N)``, sets leverage only on
opening orders, and identifies hedge legs with ``position_side``/``position_id``.
The shared CCXT broker follows the same model for swaps and dated futures, and
maps CCXT option markets to LumiBot option assets.
"""

import datetime
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.entities import Asset, Order
from tests.test_ccxt_unified_spot import make_broker  # noqa: F401  (fixture)

EXPIRY = datetime.date(2026, 12, 25)
EXPIRY_MS = int(datetime.datetime(2026, 12, 25, 8, tzinfo=datetime.timezone.utc).timestamp() * 1000)


@pytest.fixture
def every_type(make_broker):
    broker = make_broker("okx")
    spot = broker.api.markets["BTC/USDT"]

    def market(symbol, kind, settle, quote, inverse=False, **extra):
        m = deepcopy(spot)
        m.update(id=symbol.replace("/", "-").replace(":", "-"), symbol=symbol, quote=quote, quoteId=quote,
                 settle=settle, settleId=settle, type=kind, spot=False, swap=kind == "swap",
                 future=kind == "future", option=kind == "option", contract=True, linear=not inverse,
                 inverse=inverse, contractSize=1 if not inverse else 100,
                 **{"expiry": None, "strike": None, "optionType": None, **extra})
        m["precision"] = {"amount": 1, "price": 0.0005 if kind == "option" else 0.5}
        return m

    broker.api.set_markets([
        spot,
        market("BTC/USDT:USDT", "swap", "USDT", "USDT"),
        market("BTC/USDT:USDT-261225", "future", "USDT", "USDT", expiry=EXPIRY_MS),
        market("BTC/USD:BTC-261225-100000-C", "option", "BTC", "USD", inverse=True,
               expiry=EXPIRY_MS, strike=100000, optionType="call"),
        market("BTC/USD:BTC-261225-90000-P", "option", "BTC", "USD", inverse=True,
               expiry=EXPIRY_MS, strike=90000, optionType="put"),
    ])
    broker.api.set_leverage = Mock(return_value={})
    return broker


def test_bitunix_style_symbol_resolves_to_the_perpetual(every_type):
    # Bitunix strategies write the exchange symbol; CCXT resolves it to the unified perp.
    assert every_type.get_market(Asset("BTCUSDT", asset_type="crypto_future"))["symbol"] == "BTC/USDT:USDT"
    assert every_type.get_market(Asset("BTC/USDT:USDT", asset_type="crypto_future"))["type"] == "swap"


def test_dated_future_uses_crypto_future_with_full_symbol(every_type):
    asset = every_type.get_asset("BTC/USDT:USDT-261225")
    assert asset.asset_type == "crypto_future" and asset.symbol == "BTC/USDT:USDT-261225"
    assert every_type.get_market(asset)["future"]


def test_option_market_maps_to_a_lumibot_option_and_back(every_type):
    asset = every_type.get_asset("BTC/USD:BTC-261225-100000-C")
    assert asset.asset_type == "option"
    assert (asset.symbol, asset.expiration, asset.strike, asset.right) == ("BTC", EXPIRY, 100000, "CALL")
    assert every_type.get_market(asset)["symbol"] == "BTC/USD:BTC-261225-100000-C"
    put = Asset("BTC", asset_type="option", expiration=EXPIRY, strike=90000, right="put")
    assert every_type.get_market(put)["symbol"] == "BTC/USD:BTC-261225-90000-P"
    with pytest.raises(ValueError):
        every_type.get_market(Asset("BTC", asset_type="option", expiration=EXPIRY, strike=1, right="call"))


def test_option_order_uses_the_option_symbol_and_contract_count(every_type):
    asset = Asset("BTC", asset_type="option", expiration=EXPIRY, strike=100000, right="call")
    submitted = every_type._submit_order(Order("s", asset, 2, "buy", order_type="limit", limit_price=Decimal("0.05")))
    assert every_type.api.create_order.called, submitted.error_message
    args, _ = every_type.api.create_order.call_args
    assert args[0] == "BTC/USD:BTC-261225-100000-C" and args[1] == "limit" and args[3] == "2"


def test_opening_order_sets_asset_leverage_once_like_bitunix(every_type):
    asset = Asset("BTC/USDT:USDT", asset_type="crypto_future", leverage=5)
    every_type._submit_order(Order("s", asset, 1, "buy"))
    every_type._submit_order(Order("s", asset, 1, "buy"))
    every_type.api.set_leverage.assert_called_once_with(5, "BTC/USDT:USDT", {})


def test_closing_and_default_leverage_never_touch_account_leverage(every_type):
    close = Order("s", Asset("BTC/USDT:USDT", asset_type="crypto_future", leverage=5), 1, "sell")
    close.reduce_only = True
    every_type._submit_order(close)
    every_type._submit_order(Order("s", Asset("BTC/USDT:USDT", asset_type="crypto_future"), 1, "buy"))
    every_type.api.set_leverage.assert_not_called()


def test_leverage_failure_is_logged_and_the_order_still_goes(every_type):
    every_type.api.set_leverage = Mock(side_effect=RuntimeError("leverage not supported"))
    every_type._submit_order(Order("s", Asset("BTC/USDT:USDT", asset_type="crypto_future", leverage=3), 1, "buy"))
    assert every_type.api.create_order.called


def test_positions_cover_every_contract_type_present(every_type):
    every_type.api.fetch_balance = Mock(return_value={"total": {}})
    every_type.api.has = dict(every_type.api.has, fetchPositions=True)
    calls = []

    def fetch_positions(symbols=None, params=None):
        calls.append((params or {}).get("type"))
        rows = {"swap": [{"symbol": "BTC/USDT:USDT", "contracts": 2, "side": "long", "contractSize": 1}],
                "future": [{"symbol": "BTC/USDT:USDT-261225", "contracts": 1, "side": "short", "contractSize": 1}],
                "option": [{"symbol": "BTC/USD:BTC-261225-100000-C", "contracts": 3, "side": "long",
                            "contractSize": 1}]}
        return rows.get((params or {}).get("type"), [])

    every_type.api.fetch_positions = fetch_positions
    positions = every_type._pull_positions(SimpleNamespace(name="s", quote_asset=Asset("USDT", "crypto")))
    assert sorted(calls) == ["future", "option", "swap"]
    by_type = {p.asset.asset_type: p for p in positions}
    assert by_type["crypto_future"] and by_type["option"].asset.strike == 100000
    assert sorted(float(p.quantity) for p in positions) == [-1.0, 2.0, 3.0]


def test_swap_only_account_keeps_the_single_swap_call(make_broker):
    broker = make_broker("bybit")
    spot = broker.api.markets["BTC/USDT"]
    perp = dict(deepcopy(spot), symbol="BTC/USDT:USDT", id="BTCUSDT", type="swap", spot=False, swap=True,
                contract=True, linear=True, settle="USDT", contractSize=1)
    broker.api.set_markets([spot, perp])
    broker.api.fetch_positions = Mock(return_value=[])
    broker._fetch_derivative_positions()
    broker.api.fetch_positions.assert_called_once_with(params={"type": "swap"})
