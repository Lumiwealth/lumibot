from types import SimpleNamespace

import pandas as pd
import pytest
from alpaca.trading.enums import PositionSide

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.entities import Asset, Order
from lumibot.tools.symbol_normalization import (
    build_ccxt_crypto_symbol,
    crypto_ccxt_symbol_candidates,
    normalize_symbol_for_broker,
    normalize_symbol_for_internal,
    parse_crypto_pair_symbol,
    resolve_ccxt_market_symbol,
)


def test_normalize_symbol_for_internal_uses_dot_canonical_format():
    assert normalize_symbol_for_internal("BRK/B") == "BRK.B"
    assert normalize_symbol_for_internal("BRK B") == "BRK.B"
    assert normalize_symbol_for_internal("brk.b") == "BRK.B"


def test_normalize_symbol_for_broker_maps_class_share_separator_by_broker():
    assert normalize_symbol_for_broker("BRK.B", "tradier") == "BRK/B"
    assert normalize_symbol_for_broker("BRK.B", "Schwab") == "BRK/B"
    assert normalize_symbol_for_broker("BRK.B", "interactive_brokers") == "BRK B"
    assert normalize_symbol_for_broker("BRK.B", "alpaca") == "BRK.B"
    assert normalize_symbol_for_broker("BRK.B", "unknown-broker") == "BRK.B"


def test_non_equity_symbols_are_not_rewritten():
    assert normalize_symbol_for_internal("BTC/USD", asset_type=Asset.AssetType.CRYPTO) == "BTC/USD"
    assert normalize_symbol_for_broker("BTC/USD", "tradier", asset_type=Asset.AssetType.CRYPTO) == "BTC/USD"


def test_tradier_pull_positions_normalizes_class_share_symbol():
    broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True, connect_stream=False)
    broker.tradier.account.get_positions = lambda: pd.DataFrame([{"symbol": "BRK/B", "quantity": 3.0}])

    positions = broker._pull_positions("unit_test_strategy")
    assert len(positions) == 1
    assert positions[0].asset.symbol == "BRK.B"


def test_tradier_parse_broker_order_normalizes_class_share_symbol():
    broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True, connect_stream=False)
    response = {
        "id": 123,
        "type": "market",
        "side": "buy",
        "symbol": "BRK/B",
        "class": "equity",
        "quantity": 1,
        "status": "submitted",
        "tag": "unit-test",
        "duration": "day",
        "create_date": "2026-02-25T14:31:39.559Z",
    }

    parsed = broker._parse_broker_order(response, "unit_test_strategy")
    assert parsed.asset.symbol == "BRK.B"


def test_alpaca_parse_broker_position_normalizes_class_share_symbol():
    broker = Alpaca.__new__(Alpaca)
    raw_position = SimpleNamespace(
        asset_class="us_equity",
        symbol="BRK/B",
        qty="3",
        avg_entry_price="500.0",
        unrealized_pl="0.0",
        current_price="500.0",
        side=PositionSide.LONG,
        market_value="1500.0",
    )

    parsed = Alpaca._parse_broker_position(broker, raw_position, "unit_test_strategy")
    assert parsed.asset.symbol == "BRK.B"


def test_alpaca_parse_broker_order_treats_equity_slash_symbol_as_class_share_not_crypto():
    broker = Alpaca.__new__(Alpaca)
    response = {
        "id": "order-1",
        "symbol": "BRK/B",
        "asset_class": "us_equity",
        "qty": "1",
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "status": "new",
        "created_at": "2026-02-25T14:31:39.559Z",
    }

    parsed = Alpaca._parse_broker_order(broker, response, "unit_test_strategy")
    assert parsed.asset.symbol == "BRK.B"
    assert parsed.asset.asset_type == Asset.AssetType.STOCK


def test_schwab_stock_builder_uses_slash_class_share_symbol():
    pytest.importorskip("schwab")
    from lumibot.brokers.schwab import Schwab

    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    order = Order(
        "unit_test_strategy",
        Asset("BRK.B"),
        1,
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
    )

    calls = {}

    def _buy_market(symbol, quantity):
        calls["symbol"] = symbol
        calls["quantity"] = quantity
        return {"symbol": symbol, "quantity": quantity}

    builder = Schwab._prepare_stock_order_builder(
        broker,
        order,
        _buy_market,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
        lambda *args, **kwargs: None,
    )

    assert builder is not None
    assert calls["symbol"] == "BRK/B"
    assert calls["quantity"] == 1


def test_interactive_brokers_converts_dot_symbol_for_contract_and_back_for_positions():
    pytest.importorskip("ibapi")
    from lumibot.brokers.interactive_brokers import IBApp, InteractiveBrokers

    ib_app = IBApp.__new__(IBApp)
    broker = InteractiveBrokers.__new__(InteractiveBrokers)
    broker.name = "interactive_brokers"

    contract = IBApp.create_contract(ib_app, Asset("BRK.B"))
    assert contract.symbol == "BRK B"

    parsed_position = InteractiveBrokers._parse_broker_position(
        broker,
        {
            "asset_type": "stock",
            "symbol": "BRK B",
            "position": 2,
        },
        strategy="unit_test_strategy",
    )
    assert parsed_position.asset.symbol == "BRK.B"


def test_parse_crypto_pair_symbol_accepts_common_forms():
    assert parse_crypto_pair_symbol("BTC/USD") == ("BTC", "USD")
    assert parse_crypto_pair_symbol("btc-usd") == ("BTC", "USD")
    assert parse_crypto_pair_symbol("BTCUSD") == ("BTC", "USD")
    assert parse_crypto_pair_symbol("BTCBUSD") == ("BTC", "BUSD")
    assert parse_crypto_pair_symbol("BTC") == ("BTC", None)
    assert parse_crypto_pair_symbol("") == (None, None)


def test_build_ccxt_crypto_symbol_normalizes_pair_as_base_with_usd_quote():
    # BEFORE: Asset("BTC-USD") + quote USD became BTC-USD/USD and Coinbase returned no market.
    # AFTER: normalize to the CCXT unified USD form BTC/USD.
    assert build_ccxt_crypto_symbol("BTC-USD", "USD") == "BTC/USD"
    assert build_ccxt_crypto_symbol("BTC/USD", "USD") == "BTC/USD"
    assert build_ccxt_crypto_symbol("BTC", "USD") == "BTC/USD"
    assert build_ccxt_crypto_symbol("BTCUSD", "USD") == "BTC/USD"
    assert build_ccxt_crypto_symbol("ETH-EUR", "EUR") == "ETH/EUR"


def test_resolve_ccxt_market_symbol_maps_coinbase_hyphen_and_redundant_pair():
    markets = {"BTC/USD": {"id": "BTC-USD", "symbol": "BTC/USD"}}
    assert resolve_ccxt_market_symbol(markets, "BTC-USD") == "BTC/USD"
    assert resolve_ccxt_market_symbol(markets, "BTC-USD/USD") == "BTC/USD"
    assert resolve_ccxt_market_symbol(markets, "BTC/USD") == "BTC/USD"
    assert resolve_ccxt_market_symbol(markets, "ETH/USD") is None
    assert "BTC/USD" in crypto_ccxt_symbol_candidates("BTC-USD/USD")


def test_coinbase_usdt_candidate_does_not_substitute_usd_quote():
    candidates = crypto_ccxt_symbol_candidates("BTC/USDT", exchange_id="coinbase")
    assert candidates[0] == "BTC/USDT"
    assert "BTC/USD" not in candidates
    assert resolve_ccxt_market_symbol({"BTC/USD": {}}, "BTC/USDT", exchange_id="coinbase") is None


def test_order_pair_normalizes_btc_usd_pair_base():
    from lumibot.entities import Order

    order = Order(
        strategy="unit",
        asset=Asset("BTC-USD", asset_type=Asset.AssetType.CRYPTO),
        quantity=0.001,
        side="buy",
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
    )
    assert order.pair == "BTC/USD"
