"""Index weekly option roots (SPX -> SPXW) for Tradier and Schwab symbol builders."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from lumibot.entities import Asset

WEEKLY = date(2026, 11, 13)
MONTHLY = date(2026, 11, 20)  # third Friday


def _opt(symbol, expiration, right="put", strike=7555):
    return Asset(symbol, asset_type="option", expiration=expiration, strike=strike, right=right)


def test_tradier_broker_symbol_uses_weekly_root():
    from lumibot.brokers.tradier import _create_options_symbol

    assert _create_options_symbol("SPX", WEEKLY, "put", 7555).startswith("SPXW261113P")
    assert _create_options_symbol("SPX", MONTHLY, "put", 7555).startswith("SPX261120P")
    assert _create_options_symbol("XSP", WEEKLY, "put", 555).startswith("XSP261113P")
    assert _create_options_symbol("SPY", WEEKLY, "put", 555).startswith("SPY261113P")


def test_tradier_data_symbol_uses_weekly_root():
    from lumibot.data_sources.tradier_data import _create_options_symbol

    assert _create_options_symbol("SPX", WEEKLY, "put", 7555).startswith("SPXW261113P")
    assert _create_options_symbol("SPX", MONTHLY, "call", 7555).startswith("SPX261120C")


def test_tradier_position_under_spxw_maps_to_spx():
    from lumibot.brokers.tradier import Tradier

    broker = Tradier.__new__(Tradier)
    asset = Tradier._asset_from_tradier_row(broker, {"symbol": "SPXW261113P07555000"})
    assert asset.symbol == "SPX"
    assert asset.asset_type == "option"
    assert asset.expiration == WEEKLY
    assert float(asset.strike) == 7555


def test_tradier_index_chains_include_all_roots():
    from lumibot.data_sources.tradier_data import TradierData

    ds = TradierData.__new__(TradierData)
    market = MagicMock()
    market.get_option_expirations.return_value = pd.DataFrame(
        {"date": [pd.Timestamp(WEEKLY)], "strikes": [[7550.0, 7555.0]], "contract_size": [100]}
    )
    ds.tradier = SimpleNamespace(market=market)
    try:
        TradierData.get_chains(ds, Asset("SPX", asset_type="index"))
    except Exception:
        pass
    assert market.get_option_expirations.call_args.kwargs.get("include_all_roots") is True


def test_schwab_data_symbol_uses_weekly_root():
    from lumibot.data_sources.schwab_data import _schwab_option_symbol

    assert _schwab_option_symbol(_opt("SPX", WEEKLY)) == "SPXW  261113P07555000"
    assert _schwab_option_symbol(_opt("SPX", MONTHLY)) == "SPX   261120P07555000"
    assert _schwab_option_symbol(_opt("XSP", WEEKLY, strike=555)) == "XSP   261113P00555000"


def test_schwab_position_under_spxw_maps_to_spx():
    from lumibot.brokers.schwab import Schwab

    broker = Schwab.__new__(Schwab)
    asset = Schwab._asset_from_schwab_instrument(
        broker,
        {"symbol": "SPXW  261113P07555000", "assetType": "OPTION"},
        "OPTION",
        {},
        context="test",
    )
    assert asset.symbol == "SPX"
    assert asset.expiration == WEEKLY


def test_tradier_non_index_and_monthly_symbols_unchanged():
    """Regression guard: everything except index weeklies is byte-identical to the old builder."""
    from lumibot.brokers.tradier import _create_options_symbol as broker_sym
    from lumibot.data_sources.tradier_data import _create_options_symbol as data_sym
    from lumibot.tools.helpers import create_options_symbol as old

    cases = [("SPY", WEEKLY, "put", 555), ("AAPL", WEEKLY, "call", 210.5), ("QQQ", MONTHLY, "put", 480),
             ("SPX", MONTHLY, "put", 7555), ("SPX", MONTHLY, "call", 7555.5), ("XSP", WEEKLY, "call", 555)]
    for case in cases:
        assert broker_sym(*case) == old(*case)
        assert data_sym(*case) == old(*case)


def _schwab_order_symbol(asset):
    from schwab.orders.options import OptionSymbol

    from lumibot.brokers.schwab import Schwab
    from lumibot.entities import Order

    broker = Schwab.__new__(Schwab)
    broker.resolve_option_order_intent = lambda order: None
    broker._normalize_symbol_for_broker = lambda s, asset_type=None: s
    order = MagicMock()
    order.asset = asset
    order.quantity = 1
    order.limit_price = None
    order.side = Order.OrderSide.BUY_TO_OPEN
    order.order_type = Order.OrderType.MARKET
    seen = []
    capture = lambda symbol, qty: seen.append(symbol) or object()  # noqa: E731
    Schwab._prepare_option_order_builder(broker, order, *([capture] * 8), OptionSymbol)
    return seen[0]


def test_schwab_order_builder_symbols():
    assert _schwab_order_symbol(_opt("SPX", WEEKLY)) == "SPXW  261113P07555000"
    assert _schwab_order_symbol(_opt("SPX", MONTHLY)) == "SPX   261120P07555000"
    assert _schwab_order_symbol(_opt("SPY", WEEKLY, strike=555)) == "SPY   261113P00555000"
    assert _schwab_order_symbol(_opt("AAPL", WEEKLY, right="call", strike=210)) == "AAPL  261113C00210000"


def test_interactive_brokers_legacy_contract_current_behavior():
    """Documents current behavior only: IB keeps symbol SPX and sets no trading class."""
    from lumibot.brokers.interactive_brokers import IBApp

    contract = IBApp.create_contract(IBApp.__new__(IBApp), _opt("SPX", WEEKLY))
    assert contract.symbol == "SPX"
    assert contract.lastTradeDateOrContractMonth == "20261113"
    assert getattr(contract, "tradingClass", "") == ""
