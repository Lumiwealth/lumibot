"""Shared index option root rule (2026-11-17): SPX weeklies are SPXW, monthlies stay SPX."""

from datetime import date

import pytest

from lumibot.entities import Asset
from lumibot.tools.index_option_roots import index_option_root, index_underlying_symbol, occ_option_symbol


@pytest.mark.parametrize(
    "symbol,expiration,root",
    [
        ("SPX", date(2026, 11, 13), "SPXW"),
        ("SPX", date(2026, 11, 20), "SPX"),
        ("SPX", date(2025, 4, 17), "SPX"),  # Good Friday 2025-04-18 is the third Friday
        ("SPX", date(2025, 4, 10), "SPXW"),
        ("NDX", date(2026, 11, 13), "NDXP"),
        ("NDX", date(2026, 11, 20), "NDX"),
        ("RUT", date(2026, 11, 13), "RUTW"),
        ("RUT", date(2026, 11, 20), "RUT"),
        ("VIX", date(2026, 11, 18), "VIX"),
        ("VIX", date(2026, 11, 20), "VIXW"),
        ("XSP", date(2026, 11, 13), "XSP"),
        ("SPXW", date(2026, 11, 20), "SPXW"),
        ("AAPL", date(2026, 11, 13), "AAPL"),
    ],
)
def test_index_option_root(symbol, expiration, root):
    assert index_option_root(symbol, expiration) == root


def test_weekly_root_maps_back_to_the_index():
    assert index_underlying_symbol("SPXW") == "SPX"
    assert index_underlying_symbol("NDXP") == "NDX"
    assert index_underlying_symbol("SPY") == "SPY"


def test_occ_option_symbol_applies_the_root():
    put = Asset("SPX", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 11, 13), strike=7555, right="PUT")
    assert occ_option_symbol(put) == "SPXW261113P07555000"


def test_alpaca_live_quote_symbol_uses_the_weekly_root():
    from lumibot.data_sources.alpaca_data import AlpacaData

    weekly = Asset("SPX", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 11, 13), strike=7555, right="put")
    monthly = Asset("SPX", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 11, 20), strike=7555, right="put")
    assert AlpacaData._latest_quote_option_symbol(weekly) == "SPXW261113P07555000"
    assert AlpacaData._latest_quote_option_symbol(monthly) == "SPX261120P07555000"
