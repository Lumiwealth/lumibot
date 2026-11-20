"""OCC option symbols keep the exact strike for the Tradier and Schwab builders.

A strike such as 16.33 is 16329.999... once multiplied by 1000 in binary floating
point, so truncating it encodes a different (non-existent) contract.
"""

from datetime import date

import pytest

from lumibot.entities import Asset

EXPIRATION = date(2026, 12, 18)

# Strikes that are not whole or half dollars mostly come from adjusted contracts
# (splits, special dividends), e.g. 100 / 3 = 33.33 or 20 / 3 = 6.67.
STRIKES = [
    (2.3, "00002300"),
    (6.67, "00006670"),
    (8.03, "00008030"),
    (16.33, "00016330"),
    (33.33, "00033330"),
    (148.6, "00148600"),
    (210.5, "00210500"),
    (7555, "07555000"),
]


@pytest.mark.parametrize("strike,encoded", STRIKES)
def test_create_options_symbol_keeps_exact_strike(strike, encoded):
    from lumibot.tools.helpers import create_options_symbol

    assert create_options_symbol("XYZ", EXPIRATION, "call", strike) == f"XYZ261218C{encoded}"


@pytest.mark.parametrize("strike,encoded", STRIKES)
def test_tradier_option_symbols_keep_exact_strike(strike, encoded):
    from lumibot.brokers.tradier import _create_options_symbol as broker_symbol
    from lumibot.data_sources.tradier_data import _create_options_symbol as data_symbol

    assert broker_symbol("XYZ", EXPIRATION, "put", strike) == f"XYZ261218P{encoded}"
    assert data_symbol("XYZ", EXPIRATION, "put", strike) == f"XYZ261218P{encoded}"


@pytest.mark.parametrize("strike,encoded", STRIKES)
def test_schwab_option_symbol_keeps_exact_strike(strike, encoded):
    from lumibot.data_sources.schwab_data import _schwab_option_symbol

    option = Asset("XYZ", asset_type="option", expiration=EXPIRATION, strike=strike, right="CALL")
    assert _schwab_option_symbol(option) == f"XYZ   261218C{encoded}"
