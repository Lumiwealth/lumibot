import datetime as dt

from lumibot.tools.symbol_parser import parse_symbol


def test_parse_option_symbol_requires_full_match_and_valid_date():
    """Verify OCC option parsing only accepts full matches with valid expiration dates."""
    parsed = parse_symbol("AAPL250621C00100000")

    assert parsed["type"] == "option"
    assert parsed["stock_symbol"] == "AAPL"
    assert parsed["expiration_date"] == dt.date(2025, 6, 21)
    assert parsed["option_type"] == "CALL"
    assert parsed["strike_price"] == 100.0

    assert parse_symbol("AAPL250621C00100000XYZ") == {
        "type": "stock",
        "stock_symbol": "AAPL250621C00100000XYZ",
    }
    assert parse_symbol("AAPL991332C00100000") == {"type": None}


def test_parse_symbol_normalizes_stock_and_option_input():
    """Verify stock and option inputs are stripped, uppercased, and rejected when empty."""
    parsed = parse_symbol("  aapl250621c00100000  ")

    assert parsed["type"] == "option"
    assert parsed["stock_symbol"] == "AAPL"
    assert parsed["expiration_date"] == dt.date(2025, 6, 21)
    assert parsed["option_type"] == "CALL"
    assert parsed["strike_price"] == 100.0

    assert parse_symbol(" spy ") == {"type": "stock", "stock_symbol": "SPY"}
    assert parse_symbol("") == {"type": None}
    assert parse_symbol("   ") == {"type": None}
    assert parse_symbol(None) == {"type": None}


def test_parse_option_symbol_uses_2000_based_occ_years():
    """Verify OCC YY expirations are interpreted as years in the 2000s."""
    parsed = parse_symbol("AAPL990101P00100000")

    assert parsed["type"] == "option"
    assert parsed["expiration_date"] == dt.date(2099, 1, 1)
    assert parsed["option_type"] == "PUT"
