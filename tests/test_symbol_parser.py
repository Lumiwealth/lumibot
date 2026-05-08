import datetime as dt

from lumibot.tools.symbol_parser import parse_symbol


def test_parse_option_symbol_requires_full_match_and_valid_date():
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
