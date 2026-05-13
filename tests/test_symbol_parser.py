from datetime import date

from lumibot.tools.symbol_parser import parse_symbol


def test_parse_symbol_uses_2000_based_option_expiration_year():
    parsed = parse_symbol("AAPL990101C00100000")

    assert parsed["type"] == "option"
    assert parsed["expiration_date"] == date(2099, 1, 1)


def test_parse_symbol_invalid_option_expiration_returns_none_type():
    assert parse_symbol("AAPL990230C00100000") == {"type": None}
