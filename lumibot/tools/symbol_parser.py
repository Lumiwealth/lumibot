import datetime as dt
import re
from typing import Any

_OPTION_SYMBOL_PATTERN = re.compile(r"([A-Z]+)(\d{6})([CP])(\d+)")


def parse_symbol(symbol: object) -> dict[str, Any]:
    """
    Parse the given symbol and determine if it's an option or a stock.
    For options, extract and return the stock symbol, expiration date (as a datetime.date object),
    type (call or put), and strike price.
    For stocks, simply return the stock symbol.
    TODO: Crypto and Forex support
    """
    if not isinstance(symbol, str):
        return {"type": None}

    match = _OPTION_SYMBOL_PATTERN.match(symbol)
    if match:
        stock_symbol, expiration, option_type, strike_price = match.groups()
        expiration_date = dt.datetime.strptime(expiration, "%y%m%d").date()
        option_type = "CALL" if option_type == "C" else "PUT"
        return {
            "type": "option",
            "stock_symbol": stock_symbol,
            "expiration_date": expiration_date,
            "option_type": option_type,
            "strike_price": round(float(strike_price) / 1000, 3),
        }
    return {"type": "stock", "stock_symbol": symbol}
