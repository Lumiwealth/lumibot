import datetime as dt
import re

_OPTION_SYMBOL_PATTERN = re.compile(r"([A-Z]+)(\d{6})([CP])(\d+)")


def parse_symbol(symbol):
    """
    Parse the given symbol and determine if it's an option or a stock.
    For options, extract and return the stock symbol, expiration date, type, and strike price.
    For stocks, simply return the stock symbol.
    """
    if not isinstance(symbol, str):
        return {"type": None}

    match = _OPTION_SYMBOL_PATTERN.fullmatch(symbol)
    if match:
        stock_symbol, expiration, option_type, strike_price = match.groups()
        try:
            yy = int(expiration[0:2])
            mm = int(expiration[2:4])
            dd = int(expiration[4:6])
            expiration_date = dt.date(2000 + yy, mm, dd)
        except ValueError:
            return {"type": None}
        option_type = "CALL" if option_type == "C" else "PUT"
        return {
            "type": "option",
            "stock_symbol": stock_symbol,
            "expiration_date": expiration_date,
            "option_type": option_type,
            "strike_price": round(float(strike_price) / 1000, 3),
        }
    return {"type": "stock", "stock_symbol": symbol}
