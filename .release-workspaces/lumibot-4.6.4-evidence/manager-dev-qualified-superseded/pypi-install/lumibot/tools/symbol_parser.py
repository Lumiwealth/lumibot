import datetime as dt
import re

_OPTION_SYMBOL_PATTERN = re.compile(r"([A-Z]+)(\d{6})([CP])(\d+)")


def _parse_occ_expiration(expiration: str):
    year = 2000 + int(expiration[0:2])
    month = int(expiration[2:4])
    day = int(expiration[4:6])
    return dt.date(year, month, day)


def parse_symbol(symbol):
    """
    Parse the given symbol and determine if it's an option or a stock.
    For options, extract and return the stock symbol, expiration date (as a datetime.date object),
    type (call or put), and strike price.
    For stocks, simply return the stock symbol.
    TODO: Crypto and Forex support
    """
    if not isinstance(symbol, str):
        return {"type": None}

    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        return {"type": None}

    match = _OPTION_SYMBOL_PATTERN.fullmatch(normalized_symbol)
    if match:
        stock_symbol, expiration, option_type, strike_price = match.groups()
        try:
            expiration_date = _parse_occ_expiration(expiration)
        except ValueError:
            return {"type": None}
        option_type = "CALL" if option_type == "C" else "PUT"
        return {
            "type": "option",
            "stock_symbol": stock_symbol,
            "expiration_date": expiration_date,
            "option_type": option_type,
            # OCC option symbols encode strikes in thousandths; keep 3 decimals after scaling.
            "strike_price": round(float(strike_price) / 1000, 3),
        }
    return {"type": "stock", "stock_symbol": normalized_symbol}
