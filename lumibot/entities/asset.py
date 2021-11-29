from collections import UserDict
from datetime import date
from typing import Optional

from pydantic import BaseModel, validator


class Asset(BaseModel, frozen=True, extra="forbid"):
    """
    This is a base class for Assets including stocks, futures, options
    and forex.

    Parameters
    ----------
    symbol : str
        Symbol of the stock or underlying in case of futures/options.
    asset_type : str
        Type of the asset. Asset types are only 'stock', 'option', 'future', 'forex'
        default : 'stock'
    expiration : datetime.date
        Option or futures expiration.
        The datetime.date will be converted to broker specific formats.
        IB Format: for options "YYYYMMDD", for futures "YYYYMM"
    strike : str
        Options strike as string.
    right : str
        'CALL' or 'PUT'
        default : ""
    multiplier : int
        Price multiplier.
        default : 1
    currency : str
        Base currency, (default=None)

    Attributes
    ----------
    symbol : string
        The symbol used to retrieve stock quotes if stock. The underlying
        symbol if option. For Forex: The base currency.
    asset_type (string, default: `stock`): One of the following:
        - 'stock'
        - 'option'
        - 'future'
        - 'forex'
    expiration : datetime.date
        Contract expiration dates for futures and options.
    strike : float
        Contract strike price.
    right : str
        Option call or put.
    multiplier : int
        Contract leverage over the underlying.
    currency : string
        Conversion currency.
    _asset_types : list of str
        Acceptable asset types.
    _right : list of str
        Acceptable values for right.

    Methods
    -------
    asset_type_must_be_one_of(@"asset_type")
        validates asset types.
    right_must_be_one_of(@"right")
        validates rights types.

    Example
    -------
    >>> # Create an Asset object for a stock.
    >>> asset = Asset(symbol="AAPL")

    >>> # Create an Asset object for a futures contract.
    >>> asset = Asset(symbol="ES", asset_type='future', expiration=datetime.date(2021, 12, 17))

    >>> # Create an Asset object for an options contract.
    >>> asset = Asset(
    >>>     symbol="AAPL",
    >>>     asset_type='option',
    >>>     expiration=datetime.date(2021, 11, 26),
    >>>     strike=155,
    >>>     right= 'CALL',
    >>>     multiplier=100,
    >>>     currency="USD"
    >>> )
    """
    symbol: str
    asset_type: str = "stock"
    expiration: Optional[date] = None
    strike: Optional[str] = ""
    right: Optional[str] = None
    multiplier: int = 1
    currency: Optional[str] = "USD"
    _asset_types: list = ["stock", "option", "future", "forex"]
    _right: list = ["CALL", "PUT"]

    def __repr__(self):
        if self.asset_type == "future":
            return f"{self.symbol}, {self.expiration}"
        elif self.asset_type == "option":
            return f"{self.symbol}, {self.expiration} {self.strike} {self.right}"
        else:
            return f"{self.symbol}"

    def __str__(self):
        if self.asset_type == "future":
            return f"{self.symbol}, {self.expiration}"
        elif self.asset_type == "option":
            return f"{self.symbol}, {self.expiration} {self.strike} {self.right}"
        else:
            return f"{self.symbol}"

    @validator("asset_type")
    def asset_type_must_be_one_of(cls, v):
        if v not in cls._asset_types:
            raise ValueError(
                f"`asset_type` must be one of {', '.join(cls._asset_types)}"
            )
        return v

    @validator("right")
    def right_must_be_one_of(cls, v):
        if v is None:
            return

        v = v.upper()
        if v not in cls._right:
            raise ValueError(
                f"`right` is {v} must be one of {', '.join(cls._right)}, upper case."
            )
        return v


class AssetsMapping(UserDict):
    def __init__(self, mapping):
        UserDict.__init__(self, mapping)
        symbols_mapping = {k.symbol: v for k, v in mapping.items()}
        self._symbols_mapping = symbols_mapping

    def __missing__(self, key):
        if isinstance(key, str):
            if key in self._symbols_mapping:
                return self._symbols_mapping[key]
        raise KeyError(key)

    def __contains__(self, key):
        if isinstance(key, str):
            return key in self._symbols_mapping
        return key in self.data

    def __setitem__(self, key, value):
        if isinstance(key, str):
            self.data[Asset(symbol=key)] = value
        else:
            self.data[key] = value
